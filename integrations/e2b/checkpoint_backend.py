# SPDX-License-Identifier: Apache-2.0
"""Persistent same-identity pause/resume; independent of pool refill policy."""
import time
import json
import os
import subprocess
from pathlib import Path
from server import APIError

class CheckpointBackend:
    def recover_checkpoint(self, sid):
        """Recreate only owned runtime resources; immutable checkpoint is authority."""
        with self.lock:
            r = dict(self.get(sid))
        manifest = json.loads((Path(self.args.sandbox_state)/sid/'durable-checkpoint/checkpoint.json').read_text())
        if manifest.get('sandbox_id') != sid or manifest.get('version') != 2:
            raise RuntimeError('Cold recovery requires a version 2 identity checkpoint')
        mac = manifest['network']['intfs'][0]['hwAddr']
        ip = r.get('ip')
        if not ip:
            raise RuntimeError('Checkpoint is missing its assigned IP')
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        reuse = False
        if r.get('recovery_boot_id') == boot:
            try:
                reuse = (self.process_start(r['holder_pid']) == r['holder_start'] and
                         os.stat(r['netns']).st_ino == r['holder_inode'])
            except (OSError, KeyError): pass
        if not reuse:
            with self.lock:
                current = self.get(sid)
                current.update(netns='', recovery_boot_id=boot, checkpoint_ip=ip)
                for key in ('holder_pid', 'holder_start', 'holder_inode'): current.pop(key, None)
                self.save_record(sid)
            # Only this sandbox's old lease is released. No stale /proc PID is entered.
            self.cni_call(sid, 'DEL')
            name = self.get(sid)['namespace']
            if name != 'kua-'+sid:
                raise RuntimeError('Checkpoint namespace ownership is invalid')
            if Path('/var/run/netns', name).exists():
                subprocess.run(['ip','netns','delete',name],check=True,capture_output=True,timeout=10)
            self.namespace(sid)
        if not self.address(self.get(sid)):
            self.cni_call(sid, 'ADD')
        if self.address(self.get(sid)) != ip:
            raise RuntimeError('CNI did not restore the checkpoint IP; recovery remains blocked')
        ns = self.get(sid)['netns']
        subprocess.run(['nsenter','--net='+ns,'ip','link','set','dev','eth0','address',mac],check=True,capture_output=True,timeout=10)
        result = self.admin('sandbox-checkpoint-recover', sandbox_id=sid, netns=ns)
        with self.lock:
            self.get(sid).update(pid=result['live_fork']['pid'],network_state='ready',network_error='',status='paused',checkpoint_recovery_pending=False)
            self.save_record(sid)

    def pause(self, sid):
        self.get_public(sid)
        with self.operation(sid):
            with self.lock:
                r = self.get(sid)
                if r['status'] == 'paused':
                    return
                if r['status'] not in ('running', 'pausing'):
                    raise APIError(423, 'Sandbox cannot pause in its current state')
                job = self.jobs.get(sid)
            # CNI owns the same network being checkpointed; complete it before
            # closing the guest barrier. Never hold the records lock while waiting.
            if job:
                job.result(timeout=60)
            with self.lock:
                self.get(sid)['status'] = 'pausing'
                self.save_record(sid)
            result = self.admin('sandbox-checkpoint-pause', sandbox_id=sid)
            if result.get('status') != 'paused' or not result.get('guest_ram_released'):
                raise APIError(502, 'Checkpoint did not release guest RAM')
            with self.lock:
                self.get(sid).update(status='paused', checkpoint=result.get('checkpoint'))
                self.save_record(sid)

    def connect(self, sid, body):
        try:
            timeout = int(body.get('timeout') or 300)
        except (ValueError, TypeError):
            raise APIError(400, 'timeout must be 1..86400 seconds')
        if not 1 <= timeout <= 86400:
            raise APIError(400, 'timeout must be 1..86400 seconds')
        self.get_public(sid)
        with self.operation(sid):
            if self.get(sid).get('checkpoint_recovery_pending'):
                self.recover_checkpoint(sid)
            with self.lock:
                status = self.get(sid)['status']
                if status not in ('running', 'paused', 'resuming'):
                    raise APIError(409, 'Sandbox cannot resume in its current state')
                if status != 'running':
                    self.get(sid)['status'] = 'resuming'
                    self.save_record(sid)
            if status != 'running':
                result = self.admin('sandbox-checkpoint-resume', sandbox_id=sid)
                if result.get('status') != 'running':
                    raise APIError(502, 'Checkpoint did not resume')
            with self.lock:
                r = self.get(sid)
                r.update(status='running', expiry=max(r['expiry'], time.time()+timeout))
                r.pop('checkpoint', None)
                self.save_record(sid)
            return self.response(sid)
