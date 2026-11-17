# SPDX-License-Identifier: Apache-2.0
"""Persistent same-identity pause/resume; independent of pool refill policy."""
import time
from server import APIError

class CheckpointBackend:
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
