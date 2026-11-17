# SPDX-License-Identifier: Apache-2.0
"""Default E2B backend: live Kuasar VMs with independently progressing CNI."""
import base64
import concurrent.futures
import datetime
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import threading
import time
from server import APIError, Backend, trace_timing
import vm_rpc


class VMBackend(Backend):
    def __init__(self, args):
        super().__init__(args)
        self.cni = json.loads(Path(args.cni_config).read_text())
        self.plugin = Path(args.cni_bin) / self.cni['type']
        self.jobs = {}
        self.operations = {sid: threading.RLock() for sid in self.records}
        self.holders = {}
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix='e2b-cni')
        self.closed = threading.Event()
        self.template_pool = None
        for sid, record in list(self.records.items()):
            if record.get('backend') != 'vm':
                raise ValueError('state file belongs to a different backend')
            if record.get('pool_state') in ('preparing', 'ready', 'binding', 'quarantined'):
                record['status'] = 'deleting'
            if record.get('status') == 'deleting':
                continue  # expiry/reconciliation below retries owned cleanup
            try:
                info = self.admin('sandbox-get', sandbox_id=sid)
                record['status'] = 'running'
                if info.get('network_ready'):
                    record['network_state'] = 'ready'
                    record['ip'] = self.address(record)
                else:
                    self.jobs[sid] = self.pool.submit(self.provision, sid)
            except Exception as error:
                record.update(status='failed', network_state='failed', network_error=str(error))
        with self.lock:
            self.save()
        if os.environ.get('E2B_PREPARE_EXEC_CACHE') == '1':
            from exec_cache import prepare
            source = self.admin('sandbox-get', sandbox_id=self.args.source)
            prepare(source['live_fork']['vsock_socket'],
                    str(self.state)+'.exec-preparation.json')

        # Never reassign an unclaimed slot across adapter epochs. Reconcile its
        # durable ownership first; leased user sandboxes keep normal recovery.
        for sid, record in list(self.records.items()):
            if record.get('pool_state') in ('preparing', 'ready', 'binding', 'quarantined'):
                self.destroy(sid)
        capacity = int(os.environ.get('E2B_TEMPLATE_POOL_SIZE', '0'))
        if capacity:
            from template_pool import TemplatePool
            self.template_pool = TemplatePool(self, capacity,
                float(os.environ.get('E2B_TEMPLATE_POOL_REFILL_DELAY_MS', '1000'))/1000)

    def get_public(self, sid):
        with self.lock:
            record = self.get(sid)
            if record.get('pool_state') in ('preparing', 'ready', 'binding', 'quarantined'):
                raise APIError(404, 'Sandbox not found')
            return record

    def admin(self, action, **kwargs):
        started_ns = time.perf_counter_ns()
        try:
            return vm_rpc.admin(self.args.admin_socket, action, **kwargs)
        finally:
            trace_timing('admin', started_ns, action=action, sid=kwargs.get('sandbox_id'))

    def operation(self, sid):
        with self.lock:
            self.get(sid)
            return self.operations.setdefault(sid, threading.RLock())

    def response(self, sid):
        with self.lock:
            self.get_public(sid)
            response = super().response(sid)
            r = self.get(sid)
            response['metadata'] = dict(response['metadata'])
            response['metadata'].update({'kuasar.network.status': r.get('network_state', 'pending'),
                                         'kuasar.network.ip': r.get('ip', '')})
            if r.get('network_error'):
                response['metadata']['kuasar.network.error'] = r['network_error']
            return response

    @staticmethod
    def process_start(pid):
        return Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19]

    def namespace(self, sid):
        with self.lock:
            r = self.get(sid)
            name = r['namespace']
        subprocess.run(['ip', 'netns', 'add', name], check=True, capture_output=True, timeout=30)
        holder = subprocess.Popen(['ip', 'netns', 'exec', name, 'sleep', 'infinity'],
                                  stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.holders[sid] = holder
        expected = os.stat('/var/run/netns/'+name).st_ino
        deadline = time.monotonic()+5
        path = f'/proc/{holder.pid}/ns/net'
        while os.stat(path).st_ino != expected:
            if holder.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError('namespace holder failed')
            time.sleep(.0001)
        with self.lock:
            r.update(netns=path, holder_pid=holder.pid, holder_start=self.process_start(holder.pid), holder_inode=expected)
            self.save()
        return path

    def cni_call(self, sid, action):
        with self.lock:
            r = dict(self.get(sid))
        ns = r.get('netns', '')
        try:
            if os.stat(ns).st_ino != r.get('holder_inode'):
                ns = ''
        except OSError:
            ns = ''
        if action == 'ADD' and not ns:
            raise RuntimeError('owned network namespace is missing')
        env = {**os.environ, 'CNI_COMMAND': action, 'CNI_CONTAINERID': sid,
               'CNI_NETNS': ns, 'CNI_IFNAME': 'eth0', 'CNI_PATH': self.args.cni_bin}
        p = subprocess.run([str(self.plugin)], input=json.dumps(self.cni), text=True,
                           capture_output=True, env=env, timeout=30)
        if p.returncode:
            raise RuntimeError('CNI '+action+': '+p.stdout+p.stderr)
        return json.loads(p.stdout) if p.stdout else {}

    def address(self, record):
        p = subprocess.run(['nsenter', '--net='+record['netns'], 'ip', '-j', '-4', 'addr', 'show', 'dev', 'eth0'],
                           capture_output=True, text=True, timeout=5)
        if p.returncode:
            return ''
        for interface in json.loads(p.stdout):
            for address in interface.get('addr_info', []):
                if address.get('scope') == 'global':
                    return address['local']
        return ''

    def provision(self, sid):
        try:
            with self.lock:
                r = self.get(sid)
                if r.get('status') == 'deleting':
                    return
                record = dict(r)
            # Reconcile an ADD whose result may have been lost on adapter restart.
            ip = self.address(record)
            if not ip:
                self.cni_call(sid, 'ADD')
                ip = self.address(record)
            with self.lock:
                r = self.get(sid)
                if r.get('status') == 'deleting':
                    return
                r.update(network_state='configuring', ip=ip)
                self.save()
            self.admin('sandbox-network-complete', sandbox_id=sid)
            with self.lock:
                r = self.get(sid)
                if r.get('status') != 'deleting':
                    r.update(network_state='ready', network_error='')
                    self.save()
        except Exception as error:
            with self.lock:
                if sid in self.records and self.records[sid].get('status') != 'deleting':
                    self.records[sid].update(network_state='failed', network_error=str(error))
                    self.save()
            # Failure belongs to network readiness; the already restored guest
            # remains executable. Do not turn it into a successful Ready response.

    def validate_create(self, body):
        if body.get('templateID', self.args.template) not in getattr(self, 'image_catalog', {self.args.template: None}):
            raise APIError(404, 'Unknown preloaded template')
        if 'allowInternetAccess' in body or any(body.get(k) for k in ('envVars', 'volumeMounts', 'network', 'iam')):
            raise APIError(400, 'Changing inherited application/network policy is unsupported')
        timeout = int(body.get('timeout', 300))
        if not 1 <= timeout <= 86400:
            raise APIError(400, 'timeout must be 1..86400 seconds')
        return timeout

    def create(self, body, parent=None):
        self.validate_create(body)
        if parent is not None:
            self.get_public(parent)
            return self._create_live(body, parent)
        if self.template_pool is not None and self.template_pool.enabled:
            return self.template_pool.lease(body)
        return self._create_live(body)

    def _create_live(self, body, parent=None, _pool_owned=False, _private=False):
        timeout = self.validate_create(body)
        # Serialize forks/deletion only for their source, never all sandbox RPCs.
        started_ns = time.perf_counter_ns()
        source_lock = self.operation(parent) if parent else threading.RLock()
        with source_lock:
            if parent:
                with self.lock:
                    r = self.get(parent)
                    if r.get('status') != 'running':
                        raise APIError(409, 'Source is not running')
                    job = self.jobs.get(parent)
                if job:
                    job.result(timeout=45)
                with self.lock:
                    if self.get(parent).get('network_state') != 'ready':
                        raise APIError(409, 'Source network is not ready for another fork')
            source = parent or self.args.source
            sid = 'e2b-'+secrets.token_hex(12)
            with self.lock:
                self.operations[sid] = threading.RLock()
                self.records[sid] = {'backend': 'vm', 'status': 'creating', 'network_state': 'pending',
                    'namespace': 'kua-'+sid, 'uid': sid, 'token': secrets.token_urlsafe(32),
                    'startedAt': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    'expiry': time.time()+timeout, 'metadata': body.get('metadata', {})}
                self.records[sid].update(getattr(self, 'fixed_image_fields', {}))
                if parent:
                    for field in ('template_id', 'exec_root', 'image_manifest', 'image_sha256', 'image_disk', 'oci_config', 'entrypoint_pid', 'slot_disk'):
                        if field in self.records[parent]:
                            self.records[sid][field] = self.records[parent][field]
                if _pool_owned:
                    self.records[sid].update(pool_origin=True, pool_state='preparing')
                elif _private:
                    self.records[sid]['pool_state'] = 'preparing'
                try:
                    self.save()
                except Exception:
                    self.records[sid]['status'] = 'deleting'
                    try:
                        self.save()
                    except Exception:
                        pass
                    raise
            # Expiry/delete cannot race namespace creation or an in-flight fork.
            with self.operation(sid):
                try:
                    trace_timing('create_record', started_ns, sid=sid)
                    ns_start = time.perf_counter_ns()
                    ns = self.namespace(sid)
                    trace_timing('namespace', ns_start, sid=sid)
                    info = self.admin('sandbox-fork', source_id=source, sandbox_id=sid, netns=ns, defer_network=True)
                    completion_ns = time.perf_counter_ns()
                    with self.lock:
                        trace_timing('completion_lock_wait', completion_ns, sid=sid)
                        save_ns = time.perf_counter_ns()
                        self.records[sid].update(status='running', pid=info['live_fork']['pid'],
                            vsock_socket=info['live_fork'].get('vsock_socket', str(Path(self.args.sandbox_state)/sid/'task.vsock')))
                        self.save()
                        trace_timing('running_record', save_ns, sid=sid)
                        self.jobs[sid] = self.pool.submit(self.provision, sid)
                    response = sid if (_pool_owned or _private) else self.response(sid)
                    trace_timing('create_total', started_ns, sid=sid)
                    return response
                except Exception:
                    self.destroy(sid)
                    raise

    def destroy(self, sid):
        with self.operation(sid):
            with self.lock:
                r = self.get(sid)
                was_pooled = bool(r.get('pool_origin'))
                was_assigned = r.get('pool_state') == 'leased'
                r['status'] = 'deleting'
                self.save()
                job = self.jobs.get(sid)
            # A running ADD must finish before DEL. No records lock is held while
            # waiting; the worker can observe deletion and skip guest activation.
            if job:
                job.cancel()
                if not job.cancelled():
                    job.result(timeout=50)
            try:
                self.admin('sandbox-get', sandbox_id=sid)
            except RuntimeError as error:
                if 'not found' not in str(error):
                    raise
            else:
                self.admin('sandbox-destroy', sandbox_id=sid)
            self.cni_call(sid, 'DEL')
            with self.lock:
                r = dict(self.get(sid))
            holder = self.holders.pop(sid, None)
            if holder is not None:
                holder.terminate()
                holder.wait(timeout=5)
            elif r.get('holder_pid'):
                try:
                    if self.process_start(r['holder_pid']) == r['holder_start']:
                        os.kill(r['holder_pid'], signal.SIGTERM)
                except FileNotFoundError:
                    pass
            path = Path('/var/run/netns')/r['namespace']
            if path.exists():
                subprocess.run(['ip', 'netns', 'delete', r['namespace']], check=True, capture_output=True, timeout=10)
            with self.lock:
                del self.records[sid]
                self.jobs.pop(sid, None)
                self.operations.pop(sid, None)
                self.save()
                if self.template_pool is not None and was_pooled:
                    self.template_pool.capacity_changed(sid, assigned=was_assigned)

    def execute(self, sid, argv, stdin=b''):
        # Serialize active command IO against fork/destroy of this VM. Network
        # provisioning never holds this lock, and execution never joins its job.
        started_ns = time.perf_counter_ns()
        self.get_public(sid)
        with self.operation(sid):
            with self.lock:
                if self.get(sid).get('status') != 'running':
                    raise APIError(409, 'Sandbox is not running')
            rpc_start = time.perf_counter_ns()
            with self.lock:
                record = self.get(sid)
                path = record.get('vsock_socket', str(Path(self.args.sandbox_state)/sid/'task.vsock'))
                if record.get('exec_root'):
                    argv = ['/bin/busybox', 'chroot', record['exec_root'], *argv]
            result = vm_rpc.execute(Path(path), argv, stdin)
            trace_timing('execute_rpc', rpc_start, sid=sid, guest=result.get('timing'))
            trace_timing('execute_total', started_ns, sid=sid)
            return result

    def file_exec(self, sid, args, data=None):
        if args[:2] == ['/bin/busybox', 'cat']:
            result = self.execute(sid, ['/usr/bin/base64', args[2]])
            if not result['exit_code']:
                return base64.b64decode(result['stdout'])
        else:
            result = self.execute(sid, args, data or b'')
            if not result['exit_code']:
                return result['stdout']
        raise APIError(404 if b'No such file' in result['stderr'] else 500, result['stderr'].decode(errors='replace'))

    def reap(self):
        while not self.closed.wait(1):
            with self.lock:
                expired = [sid for sid, r in self.records.items()
                           if (r.get('pool_state') not in ('preparing', 'ready')
                               and r['expiry'] < time.time()) or r.get('status') == 'deleting']
            for sid in expired:
                try:
                    self.destroy(sid)
                except Exception as error:
                    print('expiry cleanup:', sid, str(error), flush=True)

    def close(self):
        self.closed.set()
        if self.template_pool is not None:
            self.template_pool.close()
        self.pool.shutdown(wait=True, cancel_futures=False)
