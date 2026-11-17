#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Restricted E2B SDK API backed by live Kuasar VM forks by default.

CNI readiness progresses independently of vsock execution. An explicit Kubernetes
backend preserves Pod/CRI ownership and kubelet exec. Neither creates RAM images.
"""
import argparse
import base64
import datetime
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import selectors
import socket
import struct
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, parse_qs


def trace_timing(stage, started_ns, **details):
    if os.environ.get('E2B_TRACE_TIMING'):
        ended_ns = time.perf_counter_ns()
        print('E2B_TIMING '+json.dumps(dict(stage=stage, started_ns=started_ns,
              ended_ns=ended_ns, elapsed_ms=(ended_ns-started_ns)/1e6, **details)), flush=True)


class APIError(Exception):
    def __init__(self, code, message):
        self.code, self.message = code, message


def read_process_start(stderr):
    """kubectl diagnostics can precede the guest wrapper's PID marker."""
    diagnostics = bytearray()
    for line in iter(stderr.readline, b''):
        marker = re.fullmatch(rb'\x1ePID:([0-9]+)\n', line)
        if marker:
            return int(marker[1]), bytes(diagnostics)
        diagnostics.extend(line)
    return None, bytes(diagnostics)


class Backend:
    def __init__(self, args):
        self.args = args
        self.key = Path(args.api_key_file).read_text().strip()
        if not self.key:
            raise ValueError('empty API key')
        self.lock = threading.RLock()
        self.state = Path(args.state)
        self.durable_state = None
        if os.environ.get('E2B_DURABLE_STORE') == 'sqlite':
            from durable_state import DurableState
            self.durable_state = DurableState(self.state)
        self.records = (self.durable_state.load() if self.durable_state else
                        json.loads(self.state.read_text()) if self.state.exists() else {})

    def save(self):
        if self.durable_state:
            self.durable_state.save(self.records)
            return
        self.state.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state.with_suffix('.tmp')
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as f:
            json.dump(self.records, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.state)
        directory = os.open(self.state.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def save_record(self, sid):
        """Persist one explicitly mutated record while holding self.lock.

        Full save remains the fallback for legacy JSON stores and test backends.
        This never removes or changes another sandbox's durable record.
        """
        if getattr(self, 'durable_state', None):
            self.durable_state.save(self.records, only=(sid,))
        else:
            self.save()

    def argv(self, *args):
        return [self.args.k3s, 'kubectl', '--kubeconfig', self.args.kubeconfig,
                '-n', self.args.namespace, *map(str, args)]

    def kubectl(self, *args, data=None, timeout=150):
        p = subprocess.run(self.argv(*args), input=data, capture_output=True, timeout=timeout)
        if p.returncode:
            raise APIError(502, p.stderr.decode(errors='replace'))
        return p.stdout

    def get(self, sid):
        if sid not in self.records:
            raise APIError(404, 'Sandbox not found')
        return self.records[sid]

    def response(self, sid):
        r = self.get(sid)
        return {'sandboxID': sid, 'templateID': r.get('template_id', self.args.template), 'clientID': 'kuasar',
                'envdVersion': '0.5.7', 'envdAccessToken': r['token'], 'domain': 'localhost',
                'startedAt': r['startedAt'], 'endAt': datetime.datetime.fromtimestamp(r['expiry'], datetime.timezone.utc).isoformat(),
                'state': 'running', 'cpuCount': getattr(self.args, 'cpu_count', 2), 'memoryMB': getattr(self.args, 'memory_mb', 512), 'diskSizeMB': 0,
                'metadata': r.get('metadata', {})}

    def create(self, body, parent=None):
        with self.lock:
            if body.get('templateID', self.args.template) != self.args.template:
                raise APIError(404, 'Unknown preloaded template')
            if 'allowInternetAccess' in body or any(body.get(k) for k in ('envVars', 'volumeMounts', 'network', 'iam')):
                raise APIError(400, 'Changing inherited application/network policy is unsupported')
            timeout = int(body.get('timeout', 300))
            if not 1 <= timeout <= 86400:
                raise APIError(400, 'timeout must be 1..86400 seconds')
            source = self.get(parent)['uid'] if parent else self.args.source
            sid = 'e2b-' + secrets.token_hex(12)
            pod = {'apiVersion': 'v1', 'kind': 'Pod',
                   'metadata': {'name': sid, 'labels': {'kuasar.io/e2b-sandbox': sid},
                                'annotations': {'io.kuasar.fork.source': source, 'io.kuasar.fork.allow': 'true'}},
                   'spec': {'nodeName': self.args.node, 'runtimeClassName': self.args.runtime_class,
                            'serviceAccountName': self.args.service_account,
                            'automountServiceAccountToken': False, 'enableServiceLinks': False,
                            'restartPolicy': 'Never', 'terminationGracePeriodSeconds': 2,
                            'tolerations': [{'operator': 'Exists'}],
                            'containers': [{'name': self.args.container, 'image': self.args.image,
                                            'imagePullPolicy': 'Never', 'command': json.loads(self.args.command_json)}]}}
            # Persist intent before creation so a restart retains cleanup ownership.
            self.records[sid] = {'token': secrets.token_urlsafe(32), 'uid': '',
                                 'startedAt': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                 'expiry': time.time() + timeout, 'metadata': body.get('metadata', {})}
            self.save()
            try:
                self.kubectl('create', '-f', '-', data=json.dumps(pod).encode())
                self.kubectl('wait', '--for=condition=Ready', 'pod/' + sid, '--timeout=120s')
                observed = json.loads(self.kubectl('get', 'pod', sid, '-o', 'json'))
                self.records[sid].update(uid=observed['metadata']['uid'], ip=observed['status']['podIP'])
                self.save()
                return self.response(sid)
            except Exception:
                self.destroy(sid)
                raise

    def destroy(self, sid):
        with self.lock:
            self.get(sid)
            self.kubectl('delete', 'pod', sid, '--ignore-not-found', '--wait=true', '--timeout=90s')
            del self.records[sid]
            self.save()

    def exec_args(self, sid, args, stdin=False):
        self.get(sid)
        return self.argv('exec', *(['-i'] if stdin else []), sid, '-c', self.args.container, '--', *args)

    def file_exec(self, sid, args, data=None):
        p = subprocess.run(self.exec_args(sid, args, data is not None), input=data, capture_output=True, timeout=60)
        if p.returncode:
            raise APIError(404 if b'No such file' in p.stderr else 500, p.stderr.decode(errors='replace'))
        return p.stdout

    def reap(self):
        while True:
            time.sleep(1)
            with self.lock:
                for sid, r in list(self.records.items()):
                    if r['expiry'] < time.time():
                        try:
                            self.destroy(sid)
                        except Exception as e:
                            print('expiry cleanup:', sid, str(e), flush=True)


def vm_process_argv(process):
    """Preserve cwd/login-shell semantics without an empty env trampoline."""
    envs = process.get('envs', {})
    command = [process['cmd'], *process.get('args', [])]
    if envs:
        command = ['/bin/busybox', 'env', *[k+'='+v for k, v in envs.items()], *command]
    return ['/bin/sh', '-c', 'cd "$1" || exit; shift; exec "$@"',
            'exec', process.get('cwd') or '/', *command]


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    # Headers and small JSON/Connect frames are separate writes. Avoid Nagle
    # waiting for a delayed ACK before delivering the response body.
    disable_nagle_algorithm = True

    def respond(self, code, body=None, content_type='application/json'):
        data = body if isinstance(body, bytes) else json.dumps(body).encode() if body is not None else b''
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def body(self):
        limit = 64 * 1024 * 1024
        if self.headers.get('Transfer-Encoding', '').lower() == 'chunked':
            data = bytearray()
            while True:
                line = self.rfile.readline(128)
                if not line.endswith(b'\r\n'):
                    raise APIError(400, 'Invalid chunk header')
                size = int(line.split(b';', 1)[0].strip(), 16)
                if not size:
                    while True:
                        trailer = self.rfile.readline(8192)
                        if trailer in (b'\r\n', b''):
                            break
                    break
                if len(data) + size > limit:
                    raise APIError(413, 'Request exceeds 64 MiB')
                chunk = self.rfile.read(size)
                if len(chunk) != size or self.rfile.read(2) != b'\r\n':
                    raise APIError(400, 'Truncated chunk')
                data.extend(chunk)
            data = bytes(data)
        else:
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 <= size <= limit:
                raise APIError(413, 'Request exceeds 64 MiB')
            data = self.rfile.read(size)
        if self.headers.get('Content-Encoding') == 'gzip':
            import zlib
            data = zlib.decompressobj(16 + zlib.MAX_WBITS).decompress(data, limit + 1)
            if len(data) > limit:
                raise APIError(413, 'Expanded request exceeds 64 MiB')
        return data

    def handle_request(self):
        backend = self.server.backend
        url = urlsplit(self.path)
        path = url.path
        if path == '/template-pool':
            if not hmac.compare_digest(self.headers.get('X-API-Key', ''), backend.key):
                raise APIError(401, 'Invalid API key')
            pool = getattr(backend, 'template_pool', None)
            if pool is None:
                return self.respond(200, {'enabled': False, 'capacity': 0, 'ready': 0})
            if self.command == 'GET':
                return self.respond(200, pool.status())
            if self.command == 'POST' and json.loads(self.body() or b'{}') == {'drain': True}:
                return self.respond(200, pool.drain())
            raise APIError(400, 'Expected GET or POST with drain=true')
        if path.startswith('/sandboxes') or path.startswith('/v2/sandboxes'):
            if not hmac.compare_digest(self.headers.get('X-API-Key', ''), backend.key):
                raise APIError(401, 'Invalid API key')
            data = json.loads(self.body() or b'{}') if self.command == 'POST' else {}
            parts = path.strip('/').split('/')
            if parts[0] == 'v2':
                parts = parts[1:]
            if len(parts) == 1 and self.command == 'POST':
                return self.respond(201, backend.create(data))
            sid = parts[1] if len(parts) > 1 else ''
            if sid and hasattr(backend, 'get_public'):
                backend.get_public(sid)
            if len(parts) == 3 and parts[2] == 'fork' and self.command == 'POST':
                count = data.get('count', 1)
                if count != 1:
                    raise APIError(400, 'This implementation supports count=1 per live fork')
                return self.respond(201, [{'sandbox': backend.create(data, sid)}])
            if len(parts) == 3 and parts[2] == 'entrypoint' and self.command == 'GET' and hasattr(backend, 'entrypoint'):
                return self.respond(200, backend.entrypoint(sid))
            if len(parts) == 2 and self.command == 'DELETE':
                backend.destroy(sid)
                return self.respond(204)
            if len(parts) == 2 and self.command == 'GET':
                return self.respond(200, backend.response(sid))
            if len(parts) == 3 and parts[2] == 'connect' and self.command == 'POST':
                return self.respond(201, backend.response(sid))
            raise APIError(501, 'Unsupported control operation')
        if path == '/images/reload' and self.command == 'POST' and hasattr(backend, 'reload_images'):
            if not hmac.compare_digest(self.headers.get('X-API-Key', ''), backend.key):
                raise APIError(401, 'Invalid API key')
            if json.loads(self.body() or b'{}') != {}:
                raise APIError(400, 'Expected an empty reload request')
            return self.respond(200, backend.reload_images())
        sid = self.headers.get('E2b-Sandbox-Id', '')
        record = (backend.get_public(sid) if hasattr(backend, 'get_public') else backend.get(sid))
        if not hmac.compare_digest(self.headers.get('X-Access-Token', ''), record['token']):
            raise APIError(401, 'Invalid sandbox access token')
        auth = self.headers.get('Authorization')
        self.process_user = None
        if auth:
            try:
                scheme, encoded = auth.split(' ', 1)
                username, password = base64.b64decode(encoded, validate=True).decode().split(':', 1)
                if scheme != 'Basic' or password or not username or '\0' in username: raise ValueError()
                self.process_user = username
            except (ValueError, UnicodeError):
                raise APIError(403, 'Invalid process user')
            if self.process_user != 'root' and not record.get('oci_config'):
                raise APIError(403, 'This template supports the root user only')
        if path == '/health':
            return self.respond(200, {'status': 'ok'})
        if path == '/files':
            params = parse_qs(url.query)
            if params.get('username', ['root'])[0] != 'root':
                raise APIError(403, 'This template supports the root user only')
            file = params.get('path', [''])[0]
            if (file and (not file.startswith('/') or '\0' in file)) or (not file and self.command == 'GET'):
                raise APIError(400, 'An absolute guest path is required')
            if self.command == 'GET':
                data = backend.file_exec(sid, ['/bin/busybox', 'cat', file])
                return self.respond(200, data, 'application/octet-stream')
            if self.command == 'POST':
                content_type = self.headers.get('Content-Type', '')
                data = self.body()
                if content_type.startswith('multipart/form-data;'):
                    from email.parser import BytesParser
                    from email.policy import default
                    mime = BytesParser(policy=default).parsebytes(('Content-Type: ' + content_type + '\r\nMIME-Version: 1.0\r\n\r\n').encode() + data)
                    files = [(file or part.get_filename(), part.get_payload(decode=True)) for part in mime.iter_parts()]
                elif content_type == 'application/octet-stream':
                    files = [(file, data)]
                else:
                    raise APIError(415, 'Unsupported upload content type')
                result = []
                for target, data in files:
                    if not target or not target.startswith('/') or '\0' in target:
                        raise APIError(400, 'An absolute guest path is required')
                    backend.file_exec(sid, ['/bin/sh', '-c', '/bin/busybox mkdir -p "$(/bin/busybox dirname "$1")" && /bin/busybox cat > "$1"', 'write', target], data)
                    result.append({'name': target.rsplit('/', 1)[-1], 'path': target, 'type': 'file'})
                return self.respond(200, result)
        if path == '/process.Process/Start' and self.command == 'POST':
            raw = self.body()
            if len(raw) < 5 or raw[0] != 0 or struct.unpack('>I', raw[1:5])[0] != len(raw) - 5:
                raise APIError(400, 'Expected one Connect JSON request envelope')
            return self.start_process(sid, json.loads(raw[5:]))
        raise APIError(501, 'Unsupported envd operation')

    def start_process(self, sid, request):
        process_started_ns = time.perf_counter_ns()
        started = time.perf_counter()
        start_unix = time.time()
        if request.get('pty') or request.get('stdin'):
            raise APIError(400, 'PTY and interactive stdin are unsupported')
        process = request['process']
        envs = process.get('envs', {})
        if any(not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*', k) for k in envs):
            raise APIError(400, 'Invalid environment variable name')
        if hasattr(self.server.backend, 'execute_process_stream'):
            events = self.server.backend.execute_process_stream(sid, process, self.process_user)
            try:
                first = next(events)  # Errors before spawn can still be HTTP errors.
                if not first.get('pid'):
                    raise APIError(502, 'Missing guest process start')
                self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.send_response(200)
                self.send_header('Content-Type', 'application/connect+json')
                self.send_header('Connection', 'close')
                self.end_headers()
                self.close_connection = True
                def emit_stream(data, flags=0):
                    encoded = json.dumps(data).encode()
                    self.wfile.write(bytes([flags]) + struct.pack('>I', len(encoded)) + encoded)
                    self.wfile.flush()
                emit_stream({'event': {'start': first}})
                exited = False
                for event in events:
                    if 'exit_code' in event:
                        exited = True
                        emit_stream({'event': {'end': {'exitCode': event['exit_code'],
                                     'exited': True, 'status': 'exited'}}})
                    else:
                        for output in ('stdout', 'stderr'):
                            if event.get(output):
                                emit_stream({'event': {'data': {output: base64.b64encode(event[output]).decode()}}})
                if not exited:
                    raise RuntimeError('guest stream ended without exit status')
                emit_stream({}, 2)
                trace_timing('process_stream_result', process_started_ns, sid=sid)
            finally:
                events.close()
            return
        if hasattr(self.server.backend, 'execute'):
            argv = vm_process_argv(process)
            if hasattr(self.server.backend, 'execute_process'):
                result = self.server.backend.execute_process(sid, process, self.process_user)
            else:
                result = self.server.backend.execute(sid, argv)
            trace_timing('process_result', process_started_ns, sid=sid, env_count=len(envs), cwd=process.get('cwd') or '/', executable=process['cmd'])
            # The VM RPC already returned all output. Emit the same Connect
            # frames together, with a bounded HTTP body and reusable connection.
            frames = []
            def emit(data, flags=0):
                encoded = json.dumps(data).encode()
                frames.append(bytes([flags]) + struct.pack('>I', len(encoded)) + encoded)
            emit({'event': {'start': {'pid': result['pid']}}})
            for output in ('stdout', 'stderr'):
                if result[output]:
                    emit({'event': {'data': {output: base64.b64encode(result[output]).decode()}}})
            emit({'event': {'end': {'exitCode': result['exit_code'], 'exited': True, 'status': 'exited'}}})
            emit({}, 2)
            self.respond(200, b''.join(frames), 'application/connect+json')
            return
        argv = ['/bin/sh', '-c', 'cd "$1" || exit; shift; printf "\\036PID:%s\\n" "$$" >&2; exec "$@"',
                'exec', process.get('cwd') or '/', '/bin/busybox', 'env',
                *[k + '=' + v for k, v in envs.items()], process['cmd'], *process.get('args', [])]
        child = subprocess.Popen(self.server.backend.exec_args(sid, argv), stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        spawned = time.perf_counter()
        pid, diagnostics = read_process_start(child.stderr)
        pid_received = time.perf_counter()
        if pid is None:
            stdout, stderr = child.communicate(timeout=20)
            raise APIError(502, (diagnostics + stderr).decode(errors='replace'))
        if diagnostics:
            self.log_message("kubectl before guest PID: %s", diagnostics.decode(errors='replace').rstrip())
        self.send_response(200)
        self.send_header('Content-Type', 'application/connect+json')
        self.send_header('Connection', 'close')
        self.end_headers()
        self.close_connection = True
        def event(data, flags=0):
            encoded = json.dumps(data).encode()
            self.wfile.write(bytes([flags]) + struct.pack('>I', len(encoded)) + encoded)
            self.wfile.flush()
        try:
            event({'event': {'start': {'pid': pid}}})
            with selectors.DefaultSelector() as select:
                select.register(child.stdout, selectors.EVENT_READ, 'stdout')
                select.register(child.stderr, selectors.EVENT_READ, 'stderr')
                while select.get_map():
                    for key, _ in select.select(timeout=1):
                        data = os.read(key.fileobj.fileno(), 65536)
                        if data:
                            event({'event': {'data': {key.data: base64.b64encode(data).decode()}}})
                        else:
                            select.unregister(key.fileobj)
            status = child.wait()
            if os.environ.get('KUASAR_RPC_TIMING') == '1':
                self.log_message('E2B_EXEC_TIMING %s', json.dumps(dict(
                    sandbox_id=sid, start_unix=start_unix,
                    spawn_ms=(spawned-started)*1000,
                    guest_pid_ms=(pid_received-spawned)*1000,
                    output_and_exit_ms=(time.perf_counter()-pid_received)*1000)))
            event({'event': {'end': {'exitCode': status, 'exited': True, 'status': 'exited'}}})
            event({}, 2)
        finally:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=10)

    def dispatch(self):
        started_ns = time.perf_counter_ns()
        try:
            self.handle_request()
        except APIError as e:
            self.close_connection = True
            self.respond(e.code, {'code': e.code, 'message': e.message})
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            self.respond(500, {'code': 500, 'message': str(e)})
        finally:
            trace_timing('http', started_ns, method=self.command, path=urlsplit(self.path).path, sid=self.headers.get('E2b-Sandbox-Id'), thread=threading.get_native_id())

    do_POST = do_GET = do_DELETE = dispatch


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--listen', default='127.0.0.1')
    p.add_argument('--port', type=int, default=18881)
    p.add_argument('--backend', choices=['vm', 'kubernetes'], default='vm')
    p.add_argument('--cpu-count', type=int, default=2)
    p.add_argument('--memory-mb', type=int, default=512)
    p.add_argument('--k3s')
    p.add_argument('--kubeconfig')
    p.add_argument('--admin-socket')
    p.add_argument('--sandbox-state')
    p.add_argument('--cni-bin')
    p.add_argument('--cni-config')
    for name in ('source', 'api-key-file', 'state'):
        p.add_argument('--' + name, required=True)
    p.add_argument('--namespace', default='default')
    p.add_argument('--node', default='fork-research')
    p.add_argument('--runtime-class', default='kuasar-fork')
    p.add_argument('--service-account', default='fork-test')
    p.add_argument('--template', default='base')
    p.add_argument('--container', default='samecontainer')
    p.add_argument('--command-json', default='["/bin/fork-workload","init"]')
    p.add_argument('--image', default='docker.io/library/fork-workload:latest')
    args = p.parse_args()
    command = json.loads(args.command_json)
    if not isinstance(command, list) or not command or not all(isinstance(x, str) for x in command):
        p.error('--command-json must be a nonempty JSON string array')
    if args.backend == 'vm':
        for name in ('admin_socket', 'sandbox_state', 'cni_bin', 'cni_config'):
            if not getattr(args, name):
                p.error('--'+name.replace('_', '-')+' is required for the VM backend')
        import sys
        sys.modules.setdefault('server', sys.modules[__name__])
        if os.environ.get('E2B_IMAGE_CATALOG'):
            from image_pool import ImagePoolBackend
            backend = ImagePoolBackend(args)
        else:
            from vm_backend import VMBackend
            backend = VMBackend(args)
    else:
        if not args.k3s or not args.kubeconfig:
            p.error('--k3s and --kubeconfig are required for the Kubernetes backend')
        backend = Backend(args)
    server = ThreadingHTTPServer((args.listen, args.port), Handler)
    server.backend = backend
    threading.Thread(target=backend.reap, daemon=True).start()
    print('E2B compatible API listening on', server.server_address, flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        if hasattr(backend, 'close'):
            backend.close()


if __name__ == '__main__':
    main()
