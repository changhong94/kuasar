# SPDX-License-Identifier: Apache-2.0
import io
import unittest
import socket
import threading
import http.client
from http.server import ThreadingHTTPServer
from server import Handler
from server import read_process_start, vm_process_argv


class ProcessStartTests(unittest.TestCase):
    def test_client_diagnostic_precedes_guest_pid(self):
        warning = b"E1114 websocket.go:296] Unknown stream id 1, discarding message\n"
        stream = io.BytesIO(warning + b"\x1ePID:3021\nguest stderr\n")
        self.assertEqual(read_process_start(stream), (3021, warning))
        self.assertEqual(stream.read(), b"guest stderr\n")

    def test_missing_pid_is_a_real_failure(self):
        self.assertEqual(read_process_start(io.BytesIO(b"exec failed\n")),
                         (None, b"exec failed\n"))

    def test_diagnostic_is_not_a_guest_marker(self):
        self.assertEqual(read_process_start(io.BytesIO(b"warning PID:3\n\x1ePID:4\n")),
                         (4, b"warning PID:3\n"))


class TransportTests(unittest.TestCase):
    def test_vm_connect_frames_preserve_output_and_reuse_connection(self):
        import json, struct, base64
        from types import SimpleNamespace
        class Probe(Handler):
            def do_GET(self):
                self.start_process('slot', {'process': {'cmd': '/bin/bash', 'args': ['-l', '-c', 'exit 7']}})
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Probe)
        server.backend = SimpleNamespace(execute=lambda *args: dict(pid=42, stdout=b'out', stderr=b'err', exit_code=7))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = http.client.HTTPConnection(*server.server_address)
        try:
            connection = None
            for _ in range(2):
                client.request('GET', '/')
                if connection is None:
                    connection = client.sock
                self.assertIs(client.sock, connection)
                response = client.getresponse()
                self.assertFalse(response.will_close)
                body = response.read()
                self.assertEqual(len(body), int(response.getheader('Content-Length')))
                frames = []
                while body:
                    flags, size = body[0], struct.unpack('>I', body[1:5])[0]
                    frames.append((flags, json.loads(body[5:5+size])))
                    body = body[5+size:]
                self.assertEqual(frames[-1], (2, {}))
                self.assertEqual(frames[0][1]['event']['start']['pid'], 42)
                self.assertEqual(base64.b64decode(frames[1][1]['event']['data']['stdout']), b'out')
                self.assertEqual(base64.b64decode(frames[2][1]['event']['data']['stderr']), b'err')
                self.assertEqual(frames[3][1]['event']['end']['exitCode'], 7)
        finally:
            client.close()
            server.shutdown();server.server_close();thread.join()

    def test_keepalive_responses_disable_nagle(self):
        class Probe(Handler):
            def do_GET(self):
                value = self.connection.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
                self.respond(200, {'nodelay': value})
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Probe)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = http.client.HTTPConnection(*server.server_address)
        try:
            for _ in range(2):
                client.request('GET', '/')
                response = client.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(response.read(), b'{"nodelay": 1}')
        finally:
            client.close()
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()


class VMCommandTests(unittest.TestCase):
    def test_empty_environment_preserves_cwd_and_argument_boundaries(self):
        import subprocess, tempfile
        with tempfile.TemporaryDirectory() as cwd:
            argv = vm_process_argv(dict(cmd='/bin/sh', args=['-c', 'printf "%s:%s" "$PWD" "$1"', 'test', 'a;$(false)'], cwd=cwd))
            self.assertNotIn('/bin/busybox', argv)
            self.assertEqual(subprocess.check_output(argv, text=True), cwd+':a;$(false)')

    def test_nonempty_environment_retains_env_program(self):
        argv = vm_process_argv(dict(cmd='/bin/bash', args=['-l', '-c', 'printf READY'], envs={'X': 'a b;$(false)'}, cwd='/tmp'))
        self.assertEqual(argv[5:], ['/bin/busybox', 'env', 'X=a b;$(false)', '/bin/bash', '-l', '-c', 'printf READY'])

    def test_missing_cwd_still_fails_without_executing_command(self):
        import subprocess
        argv = vm_process_argv(dict(cmd='/bin/sh', args=['-c', 'printf WRONG'], cwd='/nonexistent-kuasar-test-cwd'))
        done = subprocess.run(argv, capture_output=True)
        self.assertNotEqual(done.returncode, 0)
        self.assertEqual(done.stdout, b'')

class NativeStreamingTests(unittest.TestCase):
    def test_http_stdout_arrives_before_backend_exit(self):
        import json,struct,base64
        from types import SimpleNamespace
        release=threading.Event()
        def events(*args):
            yield {'pid':123}
            yield {'stdout':b'READY','stderr':b''}
            if not release.wait(2):raise RuntimeError('client never received live output')
            yield {'stdout':b'','stderr':b'error'}
            yield {'exit_code':7}
        class Probe(Handler):
            def do_GET(self):
                self.process_user='root'
                self.start_process('slot',{'process':{'cmd':'python3','args':[]}})
            def log_message(self,*args):pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Probe)
        server.backend=SimpleNamespace(execute_process_stream=events)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        client=http.client.HTTPConnection(*server.server_address,timeout=2)
        try:
            client.request('GET','/');response=client.getresponse()
            def read():
                header=response.read(5);flags,n=header[0],struct.unpack('>I',header[1:])[0]
                return flags,json.loads(response.read(n))
            self.assertEqual(read()[1]['event']['start']['pid'],123)
            self.assertEqual(base64.b64decode(read()[1]['event']['data']['stdout']),b'READY')
            release.set()
            self.assertEqual(base64.b64decode(read()[1]['event']['data']['stderr']),b'error')
            self.assertEqual(read()[1]['event']['end']['exitCode'],7)
            self.assertEqual(read(),(2,{}))
        finally:
            release.set();client.close();server.shutdown();server.server_close();thread.join()
