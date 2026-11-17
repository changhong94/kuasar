# SPDX-License-Identifier: Apache-2.0
import io
import unittest
import socket
import threading
import http.client
from http.server import ThreadingHTTPServer
from server import Handler
from server import read_process_start


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
