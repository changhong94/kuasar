import io,struct,unittest
from unittest.mock import patch
import vm_rpc

class FakeSocket:
    def __init__(self, frames):self.input=io.BytesIO(b'OK 1024\n'+frames);self.closed=False
    def __enter__(self):return self
    def __exit__(self,*args):self.closed=True
    def connect(self,*args):pass
    def settimeout(self,*args):pass
    def sendall(self,*args):pass
    def recv(self,n):return self.input.read(n)

def frame(payload=b'',flags=0):return struct.pack('>IIBB',len(payload),1,3,flags)+payload

class StreamTests(unittest.TestCase):
    def test_output_is_yielded_before_exit_and_close_releases_socket(self):
        sock=FakeSocket(frame(vm_rpc.integer(1,42))+frame(vm_rpc.blob(2,b'\xffREADY')))
        with patch.object(vm_rpc.socket,'socket',return_value=sock):
            stream=vm_rpc.execute_stream('/unused',['python3'])
            self.assertEqual(next(stream),{'pid':42})
            self.assertEqual(next(stream)['stdout'],b'\xffREADY')
            stream.close()
        self.assertTrue(sock.closed)

    def test_exit_and_stderr_are_preserved(self):
        sock=FakeSocket(frame(vm_rpc.integer(1,42))+frame(vm_rpc.blob(3,b'err'))+
                        frame(vm_rpc.integer(4,1)+vm_rpc.integer(5,7))+frame(flags=5))
        with patch.object(vm_rpc.socket,'socket',return_value=sock):
            events=list(vm_rpc.execute_stream('/unused',['command']))
        self.assertEqual(events[1]['stderr'],b'err');self.assertEqual(events[2]['exit_code'],7)
        self.assertTrue(sock.closed)

    def test_missing_exit_is_rejected(self):
        sock=FakeSocket(frame(vm_rpc.integer(1,42))+frame(flags=5))
        with patch.object(vm_rpc.socket,'socket',return_value=sock):
            with self.assertRaises(ValueError):list(vm_rpc.execute_stream('/unused',['command']))
