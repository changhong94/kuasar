# SPDX-License-Identifier: Apache-2.0
"""Run the native worker tests in a private PID/mount namespace, never on the host."""
import json,os,shutil,socket,struct,subprocess,time,unittest
from pathlib import Path

@unittest.skipUnless(os.getpid()==1 and os.environ.get('OCI_WORKER_TEST_BINARY'),
                     'requires isolated PID 1 and OCI_WORKER_TEST_BINARY')
class WorkerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(['mount','--make-rprivate','/'],check=True)
        subprocess.run(['mount','-t','tmpfs','tmpfs','/run'],check=True)
        cls.binary=os.environ['OCI_WORKER_TEST_BINARY']
        root=Path('/run/image-root')
        for d in ['bin','etc']: (root/d).mkdir(parents=True,exist_ok=True)
        shutil.copyfile(os.environ['OCI_WORKER_TEST_PROBE'],root/'bin/probe')
        (root/'bin/probe').chmod(0o755)
        (root/'etc/passwd').write_text('root:x:0:0::/:/bin/probe\napp:x:1234:2345::/home/app:/bin/probe\n')
        (root/'etc/group').write_text('app:x:2345:\nextra:x:3456:app\n')
    def setUp(self):
        shutil.rmtree('/run/oci-entrypoint',ignore_errors=True)
        shutil.rmtree('/run/image-root/work',ignore_errors=True)
        Path('/run/oci-launch.sock').unlink(missing_ok=True)
    def worker(self):
        child=subprocess.run([self.binary,'prepare'],check=True,capture_output=True,text=True)
        self.assertGreater(int(child.stdout),1)
        return int(child.stdout)
    def send(self,args):
        payload=b''.join(x.encode()+b'\0' for x in args)
        with socket.socket(socket.AF_UNIX) as s:
            s.settimeout(5);s.connect('/run/oci-launch.sock')
            s.sendall(struct.pack('!I',len(payload))+payload)
            data=b''
            while len(data)<8:
                part=s.recv(8-len(data));self.assertTrue(part);data+=part
        return struct.unpack('!II',data)
    def argv(self,user='0',command='/bin/probe'):
        return ['/usr/bin/oci-launch','start','/run/image-root','/run/oci-entrypoint',
                '/work',''+user,'1','MARKER=fresh','--',command]
    def exit_file(self,code):
        p=Path('/run/oci-entrypoint/exit')
        for _ in range(500):
            if p.exists():break
            time.sleep(.01)
        self.assertEqual(p.read_text().strip(),str(code))
    def test_fresh_process_user_env_workdir_and_reaping(self):
        worker=self.worker()
        self.assertFalse(Path('/run/image-root/work').exists())
        self.assertEqual(Path('/run/oci-entrypoint/stdout').read_bytes(),b'')
        self.assertFalse(Path('/run/oci-entrypoint/exit').exists())
        pid,error=self.send(self.argv('app'));self.assertEqual(error,0)
        self.assertNotEqual(worker,pid);self.exit_file(7)
        out=Path('/run/oci-entrypoint/stdout').read_text()
        self.assertIn('1234:2345:/work:fresh:1:3456',out)
        self.assertFalse(Path('/run/oci-launch.sock').exists())
        self.assertEqual(os.waitpid(worker,0)[1],0)
    def test_exec_failure_is_reported_before_publication(self):
        worker=self.worker();pid,error=self.send(self.argv(command='/missing'))
        self.assertNotEqual(error,0);self.assertGreater(pid,1);self.exit_file(126)
        os.waitpid(worker,0)
    def test_unknown_user_fails_closed(self):
        worker=self.worker();_,error=self.send(self.argv('unknown'))
        self.assertNotEqual(error,0);self.exit_file(126);os.waitpid(worker,0)
    def test_explicit_gid_omits_supplementary_groups(self):
        worker=self.worker();_,error=self.send(self.argv('1234:4567'))
        self.assertEqual(error,0);self.exit_file(7)
        self.assertIn('1234:4567:/work:fresh:0',Path('/run/oci-entrypoint/stdout').read_text())
        os.waitpid(worker,0)
    def test_disconnected_client_does_not_orphan_supervision(self):
        worker=self.worker();payload=b''.join(x.encode()+b'\0' for x in self.argv())
        with socket.socket(socket.AF_UNIX) as s:
            s.connect('/run/oci-launch.sock');s.sendall(struct.pack('!I',len(payload))+payload)
        self.exit_file(7);self.assertEqual(os.waitpid(worker,0)[1],0)
    def test_invalid_contract_never_starts_process(self):
        worker=self.worker();args=self.argv();args[2]='/'
        pid,error=self.send(args);self.assertEqual(pid,0);self.assertNotEqual(error,0)
        self.assertEqual(Path('/run/oci-entrypoint/stdout').read_bytes(),b'')
        self.assertFalse(Path('/run/oci-entrypoint/exit').exists());os.waitpid(worker,0)
    def test_legacy_launcher_remains_compatible(self):
        result=subprocess.run([self.binary,*self.argv()[1:]],check=True,capture_output=True,text=True)
        self.assertGreater(int(result.stdout),1);self.exit_file(7)

if __name__=='__main__':unittest.main()
