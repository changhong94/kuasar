# SPDX-License-Identifier: Apache-2.0
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from oci_config import normalize, launch_argv

class ConfigTests(unittest.TestCase):
    def test_entrypoint_cmd_are_literal_argv(self):
        c=normalize(dict(Env=['A=old','A=new','vendor.key=literal'],Entrypoint=['/app','a b'],Cmd=['$(not-executed)'],User='1001:1002',WorkingDir='/work'))
        a=launch_argv(c,start=True)
        self.assertEqual(a[-3:],['/app','a b','$(not-executed)'])
        self.assertIn('A=new',a);self.assertNotIn('A=old',a)
        self.assertEqual(a[4:6],['/work','1001:1002'])
    def test_cmd_only_and_exec_overrides(self):
        c=normalize(dict(Cmd=['/command'],Env=['A=1']))
        a=launch_argv(c,command=['/other'],cwd='/tmp',user='0',env={'A':'2'})
        self.assertEqual(a[-1],'/other');self.assertEqual(a[4],'/tmp');self.assertIn('A=2',a)
    def test_reject_bad_config(self):
        for c in [dict(Cmd='shell'),dict(Cmd=['x\0']),dict(Cmd=['x'],WorkingDir='relative'),dict(Cmd=['x'],Env=['bad']),dict(Cmd=['x'],User='missing:'),{}]:
            with self.assertRaises(ValueError):normalize(c)

@unittest.skipUnless(os.geteuid()==0 and os.environ.get('OCI_LAUNCH_TEST_BINARY'), 'requires owned chroot fixture and static launcher')
class LauncherTests(unittest.TestCase):
    def setUp(self):
        import shutil
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)/'root';self.root.mkdir()
        for n in ['bin','etc','work']:(self.root/n).mkdir()
        self.root.chmod(0o755);(self.root/'work').chmod(0o777)
        shutil.copy2(os.environ['OCI_TEST_BUSYBOX'],self.root/'bin/busybox')
        (self.root/'etc/passwd').write_text('root:x:0:0::/root:/bin/sh\nprobe:x:1001:1002::/work:/bin/sh\n')
        (self.root/'etc/group').write_text('root:x:0:\nteam:x:1002:\nextra:x:1003:probe\n')
    def tearDown(self):self.tmp.cleanup()
    def run_as(self,user):
        return subprocess.run([os.environ['OCI_LAUNCH_TEST_BINARY'],'exec',str(self.root),'-','/work',user,'1','A=literal space','--','/bin/busybox','sh','-c','/bin/busybox id; printf "%s|%s|%s" "$PWD" "$A" "$HOME"'],capture_output=True,text=True,timeout=5)
    def test_named_user_groups_environment_and_cwd(self):
        r=self.run_as('probe');self.assertEqual(r.returncode,0,r.stderr);self.assertIn('uid=1001',r.stdout);self.assertIn('1003',r.stdout);self.assertIn('/work|literal space|/work',r.stdout)
    def test_explicit_group_drops_supplementary_groups(self):
        r=self.run_as('probe:team');self.assertEqual(r.returncode,0,r.stderr);self.assertNotIn('1003',r.stdout)
    def test_numeric_user_without_passwd_and_unknown_name(self):
        r=self.run_as('1234:1235');self.assertEqual(r.returncode,0,r.stderr);self.assertIn('uid=1234',r.stdout)
        self.assertNotEqual(self.run_as('absent').returncode,0)
    def start_command(self, command):
        import time
        state=Path(self.tmp.name)/'state'
        r=subprocess.run([os.environ['OCI_LAUNCH_TEST_BINARY'],'start',str(self.root),str(state),'/work','probe','0','--',*command],capture_output=True,text=True,timeout=5)
        for _ in range(200):
            if (state/'exit').exists():return r,state
            time.sleep(.01)
        self.fail('supervisor did not publish exit status')
    def test_supervisor_records_real_exit_and_streams(self):
        r,state=self.start_command(['/bin/busybox','sh','-c','printf out; printf err >&2; exit 7'])
        self.assertEqual(r.returncode,0,r.stderr);self.assertGreater(int(r.stdout),0)
        self.assertEqual((state/'stdout').read_text(),'out');self.assertEqual((state/'stderr').read_text(),'err');self.assertEqual((state/'exit').read_text().strip(),'7')
    def test_exec_failure_is_not_acknowledged_as_started(self):
        r,state=self.start_command(['/missing'])
        self.assertNotEqual(r.returncode,0);self.assertEqual((state/'exit').read_text().strip(),'126')
