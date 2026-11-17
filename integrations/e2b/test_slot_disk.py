import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from slot_disk import SlotDisks

@unittest.skipUnless(os.environ.get('TEST_REFLINK_DIR'), 'requires isolated reflink filesystem')
class SlotDiskTests(unittest.TestCase):
    def test_different_images_same_generic_device_and_single_use(self):
        with tempfile.TemporaryDirectory(dir=os.environ['TEST_REFLINK_DIR']) as tmp:
            root=Path(tmp); catalog={}
            for name, byte in [('a',b'A'),('b',b'B')]:
                p=root/(name+'.image');p.write_bytes(byte*4096)
                catalog[name]={'disk':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
            disks=SlotDisks(root/'slots',catalog)
            paths=[]
            for name in catalog:
                p=disks.path/('e2b-'+name+'.raw');disks.create(p);paths.append(p)
                # VMM retains an already-open descriptor throughout binding.
                with p.open('rb') as reader:
                    self.assertEqual(reader.read(4096), bytes(4096))
                    disks.bind(str(p),catalog[name]);reader.seek(0)
                    self.assertEqual(reader.read(4096),Path(catalog[name]['disk']).read_bytes())
                    reader.seek(4096);self.assertEqual(reader.read(4096),bytes(4096))
                with self.assertRaises(RuntimeError):disks.bind(str(p),catalog[name])
            disks.collect({str(paths[0])})
            self.assertTrue(paths[0].exists());self.assertFalse(paths[1].exists())
            disks.collect(set());self.assertFalse(paths[0].exists())

@unittest.skipUnless(os.geteuid()==0 and os.environ.get('IMAGE_MOUNT_TEST_BINARY'), 'requires owned chroot and launcher')
class CommandLauncherTests(unittest.TestCase):
    def test_cwd_environment_literal_arguments_and_exit_status(self):
        import shutil
        import subprocess
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'bin').mkdir();(root/'work').mkdir()
            shutil.copy2(os.environ['OCI_TEST_BUSYBOX'], root/'bin/busybox')
            result=subprocess.run([os.environ['IMAGE_MOUNT_TEST_BINARY'],'exec',tmp,'/work','1','VALUE=a b;$x',
                '/bin/busybox','sh','-c','printf "%s|%s|%s" "$PWD" "$VALUE" "$1"; exit 17','probe','literal $(false)'],capture_output=True,text=True)
            self.assertEqual(result.returncode,17)
            self.assertEqual(result.stdout,'/work|a b;$x|literal $(false)')
