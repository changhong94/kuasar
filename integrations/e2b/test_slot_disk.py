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

    def test_hot_added_image_binds_an_existing_device(self):
        with tempfile.TemporaryDirectory(dir=os.environ['TEST_REFLINK_DIR']) as tmp:
            root = Path(tmp)
            first = root/'first'; first.write_bytes(b'A'*4096)
            a = dict(disk=str(first), sha256=hashlib.sha256(first.read_bytes()).hexdigest())
            disks = SlotDisks(root/'slots', {'a':a})
            slot = disks.path/'e2b-existing.raw'; disks.create(slot)
            second = root/'second'; second.write_bytes(b'B'*4096)
            b = dict(disk=str(second), sha256=hashlib.sha256(second.read_bytes()).hexdigest())
            disks.prepare_images([b]); disks.bind(slot, b)
            with slot.open('rb') as f: self.assertEqual(f.read(4096), b'B'*4096)

    def test_invalid_image_is_not_cached(self):
        with tempfile.TemporaryDirectory(dir=os.environ['TEST_REFLINK_DIR']) as tmp:
            root = Path(tmp); source=root/'image'; source.write_bytes(b'A'*4096)
            a = dict(disk=str(source), sha256=hashlib.sha256(source.read_bytes()).hexdigest())
            disks = SlotDisks(root/'slots', {'a':a})
            bad = {**a, 'sha256':'0'*64}
            with self.assertRaisesRegex(ValueError, 'checksum'): disks.prepare_images([bad])
            self.assertFalse((disks.cache/('0'*64+'.raw')).exists())
