# SPDX-License-Identifier: Apache-2.0
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from manifest_templates import ManifestTemplates


class ManifestTemplateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root/'image-test'
        self.source.mkdir()
        for name, data in {'config.json':json.dumps({'disks':[{'path':str(self.source/'disk-0.raw')}]}).encode(),
                           'manifest.json':b'{"version":1}', 'state.json':b'{}',
                           'memory-ranges':b'guest memory', 'disk-0.raw':b'file state'}.items():
            (self.source/name).write_bytes(data)
        self.backend = ManifestTemplates('unused', 'unused', self.root)
        self.objects = {}
        def invoke(action, value, output=None):
            if action == 'store':
                data = Path(value).read_bytes()
                key = hashlib.sha256(data).hexdigest()
                self.objects[key] = data
                return dict(manifest=key,logical_bytes=len(data),stats={})
            data = self.objects[value]
            Path(output).write_bytes(data)
            Path(output).chmod(0o400)
            return dict(logical_bytes=len(data),materialized_data_bytes=len(data))
        self.backend.invoke = invoke

    def test_restore_relocates_and_keeps_shared_descendant_source(self):
        descriptor = self.backend.publish(self.source)
        key = 'remote-e2b-'+'a'*24
        result = self.backend.restore(descriptor,key)
        path = Path(result['directory'])
        self.assertEqual(json.loads((path/'config.json').read_text())['disks'][0]['path'],str(path/'disk-0.raw'))
        self.assertEqual((path/'memory-ranges').read_bytes(), b'guest memory')
        # A descendant retains the source after the original owner is destroyed.
        self.backend.collect({key})
        self.assertTrue(path.exists())
        self.backend.collect(set())
        self.assertFalse(path.exists())
        self.assertTrue(self.source.exists())

    def test_capture_reference_reuses_store_without_second_upload(self):
        original = self.backend.publish(self.source)
        (self.source/'chunk-reference.json').write_text(json.dumps(
            dict(version=1,manifest='root-key',files=original['files'])))
        def no_upload(*args):
            raise AssertionError('capture already persisted these chunks')
        self.backend.invoke = no_upload
        result = self.backend.publish(self.source)
        self.assertEqual(result['root_manifest'], 'root-key')
        self.assertEqual(result['files'], original['files'])

    def test_bad_size_never_publishes(self):
        descriptor = self.backend.publish(self.source)
        descriptor['files'][0]['logical_bytes'] += 1
        with self.assertRaises(ValueError):
            self.backend.restore(descriptor,'remote-e2b-'+'b'*24)
        self.assertEqual(list(self.root.iterdir()),[self.source])

    def test_path_escape_and_symlink_rejected(self):
        with self.assertRaises(ValueError): self.backend.owned_path('../escape')
        (self.source/'link').symlink_to(self.source/'state.json')
        with self.assertRaises(ValueError): self.backend.publish(self.source)

    def test_unpublished_dependency_never_publishes(self):
        (self.source/'config.json').write_text(json.dumps({'path':str(self.source/'missing.raw')}))
        descriptor = self.backend.publish(self.source)
        with self.assertRaises(ValueError):
            self.backend.restore(descriptor,'remote-e2b-'+'c'*24)
        self.assertEqual(list(self.root.iterdir()),[self.source])

if __name__ == '__main__':
    unittest.main()
