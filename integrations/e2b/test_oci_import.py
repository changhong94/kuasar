# SPDX-License-Identifier: Apache-2.0
"""Exercise publication and metadata selection with synthetic OCI descriptors."""
import argparse
import contextlib
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import oci_import


class ImportTests(unittest.TestCase):
    def import_image(self, filesystem='ext4', architecture='amd64'):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        args = argparse.Namespace(store=root/'store', catalog=root/'catalog.json',
            source='oci:fixture:image', name='arbitrary-name', architecture='amd64',
            filesystem=filesystem, disk_mib=64, skopeo='skopeo', umoci='umoci',
            mkfs_erofs='mkfs.erofs')
        calls = []
        def execute(argv, **kwargs):
            calls.append(argv)
            staging = next(args.store.iterdir())
            layout = staging/'oci'
            if argv[0] == 'skopeo':
                blobs = layout/'blobs/sha256'; blobs.mkdir(parents=True)
                def blob(value):
                    raw = json.dumps(value).encode(); sha = hashlib.sha256(raw).hexdigest()
                    (blobs/sha).write_bytes(raw)
                    return {'digest': 'sha256:'+sha, 'size': len(raw)}
                config = blob({'os':'linux', 'architecture':architecture,
                               'config':{'Cmd':['/application'], 'User':'1001',
                                         'WorkingDir':'/work', 'Env':['X=value']}})
                manifest = blob({'config':config, 'layers':[]})
                (layout/'index.json').write_text(json.dumps({'manifests':[manifest]}))
            elif argv[0] == 'umoci':
                (staging/'bundle/rootfs').mkdir(parents=True)
            elif argv[0] == 'truncate':
                Path(argv[-1]).write_bytes(b'filesystem')
            elif argv[0] == 'mkfs.erofs':
                Path(argv[-2]).write_bytes(b'filesystem')
        with patch.object(oci_import.subprocess, 'run', side_effect=execute), contextlib.redirect_stdout(io.StringIO()):
            oci_import.run(args)
        return args, calls, json.loads(args.catalog.read_text())['arbitrary-name']

    def test_ext4_keeps_oci_defaults_and_publishes_immutable_disk(self):
        args, calls, record = self.import_image()
        self.assertEqual(record['oci_config']['Cmd'], ['/application'])
        self.assertEqual(record['oci_config']['User'], '1001')
        self.assertEqual(record['oci_config']['WorkingDir'], '/work')
        self.assertEqual(record['filesystem'], 'ext4')
        self.assertEqual(Path(record['disk']).stat().st_mode & 0o222, 0)
        self.assertIn('mkfs.ext4', [x[0] for x in calls])

    def test_erofs_requires_preserved_mtime(self):
        args, calls, record = self.import_image('erofs')
        mkfs = next(x for x in calls if x[0] == 'mkfs.erofs')
        self.assertEqual(mkfs[1], '--preserve-mtime')
        self.assertEqual(record['filesystem'], 'erofs')

    def test_mismatched_platform_fails_before_publication(self):
        with self.assertRaisesRegex(ValueError, 'platform'):
            self.import_image(architecture='arm64')
