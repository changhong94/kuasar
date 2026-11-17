# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from unittest.mock import patch
import test_image_pool as image_tests
from server import APIError

class SnapshotPoolTests(unittest.TestCase):
    setUp = image_tests.ImagePoolTests.setUp
    tearDown = image_tests.ImagePoolTests.tearDown
    def test_snapshot_bind_remains_private_until_identity_reset(self):
        b = self.backend
        def bind(action, **kw):
            self.assertEqual(action, 'sandbox-snapshot-bind')
            sid = kw['sandbox_id']
            self.assertEqual(b.committed[sid]['pool_state'], 'binding')
            with self.assertRaises(APIError): b.get_public(sid)
            return {'ok': True}
        with patch.dict(os.environ, E2B_SNAPSHOT_POOL='1'), \
             patch.object(b, 'prepare_snapshot', return_value=dict(key='template', fields={'exec_root':'/run/image-root'})), \
             patch.object(b, 'admin', side_effect=bind):
            response = b.create({'templateID':'second'})
        sid = response['sandboxID']
        self.assertEqual(b.committed[sid]['pool_state'], 'leased')
        self.assertEqual(b.committed[sid]['template_id'], 'second')
        self.assertEqual(b.committed[sid]['snapshot_template'], 'template')

    def test_failed_restore_destroys_slot_without_publication(self):
        b = self.backend
        before = set(b.records)
        with patch.dict(os.environ, E2B_SNAPSHOT_POOL='1'), \
             patch.object(b, 'prepare_snapshot', return_value=dict(key='template', fields={})), \
             patch.object(b, 'admin', side_effect=RuntimeError('restore failed')):
            with self.assertRaises(RuntimeError): b.create({})
        self.assertTrue(before - set(b.records))
        self.assertFalse(any(r.get('pool_state')=='leased' for r in b.committed.values()))
