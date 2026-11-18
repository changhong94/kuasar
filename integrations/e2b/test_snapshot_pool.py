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
             patch.object(b, 'prepare_snapshot', return_value=dict(key='template', fields={'exec_root':'/run/image-root','oci_config':b.image_catalog['second']['oci_config']})), \
             patch.object(b, 'admin', side_effect=bind), \
             patch('image_pool.vm_rpc.execute',return_value={'exit_code':0,'stdout':b'123'}):
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

    def test_entrypoint_starts_after_bind_before_publication(self):
        b = self.backend
        events = []
        def bind(action, **kw):
            events.append('bind')
            return {'ok': True}
        def start(sid, record):
            self.assertEqual(events, ['bind'])
            with self.assertRaises(APIError): b.get_public(sid)
            self.assertEqual(record['oci_config'], b.image_catalog['base']['oci_config'])
            events.append('start')
            record['entrypoint_pid'] = 123
        with patch.dict(os.environ, E2B_SNAPSHOT_POOL='1'), \
             patch.object(b, 'prepare_snapshot', return_value=dict(key='template', fields={'oci_config':b.image_catalog['base']['oci_config']})), \
             patch.object(b, 'admin', side_effect=bind), \
             patch.object(b, 'start_entrypoint', side_effect=start):
            response = b.create({})
        self.assertEqual(events, ['bind', 'start'])
        self.assertEqual(b.get_public(response['sandboxID'])['entrypoint_pid'], 123)

    def test_snapshot_entrypoint_failure_never_publishes(self):
        b = self.backend
        before = set(b.records)
        with patch.dict(os.environ, E2B_SNAPSHOT_POOL='1'), \
             patch.object(b, 'prepare_snapshot', return_value=dict(key='template', fields={})), \
             patch.object(b, 'admin', return_value={'ok':True}), \
             patch.object(b, 'start_entrypoint', side_effect=RuntimeError('entrypoint failed')):
            with self.assertRaisesRegex(RuntimeError, 'entrypoint failed'): b.create({})
        self.assertTrue(before - set(b.records))
        self.assertFalse(any(r.get('pool_state')=='leased' for r in b.committed.values()))

    def test_prepared_worker_failure_keeps_lease_private(self):
        b = self.backend
        def start(path, argv, **kwargs):
            self.assertEqual(kwargs, {'method': 'StartOCIProcess'})
            pending = [sid for sid, r in b.records.items() if r.get('pool_state') == 'binding']
            self.assertEqual(len(pending), 1)
            with self.assertRaises(APIError):
                b.get_public(pending[0])
            return {'exit_code': 126, 'stderr': b'exec failed'}
        with patch.dict(os.environ, E2B_SNAPSHOT_POOL='1'), \
             patch.object(b, 'prepare_snapshot', return_value=dict(key='template', fields={
                 'oci_config': b.image_catalog['base']['oci_config'], 'prepared_oci': True})), \
             patch.object(b, 'admin', return_value={'ok': True}), \
             patch('image_pool.vm_rpc.execute', side_effect=start):
            with self.assertRaisesRegex(RuntimeError, 'OCI entrypoint failed'):
                b.create({})
        self.assertFalse(any(r.get('pool_state') == 'leased' for r in b.committed.values()))
