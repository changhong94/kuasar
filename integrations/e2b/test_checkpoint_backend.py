# SPDX-License-Identifier: Apache-2.0
import json
import time
from unittest.mock import patch
from test_vm_backend import VMBackendTests
from server import APIError

class CheckpointTests(VMBackendTests):
    def setUp(self):
        super().setUp()
        self.backend.get(self.sid).update(token='test-token', startedAt='2026-11-17T00:00:00Z')

    def test_pause_persists_intent_and_releases_ram_before_ack(self):
        def admin(action, **kwargs):
            self.assertEqual(action, 'sandbox-checkpoint-pause')
            self.assertEqual(json.loads(self.backend.state.read_text())[self.sid]['status'], 'pausing')
            return dict(status='paused', guest_ram_released=True, checkpoint='/owned/checkpoint')
        with patch.object(self.backend, 'admin', side_effect=admin):
            self.backend.pause(self.sid)
        self.assertEqual(self.backend.get(self.sid)['status'], 'paused')
        self.assertEqual(json.loads(self.backend.state.read_text())[self.sid]['status'], 'paused')

    def test_pause_failure_never_claims_success(self):
        with patch.object(self.backend, 'admin', side_effect=RuntimeError('snapshot sync failed')):
            with self.assertRaises(RuntimeError): self.backend.pause(self.sid)
        self.assertEqual(self.backend.get(self.sid)['status'], 'pausing')
        with self.assertRaises(APIError): self.backend.execute(self.sid, ['true'])

    def test_pause_without_ram_release_is_rejected(self):
        with patch.object(self.backend, 'admin', return_value=dict(status='paused', guest_ram_released=False)):
            with self.assertRaises(APIError): self.backend.pause(self.sid)
        self.assertEqual(self.backend.get(self.sid)['status'], 'pausing')

    def test_paused_pause_is_idempotent(self):
        self.backend.get(self.sid)['status'] = 'paused'
        with patch.object(self.backend, 'admin') as admin:
            self.backend.pause(self.sid)
            admin.assert_not_called()

    def test_resume_preserves_identity_and_refreshes_timeout(self):
        r = self.backend.get(self.sid)
        r.update(status='paused', expiry=time.time()-20, checkpoint='/owned/checkpoint')
        def admin(action, **kwargs):
            self.assertEqual(action, 'sandbox-checkpoint-resume')
            self.assertEqual(json.loads(self.backend.state.read_text())[self.sid]['status'], 'resuming')
            return dict(status='running')
        with patch.object(self.backend, 'admin', side_effect=admin):
            out = self.backend.connect(self.sid, {'timeout': 60})
        self.assertEqual(out['sandboxID'], self.sid)
        self.assertEqual(out['envdAccessToken'], 'test-token')
        self.assertEqual(out['state'], 'running')
        self.assertGreater(r['expiry'], time.time()+59)
        self.assertNotIn('checkpoint', r)

    def test_resume_failure_keeps_commands_blocked(self):
        self.backend.get(self.sid)['status'] = 'paused'
        with patch.object(self.backend, 'admin', side_effect=RuntimeError('restore failed')):
            with self.assertRaises(RuntimeError): self.backend.connect(self.sid, {})
        self.assertEqual(self.backend.get(self.sid)['status'], 'resuming')
        with self.assertRaises(APIError): self.backend.execute(self.sid, ['true'])

    def test_running_connect_does_not_restore_vm(self):
        with patch.object(self.backend, 'admin') as admin:
            self.backend.connect(self.sid, {'timeout': 60})
            admin.assert_not_called()

    def test_invalid_timeout_does_not_mutate_paused_record(self):
        self.backend.get(self.sid)['status'] = 'paused'
        with self.assertRaises(APIError): self.backend.connect(self.sid, {'timeout': -1})
        self.assertEqual(self.backend.get(self.sid)['status'], 'paused')

    def test_restart_keeps_paused_guest_suspended(self):
        from vm_backend import VMBackend
        self.backend.get(self.sid).update(status='paused', expiry=time.time()-1)
        self.backend.save()
        actions=[]
        def admin(backend, action, **kwargs):
            actions.append(action)
            return dict(status='paused', guest_ram_released=True)
        with patch.object(VMBackend, 'admin', admin):
            restored=VMBackend(self.backend.args)
        try:
            self.assertEqual(restored.get(self.sid)['status'], 'paused')
            self.assertEqual(actions, ['sandbox-checkpoint-status', 'sandbox-checkpoint-pause'])
        finally:
            restored.close()

    def test_paused_guest_is_not_expired_by_running_timeout(self):
        self.backend.get(self.sid).update(status='paused', expiry=time.time()-100)
        with patch.object(self.backend.closed, 'wait', side_effect=[False, True]), \
             patch.object(self.backend, 'destroy') as destroy:
            self.backend.reap()
            destroy.assert_not_called()

    def test_pause_transition_does_not_return_sdk_already_paused_code(self):
        self.backend.get(self.sid)['status']='resuming'
        with self.assertRaises(APIError) as error: self.backend.pause(self.sid)
        self.assertEqual(error.exception.code,423)
