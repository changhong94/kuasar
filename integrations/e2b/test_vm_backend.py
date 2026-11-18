# SPDX-License-Identifier: Apache-2.0
import concurrent.futures
import json
import subprocess
import sys
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from vm_backend import VMBackend
import vm_rpc


class VMBackendTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        p = Path(self.tmp.name)
        (p/'key').write_text('test-key')
        (p/'cni').write_text(json.dumps({'type': 'bridge'}))
        args = SimpleNamespace(api_key_file=str(p/'key'), state=str(p/'state'), cni_config=str(p/'cni'),
                               cni_bin=str(p), sandbox_state=str(p), admin_socket=str(p/'admin'), template='base')
        self.backend = VMBackend(args)
        self.sid = 'e2b-test-sandbox'
        self.backend.records[self.sid] = {'backend': 'vm', 'status': 'running', 'network_state': 'pending',
                                         'namespace': 'nonexistent-test-namespace', 'expiry': time.time()+60}
        self.backend.operations[self.sid] = threading.RLock()

    def tearDown(self):
        self.backend.close()
        self.tmp.cleanup()

    def test_template_builder_is_private_from_first_durable_record(self):
        from server import APIError
        self.backend.args.source = 'private-seed'
        def namespace(sid):
            saved = json.loads(self.backend.state.read_text())[sid]
            self.assertEqual(saved['pool_state'], 'preparing')
            self.assertNotIn('pool_origin', saved)
            with self.assertRaises(APIError):
                self.backend.get_public(sid)
            return 'private-test-netns'
        with patch.object(self.backend, 'namespace', side_effect=namespace), \
             patch.object(self.backend, 'admin', return_value={'live_fork': {'pid': 123, 'vsock_socket':'test'}}), \
             patch.object(self.backend, 'provision'):
            sid = self.backend._create_live({}, _private=True)
            self.backend.jobs[sid].result(timeout=5)
        with self.assertRaises(APIError): self.backend.get_public(sid)
        self.assertNotIn('pool_origin', self.backend.get(sid))

    def test_restart_discards_private_slots_but_preserves_leases(self):
        self.backend.records.clear()
        for state in ('preparing', 'ready', 'binding', 'quarantined', 'leased'):
            self.backend.records[state] = dict(backend='vm', status='running',
                network_state='ready', expiry=time.time()+60, pool_origin=True,
                pool_state=state)
        self.backend.save()
        args = self.backend.args
        removed = []
        def destroy(backend, sid):
            removed.append(sid)
            del backend.records[sid]
            backend.save()
        with patch.object(VMBackend, 'admin', return_value={'network_ready': True}), \
             patch.object(VMBackend, 'address', return_value='10.0.0.2'), \
             patch.object(VMBackend, 'destroy', destroy):
            recovered = VMBackend(args)
        try:
            self.assertEqual(set(removed), {'preparing', 'ready', 'binding', 'quarantined'})
            self.assertEqual(set(recovered.records), {'leased'})
            self.assertEqual(recovered.records['leased']['status'], 'running')
        finally:
            recovered.close()

    def test_execution_does_not_join_blocked_cni(self):
        started, release = threading.Event(), threading.Event()
        def cni(sid, action):
            started.set()
            self.assertTrue(release.wait(5))
        self.backend.address = lambda r: ''
        self.backend.cni_call = cni
        self.backend.admin = lambda *a, **k: {}
        job = self.backend.pool.submit(self.backend.provision, self.sid)
        self.backend.jobs[self.sid] = job
        self.assertTrue(started.wait(2))
        try:
            with patch('vm_rpc.execute', return_value={'stdout': b'print'}):
                self.assertEqual(self.backend.execute(self.sid, ['python3'])['stdout'], b'print')
            self.assertFalse(job.done())
        finally:
            release.set()
        job.result(5)

    def test_network_failure_preserves_execution_and_records_failure(self):
        self.backend.address = lambda r: ''
        self.backend.cni_call = lambda *a: (_ for _ in ()).throw(RuntimeError('CNI failed'))
        self.backend.provision(self.sid)
        r = self.backend.records[self.sid]
        self.assertEqual((r['status'], r['network_state']), ('running', 'failed'))
        self.assertIn('CNI failed', r['network_error'])
        with patch('vm_rpc.execute', return_value={'stdout': b'ok'}):
            self.assertEqual(self.backend.execute(self.sid, ['echo'])['stdout'], b'ok')

    def test_delete_joins_add_and_never_activates_deleted_child(self):
        started, release = threading.Event(), threading.Event()
        actions = []
        def cni(sid, action):
            actions.append(action)
            if action == 'ADD':
                started.set()
                self.assertTrue(release.wait(5))
        self.backend.cni_call = cni
        self.backend.address = lambda r: ''
        self.backend.admin = lambda action, **kwargs: actions.append(action) or {}
        job = self.backend.pool.submit(self.backend.provision, self.sid)
        self.backend.jobs[self.sid] = job
        self.assertTrue(started.wait(2))
        with concurrent.futures.ThreadPoolExecutor() as pool:
            deleted = pool.submit(self.backend.destroy, self.sid)
            deadline = time.monotonic()+2
            while self.backend.records[self.sid]['status'] != 'deleting' and time.monotonic() < deadline:
                time.sleep(.001)
            self.assertFalse(deleted.done())
            release.set()
            deleted.result(5)
        self.assertNotIn('sandbox-network-complete', actions)
        self.assertLess(actions.index('ADD'), actions.index('DEL'))
        self.assertNotIn(self.sid, self.backend.records)
        self.assertNotIn(self.sid, self.backend.jobs)

    def test_creation_holds_child_lifecycle_lock_until_fork_finishes(self):
        entered, release = threading.Event(), threading.Event()
        ids = []
        def namespace(sid):
            ids.append(sid)
            entered.set()
            self.assertTrue(release.wait(5))
            return '/unused-test-namespace'
        self.backend.records[self.sid].update(template_id='other', exec_root='/run/image-root', image_disk='/test.ext4', image_manifest='sha256:test', image_sha256='test', oci_config={'Cmd':['/app']}, entrypoint_pid=123)
        self.backend.records[self.sid]['network_state'] = 'ready'
        self.backend.namespace = namespace
        self.backend.admin = lambda *a, **k: {'live_fork': {'pid': 1}}
        self.backend.provision = lambda sid: None
        with concurrent.futures.ThreadPoolExecutor() as pool:
            creating = pool.submit(self.backend.create, {}, self.sid)
            self.assertTrue(entered.wait(2))
            lock = self.backend.operation(ids[0])
            try:
                self.assertFalse(lock.acquire(blocking=False))
            finally:
                release.set()
            child = creating.result(5)['sandboxID']
            for field in ('template_id', 'exec_root', 'image_disk', 'image_manifest', 'image_sha256', 'oci_config', 'entrypoint_pid'):
                self.assertEqual(self.backend.records[child][field], self.backend.records[self.sid][field])

    def test_existing_cni_interface_is_reconciled_without_second_add(self):
        self.backend.address = lambda r: '10.74.0.3'
        self.backend.cni_call = lambda *a: self.fail('must not ADD twice')
        actions = []
        self.backend.admin = lambda action, **kwargs: actions.append(action) or {}
        self.backend.provision(self.sid)
        self.assertEqual(actions, ['sandbox-network-complete'])
        self.assertEqual(self.backend.records[self.sid]['network_state'], 'ready')
        self.assertEqual(json.loads(self.backend.state.read_text())[self.sid]['ip'], '10.74.0.3')

    def test_execution_serializes_against_fork_and_delete_only(self):
        entered, release = threading.Event(), threading.Event()
        def execute(*args):
            entered.set()
            self.assertTrue(release.wait(5))
            return {}
        with patch('vm_rpc.execute', side_effect=execute), concurrent.futures.ThreadPoolExecutor() as pool:
            running = pool.submit(self.backend.execute, self.sid, ['sleep'])
            self.assertTrue(entered.wait(2))
            lock = self.backend.operation(self.sid)
            self.assertFalse(lock.acquire(blocking=False))
            release.set()
            running.result(5)
            self.assertTrue(lock.acquire(blocking=False))
            lock.release()

    def test_holder_exit_wait_reaps_and_accepts_already_reaped_child(self):
        child = subprocess.Popen([sys.executable, '-c', 'pass'])
        try:
            VMBackend.wait_holder(child)
            self.assertEqual(child.returncode, 0)
            VMBackend.wait_holder(child)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()

    def test_delete_missing_vm_still_reclaims_cni(self):
        actions = []
        def admin(action, **kwargs):
            actions.append(action)
            raise RuntimeError(f'sandbox {self.sid} not found')
        self.backend.admin = admin
        self.backend.cni_call = lambda sid, action: actions.append(action)
        self.backend.destroy(self.sid)
        self.assertEqual(actions, ['sandbox-destroy', 'DEL'])
        self.assertNotIn(self.sid, self.backend.records)

    def test_delete_failure_does_not_release_network_or_record(self):
        actions = []
        def admin(*args, **kwargs):
            raise RuntimeError('VM exit timeout; device not found')
        self.backend.admin = admin
        self.backend.cni_call = lambda *args: actions.append(args)
        with self.assertRaisesRegex(RuntimeError, 'VM exit timeout'):
            self.backend.destroy(self.sid)
        self.assertEqual(actions, [])
        self.assertEqual(self.backend.records[self.sid]['status'], 'deleting')

    def test_direct_pool_prepares_empty_kvm_after_cni_without_guest_fork(self):
        self.backend.direct_pool_prepare = True
        self.backend.args.source = 'seed'
        events = []
        def admin(action, **kw):
            events.append(action)
            assert kw['network_identity'] == 'fresh'
            return {'network_identity': 'fresh', 'live_fork': {'pid': 123, 'vsock_socket': 'private-vsock'}}
        with patch.object(self.backend, 'namespace', return_value='owned-ns'), \
             patch.object(self.backend, 'cni_call', side_effect=lambda sid, action: events.append(action)), \
             patch.object(self.backend, 'address', return_value='10.0.0.2'), \
             patch.object(self.backend, 'admin', side_effect=admin), \
             patch.object(self.backend, 'provision') as provision:
            sid = self.backend._create_live({}, _pool_owned=True)
        self.assertEqual(events, ['ADD', 'sandbox-pool-prepare'])
        provision.assert_not_called()
        self.assertTrue(self.backend.jobs[sid].done())
        self.assertTrue(self.backend.records[sid]['direct_prepared'])
        self.assertEqual(self.backend.records[sid]['pool_state'], 'preparing')
        self.assertEqual(self.backend.records[sid]['network_state'], 'ready')

    def test_direct_prepare_without_identity_ack_fails_before_publication(self):
        self.backend.direct_pool_prepare = True
        self.backend.args.source = 'seed'
        cleaned = []
        with patch.object(self.backend, 'namespace', return_value='owned-ns'), \
             patch.object(self.backend, 'cni_call'), \
             patch.object(self.backend, 'address', return_value='10.0.0.2'), \
             patch.object(self.backend, 'admin', return_value={'live_fork': {'pid': 123}}), \
             patch.object(self.backend, 'destroy', side_effect=cleaned.append):
            with self.assertRaisesRegex(RuntimeError, 'confirm fresh network identity'):
                self.backend._create_live({}, _pool_owned=True)
        self.assertEqual(len(cleaned), 1)
        self.assertNotIn(cleaned[0], self.backend.jobs)

    def test_direct_prepare_failure_retains_normal_cleanup(self):
        self.backend.direct_pool_prepare = True
        self.backend.args.source = 'seed'
        cleaned = []
        with patch.object(self.backend, 'namespace', return_value='owned-ns'), \
             patch.object(self.backend, 'cni_call'), \
             patch.object(self.backend, 'address', return_value='10.0.0.2'), \
             patch.object(self.backend, 'admin', side_effect=RuntimeError('KVM preparation failed')), \
             patch.object(self.backend, 'destroy', side_effect=cleaned.append):
            with self.assertRaisesRegex(RuntimeError, 'KVM preparation failed'):
                self.backend._create_live({}, _pool_owned=True)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(self.backend.records[cleaned[0]]['pool_state'], 'preparing')

    def test_network_handoff_commit_failure_quarantines_even_after_commit(self):
        from unittest.mock import Mock
        from server import APIError
        self.backend.records[self.sid].update(pool_origin=True, pool_state='leased', direct_prepared=True)
        idle = dict(backend='vm', network_only=True, network_slot='net-owned',
                    status='network_idle', pool_state='network_idle', expiry=time.time()+60)
        self.backend.network_pool = Mock()
        self.backend.network_pool.retire.return_value = idle
        self.backend.template_pool = Mock(enabled=True)
        original = self.backend.save
        failed = False
        def save():
            nonlocal failed
            original()
            if 'net-owned' in self.backend.records and not failed:
                failed = True
                raise OSError('acknowledgment lost after commit')
        with patch.object(self.backend, 'save', side_effect=save), \
             patch.object(self.backend, 'admin', return_value={}), \
             patch.object(self.backend, 'cni_call') as cni:
            with self.assertRaises(OSError):self.backend.destroy(self.sid)
        cni.assert_not_called()
        self.backend.template_pool.capacity_changed.assert_not_called()
        self.assertEqual(self.backend.records['net-owned']['status'], 'deleting')
        self.assertEqual(self.backend.records['net-owned']['pool_state'], 'quarantined')
        with self.assertRaises(APIError):self.backend.get_public('net-owned')
        saved=json.loads(self.backend.state.read_text())
        self.assertEqual(saved['net-owned']['status'], 'deleting')
        self.assertNotIn(self.sid, saved)


class WireTests(unittest.TestCase):
    def test_known_fields_and_unknown_fixed_fields(self):
        self.assertEqual(vm_rpc.fields(vm_rpc.integer(1, 300)+vm_rpc.blob(3, b'abc')), {1: 300, 3: b'abc'})
        for value in [b'\x80', b'\x1a\x04abc', b'\x00', b'\x0b']:
            with self.assertRaises(ValueError):
                vm_rpc.fields(value)


if __name__ == '__main__':
    unittest.main()
