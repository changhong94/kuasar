# SPDX-License-Identifier: Apache-2.0
import concurrent.futures
import json
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
            creating.result(5)

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


class WireTests(unittest.TestCase):
    def test_known_fields_and_unknown_fixed_fields(self):
        self.assertEqual(vm_rpc.fields(vm_rpc.integer(1, 300)+vm_rpc.blob(3, b'abc')), {1: 300, 3: b'abc'})
        for value in [b'\x80', b'\x1a\x04abc', b'\x00', b'\x0b']:
            with self.assertRaises(ValueError):
                vm_rpc.fields(value)


if __name__ == '__main__':
    unittest.main()
