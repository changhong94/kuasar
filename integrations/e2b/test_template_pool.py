# SPDX-License-Identifier: Apache-2.0
import concurrent.futures
import copy
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from server import APIError
from vm_backend import VMBackend
from template_pool import TemplatePool


class FakeBackend(VMBackend):
    def __init__(self, state):
        self.state = state
        self.lock = threading.RLock()
        self.records = {}
        self.jobs = {}
        self.created = 0
        self.template_pool = None
        self.args = SimpleNamespace(template='base')
        self.committed = {}
        self.fail_assignment = False
        self.parents = []

    def save(self):
        self.committed = copy.deepcopy(self.records)
        if self.fail_assignment and any(r.get('pool_state') == 'leased' for r in self.records.values()):
            self.fail_assignment = False
            raise OSError('injected failure after commit')

    def _create_live(self, body, parent=None, _pool_owned=False):
        with self.lock:
            self.created += 1
            sid = str(self.created)
            self.parents.append(parent)
            self.records[sid] = dict(status='running', network_state='ready', value=0)
            if _pool_owned:
                self.records[sid].update(pool_origin=True, pool_state='preparing')
            self.save()
            job = concurrent.futures.Future()
            job.set_result(None)
            self.jobs[sid] = job
            return sid if _pool_owned else self.response(sid)

    def response(self, sid):
        self.get_public(sid)
        return {'sandboxID': sid, **self.records[sid]}

    def destroy(self, sid):
        with self.lock:
            del self.records[sid]
            self.jobs.pop(sid, None)
            self.save()
            if self.template_pool:
                self.template_pool.capacity_changed()


class PoolTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.backend = FakeBackend(Path(self.tmp.name)/'state')
        self.pool = None

    def tearDown(self):
        if self.pool:
            self.pool.close()
        self.tmp.cleanup()

    def start(self, n):
        self.pool = TemplatePool(self.backend, n, refill_delay=0)
        self.backend.template_pool = self.pool

    def test_private_slots_and_concurrent_single_use_assignment(self):
        self.start(10)
        for sid in self.backend.records:
            with self.assertRaises(APIError):
                self.backend.get_public(sid)
        with concurrent.futures.ThreadPoolExecutor(10) as workers:
            rows = list(workers.map(lambda _: self.backend.create({'metadata': {'leased': 'yes'}}), range(10)))
        self.assertEqual(len({r['sandboxID'] for r in rows}), 10)
        self.assertEqual(self.backend.created, 10)
        self.assertEqual(self.pool.status()['assigned'], 10)
        for row in rows:
            self.assertEqual(self.backend.committed[row['sandboxID']]['pool_state'], 'leased')
            self.assertEqual(row['metadata'], {'leased': 'yes'})

    def test_exhaustion_waits_for_retirement_and_replacement(self):
        self.start(1)
        first = self.backend.create({})['sandboxID']
        entered = threading.Event()
        def create():
            entered.set()
            return self.backend.create({})
        with concurrent.futures.ThreadPoolExecutor() as workers:
            waiting = workers.submit(create)
            self.assertTrue(entered.wait(2))
            time.sleep(.02)
            self.assertFalse(waiting.done())
            self.assertEqual(self.backend.created, 1)
            self.backend.destroy(first)
            second = waiting.result(2)['sandboxID']
        self.assertNotEqual(first, second)
        self.assertEqual(self.pool.status()['owned'], 1)

    def test_failed_acknowledgment_quarantines_slot_even_after_commit(self):
        self.start(1)
        self.backend.fail_assignment = True
        with self.assertRaises(OSError):
            self.backend.create({})
        self.assertEqual(self.pool.status()['ready'], 0)
        self.assertEqual(next(iter(self.backend.committed.values()))['pool_state'], 'quarantined')
        self.pool.drain()
        self.assertEqual(self.backend.records, {})

    def test_explicit_parent_never_consumes_a_prepared_slot(self):
        self.start(2)
        parent = self.backend.create({})['sandboxID']
        self.backend.records[parent]['value'] = 123
        ready = self.pool.status()['ready']
        child = self.backend.create({}, parent)['sandboxID']
        self.assertEqual(self.backend.parents[-1], parent)
        self.assertEqual(self.pool.status()['ready'], ready)
        self.assertFalse(self.backend.records[child].get('pool_origin'))
        self.pool.drain()
        self.assertIn(parent, self.backend.records)
        self.assertIn(child, self.backend.records)

    def test_invalid_create_cannot_consume_slot(self):
        self.start(1)
        with self.assertRaises(APIError):
            self.backend.create({'timeout': 0})
        self.assertEqual(self.pool.status()['ready'], 1)

    def test_recovered_leases_count_against_capacity(self):
        self.backend.records['existing'] = dict(pool_origin=True, pool_state='leased',
                                                status='running', network_state='ready')
        self.start(1)
        self.assertEqual(self.backend.created, 0)
        self.assertEqual(self.pool.status()['assigned'], 1)
        self.assertEqual(self.pool.status()['ready'], 0)


    def test_drain_wakes_exhausted_clients_without_assigning(self):
        self.start(1)
        first = self.backend.create({})['sandboxID']
        with concurrent.futures.ThreadPoolExecutor() as workers:
            waiting = workers.submit(self.backend.create, {})
            deadline = time.monotonic()+2
            while self.pool.status()['misses'] == 0 and time.monotonic()<deadline:
                time.sleep(.001)
            self.assertEqual(self.pool.status()['misses'], 1)
            self.pool.drain()
            with self.assertRaises(APIError):
                waiting.result(2)
        self.assertIn(first, self.backend.records)
        self.assertEqual(self.backend.created, 1)

    def test_network_failure_never_publishes_ready_slot(self):
        original = self.backend._create_live
        def failed(*args, **kwargs):
            sid = original(*args, **kwargs)
            self.backend.records[sid]['network_state'] = 'failed'
            return sid
        self.backend._create_live = failed
        with self.assertRaises(RuntimeError):
            self.start(1)
        self.assertEqual(self.backend.records, {})


    def test_private_claim_cannot_execute_or_be_reassigned_before_binding(self):
        self.start(1)
        sid = self.pool.lease({}, private=True)
        self.assertEqual(self.backend.committed[sid]['pool_state'], 'binding')
        self.assertEqual(self.pool.status()['ready'], 0)
        with self.assertRaises(APIError):
            self.backend.get_public(sid)
        self.pool.drain()
        self.assertEqual(self.backend.records, {})
