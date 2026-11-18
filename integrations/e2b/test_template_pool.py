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

    def start(self, n, workers=1):
        self.pool = TemplatePool(self.backend, n, workers=workers)
        self.backend.template_pool = self.pool

    def test_startup_prepares_in_parallel_before_publishing_ready(self):
        barrier = threading.Barrier(4)
        lock = threading.Lock()
        active = peak = entered = 0
        def prepare(sid):
            nonlocal active, peak, entered
            with lock:
                active += 1
                peak = max(peak, active)
                entered += 1
                first_wave = entered <= 4
            try:
                if first_wave:
                    barrier.wait(timeout=3)
                self.assertNotEqual(self.backend.get(sid).get('pool_state'), 'ready')
            finally:
                with lock:
                    active -= 1
        self.backend.prepare_slot = prepare
        self.start(9, workers=4)
        self.assertEqual(peak, 4)
        self.assertEqual(entered, 9)
        self.assertEqual(self.pool.status()['ready'], 9)
        self.assertEqual(self.pool.status()['preparing'], 0)
        self.assertEqual(self.pool.status()['pending'], 0)
        self.assertEqual(len(self.backend.committed), 9)

    def test_parallel_reservations_are_bounded_before_record_creation(self):
        self.pool = TemplatePool(self.backend, 2, workers=8)
        self.backend.template_pool = self.pool
        original = self.backend._create_live
        gate = threading.Event()
        entered = threading.Barrier(3)
        def blocked(*args, **kwargs):
            entered.wait(timeout=3)
            gate.wait(timeout=3)
            return original(*args, **kwargs)
        self.backend._create_live = blocked
        try:
            ids = [self.pool.lease({})['sandboxID'] for _ in range(2)]
            entered.wait(timeout=3)
            self.assertEqual(self.pool.status()['pending'], 2)
            self.assertEqual(self.pool.status()['preparing'], 2)
            self.assertEqual(self.backend.created, 2)
        finally:
            gate.set()
        with self.pool.condition:
            self.assertTrue(self.pool.condition.wait_for(lambda: self.pool.status()['ready'] == 2, 3))
        self.assertEqual(self.backend.created, 4)
        self.assertEqual(self.pool.status()['assigned'], 2)
        import json
        events = [json.loads(line) for line in self.pool.events.read_text().splitlines()]
        replacements = [e['vacancy_sid'] for e in events if e['event'] == 'prepared' and e['reason'] == 'refill']
        self.assertCountEqual(replacements, ids)

    def test_parallel_drain_waits_for_all_preparations(self):
        self.pool = TemplatePool(self.backend, 2, workers=2)
        self.backend.template_pool = self.pool
        original = self.backend._create_live
        gate = threading.Event()
        entered = threading.Barrier(3)
        def blocked(*args, **kwargs):
            entered.wait(timeout=3)
            gate.wait(timeout=3)
            return original(*args, **kwargs)
        self.backend._create_live = blocked
        try:
            ids = [self.pool.lease({})['sandboxID'] for _ in range(2)]
            entered.wait(timeout=3)
            with concurrent.futures.ThreadPoolExecutor(1) as executor:
                done = executor.submit(self.pool.drain)
                with self.pool.condition:
                    self.assertTrue(self.pool.condition.wait_for(lambda: not self.pool.enabled, 3))
                self.assertFalse(done.done())
                gate.set()
                done.result(timeout=3)
        finally:
            gate.set()
        self.assertCountEqual(self.backend.records, ids)
        self.assertEqual(self.pool.status()['preparing'], 0)
        self.assertEqual(self.pool.status()['pending'], 0)

    def test_parallel_failure_releases_reservation_and_retries_original_vacancy(self):
        self.start(2, workers=4)
        original = self.backend._create_live
        failed = threading.Event()
        def create(*args, **kwargs):
            with self.backend.lock:
                if not failed.is_set():
                    failed.set()
                    raise RuntimeError('injected preparation failure before record creation')
            return original(*args, **kwargs)
        self.backend._create_live = create
        ids = [self.pool.lease({})['sandboxID'] for _ in range(2)]
        with self.pool.condition:
            self.assertTrue(self.pool.condition.wait_for(
                lambda: self.pool.status()['ready'] == 2 and not self.pool.inflight, 4))
        self.assertTrue(failed.is_set())
        self.assertEqual(self.pool.pending, 0)
        self.assertEqual(self.backend.created, 4)
        import json
        events = [json.loads(line) for line in self.pool.events.read_text().splitlines()]
        replacements = [e['vacancy_sid'] for e in events if e['event'] == 'prepared' and e['reason'] == 'refill']
        self.assertCountEqual(replacements, ids)

    def test_close_wakes_waiting_claims_on_separate_ready_condition(self):
        self.start(1, workers=2)
        # Quarantined ownership consumes reserve budget without publishing Ready.
        with self.pool.condition:
            self.backend.records['1']['pool_state'] = 'quarantined'
        with concurrent.futures.ThreadPoolExecutor(1) as workers:
            waiting = workers.submit(self.pool.lease, {})
            deadline = time.monotonic()+2
            while not self.pool.misses and time.monotonic() < deadline:
                time.sleep(.001)
            self.assertEqual(self.pool.misses, 1)
            self.pool.close()
            with self.assertRaises(APIError):
                waiting.result(timeout=2)

    def test_private_slots_and_concurrent_single_use_assignment(self):
        self.start(10)
        for sid in self.backend.records:
            with self.assertRaises(APIError):
                self.backend.get_public(sid)
        with concurrent.futures.ThreadPoolExecutor(10) as workers:
            rows = list(workers.map(lambda _: self.backend.create({'metadata': {'leased': 'yes'}}), range(10)))
        self.assertEqual(len({r['sandboxID'] for r in rows}), 10)
        self.assertGreaterEqual(self.backend.created, 10)
        self.assertLessEqual(self.pool.status()['reserves'], 10)
        self.assertEqual(self.pool.status()['assigned'], 10)
        for row in rows:
            self.assertEqual(self.backend.committed[row['sandboxID']]['pool_state'], 'leased')
            self.assertEqual(row['metadata'], {'leased': 'yes'})

    def test_claim_refills_without_destroying_any_running_instance(self):
        self.start(2)
        with concurrent.futures.ThreadPoolExecutor(16) as workers:
            rows = list(workers.map(lambda _: self.backend.create({}), range(256)))
        ids = {r['sandboxID'] for r in rows}
        self.assertEqual(len(ids), 256)
        with self.pool.condition:
            self.assertTrue(self.pool.condition.wait_for(
                lambda: self.pool.status()['ready'] == 2, timeout=2))
        self.assertEqual(self.pool.status()['assigned'], 256)
        self.assertEqual(self.pool.status()['owned'], 258)
        self.assertEqual(self.pool.status()['reserves'], 2)
        for sid in ids:
            self.assertEqual(self.backend.committed[sid]['pool_state'], 'leased')
            self.backend.destroy(sid)
        self.assertEqual(self.pool.status()['ready'], 2)
        self.assertEqual(self.backend.created, 258)

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
        self.pool.close()  # settle concurrent refill before observing live fork
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

    def test_recovered_leases_do_not_consume_reserve_capacity(self):
        self.backend.records['existing'] = dict(pool_origin=True, pool_state='leased',
                                                status='running', network_state='ready')
        self.start(1)
        self.assertEqual(self.backend.created, 1)
        self.assertEqual(self.pool.status()['assigned'], 1)
        self.assertEqual(self.pool.status()['ready'], 1)


    def test_drain_never_destroys_public_leases(self):
        self.start(1)
        first = self.backend.create({})['sandboxID']
        self.pool.drain()
        with self.assertRaises(APIError):self.pool.lease({})
        self.assertIn(first, self.backend.records)
        self.assertEqual(self.pool.status()['ready'], 0)

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
        with self.pool.condition:
            sid = self.pool.lease({}, private=True)
            self.assertEqual(self.backend.committed[sid]['pool_state'], 'binding')
            self.assertEqual(self.pool.status()['ready'], 0)
        with self.assertRaises(APIError):
            self.backend.get_public(sid)
        self.pool.drain()
        self.assertEqual(self.backend.records, {})

    def test_private_binding_releases_reserve_but_remains_inaccessible(self):
        self.start(1)
        sid = self.pool.lease({}, private=True, binding={'template_id': 'base'})
        with self.pool.condition:
            self.assertTrue(self.pool.condition.wait_for(
                lambda: self.pool.status()['ready'] == 1, timeout=2))
        self.assertEqual(self.backend.committed[sid]['pool_state'], 'binding')
        self.assertEqual(self.pool.status()['binding'], 1)
        self.assertEqual(self.pool.status()['reserves'], 1)
        self.assertEqual(self.pool.status()['owned'], 2)
        with self.assertRaises(APIError):self.backend.get_public(sid)

    def test_device_preparation_overlaps_network_but_publication_waits(self):
        original = self.backend._create_live
        network = concurrent.futures.Future()
        device_done = threading.Event()
        final = []
        def create(*args, **kwargs):
            sid = original(*args, **kwargs)
            self.backend.records[sid]['network_state'] = 'pending'
            self.backend.jobs[sid] = network
            return sid
        self.backend._create_live = create
        self.backend.prepare_slot_device = lambda sid: device_done.set()
        self.backend.prepare_slot = lambda sid, **kw: final.append(kw)
        with concurrent.futures.ThreadPoolExecutor() as worker:
            starting = worker.submit(self.start, 1)
            try:
                self.assertTrue(device_done.wait(2))
                self.assertFalse(starting.done())
                self.assertEqual(final, [])
                self.assertEqual(self.backend.records['1']['pool_state'], 'preparing')
                self.backend.records['1']['network_state'] = 'ready'
            finally:
                network.set_result(None)
            starting.result(2)
        self.assertEqual(final, [{'device_prepared': True}])
        self.assertEqual(self.pool.status()['ready'], 1)

    def test_failed_device_preparation_never_publishes_slot(self):
        def fail(sid):
            raise RuntimeError('device failure')
        self.backend.prepare_slot_device = fail
        with self.assertRaisesRegex(RuntimeError, 'device failure'):
            self.start(1)
        self.assertEqual(self.backend.records, {})

    def test_refill_cannot_start_until_assignment_commit_finishes(self):
        self.start(1, workers=8)
        entered = threading.Event()
        release = threading.Event()
        original_save = self.backend.save
        original_create = self.backend._create_live
        observed = []
        def save():
            if self.backend.records.get('1', {}).get('pool_state') == 'leased' and not release.is_set():
                entered.set()
                if not release.wait(2):raise TimeoutError('test commit gate')
            original_save()
        def create(*args, **kwargs):
            observed.append(self.backend.committed['1']['pool_state'])
            return original_create(*args, **kwargs)
        self.backend.save = save
        self.backend._create_live = create
        with concurrent.futures.ThreadPoolExecutor() as workers:
            lease = workers.submit(self.backend.create, {})
            try:
                self.assertTrue(entered.wait(2))
                self.assertEqual(observed, [])
                self.assertEqual(self.backend.created, 1)
            finally:release.set()
            self.assertEqual(lease.result(2)['sandboxID'], '1')
        with self.pool.condition:
            self.assertTrue(self.pool.condition.wait_for(
                lambda: self.pool.status()['ready'] == 1, timeout=2))
        self.assertEqual(observed, ['leased'])
        self.assertIn('1', self.backend.records)
        self.assertEqual(self.pool.status()['refill_delay_ms'], 0)

    def test_template_identity_commits_before_private_lease_returns(self):
        self.start(1)
        sid=self.pool.lease({},private=True,binding={'template_id':'base','snapshot_template':'image-sealed'})
        self.assertEqual(self.backend.committed[sid]['snapshot_template'],'image-sealed')
        self.assertEqual(self.backend.committed[sid]['pool_state'],'binding')
        with self.assertRaises(APIError):self.backend.get_public(sid)
