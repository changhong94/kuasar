# SPDX-License-Identifier: Apache-2.0
"""Durably owned, single-use CoW descendants of an immutable private template.

Capacity targets unclaimed reserves, not the number of running sandboxes.
A durably assigned slot leaves the reserve budget immediately. Only template
creates lease from this pool; an explicit live-parent fork never does.
"""
from concurrent.futures import ThreadPoolExecutor
import datetime
from collections import deque
import json
import secrets
import threading
import time
from pathlib import Path
from server import APIError, trace_timing

PRIVATE = ('preparing', 'ready', 'binding', 'quarantined', 'network_idle')


class TemplatePool:
    def __init__(self, backend, capacity, workers=1):
        if not 1 <= capacity <= 128:
            raise ValueError('template pool capacity must be 1..128')
        if not 1 <= workers <= 64:
            raise ValueError('template pool workers must be 1..64')
        self.worker_count = workers
        self.pending = 0
        self.backend = backend
        self.capacity = capacity
        self.condition = threading.Condition(backend.lock)
        self.ready_condition = threading.Condition(backend.lock)
        self.enabled = True
        self.stopped = False
        self.inflight = 0
        self.epoch = secrets.token_hex(16)
        self.leases = 0
        self.misses = 0
        self.last_error = ''
        self.vacancies = deque()
        self.events = Path(str(backend.state)+'.pool-events.jsonl')
        started = time.monotonic()
        # Initial reserves obey the same bounded concurrency as replenishment.
        # _prepare reserves durably owned capacity under the assignment lock.
        # Its conservative pending/record overlap can skip a submitted task,
        # so settle each wave and fill any remaining vacancy before publishing
        # startup completion. No background refill worker exists yet.
        with ThreadPoolExecutor(max_workers=workers,
                                thread_name_prefix='template-pool-startup') as startup:
            while self._count() < capacity:
                tasks = [startup.submit(self._prepare, 'startup')
                         for _ in range(capacity-self._count())]
                for task in tasks:
                    task.result()
        self.preparation_ms = (time.monotonic()-started)*1000
        self._event('startup_complete', elapsed_ms=self.preparation_ms, capacity=capacity)
        self.workers = [threading.Thread(target=self._run, name=f'template-pool-refill-{i}',
                                         daemon=True) for i in range(workers)]
        for worker in self.workers:
            worker.start()

    def _event(self, event, **fields):
        try:
            with self.events.open('a') as f:
                f.write(json.dumps(dict(event=event, epoch=self.epoch,
                                       monotonic_ns=time.monotonic_ns(), **fields))+'\n')
        except OSError as error:
            print('template pool telemetry:', error, flush=True)

    def _count(self):
        # Binding belongs to a request after its durable assignment, even before
        # public credentials are returned. Quarantined failures remain owned
        # and unavailable until cleanup; they can never be leased again.
        return sum(bool(r.get('pool_origin')) and r.get('pool_state') not in
                   ('binding', 'leased', 'network_idle')
                   for r in self.backend.records.values())

    def _prepare(self, reason):
        started = time.monotonic()
        sid = None
        pending = True
        vacancy = None
        phases = {}
        with self.condition:
            if not self.enabled or self.stopped or self._count()+self.pending >= self.capacity:
                return
            # Reserve under the assignment lock before creating a durable record.
            # Pending may briefly double-count a record inside _create_live;
            # this is conservative and cannot exceed the reserve capacity.
            self.pending += 1
            self.inflight += 1
            vacancy = self.vacancies.popleft() if reason == 'refill' and self.vacancies else None
            self._event('prepare_started', reason=reason,
                        vacancy_sid=vacancy[0] if vacancy else None,
                        vacancy_reason='lease' if vacancy else None, inflight=self.inflight)
        try:
            phase = time.monotonic()
            sid = self.backend._create_live({'timeout': 86400}, _pool_owned=True)
            phases['create_live_ms'] = (time.monotonic()-phase)*1000
            with self.condition:
                self.pending -= 1
                pending = False
                self.condition.notify_all()
            phase = time.monotonic()
            job = self.backend.jobs[sid]
            # Device discovery is image-independent and may overlap CNI ADD.
            # Snapshot preparation freezes the guest, so it must still wait for
            # network completion; neither failure can publish a Ready slot.
            device = getattr(self.backend, "prepare_slot_device", None)
            if device:
                device(sid)
            phases['device_ms'] = (time.monotonic()-phase)*1000
            phase = time.monotonic()
            job.result(timeout=50)
            phases['network_wait_ms'] = (time.monotonic()-phase)*1000
            phase = time.monotonic()
            if self.backend.get(sid).get('network_state') != 'ready':
                raise RuntimeError('prepared slot network failed')
            prepare = getattr(self.backend, "prepare_slot", None)
            if prepare:
                if device:
                    prepare(sid, device_prepared=True)
                else:
                    prepare(sid)
            phases['stage_ms'] = (time.monotonic()-phase)*1000
            phase = time.monotonic()
            with self.condition:
                phases['publish_lock_wait_ms'] = (time.monotonic()-phase)*1000
                record = self.backend.get(sid)
                if record.get('status') != 'running' or record.get('network_state') != 'ready':
                    raise RuntimeError('prepared slot did not reach guest/network readiness')
                if self.enabled and not self.stopped:
                    record.update(pool_state='ready', pool_epoch=self.epoch,
                                  pool_ready_ns=time.monotonic_ns())
                    try:
                        phase = time.monotonic()
                        self.backend.save_record(sid)
                        phases['publish_commit_ms'] = (time.monotonic()-phase)*1000
                    except Exception:
                        record.update(pool_state='quarantined', status='deleting')
                        raise
                    self._event('prepared', sid=sid, reason=reason,
                                elapsed_ms=(time.monotonic()-started)*1000, phases=phases,
                                vacancy_sid=vacancy[0] if vacancy else None,
                                vacancy_reason='lease' if vacancy else None,
                                replacement_ms=(time.monotonic()-vacancy[1])*1000 if vacancy else None)
                    self.ready_condition.notify(1)
                    self.condition.notify_all()
                    return
            self.backend.destroy(sid)
        except Exception as error:
            if sid is not None:
                self.backend.destroy(sid)
            with self.condition:
                if vacancy:
                    self.vacancies.appendleft(vacancy)
            raise error
        finally:
            with self.condition:
                self.inflight -= 1
                if pending:
                    self.pending -= 1
                self.condition.notify_all()

    def ready_changed(self):
        """Called under backend.lock when external template staging returns a slot."""
        self.ready_condition.notify_all()
        self.condition.notify_all()

    def capacity_changed(self, sid=None, assigned=True):
        with self.condition:
            self._event('retired', sid=sid, assigned=assigned)
            # A lease already opened its vacancy. Deleting that instance must
            # not schedule a second replacement. Still wake for failed/private
            # reserve cleanup, which may unblock preparation.
            self.condition.notify_all()

    def _run(self):
        while True:
            with self.condition:
                if self.stopped:
                    return
                if not self.enabled or self._count()+self.pending >= self.capacity:
                    self.condition.wait()
                    continue
            try:
                self._prepare('refill')
                self.last_error = ''
            except Exception as error:
                self.last_error = str(error)
                self._event('refill_error', error=str(error))
                with self.condition:
                    # Failed preparation retries back off to avoid a busy loop;
                    # healthy capacity notifications have no artificial delay.
                    self.condition.wait(1)

    def lease(self, body, *, private=False, binding=None):
        if binding and (not private or set(binding)-{'template_id', 'image_disk', 'image_sha256', 'image_manifest', 'snapshot_template'}):
            raise ValueError('Image binding must be a private assignment')
        started = time.monotonic()
        deadline = started+45
        lease_start_ns = time.perf_counter_ns()
        with self.condition:
            trace_timing('pool_lock_wait', lease_start_ns)
            counted_miss = False
            while True:
                if not self.enabled or self.stopped:
                    raise APIError(503, 'Template pool is unavailable')
                ready = [(r.get('pool_ready_ns', 0), sid) for sid, r in self.backend.records.items()
                         if r.get('pool_state') == 'ready' and r.get('status') == 'running'
                         and r.get('network_state') == 'ready']
                if ready:
                    _, sid = min(ready)
                    record = self.backend.get(sid)
                    timeout = int(body.get('timeout', 300))
                    record.update(pool_state='binding' if private else 'leased', token=secrets.token_urlsafe(32),
                                  startedAt=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                  expiry=time.time()+timeout, metadata=body.get('metadata', {}))
                    if binding:
                        record.update(binding)
                    save_start_ns = time.perf_counter_ns()
                    try:
                        # Commit assignment before returning credentials. A failed
                        # acknowledgment must never return the slot to Ready.
                        self.backend.save_record(sid)
                    except Exception:
                        record.update(pool_state='quarantined', status='deleting')
                        try:
                            self.backend.save_record(sid)
                        except Exception:
                            pass  # retained in memory; startup reconciles the durable state
                        self.condition.notify_all()
                        raise
                    trace_timing('pool_assignment_save', save_start_ns, sid=sid)
                    self.leases += 1
                    # This lock prevents the refill worker observing the state
                    # transition before its durable assignment commit succeeds.
                    self.vacancies.append((sid, time.monotonic()))
                    self._event('vacated', sid=sid, reason='lease')
                    # New reserves get independent identities/resources; the
                    # claimed VM and its network are never recycled here.
                    self.condition.notify_all()
                    response = sid if private else self.backend.response(sid)
                    self._event('leased', sid=sid, elapsed_ms=(time.monotonic()-started)*1000,
                                waited=counted_miss)
                    trace_timing('pool_lease', lease_start_ns, sid=sid)
                    return response
                if not counted_miss:
                    self.misses += 1
                    counted_miss = True
                remaining = deadline-time.monotonic()
                if remaining <= 0:
                    raise APIError(503, 'No prepared template slot available')
                self.ready_condition.wait(remaining)

    def status(self):
        with self.condition:
            records = [r for r in self.backend.records.values() if r.get('pool_origin')]
            return dict(enabled=self.enabled, capacity=self.capacity,
                        ready=sum(r.get('pool_state') == 'ready' for r in records),
                        assigned=sum(r.get('pool_state') == 'leased' for r in records),
                        owned=len(records), reserves=self._count(),
                        binding=sum(r.get('pool_state') == 'binding' for r in records),
                        capacity_scope='unclaimed_reserves',
                        preparing=self.inflight, pending=self.pending, workers=self.worker_count, leases=self.leases,
                        misses=self.misses, last_error=self.last_error,
                        preparation_ms=self.preparation_ms, refill_delay_ms=0)

    def drain(self):
        with self.condition:
            self.enabled = False
            self.ready_condition.notify_all()
            self.condition.notify_all()
            deadline = time.monotonic()+180
            while self.inflight:
                remaining = deadline-time.monotonic()
                if remaining <= 0:
                    raise APIError(503, 'Pool preparation has not settled')
                self.condition.wait(remaining)
            ids = [sid for sid, r in self.backend.records.items()
                   if r.get('pool_state') in PRIVATE]
        for sid in ids:
            self.backend.destroy(sid)
        return self.status()

    def close(self):
        with self.condition:
            self.stopped = True
            self.enabled = False
            self.ready_condition.notify_all()
            self.condition.notify_all()
        for worker in self.workers:
            worker.join()
