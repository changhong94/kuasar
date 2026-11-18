# SPDX-License-Identifier: Apache-2.0
"""Durably owned, single-use CoW descendants of an immutable private template.

Capacity includes ready, assigned, and retiring pool members. Only template
creates lease from this pool; an explicit live-parent fork never does.
"""
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
    def __init__(self, backend, capacity, refill_delay=1.0):
        if not 1 <= capacity <= 128 or refill_delay < 0:
            raise ValueError('template pool capacity must be 1..128 and refill delay nonnegative')
        self.backend = backend
        self.capacity = capacity
        self.refill_delay = refill_delay
        self.condition = threading.Condition(backend.lock)
        self.enabled = True
        self.stopped = False
        self.inflight = False
        self.due = None
        self.epoch = secrets.token_hex(16)
        self.leases = 0
        self.misses = 0
        self.last_error = ''
        self.vacancies = deque()
        self.events = Path(str(backend.state)+'.pool-events.jsonl')
        started = time.monotonic()
        for _ in range(max(0, capacity-self._count())):
            self._prepare('startup')
        self.preparation_ms = (time.monotonic()-started)*1000
        self._event('startup_complete', elapsed_ms=self.preparation_ms, capacity=capacity)
        self.worker = threading.Thread(target=self._run, name='template-pool-refill', daemon=True)
        self.worker.start()

    def _event(self, event, **fields):
        try:
            with self.events.open('a') as f:
                f.write(json.dumps(dict(event=event, epoch=self.epoch,
                                       monotonic_ns=time.monotonic_ns(), **fields))+'\n')
        except OSError as error:
            print('template pool telemetry:', error, flush=True)

    def _count(self):
        return sum(bool(r.get('pool_origin')) for r in self.backend.records.values())

    def _prepare(self, reason):
        started = time.monotonic()
        sid = None
        with self.condition:
            if not self.enabled or self.stopped:
                return
            self.inflight = True
        try:
            sid = self.backend._create_live({'timeout': 86400}, _pool_owned=True)
            job = self.backend.jobs[sid]
            # Device discovery is image-independent and may overlap CNI ADD.
            # Snapshot preparation freezes the guest, so it must still wait for
            # network completion; neither failure can publish a Ready slot.
            device = getattr(self.backend, "prepare_slot_device", None)
            if device:
                device(sid)
            job.result(timeout=50)
            if self.backend.get(sid).get('network_state') != 'ready':
                raise RuntimeError('prepared slot network failed')
            prepare = getattr(self.backend, "prepare_slot", None)
            if prepare:
                if device:
                    prepare(sid, device_prepared=True)
                else:
                    prepare(sid)
            with self.condition:
                record = self.backend.get(sid)
                if record.get('status') != 'running' or record.get('network_state') != 'ready':
                    raise RuntimeError('prepared slot did not reach guest/network readiness')
                if self.enabled and not self.stopped:
                    record.update(pool_state='ready', pool_epoch=self.epoch,
                                  pool_ready_ns=time.monotonic_ns())
                    try:
                        self.backend.save()
                    except Exception:
                        record.update(pool_state='quarantined', status='deleting')
                        raise
                    vacancy = self.vacancies.popleft() if reason == 'refill' and self.vacancies else None
                    self._event('prepared', sid=sid, reason=reason,
                                elapsed_ms=(time.monotonic()-started)*1000,
                                retired_sid=vacancy[0] if vacancy else None,
                                replacement_ms=(time.monotonic()-vacancy[1])*1000 if vacancy else None)
                    self.condition.notify_all()
                    return
            self.backend.destroy(sid)
        except Exception as error:
            if sid is not None:
                self.backend.destroy(sid)
            raise error
        finally:
            with self.condition:
                self.inflight = False
                self.condition.notify_all()

    def capacity_changed(self, sid=None, assigned=True):
        with self.condition:
            if assigned:
                self.vacancies.append((sid, time.monotonic()))
                self._event('retired', sid=sid)
            if self.due is None:
                self.due = time.monotonic()+self.refill_delay
            self.condition.notify_all()

    def _run(self):
        while True:
            with self.condition:
                if self.stopped:
                    return
                if not self.enabled or self._count() >= self.capacity:
                    self.condition.wait()
                    continue
                delay = max(0, (self.due or 0)-time.monotonic())
                if delay:
                    self.condition.wait(delay)
                    continue
                self.due = None
            try:
                self._prepare('refill')
                self.last_error = ''
            except Exception as error:
                self.last_error = str(error)
                self._event('refill_error', error=str(error))
                with self.condition:
                    self.condition.wait(1)

    def lease(self, body, *, private=False, binding=None):
        if binding and (not private or set(binding)-{'template_id', 'image_disk', 'image_sha256', 'image_manifest'}):
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
                    # A successful lease consumes Ready capacity. Start the
                    # configured refill delay now, while the capacity guard in
                    # _run still counts this lease until retirement completes.
                    # Do not add another full delay after synchronous deletion.
                    if self.due is None:
                        self.due = time.monotonic()+self.refill_delay
                        self._event('refill_scheduled', sid=sid, trigger='lease',
                                    delay_ms=self.refill_delay*1000)
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
                self.condition.wait(remaining)

    def status(self):
        with self.condition:
            records = [r for r in self.backend.records.values() if r.get('pool_origin')]
            return dict(enabled=self.enabled, capacity=self.capacity,
                        ready=sum(r.get('pool_state') == 'ready' for r in records),
                        assigned=sum(r.get('pool_state') == 'leased' for r in records),
                        owned=len(records), preparing=self.inflight, leases=self.leases,
                        misses=self.misses, last_error=self.last_error,
                        preparation_ms=self.preparation_ms, refill_delay_ms=self.refill_delay*1000)

    def drain(self):
        with self.condition:
            self.enabled = False
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
            self.condition.notify_all()
        self.worker.join()
