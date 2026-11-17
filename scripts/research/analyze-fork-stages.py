#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Summarize monotonic CH fork milestones without adding parallel worker times."""
import collections
import json
import math
from pathlib import Path
import re
import statistics
import sys


def stats(values):
    values = sorted(values)
    if not values:
        return {'samples': 0}
    return dict(samples=len(values), median_ms=statistics.median(values),
                p95_ms=values[math.ceil(.95*len(values))-1],
                p99_ms=values[math.ceil(.99*len(values))-1], max_ms=values[-1])


def analyze(path):
    operations = collections.defaultdict(lambda: collections.defaultdict(dict))
    log = Path(path).read_text()
    for op, pid, phase, elapsed in re.findall(
            r'VM_FORK stage op=(\S+) pid=(\d+) phase=(\S+) elapsed_ms=([\d.]+)', log):
        operations[op][pid][phase] = float(elapsed)
    block_pauses = collections.defaultdict(lambda: collections.defaultdict(dict))
    block_accounting = collections.defaultdict(lambda: collections.defaultdict(dict))
    pausing = {}
    for line in log.splitlines():
        stage = re.search(r'VM_FORK stage op=(\S+) pid=(\d+) phase=(\S+)', line)
        if stage:
            op, pid, phase = stage.groups()
            if phase == 'pause_begin':
                pausing[pid] = op
            elif phase == 'devices_paused':
                pausing.pop(pid, None)
        block = re.search(r'VM_FORK block_pause pid=(\d+) id=(\S+) phase=(\S+) elapsed_ms=([\d.]+)', line)
        if block:
            pid, device, phase, elapsed = block.groups()
            if pid in pausing:
                block_pauses[pausing[pid]][device][phase] = float(elapsed)
                counters = re.search(r'run_ns=(\d+) wait_ns=(\d+)', line)
                if counters:
                    block_accounting[pausing[pid]][device][phase] = dict(
                        wall=float(elapsed), run=int(counters[1])/1e6, wait=int(counters[2])/1e6)
    values = collections.defaultdict(list)
    intervals = {
        'vcpu_pause': ('pause_begin', 'vcpus_paused'),
        'cpu_signal_dispatch': ('pause_begin', 'cpu_signals_sent'),
        'cpu_signal_acknowledgement': ('cpu_signals_sent', 'cpu_signal_ack'),
        'cpu_clock_notify': ('cpu_signal_ack', 'cpu_clock_notified'),
        'cpu_pause_final_ack': ('cpu_clock_notified', 'cpu_pause_ack'),
        'guest_clock_capture': ('vcpus_paused', 'guest_clock_captured'),
        'device_manager_pause': ('guest_clock_captured', 'devices_paused'),
        'device_pause': ('vcpus_paused', 'devices_paused'),
        'snapshot_capture': ('devices_paused', 'snapshot_done'),
        'cpu_state_capture': ('devices_paused', 'cpu_state_captured'),
        'memory_state_capture': ('cpu_state_captured', 'memory_state_captured'),
        'device_state_capture': ('memory_state_captured', 'device_state_captured'),
        'vm_teardown_total': ('snapshot_done', 'vm_dropped'),
        'vcpu_thread_join': ('snapshot_done', 'vcpu_threads_joined'),
        'mmio_region_release': ('device_manager_drop_begin', 'mmio_regions_cleared'),
        'vmgenid_slot_release': ('mmio_regions_cleared', 'vmgenid_unmapped'),
        'device_worker_join': ('vmgenid_unmapped', 'device_threads_joined'),
        'device_object_drop': ('device_threads_joined', 'device_manager_dropped'),
        'cpu_object_drop': ('device_manager_dropped', 'cpu_manager_dropped'),
        'memory_manager_drop': ('cpu_manager_dropped', 'memory_manager_dropped'),
        'vm_kvm_reference_drop': ('memory_manager_dropped', 'kvm_vm_dropped'),
        'retired_vm_drop_parallel': ('retired_vm_drop_begin', 'retired_vm_drop_done'),
        'retired_kvm_close_parallel': ('retired_vm_drop_begin', 'retired_kvm_closed'),
        'retirement_wait_after_pss': ('pss_before_done', 'retirement_joined'),
        'remaining_object_drop': ('device_threads_joined', 'vm_dropped'),
        'api_and_vmm_thread_shutdown': ('vm_dropped', 'api_stopped'),
        'disk_branch': ('api_stopped', 'disk_branched'),
        'pss_before': ('disk_branched', 'pss_before_done'),
        'host_fork': ('host_fork_begin', 'host_fork_done'),
        'pss_after_barrier': ('host_fork_done', 'pss_after_done'),
        'full_pfn_verification': ('pss_after_done', 'verification_done'),
        'ready_to_resume_request': ('both_ready', 'resume_begin'),
    }
    restore = {
        'fdtable_expand': ('fdtable_begin', 'fdtable_ready'),
        'api_rebuild_parallel': ('runtime_start', 'api_bound'),
        'kvm_and_memory_register': ('vmm_initialized', 'memory_registered'),
        'vcpu_state_restore': ('memory_registered', 'cpu_state_restored'),
        'device_state_restore': ('cpu_state_restored', 'device_state_restored'),
        'disk_open': ('disk_open_begin', 'disk_open_done'),
        'block_create': ('block_create_begin', 'block_create_done'),
        'net_create': ('net_create_begin', 'net_create_done'),
        'vsock_create': ('vsock_create_begin', 'vsock_create_done'),
        'vcpu_thread_rebuild': ('vm_constructed', 'vcpu_threads_ready'),
        'runtime_rebuild_total': ('runtime_start', 'runtime_ready'),
    }
    accounting = collections.defaultdict(dict)
    for op, pid, phase, elapsed, cpu, nv, ni, tid, run, wait in re.findall(
            r'VM_FORK stage op=(\S+) pid=(\d+) phase=(\S+) elapsed_ms=([\d.]+) cpu_ms=([\d.]+) voluntary=(\d+) involuntary=(\d+) tid=(\d+) run_ns=(\d+) wait_ns=(\d+)', log):
        accounting[(op, pid)][phase] = dict(wall=float(elapsed), cpu=float(cpu),
            voluntary=int(nv), involuntary=int(ni), tid=tid,
            run_ms=int(run)/1e6, wait_ms=int(wait)/1e6)
    accounted = collections.defaultdict(list)
    for (op, pid), phases in accounting.items():
        role = 'parent' if pid == op.split('-')[0] else 'child'
        for label, (start, end) in {**intervals, **restore}.items():
            if start not in phases or end not in phases:
                continue
            a, b = phases[start], phases[end]
            # Cross-thread milestones cannot use thread-local CPU counters.
            if a['tid'] != b['tid']:
                continue
            prefix = role+'.'+label
            for field in ['cpu', 'wait_ms']:
                delta = b[field]-a[field]
                if delta >= 0: accounted[prefix+'.'+field].append(delta)
            # Residual includes blocking and accounting/measurement error;
            # it is not proof of disk IO without a matching syscall record.
            accounted[prefix+'.blocked_or_unaccounted_ms'].append(max(0,
                b['wall']-a['wall']-(b['run_ms']-a['run_ms'])-(b['wait_ms']-a['wait_ms'])))
    for devices in block_accounting.values():
        for device, phases in devices.items():
            for label, start, end in [('request_drain', 'begin', 'drained'),
                                      ('metadata_sync', 'drained', 'metadata_synced'),
                                      ('worker_park', 'metadata_synced', 'workers_parked')]:
                if start not in phases or end not in phases:
                    continue
                a, b = phases[start], phases[end]
                prefix = 'block.'+device+'.'+label
                run, wait = b['run']-a['run'], b['wait']-a['wait']
                accounted[prefix+'.cpu'].append(run)
                accounted[prefix+'.wait_ms'].append(wait)
                accounted[prefix+'.blocked_or_unaccounted_ms'].append(max(0, b['wall']-a['wall']-run-wait))
    successful = 0
    for op, branches in operations.items():
        parent = branches.get(op.split('-')[0], {})
        children = [b for pid, b in branches.items() if pid != op.split('-')[0]]
        if len(children) != 1 or 'resume_done' not in parent or 'resume_done' not in children[0]:
            continue
        successful += 1
        child = children[0]
        for device, phases in block_pauses[op].items():
            if all(k in phases for k in ['drained', 'metadata_synced', 'workers_parked']):
                values['block.'+device+'.request_drain'].append(phases['drained'])
                values['block.'+device+'.metadata_sync'].append(phases['metadata_synced']-phases['drained'])
                values['block.'+device+'.worker_park'].append(phases['workers_parked']-phases['metadata_synced'])
        values['total_max_branch_pause'].append(max(parent['resume_done'], child['resume_done']))
        for label, (a, b) in intervals.items():
            if a in parent and b in parent:
                values[label].append(parent[b]-parent[a])
        for role, branch in [('parent', parent), ('child', child)]:
            for label, (a, b) in restore.items():
                if a in branch and b in branch:
                    values[role+'.'+label].append(branch[b]-branch[a])
        if all('runtime_ready' in b for b in [parent, child]) and 'verification_done' in parent:
            ready = max(parent['runtime_ready'], child['runtime_ready'])
            values['rebuild_critical_path'].append(ready-parent['verification_done'])
            values['post_ready_to_both_resumed'].append(max(parent['resume_done'], child['resume_done'])-ready)
    host_phases = collections.defaultdict(list)
    clean_log = re.sub(r'\x1b\[[0-9;]*m', '', log)
    for line in clean_log.splitlines():
        phase = re.search(r'fork timing: (async CH request|metadata before resume|both resume requests)', line)
        elapsed = re.search(r'milliseconds=([\d.e+-]+)', line)
        if phase and elapsed:
            label = phase[1]
            endpoint = re.search(r'endpoint="([^"]+)"', line)
            if endpoint:
                label += '.' + endpoint[1]
            host_phases[label].append(float(elapsed[1]))
    return dict(successful_forks_with_complete_endpoints=successful,
                host_phases={k: stats(v) for k, v in host_phases.items()},
                accounted_stages={k: stats(v) for k, v in accounted.items()},
                stages={k: stats(v) for k, v in values.items()},
                fdtable_samples=[dict(pid=int(p), before=int(b), after=int(a)) for p, b, a in re.findall(r'VM_FORK fdtable pid=(\d+) before=(\d+) after=(\d+)', log)],
                block_pauses=block_pauses,
                operations=operations)


if __name__ == '__main__':
    print(json.dumps({path: analyze(path) for path in sys.argv[1:]}, indent=2))
