#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Compare pause logs from successful PFN-verified forks, retaining failed attempts."""
import json
import math
from pathlib import Path
import re
import statistics
import sys


def distribution(values):
    ordered = sorted(values)
    return {
        'samples': len(values),
        'median_ms': statistics.median(ordered),
        'p95_ms': ordered[math.ceil(.95 * len(ordered)) - 1],
        'p99_ms': ordered[math.ceil(.99 * len(ordered)) - 1],
        'max_ms': ordered[-1],
        'mean_ms': statistics.mean(ordered),
    }


def parse(path):
    forks, unmatched = [], []
    compacted = False
    for line in Path(path).read_text().splitlines():
        if 'VM_FORK disk_compact ' in line and 'background=true' not in line:
            compacted = True
        if 'VM_FORK ram_shared_present_pages=' in line:
            forks.append({'branch_pause_ms': [], 'foreground_compaction': compacted})
            compacted = False
        match = re.search(r'VM_FORK pause_ms=([\d.]+)', line)
        if match:
            value = float(match[1])
            if forks and len(forks[-1]['branch_pause_ms']) < 2:
                forks[-1]['branch_pause_ms'].append(value)
            else:
                unmatched.append(value)
                compacted = False
    assert forks and all(len(f['branch_pause_ms']) == 2 for f in forks), forks
    values = [max(f['branch_pause_ms']) for f in forks]
    return dict(forks=forks, per_fork_max_branch_ms=values,
                summary=distribution(values), unpaired_pause_ms=unmatched)


if __name__ == '__main__':
    before, after = map(parse, sys.argv[1:3])
    result = {'metric': 'For each successful fork: max(parent pause, child pause), pause through explicit API resume',
              'before': before, 'after': after}
    if len(before['forks']) == len(after['forks']):
        indexes = [i for i, f in enumerate(before['forks']) if f['foreground_compaction']]
        if indexes:
            result['matched_foreground_compaction_generations'] = {
                'zero_based_fork_indexes': indexes,
                'before': distribution([before['per_fork_max_branch_ms'][i] for i in indexes]),
                'after': distribution([after['per_fork_max_branch_ms'][i] for i in indexes]),
            }
    print(json.dumps(result, indent=2))
