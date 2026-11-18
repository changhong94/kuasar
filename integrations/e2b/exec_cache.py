# SPDX-License-Identifier: Apache-2.0
"""Uniform, bounded image-tree preparation. Never execute applications."""
import json
from pathlib import Path
import time
import vm_rpc

MAX_READ_BYTES = 512 * 1024 * 1024


def prepare_metadata(socket, root, read_limit=0):
    """Use one policy for every image; zero disables content reads."""
    if not isinstance(read_limit, int) or not 0 <= read_limit <= MAX_READ_BYTES:
        raise ValueError("Uniform image read cache must be between 0 and 512 MiB")
    started = time.perf_counter_ns()
    result = vm_rpc.execute(Path(socket), ['/usr/bin/image-metadata', root, str(read_limit)])
    if result['exit_code']:
        raise RuntimeError('image metadata preparation failed')
    report = json.loads(result['stdout'])
    report['rpc_elapsed_ms'] = (time.perf_counter_ns()-started)/1e6
    return report
