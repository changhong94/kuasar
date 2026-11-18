# SPDX-License-Identifier: Apache-2.0
"""Image-independent metadata preparation. Never execute or preload application files."""
import json
from pathlib import Path
import time
import vm_rpc

def prepare_metadata(socket, root):
    """Walk metadata uniformly; never read regular-file contents or execute applications."""
    started = time.perf_counter_ns()
    result = vm_rpc.execute(Path(socket), ['/usr/bin/image-metadata', root, '0'])
    if result['exit_code']:
        raise RuntimeError('image metadata preparation failed')
    report = json.loads(result['stdout'])
    report['rpc_elapsed_ms'] = (time.perf_counter_ns()-started)/1e6
    return report
