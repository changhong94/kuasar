# SPDX-License-Identifier: Apache-2.0
"""Bounded, read-only seed page-cache preparation for the standard exec path.

Does not execute a user command, source profiles, or preload guest RAM wholesale.
The login shell still executes normally for each SDK request. Run only as an
explicit template preparation step before accepting requests.
"""
import json
from pathlib import Path
import time
import vm_rpc

# Standard shell launch dependencies in the matched Debian Python template.
# Missing optional files are harmless. Each read is bounded to 4 MiB.
FILES = (
    '/bin/sh', '/bin/bash', '/usr/bin/id', '/usr/bin/run-parts',
    '/lib64/ld-linux-x86-64.so.2',
    '/lib/x86_64-linux-gnu/libc.so.6',
    '/lib/x86_64-linux-gnu/libtinfo.so.6',
    '/lib/x86_64-linux-gnu/libselinux.so.1',
    '/lib/x86_64-linux-gnu/libpcre2-8.so.0',
    '/etc/ld.so.cache', '/etc/profile', '/etc/passwd', '/etc/group',
    '/etc/nsswitch.conf', '/root/.profile', '/root/.bashrc',
)


def prepare(socket, report_path):
    started = time.perf_counter_ns()
    script = ('for f do if [ -f "$f" ]; then '
              '/bin/busybox head -c 4194304 "$f" >/dev/null || exit; '
              'printf "%s\\n" "$f"; fi; done')
    result = vm_rpc.execute(Path(socket), ['/bin/busybox', 'sh', '-c', script,
                                           'prepare-exec-cache', *FILES])
    if result['exit_code']:
        raise RuntimeError('seed exec-cache preparation failed')
    report = {'kind': 'bounded-read-only-exec-cache',
              'elapsed_ms': (time.perf_counter_ns()-started)/1e6,
              'files': result['stdout'].decode().splitlines(),
              'per_file_limit_bytes': 4194304,
              'user_command_executed': False}
    Path(report_path).write_text(json.dumps(report, indent=2))
    return report
