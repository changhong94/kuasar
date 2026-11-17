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


def validate_startup_files(files):
    """Operator-provided, bounded read hints; never executable commands."""
    from pathlib import PurePosixPath
    if not isinstance(files, list) or len(files) > 32:
        raise ValueError('startup_read_files must be a list of at most 32 paths')
    for path in files:
        if (not isinstance(path, str) or not path.startswith('/')
                or '..' in PurePosixPath(path).parts
                or any(c in path for c in ('\0', '\n', '\r'))):
            raise ValueError('startup read hints require absolute image paths')
    return files


def prepare_image(socket, root, files):
    """Read selected image files before capture; do not execute their contents.

    The chroot bounds image symlinks. The mounted trusted busybox helper does
    all reads, including stat/head, regardless of executable files in the image.
    No bytecode is generated and no application interpreter is started.
    """
    files = validate_startup_files(files)
    if not files:
        return {'elapsed_ms': 0, 'files': [], 'bytes': 0, 'application_executed': False}
    started = time.perf_counter_ns()
    script = r'''set -e
remaining=134217728
for f do
    [ -f "$f" ] || continue
    size=$(/.__kuasar/busybox stat -Lc %s "$f")
    [ "$size" -le 67108864 ] || { echo 'startup file exceeds 64 MiB' >&2; exit 1; }
    [ "$size" -le "$remaining" ] || { echo 'startup reads exceed 128 MiB' >&2; exit 1; }
    /.__kuasar/busybox head -c "$size" "$f" >/dev/null
    remaining=$((remaining-size))
    printf '%s\t%s\n' "$size" "$f"
done
'''
    result = vm_rpc.execute(Path(socket), ['/bin/busybox', 'chroot', root,
                            '/.__kuasar/busybox', 'sh', '-c', script,
                            'read-startup-files', *files])
    if result['exit_code']:
        raise RuntimeError('read-only image cache preparation failed')
    completed = []
    for line in result['stdout'].decode().splitlines():
        size, path = line.split('\t', 1)
        completed.append({'path': path, 'bytes': int(size)})
    return {'elapsed_ms': (time.perf_counter_ns()-started)/1e6,
            'files': completed, 'bytes': sum(x['bytes'] for x in completed),
            'application_executed': False}


def prepare_metadata(socket, root, read_limit=0):
    """Uniform metadata preparation with optional bounded regular-file caching; no application execution."""
    started = time.perf_counter_ns()
    result = vm_rpc.execute(Path(socket), ['/usr/bin/image-metadata', root, str(read_limit)])
    if result['exit_code']:
        raise RuntimeError('image metadata preparation failed')
    report = json.loads(result['stdout'])
    report['rpc_elapsed_ms'] = (time.perf_counter_ns()-started)/1e6
    return report
