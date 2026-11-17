# SPDX-License-Identifier: Apache-2.0
"""A single CoW runtime pool, binding a content-pinned application disk on lease."""
import base64
import hashlib
import http.client
import json
import os
from pathlib import Path
import socket
import time
from server import APIError, trace_timing
from vm_backend import VMBackend
import vm_rpc
from oci_config import normalize, launch_argv


class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost', timeout=30)
        self.path = str(path)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def add_image_disk(api_socket, path):
    client = UnixHTTP(api_socket)
    try:
        body = json.dumps(dict(path=str(path), readonly=True, image_type='Raw',
                               id='application-root', serial='app-root'))
        client.request('PUT', '/api/v1/vm.add-disk', body,
                       {'Content-Type': 'application/json'})
        response = client.getresponse()
        data = response.read(65537)
        if not 200 <= response.status < 300 or len(data) > 65536:
            raise RuntimeError('Image disk attachment failed: '+data[:1024].decode(errors='replace'))
        return json.loads(data)
    finally:
        client.close()


MOUNT_IMAGE = r'''set -e
n=0
while [ ! -b /dev/vdb ]; do
 n=$((n+1)); [ "$n" -lt 500 ]; /bin/busybox sleep 0.01
done
[ "$(/bin/busybox cat /sys/block/vdb/serial)" = app-root ]
/bin/busybox mkdir -p /run/image-lower /run/image-upper /run/image-work /run/image-root
/bin/busybox mount -t ext4 -o ro /dev/vdb /run/image-lower
/bin/busybox mount -t overlay overlay -o lowerdir=/run/image-lower,upperdir=/run/image-upper,workdir=/run/image-work /run/image-root
if [ ! -e /run/image-root/bin/busybox ]; then
 /bin/busybox touch /run/image-root/bin/busybox
 helper=$(/bin/busybox readlink -f /run/image-root/bin/busybox)
 /bin/busybox mount --bind /bin/busybox "$helper"
 /bin/busybox mount -o remount,bind,ro /bin/busybox "$helper"
fi
/bin/busybox mkdir -p /run/image-root/.__kuasar /run/image-root/dev /run/image-root/proc /run/image-root/sys /run/image-root/run
/bin/busybox touch /run/image-root/.__kuasar/busybox
/bin/busybox mount --bind /bin/busybox /run/image-root/.__kuasar/busybox
/bin/busybox mount -o remount,bind,ro /run/image-root/.__kuasar/busybox
/bin/busybox mount --bind /dev /run/image-root/dev
/bin/busybox mount --bind /proc /run/image-root/proc
/bin/busybox mount --bind /sys /run/image-root/sys
[ -x /run/image-root/bin/sh ]
'''


MOUNT_OCI_IMAGE = MOUNT_IMAGE[:MOUNT_IMAGE.index('if [ ! -e')]+r'''[ ! -L /run/image-root/.__kuasar ]
/bin/busybox mkdir -p /run/image-root/.__kuasar /run/image-root/dev /run/image-root/proc /run/image-root/sys
for d in dev proc sys; do
 [ ! -L "/run/image-root/$d" ]
 /bin/busybox mount --bind "/$d" "/run/image-root/$d"
done
[ ! -e /run/image-root/.__kuasar/busybox ]
/bin/busybox touch /run/image-root/.__kuasar/busybox
/bin/busybox mount --bind /bin/busybox /run/image-root/.__kuasar/busybox
/bin/busybox mount -o remount,bind,ro /run/image-root/.__kuasar/busybox
'''


class ImagePoolBackend(VMBackend):
    def __init__(self, args):
        started = time.monotonic()
        self.image_catalog = json.loads(Path(os.environ['E2B_IMAGE_CATALOG']).read_text())
        if args.template not in self.image_catalog:
            raise ValueError('Default template is absent from the image catalog')
        self.image_stats = {}
        for key, image in self.image_catalog.items():
            if 'oci_config' in image:
                image['oci_config'] = normalize(image['oci_config'])
            path = Path(image['disk']).resolve(strict=True)
            if not path.is_file() or path.stat().st_mode & 0o222:
                raise ValueError('Image disks must be immutable read-only regular files')
            with path.open('rb') as f:
                if hashlib.file_digest(f, 'sha256').hexdigest() != image['sha256']:
                    raise ValueError('Image digest mismatch: '+key)
            image['disk'] = str(path)
            st = path.stat()
            self.image_stats[key] = (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)
        validation_ms = (time.monotonic()-started)*1000
        self.fixed_template = os.environ.get('E2B_FIXED_IMAGE')
        self.fixed_image_fields = {}
        if self.fixed_template:
            image = self.image_catalog[self.fixed_template]
            source = vm_rpc.admin(args.admin_socket, 'sandbox-get', sandbox_id=args.source)
            add_image_disk(source['live_fork']['api_socket'], image['disk'])
            result = vm_rpc.execute(source['live_fork']['vsock_socket'], ['/bin/sh', '-c', MOUNT_IMAGE])
            if result['exit_code']:
                raise RuntimeError('Fixed comparison template binding failed: '+result['stderr'].decode(errors='replace'))
            self.fixed_image_fields = dict(template_id=self.fixed_template, exec_root='/run/image-root',
                image_manifest=image['manifest'], image_sha256=image['sha256'], image_disk=image['disk'])
        super().__init__(args)
        Path(str(self.state)+'.catalog-preparation.json').write_text(json.dumps(dict(
            validation_ms=validation_ms, images=list(self.image_catalog)), indent=2))

    def create(self, body, parent=None):
        if parent is not None:
            record = self.get_public(parent)
            inherited = record.get('template_id', self.args.template)
            if body.get('templateID', inherited) != inherited:
                raise APIError(400, 'A live-parent fork cannot change its application image')
            return super().create({**body, 'templateID': inherited}, parent)
        if self.fixed_template:
            if body.get('templateID', self.fixed_template) != self.fixed_template:
                raise APIError(400, 'Fixed pool cannot change its application image')
            return super().create({**body, 'templateID': self.fixed_template})
        self.validate_create(body)
        template = body.get('templateID', self.args.template)
        image = self.image_catalog[template]
        st = Path(image['disk']).stat()
        if (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns) != self.image_stats[template]:
            raise APIError(409, 'Application image changed since catalog validation')
        if self.template_pool is None or not self.template_pool.enabled:
            raise APIError(503, 'Generic runtime pool is unavailable')
        started_ns = time.perf_counter_ns()
        sid = self.template_pool.lease(body, private=True)
        with self.operation(sid):
            try:
                with self.lock:
                    record = self.get(sid)
                    record.update(template_id=template, image_disk=image['disk'],
                                  image_sha256=image['sha256'], image_manifest=image['manifest'])
                    self.save()  # durable binding intent before changing the VM
                info = self.admin('sandbox-get', sandbox_id=sid)
                attached_ns = time.perf_counter_ns()
                add_image_disk(info['live_fork']['api_socket'], image['disk'])
                trace_timing('image_disk_attach', attached_ns, sid=sid, template=template)
                mounted_ns = time.perf_counter_ns()
                result = vm_rpc.execute(info['live_fork']['vsock_socket'],
                                        ['/bin/sh', '-c', MOUNT_OCI_IMAGE if 'oci_config' in image else MOUNT_IMAGE])
                if result['exit_code']:
                    raise RuntimeError('Application image mount failed: '+result['stderr'].decode(errors='replace'))
                trace_timing('image_mount', mounted_ns, sid=sid, template=template)
                if 'oci_config' in image:
                    config = image['oci_config']
                    result = vm_rpc.execute(info['live_fork']['vsock_socket'], launch_argv(config, start=True))
                    if result['exit_code']:
                        raise RuntimeError('OCI entrypoint failed: '+result['stderr'].decode(errors='replace'))
                    record.update(oci_config=config, entrypoint_pid=int(result['stdout'].strip()))
                with self.lock:
                    record.update(exec_root='/run/image-root', pool_state='leased')
                    self.save()  # publish only after disk, mounts and ownership are ready
                trace_timing('image_bind_total', started_ns, sid=sid, template=template)
                return self.response(sid)
            except Exception:
                with self.lock:
                    record.update(pool_state='quarantined', status='deleting')
                    try:
                        self.save()
                    except Exception:
                        pass
                self.destroy(sid)
                raise


    def execute_process(self, sid, process, user=None):
        record = self.get_public(sid)
        if 'oci_config' not in record:
            from server import vm_process_argv
            return self.execute(sid, vm_process_argv(process))
        command = [process['cmd'], *process.get('args', [])]
        argv = launch_argv(record['oci_config'], command=command, cwd=process.get('cwd'),
                           user=user, env=process.get('envs'))
        return self.raw_execute(sid, argv)

    def raw_execute(self, sid, argv, stdin=b''):
        self.get_public(sid)
        with self.operation(sid):
            record = self.get_public(sid)
            if record['status'] != 'running':
                raise APIError(409, 'Sandbox is not running')
            return vm_rpc.execute(record['vsock_socket'], argv, stdin)

    def entrypoint(self, sid):
        record = self.get_public(sid)
        if 'entrypoint_pid' not in record:
            raise APIError(404, 'Sandbox has no OCI entrypoint')
        script = "set -e; if [ -f /run/oci-entrypoint/exit ]; then cat /run/oci-entrypoint/exit; else echo running; fi; /bin/busybox head -c 65536 /run/oci-entrypoint/stdout | /bin/busybox od -An -v -tx1; echo __STDERR__; /bin/busybox head -c 65536 /run/oci-entrypoint/stderr | /bin/busybox od -An -v -tx1"
        result = self.raw_execute(sid, ['/bin/sh', '-c', script])
        if result['exit_code']: raise APIError(500, 'Cannot read entrypoint status')
        state, _, output = result['stdout'].partition(b'\n')
        stdout, separator, stderr = output.partition(b'__STDERR__\n')
        if not separator: raise APIError(500, 'Invalid entrypoint status framing')
        return dict(pid=record['entrypoint_pid'], running=state==b'running',
                    exit_code=None if state==b'running' else int(state),
                    stdout=bytes.fromhex(stdout.decode()).decode(errors='replace'),
                    stderr=bytes.fromhex(stderr.decode()).decode(errors='replace'))


    def file_exec(self, sid, args, data=None):
        record = self.get_public(sid)
        if 'oci_config' not in record:
            return super().file_exec(sid, args, data)
        helper = '/.__kuasar/busybox'
        if args[:2] == ['/bin/busybox', 'cat']:
            command = [helper, 'od', '-An', '-v', '-tx1', args[2]]
        else:
            command = [helper, 'sh', '-c', helper+' mkdir -p "$('+helper+' dirname "$1")" && '+helper+' cat > "$1"', 'write', args[-1]]
        result = self.raw_execute(sid, launch_argv(record['oci_config'], command=command, user='0'), data or b'')
        if result['exit_code']:
            raise APIError(500, result['stderr'].decode(errors='replace'))
        return bytes.fromhex(result['stdout'].decode()) if args[:2] == ['/bin/busybox', 'cat'] else result['stdout']


    def reload_images(self):
        """Add immutable image IDs without replacing or rebuilding VM slots.

        Existing IDs cannot be rebound: running forks retain their pinned image.
        Validation occurs off the lifecycle lock. Stats are installed before the
        new catalog keys become visible, so concurrent creates see complete IDs.
        """
        started = time.monotonic()
        images = json.loads(Path(os.environ['E2B_IMAGE_CATALOG']).read_text())
        stats = {}
        for key, image in images.items():
            if 'oci_config' in image: image['oci_config'] = normalize(image['oci_config'])
            path = Path(image['disk']).resolve(strict=True)
            if not path.is_file() or path.stat().st_mode & 0o222:
                raise APIError(400, 'Image disks must be read-only regular files')
            with path.open('rb') as f:
                if hashlib.file_digest(f, 'sha256').hexdigest() != image['sha256']:
                    raise APIError(400, 'Image digest mismatch')
            image['disk'] = str(path)
            st = path.stat(); stats[key] = (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)
        with self.lock:
            for key in self.image_catalog.keys() & images.keys():
                if images[key] != self.image_catalog[key] or stats[key] != self.image_stats[key]:
                    raise APIError(409, 'Use a new image ID instead of rebinding an existing ID')
            added = sorted(images.keys() - self.image_catalog.keys())
            self.image_stats.update({key: stats[key] for key in added})
            self.image_catalog.update({key: images[key] for key in added})
        return dict(added=added, images=sorted(self.image_catalog), validation_ms=(time.monotonic()-started)*1000)
