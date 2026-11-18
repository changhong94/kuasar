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
import threading
import uuid
from server import APIError, trace_timing
from vm_backend import VMBackend
import vm_rpc
import exec_cache
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


MOUNT_OCI_IMAGE = r'''set -e
n=0
while [ ! -b /dev/vdb ]; do
 n=$((n+1)); [ "$n" -lt 500 ]; /bin/busybox sleep 0.01
done
[ "$(/bin/busybox cat /sys/block/vdb/serial)" = app-root ]
/bin/busybox mkdir -p /run/image-lower /run/image-upper /run/image-work /run/image-root
/bin/busybox mount -o ro /dev/vdb /run/image-lower
/bin/busybox mount -t overlay overlay -o lowerdir=/run/image-lower,upperdir=/run/image-upper,workdir=/run/image-work /run/image-root
[ ! -L /run/image-root/.__kuasar ]
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
            self.validate_image_config(image)
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
        if os.environ.get('E2B_FIXED_IMAGE'):
            raise ValueError('Fixed-image pools are unsupported; import an OCI image instead')
        if int(os.environ.get('E2B_TEMPLATE_READ_CACHE_BYTES', '0')):
            raise ValueError('Application-file preloading is unsupported')
        self.slot_disks = None
        if os.environ.get("E2B_PREPARE_IMAGE_DEVICE") == "1":
            from slot_disk import SlotDisks
            self.slot_disks = SlotDisks(Path(args.state).parent / "slot-disks", self.image_catalog)
        self.direct_pool_prepare = os.environ.get('E2B_SNAPSHOT_POOL') == '1'
        self.snapshot_templates = {}
        self.snapshot_lock = threading.RLock()
        super().__init__(args)
        if (os.environ.get('E2B_SNAPSHOT_POOL') == '1'
                and os.environ.get('E2B_EAGER_IMAGE_TEMPLATES') == '1'):
            for key in self.image_catalog:
                self.prepare_snapshot(key)
        Path(str(self.state)+'.catalog-preparation.json').write_text(json.dumps(dict(
            validation_ms=validation_ms, images=list(self.image_catalog)), indent=2))

    @staticmethod
    def validate_image_config(image):
        if image.get('startup_read_files'):
            raise ValueError('Image-specific startup reads are unsupported')
        if 'oci_config' not in image:
            raise ValueError('OCI configuration is required; reimport the image')
        image['oci_config'] = normalize(image['oci_config'])

    def start_entrypoint(self, sid, record):
        # Called while the slot is private and its operation guard is held.
        result = vm_rpc.execute(record['vsock_socket'], launch_argv(record['oci_config'], start=True))
        if result['exit_code']:
            raise RuntimeError('OCI entrypoint failed: '+result.get('stderr', b'').decode(errors='replace'))
        record['entrypoint_pid'] = int(result['stdout'].strip())

    def prepare_snapshot(self, template):
        """One immutable file template per image; the VM slot pool remains shared."""
        with self.snapshot_lock:
            if template in self.snapshot_templates:
                return self.snapshot_templates[template]
            started = time.monotonic()
            response = self._create_image({'templateID': template, 'timeout': 86400}, _snapshot_builder=True)
            sid = response['sandboxID']
            try:
                info = self.admin('sandbox-get', sandbox_id=sid)['live_fork']
                # Templates contain mounted filesystems and runtime state only.
                # No application entrypoint, interpreter, or selected-file reads.
                metadata_cache = (exec_cache.prepare_metadata(info['vsock_socket'], '/run/image-root')
                                  if os.environ.get('E2B_TEMPLATE_METADATA', '1') == '1' else None)
                key = 'image-' + uuid.uuid4().hex
                saved = self.admin('sandbox-snapshot-capture', sandbox_id=sid, template=key)
                record = self.get(sid)
                fields = {k: record[k] for k in ('exec_root', 'oci_config', 'entrypoint_pid', 'filesystem_write_backing', 'filesystem_size_mb') if k in record}

                warm = time.monotonic()
                memory = Path(saved['directory']) / 'memory-ranges'
                # Populate shared host cache once; restore itself uses MAP_PRIVATE.
                with memory.open('rb') as f:
                    while f.read(1024*1024):
                        pass
                entry = dict(key=key, fields=fields, directory=saved['directory'],
                             application_started=False, metadata_cache=metadata_cache,
                             preparation_ms=(warm-started)*1000,
                             warmup_ms=(time.monotonic()-warm)*1000,
                             memory_bytes=memory.stat().st_size,
                             allocated_bytes=memory.stat().st_blocks*512)
                staged = self.stage_snapshot_slots(entry)
                entry.update(staged)
                self.snapshot_templates[template] = entry
                Path(str(self.state)+'.snapshot-preparation.json').write_text(json.dumps(self.snapshot_templates, indent=2))
                return entry
            finally:
                self.destroy(sid)

    def stage_snapshot_slots(self, entry):
        """Prepare small restore files, never another image-specific VM pool."""
        started = time.monotonic()
        pool = getattr(self, 'template_pool', None)
        if pool is None:
            return dict(metadata_stage_ms=0, metadata_staged_slots=0)
        with self.lock:
            ids = [sid for sid, r in self.records.items() if r.get('pool_state') == 'ready']
        count = 0
        for sid in ids:
            try:
                with self.operation(sid):
                    with self.lock:
                        record = self.records.get(sid)
                        if record is None or record.get('pool_state') != 'ready':
                            continue
                    try:
                        with self.lock:
                            # Withdraw before staging so a concurrent lease cannot
                            # publish this slot while preparation might fail.
                            if record.get('pool_state') != 'ready':
                                continue
                            record['pool_state'] = 'preparing'
                            self.save_record(sid)
                        self.admin('sandbox-snapshot-stage', sandbox_id=sid, template=entry['key'])
                        with self.lock:
                            record['pool_state'] = 'ready'
                            self.save_record(sid)
                            pool.condition.notify_all()
                        count += 1
                    except Exception:
                        with self.lock:
                            record.update(pool_state='quarantined', status='deleting')
                            try:
                                self.save_record(sid)
                            except Exception:
                                pass
                        raise
            except Exception:
                # operation() has released its lock before destruction.
                self.destroy(sid)
                raise
        return dict(metadata_stage_ms=(time.monotonic()-started)*1000,
                    metadata_staged_slots=count)

    def create(self, body, parent=None):
        if parent is not None or os.environ.get('E2B_SNAPSHOT_POOL') != '1':
            return self._create_image(body, parent)
        self.validate_create(body)
        template = body.get('templateID', self.args.template)
        entry = self.prepare_snapshot(template)
        image = self.image_catalog[template]
        sid = self.template_pool.lease(body, private=True, binding=dict(
            template_id=template, image_disk=image['disk'],
            image_sha256=image['sha256'], image_manifest=image['manifest'],
            snapshot_template=entry['key']))
        with self.operation(sid):
            try:
                started = time.perf_counter_ns()
                result = self.admin('sandbox-snapshot-bind', sandbox_id=sid, template=entry['key'])
                trace_timing('snapshot_bind', started, sid=sid, details=result)
                record = self.get(sid)
                record.update(entry['fields'])
                self.start_entrypoint(sid, record)
                with self.lock:
                    self.get(sid).update(pool_state='leased', snapshot_template=entry['key'])
                    self.save_record(sid)
                return self.response(sid)
            except Exception:
                with self.lock:
                    self.get(sid).update(pool_state='quarantined', status='deleting')
                    self.save_record(sid)
                self.destroy(sid)
                raise

    def _create_image(self, body, parent=None, _snapshot_builder=False):
        if parent is not None:
            record = self.get_public(parent)
            inherited = record.get('template_id', self.args.template)
            if body.get('templateID', inherited) != inherited:
                raise APIError(400, 'A live-parent fork cannot change its application image')
            return super().create({**body, 'templateID': inherited}, parent)
        self.validate_create(body)
        template = body.get('templateID', self.args.template)
        image = self.image_catalog[template]
        st = Path(image['disk']).stat()
        if (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns) != self.image_stats[template]:
            raise APIError(409, 'Application image changed since catalog validation')
        if self.template_pool is None or not self.template_pool.enabled:
            raise APIError(503, 'Generic runtime pool is unavailable')
        started_ns = time.perf_counter_ns()
        # Assignment and image identity form one durable private binding intent.
        # No VM mutation or credential publication precedes this commit.
        binding = dict(template_id=template, image_disk=image['disk'],
                       image_sha256=image['sha256'], image_manifest=image['manifest'])
        if _snapshot_builder:
            sid = self._create_live(body, _private=True)
            try:
                self.jobs[sid].result(timeout=50)
                self.prepare_slot(sid, keep_running=True)
                with self.lock:
                    self.get(sid).update(binding, pool_state='binding')
                    self.save()
            except Exception:
                self.destroy(sid)
                raise
        else:
            sid = self.template_pool.lease(body, private=True, binding=binding)
        with self.operation(sid):
            record = self.get(sid)
            try:
                info = self.admin('sandbox-get', sandbox_id=sid)
                attached_ns = time.perf_counter_ns()
                if record.get('slot_disk'):
                    self.slot_disks.bind(record['slot_disk'], image)
                else:
                    add_image_disk(info['live_fork']['api_socket'], image['disk'])
                trace_timing('image_disk_attach', attached_ns, sid=sid, template=template)
                mounted_ns = time.perf_counter_ns()
                helper = os.environ.get('E2B_IMAGE_MOUNT_HELPER')
                argv = ([helper, 'oci'] if helper else
                        ['/bin/sh', '-c', MOUNT_OCI_IMAGE])
                result = vm_rpc.execute(info['live_fork']['vsock_socket'], argv)
                if result['exit_code']:
                    raise RuntimeError('Application image mount failed: '+result['stderr'].decode(errors='replace'))
                trace_timing('image_mount', mounted_ns, sid=sid, template=template, guest=result.get('timing'), helper=result.get('stderr', b'').decode(errors='replace'))
                record['oci_config'] = image['oci_config']
                if not _snapshot_builder:
                    self.start_entrypoint(sid, record)
                publish_ns = time.perf_counter_ns()
                with self.lock:
                    record.update(exec_root='/run/image-root',
                                  pool_state='binding' if _snapshot_builder else 'leased')
                    self.save()  # publish only after disk, mounts and ownership are ready
                trace_timing('image_publish', publish_ns, sid=sid)
                trace_timing('image_bind_total', started_ns, sid=sid, template=template)
                return {'sandboxID': sid} if _snapshot_builder else self.response(sid)
            except Exception:
                with self.lock:
                    record.update(pool_state='quarantined', status='deleting')
                    try:
                        self.save()
                    except Exception:
                        pass
                self.destroy(sid)
                raise


    def prepare_slot_device(self, sid, keep_running=False):
        if self.get(sid).get('direct_prepared'):
            return  # No guest or disk backend exists until file snapshot restore.
        if getattr(self, 'slot_disks', None) is None:
            return
        with self.operation(sid):
            path = str(self.slot_disks.path / (sid + '.raw'))
            with self.lock:
                self.get(sid)['slot_disk'] = path
                self.save()  # ownership precedes file creation and attachment
            self.slot_disks.create(path)
            info = self.admin('sandbox-get', sandbox_id=sid)['live_fork']
            add_image_disk(info['api_socket'], path)
            # File-snapshot slots discard this guest before vmm.prepare. Only
            # the host disk configuration is needed; waiting for this temporary
            # guest's device discovery is wasted work. A template builder or a
            # live-memory slot keeps its guest and still needs that barrier.
            if keep_running or os.environ.get('E2B_SNAPSHOT_POOL') != '1':
                result = vm_rpc.execute(info['vsock_socket'],
                                       [os.environ['E2B_IMAGE_MOUNT_HELPER'], 'prepare'])
                if result['exit_code']:
                    raise RuntimeError('Generic block device preparation failed')

            writable_base = os.environ.get('E2B_WRITABLE_BASE')
            if writable_base:
                from writable_root import prepare
                prepare(self, sid, info, writable_base)

    def prepare_slot(self, sid, keep_running=False, device_prepared=False):
        if self.get(sid).get('direct_prepared'):
            with self.operation(sid):
                for entry in list(self.snapshot_templates.values()):
                    self.admin('sandbox-snapshot-stage', sandbox_id=sid, template=entry['key'])
            return
        if getattr(self, 'slot_disks', None) is None:
            return
        with self.operation(sid):
            if not device_prepared:
                self.prepare_slot_device(sid, keep_running=keep_running)

            if os.environ.get('E2B_SNAPSHOT_POOL') == '1' and not keep_running:
                self.admin('sandbox-snapshot-prepare', sandbox_id=sid)
                for entry in list(self.snapshot_templates.values()):
                    self.admin('sandbox-snapshot-stage', sandbox_id=sid, template=entry['key'])

    def destroy(self, sid):
        super().destroy(sid)
        started = time.perf_counter_ns()
        if getattr(self, 'slot_disks', None) is not None:
            with self.lock:
                self.slot_disks.collect({r['slot_disk'] for r in self.records.values() if r.get('slot_disk')})
        trace_timing('delete_disk_collect', started, sid=sid)

    def execute_process(self, sid, process, user=None):
        record = self.get_public(sid)
        argv = launch_argv(record['oci_config'],
                           command=[process['cmd'], *process.get('args', [])],
                           cwd=process.get('cwd'), user=user, env=process.get('envs'))
        return self.raw_execute(sid, argv)

    def execute_process_stream(self, sid, process, user=None):
        # Hold the same lifecycle operation guard as unary execution through
        # process exit; publication, fork and destruction cannot race this RPC.
        with self.operation(sid):
            record = self.get_public(sid)
            if record['status'] != 'running':
                raise APIError(409, 'Sandbox is not running')
            argv = launch_argv(record['oci_config'],
                               command=[process['cmd'], *process.get('args', [])],
                               cwd=process.get('cwd'), user=user, env=process.get('envs'))
            started = time.perf_counter_ns()
            for event in vm_rpc.execute_stream(record['vsock_socket'], argv):
                if 'exit_code' in event:
                    trace_timing('execute_stream', started, sid=sid, guest=event['timing'])
                yield event

    def raw_execute(self, sid, argv, stdin=b''):
        self.get_public(sid)
        with self.operation(sid):
            record = self.get_public(sid)
            if record['status'] != 'running':
                raise APIError(409, 'Sandbox is not running')
            started = time.perf_counter_ns()
            result = vm_rpc.execute(record['vsock_socket'], argv, stdin)
            trace_timing('execute_rpc', started, sid=sid, guest=result.get('timing'))
            trace_timing('execute_total', started, sid=sid)
            return result

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
            self.validate_image_config(image)
            path = Path(image['disk']).resolve(strict=True)
            if not path.is_file() or path.stat().st_mode & 0o222:
                raise APIError(400, 'Image disks must be read-only regular files')
            with path.open('rb') as f:
                if hashlib.file_digest(f, 'sha256').hexdigest() != image['sha256']:
                    raise APIError(400, 'Image digest mismatch')
            image['disk'] = str(path)
            st = path.stat(); stats[key] = (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)
        with self.lock:
            candidates = images.keys() - self.image_catalog.keys()
        if getattr(self, 'slot_disks', None) is not None:
            # Includes copy/checksum/fsync outside the lifecycle lock. Never
            # publish an ID that an already-prepared device cannot bind.
            self.slot_disks.prepare_images([images[key] for key in candidates])
        with self.lock:
            for key in self.image_catalog.keys() & images.keys():
                if images[key] != self.image_catalog[key] or stats[key] != self.image_stats[key]:
                    raise APIError(409, 'Use a new image ID instead of rebinding an existing ID')
            added = sorted(images.keys() - self.image_catalog.keys())
            self.image_stats.update({key: stats[key] for key in added})
            self.image_catalog.update({key: images[key] for key in added})
        return dict(added=added, images=sorted(self.image_catalog), validation_ms=(time.monotonic()-started)*1000)
