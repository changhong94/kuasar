# SPDX-License-Identifier: Apache-2.0
"""Image-independent durable overlay storage, prepared before slot publication."""
import json,os,stat,subprocess
from pathlib import Path
import vm_rpc

MOUNT = r'''set -e
n=0
while :; do
 device=''
 for serial in /sys/class/block/*/serial; do
  [ -f "$serial" ] || continue
  if [ "$(/bin/busybox cat "$serial")" = app-write ]; then
   name=${serial%/serial}; name=${name##*/}; device=/dev/$name; break
  fi
 done
 if [ -n "$device" ] && [ -b "$device" ] && /bin/busybox dd if="$device" of=/dev/null bs=512 count=1 2>/dev/null; then break; fi
 n=$((n+1)); [ "$n" -lt 500 ]; /bin/busybox sleep 0.01
done
/bin/busybox mkdir -p /run/image-writes /run/image-upper /run/image-work
/bin/busybox mount -t ext4 -o noatime "$device" /run/image-writes
/bin/busybox mkdir -p /run/image-writes/upper /run/image-writes/work
/bin/busybox sync
'''

def sync_dir(path):
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)

def prepare(backend, sid, info, base):
    from image_pool import UnixHTTP
    base=Path(base).resolve(strict=True)
    st=base.stat()
    if not stat.S_ISREG(st.st_mode) or st.st_mode & 0o222:
        raise ValueError('Writable storage template must be a sealed regular file')
    directory=Path(backend.args.sandbox_state)/sid/'fork-disks'
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError('Invalid managed disk directory')
    source=directory/'empty.raw';leaf=directory/'empty.qcow2'
    with source.open('xb'):
        pass
    if os.path.lexists(leaf):
        raise FileExistsError(leaf)
    subprocess.run(['cp','--reflink=always','--',str(base),str(source)],check=True,capture_output=True)
    source.chmod(0o400)
    subprocess.run(['qemu-img','create','-f','qcow2','-F','raw','-b','empty.raw','empty.qcow2'],cwd=directory,check=True,capture_output=True)
    leaf.chmod(0o600)
    for path in [source,leaf]:
        with path.open('rb') as f:os.fsync(f.fileno())
    sync_dir(directory);sync_dir(directory.parent)
    with backend.lock:
        backend.get(sid).update(filesystem_write_backing='qcow2-ext4',filesystem_size_mb=st.st_size//(1024*1024))
        backend.save_record(sid)
    client=UnixHTTP(info['api_socket'])
    try:
        client.request('PUT','/api/v1/vm.add-disk',json.dumps(dict(path=str(leaf),readonly=False,image_type='Qcow2',backing_files=True,id='application-writes',serial='app-write')),{'Content-Type':'application/json'})
        response=client.getresponse();data=response.read(65537)
        if not 200<=response.status<300 or len(data)>65536:
            raise RuntimeError('Writable root attachment failed: '+data[:1024].decode(errors='replace'))
    finally:client.close()
    result=vm_rpc.execute(info['vsock_socket'],['/bin/sh','-c',MOUNT])
    if result['exit_code']:
        raise RuntimeError('Writable root mount failed: '+result['stderr'].decode(errors='replace'))
