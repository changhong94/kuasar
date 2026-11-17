#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
import os, shutil, subprocess, sys
from pathlib import Path
out = Path(sys.argv[1]).resolve()
root = out / 'root'
repo = Path(__file__).resolve().parents[2]
release = sys.argv[2] if len(sys.argv) > 2 else '6.8.0-138-generic'
for d in ('bin','sbin','proc','sys','dev','dev/pts','run','tmp','etc','lib/modules','opt/container-rootfs/bin'):
    (root/d).mkdir(parents=True, exist_ok=True)

def copy_binary(src, dst):
    shutil.copy2(src, root/dst)
    for line in subprocess.run(['ldd',src],text=True,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL).stdout.splitlines():
        for word in line.split():
            if word.startswith('/') and Path(word).is_file():
                dest=root/word.lstrip('/'); dest.parent.mkdir(parents=True,exist_ok=True); shutil.copy2(word,dest)
                if dst.startswith("opt/container-rootfs/"):
                    container_dest=root/"opt/container-rootfs"/word.lstrip("/");container_dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(word,container_dest)
subprocess.run(['cc','-static','-O2','-Wall','-Wextra','-Werror',str(repo/'scripts/research/agent-fork-probe.c'),'-o',str(root/'bin/fork-probe')],check=True)
copy_binary('/usr/bin/busybox','bin/busybox')
copy_binary('/usr/sbin/runc','bin/runc')
copy_binary('/bin/bash','bin/bash')
copy_binary('/bin/bash','opt/container-rootfs/bin/bash')
copy_binary('/usr/bin/ss','bin/ss')
copy_binary(str(repo/'target/debug/vmm-task'),'bin/vmm-task')
subprocess.run(['strip',str(root/'bin/vmm-task')],check=True)
for name in ('sh','mount','mkdir','cat','chmod','cp','sleep','kill','insmod','ln','rm','hostname','test','echo'):
    link=root/'bin'/name
    if not link.exists(): link.symlink_to('busybox')
subprocess.run(['cc','-static','-O2','-Wall','-Wextra','-Werror',str(repo/'scripts/research/container-fork-workload.c'),'-o',str(root/'opt/container-rootfs/bin/fork-workload')],check=True)
shutil.copy2('/usr/bin/busybox',root/'opt/container-rootfs/bin/busybox')
if not (root/'opt/container-rootfs/bin/sh').exists():
    (root/'opt/container-rootfs/bin/sh').symlink_to('busybox')
for d in ('proc','dev','tmp','etc','run'):
    (root/'opt/container-rootfs'/d).mkdir(exist_ok=True)
for name in ('vsock','vmw_vsock_virtio_transport_common','vmw_vsock_virtio_transport'):
    shutil.copy2('/lib/modules/'+release+'/kernel/net/vmw_vsock/'+name+'.ko',root/'lib/modules')
for module in ('virtio_net', 'failover', 'net_failover', 'inet_diag', 'tcp_diag'):
    paths=list(Path('/lib/modules/'+release).rglob(module+'.ko'))
    if paths: shutil.copy2(paths[0],root/'lib/modules')
for name in ('resolv.conf','hosts','hostname'):
    (root/'etc'/name).write_text('')
(root/'init').write_text('''#!/bin/sh
mount -t proc proc /proc
mount -t devtmpfs dev /dev
insmod /lib/modules/failover.ko
insmod /lib/modules/net_failover.ko
insmod /lib/modules/virtio_net.ko
insmod /lib/modules/inet_diag.ko
insmod /lib/modules/tcp_diag.ko
insmod /lib/modules/vsock.ko
insmod /lib/modules/vmw_vsock_virtio_transport_common.ko
insmod /lib/modules/vmw_vsock_virtio_transport.ko
exec /bin/vmm-task
''')
if os.environ.get('FORK_WRITABLE_DISK'):
    # This kernel has ext4/virtio_blk built in. Load dependencies as well when
    # running the fixture with a modular kernel.
    modules=[]
    for driver in ('ext4','virtio_blk'):
        deps=subprocess.check_output(['modprobe','--show-depends','-S',release,driver],text=True)
        for line in deps.splitlines():
            if line.startswith('insmod '):
                src=Path(line.split()[1])
                shutil.copy2(src,root/'lib/modules'/src.name)
                command='insmod /lib/modules/'+src.name
                if command not in modules: modules.append(command)
    init=(root/'init').read_text()
    init=init.replace('exec /bin/vmm-task', '\n'.join(modules)+ '\nexec /bin/vmm-task')
    (root/'init').write_text(init)
    disk=out/'seed.raw'
    with disk.open('xb') as f: f.truncate(512*1024*1024)
    subprocess.run(['mkfs.ext4','-q','-F',str(disk)],check=True)
(root/'init').chmod(0o755)
with (out/'agent.cpio').open('wb') as f:
    files='\n'.join(['.']+[str(p.relative_to(root)) for p in root.rglob('*')])+'\n'
    subprocess.run(['cpio','--quiet','-o','-H','newc'],cwd=root,input=files.encode(),stdout=f,check=True)
print(out/'agent.cpio')
