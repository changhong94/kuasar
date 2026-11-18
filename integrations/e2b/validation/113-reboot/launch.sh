#!/bin/sh
set -eu
NESTED_DIR=/home/hong/src/research/fork-features-113/nested
exec /usr/bin/qemu-kvm -name fork-reboot-test -enable-kvm -cpu host -smp 4 -m 16384 -nodefaults -no-user-config -display none -serial file:"$NESTED_DIR/console.log" -monitor none -qmp unix:"$NESTED_DIR/qmp.sock",server=on,wait=off -pidfile "$NESTED_DIR/qemu.pid" -daemonize -kernel /boot/vmlinuz-6.6.0-72.0.0.76.r1361_99.hce3.lf.x86_64+ -initrd "$NESTED_DIR/initrd.img" -append 'root=/dev/vda rw console=ttyS0 net.ifnames=0 biosdevname=0 selinux=0 rd.lvm=0' -drive file="$NESTED_DIR/root.raw",format=raw,if=virtio,cache=none -drive file="$NESTED_DIR/data.raw",format=raw,if=virtio,cache=none -netdev user,id=n0,hostfwd=tcp:127.0.0.1:22239-:22 -device virtio-net-pci,netdev=n0
