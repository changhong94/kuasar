#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
repo=$(cd "$(dirname "$0")/../.." && pwd)
ch_repo=$(realpath "$repo/../../cloud-hypervisor/cloud-hypervisor")
kernel=$(realpath "${1:?Usage: run-live-fork.sh KERNEL [OUTPUT_DIR]}")
output=${2:-$(mktemp -d /tmp/ku-live-fork.XXXXXX)}
mkdir -p "$output/root"
output=$(realpath "$output")
cc -std=c11 -O2 -static -Wall -Wextra -Werror -DFORK_SERIAL_CONTROL \
    "$ch_repo/scripts/research/linux-fork-init.c" -o "$output/root/init"
(cd "$output/root" && printf 'init\n' | cpio --quiet -o -H newc) > "$output/initramfs.cpio"
cargo +stable build --manifest-path "$repo/Cargo.toml" -p vmm-sandboxer --bin cloud_hypervisor
python3 "$repo/scripts/research/test-live-fork.py" "$repo/target/debug/cloud_hypervisor" \
    "$ch_repo/target/debug/cloud-hypervisor" "$kernel" "$output/initramfs.cpio" "$output"
