#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
repo=$(cd "$(dirname "$0")/../.." && pwd)
ch_repo=$(realpath "$repo/../../cloud-hypervisor/cloud-hypervisor")
kernel=$(realpath "${1:?Usage: run-agent-fork.sh KERNEL MODULE_RELEASE OUTPUT_DIR}")
release=${2:?matching kernel module release required}
output=${3:?fresh short output directory required}
mkdir -p "$output"
output=$(realpath "$output")
cargo +stable build --manifest-path "$ch_repo/Cargo.toml" -p cloud-hypervisor
cargo +stable build --manifest-path "$repo/Cargo.toml" -p vmm-task --no-default-features
cargo +stable build --manifest-path "$repo/Cargo.toml" -p vmm-sandboxer --bin cloud_hypervisor --example fork-agent-client
python3 "$repo/scripts/research/build-agent-initramfs.py" "$output/guest" "$release"
python3 - "$output" "$kernel" "$ch_repo" <<'PY'
import json,sys,os
from pathlib import Path
out,kernel,ch=map(Path,sys.argv[1:])
(out/'input.toml').write_text(f'''[sandbox]
log_level = "info"
enable_tracing = false
[hypervisor]
live_fork = true
live_fork_agent = true
live_fork_writable_disk = {str(bool(os.environ.get("FORK_WRITABLE_DISK"))).lower()}
path = {json.dumps(str(ch/'target/debug/cloud-hypervisor'))}
vcpus = 2
memory_in_mb = 512
kernel_path = {json.dumps(str(kernel))}
initrd_path = {json.dumps(str(out/'guest/agent.cpio'))}
image_path = {json.dumps(str(out/"guest/seed.raw") if os.environ.get("FORK_WRITABLE_DISK") else "")}
kernel_params = "console=ttyS0 panic=-1 rdinit=/init"
hugepages = false
entropy_source = "/dev/urandom"
container_storage_backend = "virtio-blk"
[hypervisor.task]
debug = false
enable_tracing = false
''')
PY
python3 "$repo/scripts/research/${FORK_TEST_SCRIPT:-test-agent-fork.py}" "$output" "$output/input.toml"
