#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
repo=$(cd "$(dirname "$0")/../.." && pwd)
make -C "$repo/../containerd" bin/containerd bin/ctr
export FORK_TEST_SCRIPT=test-containerd-fork.py
exec "$repo/scripts/research/run-agent-fork.sh" "$@"
