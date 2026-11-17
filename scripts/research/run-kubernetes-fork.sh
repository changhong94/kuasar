#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
repo=$(cd "$(dirname "$0")/../.." && pwd)
test -x "$repo/../../tools/k3s-v1.31" || { echo "Install K3s v1.31.13+k3s1 as $repo/../../tools/k3s-v1.31" >&2; exit 1; }
if [[ ${FORK_E2B:-0} == 1 ]]; then
  "$repo/../../tools/e2b-venv/bin/python" -c 'from importlib.metadata import version; assert version("e2b") == "2.64.0"'
fi
make -C "$repo/../containerd" bin/containerd bin/ctr
export FORK_TEST_SCRIPT=test-kubernetes-fork.py
exec "$repo/scripts/research/run-agent-fork.sh" "$@"
