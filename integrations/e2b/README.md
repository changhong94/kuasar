# Default E2B SDK backend: asynchronous VM networking

The default adapter backend now uses Kuasar's VM admin API directly. The normal,
unmodified E2B Python SDK `Sandbox.create()`, `parent.fork()`, `commands.run()`,
files and `kill()` methods use this backend without a client-side feature flag.
The prior Kubernetes backend remains available with `--backend kubernetes`.
Changing the default does not change CRI/kubelet networking behavior.

A child is executable through vsock before CNI finishes. It has a real inherited
virtio NIC connected to a private, isolated TAP. The adapter owns CNI ADD in a
background worker and then invokes `sandbox-network-complete`; guest identity,
inherited TCP reset, metadata and source-identity filters complete before the TAP
gets an external connection. Fork lifecycle gates, entropy and disk durability
remain mandatory. No memory image or eager guest-RAM copy is introduced.

```sh
python3 integrations/e2b/server.py \
  --source <running-networked-template-id> \
  --admin-socket /path/to/kuasar/admin.sock \
  --sandbox-state /path/to/kuasar/state \
  --cni-bin /path/to/cni/plugins --cni-config /path/to/bridge.json \
  --api-key-file /path/to/private-key --state /path/to/private-state.json
```

Use a dedicated state file, the new structured-execution guest agent, one NIC,
`ip`, `tc`, `nsenter`, CNI bridge/host-local plugins and a root/KVM host. The
validated image is the pinned official `python:3.12-slim` filesystem with the
existing agent/tools. Binary reads use `/usr/bin/base64` from that filesystem;
other templates must provide it. The fork restrictions of the VM backend still
apply. A sandbox whose own network is pending must finish provisioning before
it can become the source of another fork; its commands do not wait for CNI.

Network readiness is separate from execution readiness:

```python
child = parent.fork(count=1, timeout=600)[0]
if isinstance(child, Exception):
    raise child
result = child.commands.run("python3 -u -c 'print(123)'", user="root")
print(result.stdout)
print(child.get_info().metadata["kuasar.network.status"])  # pending/configuring/ready/failed
print(child.get_info().metadata["kuasar.network.ip"])
child.kill()
```

The adapter persists creation/deletion intent, ownership and network state with
atomic replacement and file/directory sync. Restart reconciliation uses an
existing CNI interface instead of issuing a duplicate ADD. Network failure is
visible in metadata and leaves command execution available. Deletion marks the
sandbox as deleting, joins any active ADD, stops the VM, executes DEL and removes
the owned namespace. A late provisioning task cannot reactivate a deleted VM.
Only same-VM commands, forks and deletion are serialized; network work does not
hold the command lock or a global CNI lock.

The VM command RPC is currently unary: the adapter returns Connect output events
after the process exits. Thus the measured SDK print latency includes process
shutdown as well as startup. Foreground commands, separate stdout/stderr and exit
status are supported; this is not interactive/early-streaming execution. Guest
RPC messages are bounded to 16 MiB. There is no public port proxy, TLS ingress or
multi-tenant authorization; this remains a restricted local research adapter.

Run adapter tests with `python3 -m unittest discover -s integrations/e2b -v`.
The benchmark/fault-injection scripts and full 100-run results are saved under
`sandbox_bench/results/e2b-deferred-network` alongside the sibling research tree.

---

# Explicit Kubernetes backend

This adapter implements a restricted E2B API using Kubernetes Pods backed by the
`sandbox_fork` branches of containerd, Kuasar and Cloud Hypervisor. It is tested
with the **unmodified E2B Python SDK 2.64.0**. It does not run the upstream E2B
orchestrator or envd, and does not create a memory snapshot file.

Supported operations:

- `Sandbox.create`: derive a new Pod from a configured, running template sandbox.
- `sandbox.commands.run`: execute a command through kubelet/CRI, returning guest
  PID, stdout, stderr and exit status using E2B's Connect JSON stream protocol.
- `sandbox.files.write/read`: transfer text or binary files into the guest
  container, including multipart and octet-stream uploads.
- `sandbox.fork(count=1)`: derive another Pod from the running source Pod UID.
  The inherited main container and its descendant processes continue running.
- `sandbox.kill`: delete its Pod through Kubernetes, followed by normal kubelet
  stop/remove and CNI cleanup. Sandbox expiry also deletes the Pod.

Each sandbox has a separate Pod UID, CRI/containerd ID, network namespace, TAP,
MAC, Pod IP and access token. Requests are authenticated with a control API key;
envd-style requests require the token belonging to `E2b-Sandbox-Id`. The adapter
persists creation intent and token/UID/expiry records in its private state file.

## Starting the adapter

Prepare a running network-enabled seed, a fork RuntimeClass, a preloaded image
record and a CNI configuration. The matching container name and template workload
default to `samecontainer` and `/bin/fork-workload init` (the research fixture).
Use `--container`, `--image`, and `--command-json` to match a different seed.
The seed ID must be in containerd's `KUASAR_FORK_TEMPLATE_IDS`. Both Pod and seed
must be on the configured node. A source Pod must explicitly allow fork and be in
the configured Kubernetes namespace.

```sh
python3 integrations/e2b/server.py --backend kubernetes \
  --k3s /path/to/k3s-v1.31 --kubeconfig /path/to/kubeconfig \
  --source seed00000001 --api-key-file /path/to/private-api-key \
  --state /path/to/private-state.json
```

The default bind address is `127.0.0.1:18881`. It is a single-node, single-tenant
research adapter intended behind a trusted local endpoint. No public TLS ingress,
multi-tenant authorization, port proxy/getHost, image build/pull, code interpreter,
PTY, interactive stdin, filesystem watch or snapshot API is implemented. The
current template runs as root; other requested users are rejected. Changes to
inherited startup environment, volumes and network policy are rejected. Commands
can set their own environment and working directory. Active external exec IO
still prevents VM fork; finish the command stream before forking. Fork count is
currently one per request. Fork restoration preserves the vsock host-port
allocation cursor so stale guest packets cannot collide with newly opened RPC
connections. Hybrid-vsock handshake recovery uses bounded millisecond backoff
and respects the RPC deadline. The SDK's normal base API calls require no SDK patches:

```python
from e2b import Sandbox

options = dict(api_key="<key>", api_url="http://127.0.0.1:18881",
               sandbox_url="http://127.0.0.1:18881", request_timeout=150)
parent = Sandbox.create(timeout=600, **options)
parent.files.write("/tmp/message", b"inherited", user="root")
child = parent.fork(count=1, timeout=600)[0]
if isinstance(child, Exception):
    raise child
child.files.write("/tmp/message", b"child", user="root")
print(parent.files.read("/tmp/message", user="root"))  # inherited
print(child.commands.run("/bin/busybox cat /tmp/message", user="root").stdout)
parent.kill()
child.kill()
```

## Network fork semantics

The supported NIC is a single userspace virtio-net device backed by a persistent
named TAP in a CNI veth namespace. Vhost/DMA and shared RAM are rejected. At the
fork boundary userspace workers are quiesced or joined. The optimized parent
retains its KVM/vCPU objects; the child creates its own KVM objects over inherited
private RAM.
The single-threaded child changes to the new network namespace and restores the
same PCI/queue state using its own TAP and MAC. Seccomp permits only network
`setns` in the fork supervisor; ordinary worker policies remain restricted.

The new TAP drops all traffic until the guest agent has replaced addresses and
routes, changed its NIC MAC and destroyed inherited established TCP sockets.
Additional filters reject packets carrying the parent's MAC/IP, including stale
TX descriptors and old bound socket identities. The parent's TCP state is left
alone. Wildcard listeners continue accepting on the child's new address;
applications bound to the old specific IP must rebind. Existing external TCP
sessions are **not** cloned into independent remote sessions. Guest `ss` and
INET_DIAG/SOCK_DESTROY support are required. Connection reset errors abort the
child rather than expose an unreset network.

This is standard CNI connectivity, not a claim of network-policy or outbound
Internet-policy enforcement. The fixture verifies distinct IPv4 addresses and
MACs, parent TCP survival, absence of the parent's established TCP identity in
the child, bidirectional Pod HTTP, independent guest RAM/files, and survivor exec
and HTTP after either peer is deleted. No writable shared disks are supported.

## Reproducing validation

Use an isolated root/KVM host with the matching kernel/modules, `ip`, `tc`, `ss`,
K3s v1.31.13+k3s1 and its CNI binaries, and the adjacent `kuasar-io/ttrpc-rust` checkout on `sandbox_fork` (ordered
stream delivery), plus the Rust/Go toolchains documented in
`scripts/research/README.md`. Install the pinned SDK in
`../../tools/e2b-venv` relative to the repository. Fresh short output paths are
required for Unix sockets. The scripts create isolated containerd/K3s instances.

```sh
FORK_NETWORK=1 scripts/research/run-kubernetes-fork.sh \
  /tmp/ch-fork-vmlinuz 6.8.0-138-generic /tmp/kn-final
FORK_NETWORK=1 FORK_E2B=1 scripts/research/run-kubernetes-fork.sh \
  /tmp/ch-fork-vmlinuz 6.8.0-138-generic /tmp/ke-final
DELETE_CHILD_FIRST=1 FORK_NETWORK=1 FORK_E2B=1 \
  scripts/research/run-kubernetes-fork.sh \
  /tmp/ch-fork-vmlinuz 6.8.0-138-generic /tmp/ke-reverse
```

The seed runs a real runc workload once. Its 16-MiB application buffer and startup
nonce are inherited by both Pods. The seed is stopped before forking the parent,
so each measured boundary has exactly two RAM sharers. Combined guest-RAM PSS
must equal the pre-fork baseline, and no `memory.img` may appear. Binary file
checks cross stream window boundaries, including a file larger than 1 MiB.
The containerd changes keep stdin open after an individual output reaches EOF
and close it on process exit/cancellation; the ttrpc changes preserve DATA/EOF
ordering under backpressure. The test waits
for kubelet to reclaim both sandboxes. Keep kubeconfig, API keys and adapter state
private when saving logs or sharing results.

## Fork-to-exec latency regression

`test-e2b-sdk.py` writes monotonic end-to-end measurements to `timings.json`,
including the first SDK command in each sandbox after `fork` returns, direct
`kubectl exec`, and subsequent commands. There is no warm-up sleep after fork.
Set `MAX_EXEC_SECONDS=2` to enforce the regression ceiling and
`LATENCY_PARALLEL=1` to issue the first parent/child commands concurrently and
repeat five parallel pairs. `KUASAR_RPC_TIMING=1` enables containerd connection,
handshake-retry and State/Exec/Start/Wait timing logs. A long Wait for a running
process is expected; it is not counted as exec startup latency.

The native Kubernetes fixture records `native-timings.json` with the same optional
ceiling. All latency scenarios retain the process inheritance, CoW, networking,
binary-file, lifecycle, no-memory-image and unchanged fork-boundary PSS checks.
