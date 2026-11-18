# Default E2B SDK backend: asynchronous VM networking

The default adapter backend now uses Kuasar's VM admin API directly. The unmodified E2B Python SDK supports `Sandbox.create()`, `commands.run()`,
files and `kill()` through this backend. Live fork uses this adapter's explicit
HTTP extension, `POST /sandboxes/{parent_id}/fork`. The SDK installed on the
113 benchmark host is 2.25.1 and has no `Sandbox.fork()` method. Do not assume
a native SDK fork method is available.
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

## Forking a running sandbox over HTTP

Fork is an adapter extension, not a method added to the installed E2B SDK.
The server accepts `count=1` and returns HTTP 201 with a list containing
`{"sandbox": {"sandboxID": ...}}`. Use the returned ID with the SDK:

```python
import json
import urllib.request
from e2b import Sandbox

def fork_running(parent, *, api_key, api_url, timeout=600, **sdk_options):
    request = urllib.request.Request(
        api_url.rstrip("/") + "/sandboxes/" + parent.sandbox_id + "/fork",
        data=json.dumps({"count": 1, "timeout": timeout}).encode(),
        headers={"X-API-Key": api_key, "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=150) as response:
        child_id = json.load(response)[0]["sandbox"]["sandboxID"]
    return Sandbox.connect(
        child_id, timeout=timeout, api_key=api_key, api_url=api_url, **sdk_options
    )

options = dict(api_key="<key>", api_url="http://127.0.0.1:18887",
               sandbox_url="http://127.0.0.1:18887", request_timeout=150)
```

This endpoint derives the running parent's current state; it does not claim a
template-pool child. For end-to-end measurements, start the timer before the
HTTP request and retain SDK connection and command startup in the timed path.

Network readiness is separate from execution readiness:

```python
child = fork_running(parent, **options)
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

## Durable suspension and writable application storage (113 feature branch)

The VM backend supports SDK `pause()`/`beta_pause()` and `connect()` for
same-identity suspension. Pause joins that sandbox's CNI preparation, takes the
guest execution barrier, pauses/drains block devices, captures guest RAM/device
state and the writable disk, and syncs files and directory entries before
removing the VM's KVM objects and RAM. The VMM API/controller process remains.
Resume uses private CoW RAM mappings and a new private QCOW write layer, keeps
the sandbox ID/CID/MAC, reconnects the agent, and releases the saved barrier.
Paused sandboxes reject commands, report unhealthy to SDK `is_running()`, do
not expire under the running timeout, and can be explicitly deleted.

This operation intentionally writes a memory snapshot. Normal running-parent
fork still inherits RAM with CoW and does not write a checkpoint. Snapshot-pool
creation continues to use its prepared file-backed templates. Do not confuse
these three paths when measuring startup or storage.

Set `E2B_WRITABLE_BASE` to a sealed, read-only ext4 raw image to enable persistent
application writes for image-pool sandboxes. Each prepared generic slot owns a
private QCOW leaf with a reflink of that empty filesystem. The updated
`image-mount.c` puts overlay upper/work on the same ext4 mount. This requires
`qemu-img`, a host filesystem supporting reflinks, and the existing writable
QCOW fork support. Templates remain immutable and image-independent slots are
not split into per-image VM pools. Metadata `kuasar.filesystem.backing` reports
`qcow2-ext4`; without this setting the existing guest RAM overlay remains.

Checkpoint export first reflinks the managed disk layers while IO is paused,
then flattens those independent copies. It does not bypass the VMM's exclusive
file locks. Publication uses file and directory persistence barriers. One
writable application disk is supported. Guest RAM may contain dirty filesystem
pages; restoring the matching RAM and disk checkpoint preserves that state.

Scope: E2B direct-VM guests with no containerd container metadata. This is not
CRI Pod checkpointing. Version 2 checkpoints support recovery after a host boot
change or loss of the paused VMM controller. Persist the sandbox state, SQLite
DB plus its WAL/journal, image templates, writable bases and CNI IPAM state on
mounted durable filesystems before starting the sandboxer/adapter. Recreate the
adapter's own network namespace before starting it when deployment uses one.
The boot supervisor must reconcile its private seed lease; the isolated Linux
host fixture under `validation/113-reboot` demonstrates these dependencies.

Pause each sandbox and wait for successful acknowledgment **before rebooting**.
Startup retains durable checkpoints, rejects prior-boot PIDs, rebuilds private
namespaces/TAPs/controllers, and keeps sandboxes paused. SDK `connect()` restores
the saved processes, RAM and file state under their original sandbox identity.
The CNI plugin must restore the saved IP; failure preserves the checkpoint and
blocks resume for retry. The VMM's saved device MAC and the guest/CNI runtime MAC
are distinct identities; device state is preserved while the host endpoint is
checked against the saved runtime network. Stale TCP state is reset before the
host network gate opens. New controllers rejoin normal lifecycle monitoring.

This restores the last successfully persisted **paused** state. It does not
save uncheckpointed running RAM on arbitrary power loss or continuously persist
changes after resume. Version 1 checkpoints lack the boot/identity metadata for
cold recovery; create a version 2 checkpoint before an upgrade/reboot test.
Pausing consumes durable snapshot capacity and is not a constant-time operation.
The tests include a nested Linux kernel reboot and forced reset; this is not a
physical-device power-loss certification.

---

# Explicit Kubernetes backend

This adapter implements a restricted E2B API using Kubernetes Pods backed by the
`sandbox_fork` branches of containerd, Kuasar and Cloud Hypervisor. It is tested
through the E2B-compatible API. The current 113 benchmark environment uses
**E2B Python SDK 2.25.1** plus the HTTP fork extension documented below. It does not run the upstream E2B
orchestrator or envd, and does not create a memory snapshot file.

Supported operations:

- `Sandbox.create`: derive a new Pod from a configured, running template sandbox.
- `sandbox.commands.run`: execute a command through kubelet/CRI, returning guest
  PID, stdout, stderr and exit status using E2B's Connect JSON stream protocol.
- `sandbox.files.write/read`: transfer text or binary files into the guest
  container, including multipart and octet-stream uploads.
- `POST /sandboxes/{parent_id}/fork`: derive another Pod from the running source Pod UID.
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
child = fork_running(parent, **options)
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

## Optional seed execution-cache preparation

`E2B_PREPARE_EXEC_CACHE=1` reads the fixed Debian shell-launch dependencies listed
in `exec_cache.py` into the source guest's file cache during adapter startup,
before serving requests. Reads are limited to 4 MiB per regular file; optional
missing files are skipped. Profiles are read, never sourced, and no user command
or Python interpreter is started in advance. Every SDK command still launches
its requested executable and preserves login-shell behavior. This preparation
is suitable for the fixed Debian Python template; it is not a universal image
optimizer and is disabled unless explicitly requested.

The elapsed preparation time and files read are recorded beside adapter state as
`<state>.exec-preparation.json`. Report this cost separately from warm fork
latency. The optimization uses guest file-cache pages inherited through the
existing RAM CoW path; it creates no memory image and does not copy guest RAM.
New commands or different executable dependencies may still incur cold faults.


## Fixed-template CoW child pool

Set `E2B_TEMPLATE_POOL_SIZE` (1..128; default 0, disabled) on the VM adapter
for fixed-template creates. Startup derives private children with the existing
live CoW path, including KVM/vCPU/device restoration, guest-agent reconnection,
and independent CNI namespaces/MAC/IP identities. It waits for network readiness
before publishing a slot. It never executes a user command in preparation.
The existing seed cache option remains separate. No memory image or full RAM
copy is introduced. Existing VM safety and disk persistence barriers remain.

A create durably assigns a single-use slot, fresh access token, metadata and
expiry before responding. Ready/preparing/quarantined slots are not accessible
through sandbox or guest APIs. Capacity targets unclaimed reserve VMs, not
running user sandboxes. A successful durable assignment removes the claimed
slot from this budget, including its private image-binding phase, and wakes
bounded parallel background refill immediately. Each replacement gets its own VM and
network identity; the claimed VM is never recycled. Running instances remain
owned and recoverable until explicit deletion or expiry. Destruction does not
open a second vacancy for an already claimed slot. A failed assignment remains
quarantined and unavailable; safety and persistence barriers are unchanged.
Exhaustion waits up to 45 seconds and returns 503 if no slot becomes available.
`E2B_TEMPLATE_POOL_WORKERS` controls concurrent refill preparations (1..64,
VM-adapter default 8). Capacity reservations are made under the same lock as
assignment, before creating resources, so workers cannot overfill the reserve.
A claim opens its vacancy only after the durable assignment commits. Ready
publication still waits for device/network preparation and its durable commit.
Claim waiters and refill workers use separate conditions; one new Ready slot
wakes one waiting claim instead of waking every pending request. Draining joins
all in-flight preparations, and never publishes their slots after disabling.
Native RAM-less pool preparation hands the global transaction lock to the
registered sandbox's own lifecycle lock before network/KVM preparation. The
source configuration has already been copied; live-parent fork retains its
original safety gate. Per-slot preparation events report phase durations and
keep each vacancy associated with its worker even when jobs finish out of order.
There is no artificial normal-path refill delay. The legacy
`E2B_TEMPLATE_POOL_REFILL_DELAY_MS` setting is no longer consumed;
`refill_delay_ms` remains zero in status for compatibility. Preparation failures
retain a retry backoff. Status separates `reserves`, `binding`, `assigned` and
`owned`; `capacity_scope` is `unclaimed_reserves`. Refill events identify the
claim via `vacancy_sid`; `replacement_ms` starts at durable claim completion.
The HTTP accept backlog is 512 so concurrent clients can reach admission;
this does not bypass assignment or image-publication barriers.
Report actual preparation work and client-observed recovery separately.
There is no tenant recycling. DELETE remains synchronous: it confirms VM exit,
CNI DEL, namespace cleanup and durable record removal before returning.

Optional `E2B_SLOT_NETWORK_POOL=1` retains network resources for direct file-
snapshot slots. It requires a single IPv4 `bridge` / `host-local` CNI config,
no NAT or CNI extensions, and `conntrack` (override with `E2B_CONNTRACK_BIN`).
Unsupported configurations fail closed when enabled; leave it disabled for the
ordinary per-sandbox CNI lifecycle. It does not change explicit live-parent forks.

A network slot owns its namespace holder, veth, persistent TAP and CNI allocation.
Its IP is stable across generations, but each replacement gets a fresh sandbox
ID, access token, CID and MAC. After confirmed VM exit, old redirects and
neighbour entries are cleared, and conntrack is flushed
inside the owned namespace. Host-side deletion selects only that slot's IP;
it never flushes the host conntrack table. Independent cleanup operations run
concurrently and are all joined, even on failure, before ownership can move.
Late entries are cleared again on
claim. Administrative link state and CNI routes are retained: there is no
link down/up cycle. Routes are captured in the durable slot record. The new MAC is announced through
ARP notification. Snapshot identity
reset and the TAP gate remain prerequisites for forwarding to the new guest.
Snapshot connection setup verifies a read-only guest `Check` before sending the
identity mutation. Transport probes may reconnect; identity changes are never
blindly retried, and the existing overall/identity deadlines remain unchanged.

Retirement removes the old sandbox identity and durably creates a private idle
network record in one transaction. Claim atomically transfers ownership and
increments its generation. Failed handoff commits quarantine the resource.
Idle resources are inaccessible through sandbox APIs, are reclaimed on adapter
recovery, and are fully removed by pool drain. DELETE remains synchronous for VM
retirement and network reset; it no longer removes retained pool namespaces.
Report this separately from asynchronous delete acknowledgment and from draining
all pool-owned network resources. Reusing an IP is not a promise to reject all
future unsolicited traffic to that address; old guest processes, sockets and
connection-tracking state are not transferred to the next generation.

Explicit `/sandboxes/{parent}/fork` always derives the parent's current state;
it never consumes a template slot. At adapter recovery, private slots are
reconciled and destroyed before replacement, while leased records retain normal
sandbox recovery/expiry semantics and continue counting against capacity.
Assignment acknowledgment failures quarantine the slot instead of reissuing it.

API-key authenticated `GET /template-pool` exposes counts/preparation time.
`POST /template-pool` with `{"drain":true}` disables refill, settles preparation,
and destroys private slots only; already assigned sandboxes and live-parent
forks survive. Following drain, template creates use the ordinary live path.
`<state>.pool-events.jsonl` records preparation, assignment and replacement times
without tokens. Pool warmup, refill and PSS/FD occupancy must be reported apart
from request-to-output latency. This is a fixed-template warm-pool comparison,
not a reduction in the cost of an arbitrary live-parent fork.


## Pooled request path measurements and execution

`E2B_TRACE_TIMING=1` emits pool lock/assignment, durable commit/projection,
HTTP, vsock RPC and guest execution timings. Use separate diagnostic runs;
normal latency comparisons keep this disabled. Guest intervals include scheduling
and first-touch effects; they are not pure CPU time or page-fault counts.

The SQLite transaction still commits with synchronous EXTRA before a lease is
acknowledged. It now serializes state once and writes that exact committed JSON
to the atomic compatibility projection, avoiding a second serialization and many
small Python writes. Startup still regenerates a missing/stale projection from
SQLite; failures retain existing quarantine/reconciliation behavior.

VM execution keeps the cwd shell and the SDK's requested command/login shell.
An empty environment override skips the redundant busybox env executable;
nonempty overrides retain their prior handling. After the unary guest RPC has
returned all output, the adapter writes the same Connect frames in one response
body with Content-Length, allowing HTTP connection reuse. No command, interpreter,
login profile or command output is precomputed by these changes. The image,
guest agent, CoW path and persistence barriers are unchanged.

## Generic runtime CoW pool (experimental)

Set `E2B_IMAGE_CATALOG=/absolute/path/images.json` together with
`E2B_TEMPLATE_POOL_SIZE` to select `ImagePoolBackend`. The source VM must be a
minimal common Linux runtime with the guest agent, `/bin/sh`, `/bin/bash`, a
static `/bin/busybox`, virtio disk hotplug, ext4 and overlayfs support. Its root
is the only disk; the application disk occupies `/dev/vdb`. Prepare the common
runtime once; do not put application executables or a target-image container in
it. KVM, guest-agent connections and independent networking are prepared in the
same single pool regardless of the selected application image.

The catalog maps template IDs to content-pinned application filesystem disks:

```json
{"base":{"disk":"/images/python.ext4","sha256":"<disk SHA-256>","manifest":"sha256:<OCI manifest digest>"},
 "other":{"disk":"/images/other.ext4","sha256":"<disk SHA-256>","manifest":"sha256:<OCI manifest digest>"}}
```

Disks must be read-only regular files controlled by the deployment operator.
Startup verifies their full SHA-256 and records file identity; claim rejects a
changed identity, size or modification timestamp. Keep catalog files and disks
immutable while the service runs. Disk construction and catalog verification
are preparation costs; verification time is saved beside adapter state.

A create claims a generic slot into durable private `binding` state, attaches
its selected read-only disk through Cloud Hypervisor's existing hotplug API,
then mounts an overlay with a private guest-RAM upper layer. Only after the
mount succeeds and image metadata is durably committed is the sandbox published.
Commands, including SDK file operations, execute inside that image using chroot.
No interpreter or user command is executed to warm the slot. Failed binding
quarantines and destroys the slot. Recovery destroys unfinished private bindings;
a bound VM is never reassigned to a different tenant or image.

This interface provides a Linux command environment from an application's
filesystem. Legacy disk-only entries need a compatible architecture, `/bin/sh`, and the commands
required by the SDK; a static busybox helper is supplied for SDK operations.
Disk-only catalog entries are rejected: every image must use the OCI import
path and carry execution configuration. The common
guest kernel and runtime remain shared implementation choices. Per-image disk
artifacts still require preparation, but no per-image VM pool is required.
The writable overlay is guest RAM, matching the existing read-only-root research
configuration; it is not host-persistent application storage.

An explicit live-parent fork bypasses the generic queue and inherits the parent's
current mounted image, writable overlay, memory and processes through the existing
fork path. Changing template ID in that request is rejected. Existing fork safety
barriers, filesystem synchronization and durable assignment commits are retained.
No `memory.img` or eager RAM copy is added. Network identities remain independent.
Pool exhaustion returns an error after the existing bounded wait rather than
silently returning the bare runtime. E2B_FIXED_IMAGE and image-specific
startup_read_files are rejected. There are no executable/library pre-read lists.
E2B_TEMPLATE_READ_CACHE_BYTES optionally applies the same 0–512 MiB regular-file
read budget to every image tree during template preparation; zero (the default)
only walks metadata. This requires E2B_TEMPLATE_METADATA=1. No language or path
list selects files, no interpreter executes, symlinks and non-regular files are
skipped, and reads stay on the root filesystem. Report actual bytes, elapsed
preparation time and cache/resource costs separately from request latency.

## OCI import and application startup

`oci_import.py SOURCE --name IMAGE_ID --store DIR --catalog FILE` imports a
skopeo transport (for example a digest-pinned `docker://` reference or an OCI
layout). It selects the host architecture (or explicit `--architecture`),
requires Linux and verifies manifest/config/layer descriptors. It unpacks
whiteouts with umoci, constructs a read-only disk, and records both
manifest and config digests. It fsyncs the disk and metadata before atomically
publishing the catalog under a file lock. Failed imports never publish an ID.
All images use the same importer. `--filesystem ext4` is the default; optional
`erofs` requires a mkfs.erofs supporting `--preserve-mtime`. The filesystem
choice is explicit, never inferred from an image name or application files.
The hidden path `/.__kuasar` is reserved for runtime file-operation helpers.

The common runtime includes a statically linked `oci-launch` built from
`oci-launch.c` (`gcc -static -O2 -Wall -Wextra -Werror`). This program is in the
common runtime, not an image-specific pool. OCI entries have `oci_config` with
`Env`, `WorkingDir`, `User`, `Entrypoint` and `Cmd`. Startup uses the literal
concatenation of Entrypoint and Cmd (Cmd alone when Entrypoint is empty), clears
the launcher environment, applies image environment and working directory, and
resolves numeric/named users and groups inside the application root. An explicit
group clears supplementary memberships; otherwise image group memberships are
used. Unknown named users fail startup. OCI entrypoints do not require a shell
in the image; shell-form Docker instructions already encode their interpreter
in the image's argv. Image architecture and required kernel features must still
match the common runtime.

Creation remains private until disk binding and exec acknowledgment succeed and
ownership/config/PID metadata is durably committed. A guest supervisor owns the
entrypoint, redirects stdout/stderr, waits for it, and atomically writes its exit
status. All descendants are owned by the VM lifecycle. Create acknowledgment
means exec succeeded, not that an application-specific health check passed.
Authenticated `GET /sandboxes/ID/entrypoint` reports PID, running/exit status, and
the first 64 KiB of each output stream. Application readiness must be checked
separately. The guest RAM logs currently have no rotation policy.

Native process RPCs use image ENV/WORKDIR/USER defaults, with request environment,
cwd and explicitly authenticated user overrides. SDK shell commands still need
the requested shell in the image; a distroless image can use native argv instead.
File RPCs use the reserved static helper, so they do not require image base64 or
shell utilities. Catalog entries without OCI configuration must be reimported.

After publishing a new immutable image ID, API-key-authenticated
`POST /images/reload` with `{}` validates and adds it without draining or rebuilding
any prepared VM. Existing IDs cannot be rebound or removed through this operation;
use versioned IDs and retain artifacts referenced by running sandboxes. Validation
runs outside the lifecycle lock. With preattached devices, cache copying, checksum
verification and fsync finish before an ID is published; an import that exceeds
the existing device capacity is rejected without rebuilding slots. New file
identities become visible before their
catalog entries. A running-parent fork inherits the saved OCI config, entrypoint
PID, process state and writable overlay. It does not execute the entrypoint again.

This implements the five image configuration fields above, not every OCI runtime
facility: PID 1/container signal semantics, OCI volumes, capabilities policy,
healthcheck recipes and host-persistent writable image volumes are not added.
The existing guest-RAM writable overlay, VM isolation, fork safety/sync barriers
and durable host metadata remain. Disk/image preparation, common VM pool warmup,
application startup and background refill costs must be reported separately.

Generic-pool mount helper
-------------------------

`image-mount.c` replaces the guest shell's chain of mount utilities with a
single, image-independent static program. Build with
`gcc -static -pthread -O2 -Wall -Wextra -Werror image-mount.c -o image-mount`.
Install it into the seed before preparing any slots, then set
`E2B_IMAGE_MOUNT_HELPER` to its absolute guest path. The existing shell path
remains available when this variable is unset. A configured helper failure is
fatal to the private binding; there is no fallback that publishes a partially
mounted image.

The helper waits for an openable block device with nonzero capacity, checks the
application disk serial, mounts its lower filesystem read-only, and creates a
private overlay. Device-node existence alone is insufficient during hotplug.
The five-second readiness deadline is retained. OCI reserved paths and runtime
mount targets reject symlinks. Image assignment and image identity now share
one durable private-binding transaction, followed by the existing publication
transaction after successful mounting/entrypoint startup. SQLite EXTRA and
crash-recovery quarantine semantics are unchanged.

### Generic prepared block devices

`E2B_PREPARE_IMAGE_DEVICE=1` prepares one empty, read-only virtio block device
per generic pool slot. It requires `E2B_IMAGE_MOUNT_HELPER` and a reflink-capable
filesystem for `slot-disks` next to adapter state. The device capacity is at
least 16 GiB and grows to fit the startup catalog; disk images are page-aligned
raw ext4 or EROFS files (with the matching filesystem driver prepared in the seed). There is one VM pool for the entire catalog, not one per image.

Image import caches and verifies immutable disk content on that filesystem.
During slot preparation, ownership is committed before creating/attaching the
sparse device, and the guest waits for usable capacity and prepares read-ahead.
During a claim, durable private assignment precedes FICLONERANGE; sealing and
fsync precede guest BLKFLSBUF and mounting. No active or previously leased disk
is ever rebound. A bind/mount/publish failure quarantines and destroys the slot.
Images remain read-only; per-guest overlay writes and inherited RAM retain CoW.
Explicit live-parent forks continue to inherit the running state and keep a
reference to the immutable bound disk until the last descendant is destroyed.
Do not modify these backing files outside this lifecycle.

The OCI launcher preserves the requested cwd, literal environment values, argv,
user and login shell. No user command or login profile runs during preparation,
and there are no language-specific launch-file hints. Disk readiness and
image-independent code loading are pool preparation costs. The direct image-bind
path still performs binding, fsync, cache invalidation, mounts and command
execution inside the request; the file-template path below separately accounts
for filesystem preparation and its optional uniform file cache.


### Measured storage variants on host 108

The deployed adapter uses `E2B_SQLITE_JOURNAL=WAL` with synchronous=EXTRA.
Keep the database plus any `-wal` and `-shm` files together; use SQLite backup
for a live migration. Do not recover from the JSON view alone. Crash tests cover
uncommitted transactions and committed transactions before JSON publication.

The static mount helper detects EROFS magic or falls back to ext4. Host image
conversion must preserve file contents, ownership, modes, symlinks, xattrs and
mtime; the measured EROFS conversion used erofs-utils `--preserve-mtime` and
compared filesystem manifests before deployment. This is a storage encoding
change to the same OCI filesystem, not a new VM template per image. Generic
filesystem kernel modules are loaded before the pool is forked. Compression,
native-directory SQLite and loop-device indirection were experiments; the
selected deployment retains uncompressed EROFS, normal reflink slot disks and
WAL beside the existing adapter state. Full experiment data and failed variants
are under `results/fork-opt3-108` in the host's sixway_startup_108 workspace.

### File-backed image templates and one generic resource pool

`E2B_SNAPSHOT_POOL=1` selects the experimental file-template path. The sandboxer
and adapter must share `KUASAR_SNAPSHOT_POOL_ROOT`, an administrator-owned
persistent directory. This is a same-host, same-runtime-profile facility for
read-only application disks; it does not change explicit live-parent fork.

The adapter materializes each image once in a temporary builder sandbox. It
mounts the image and optionally walks filesystem metadata. The file-read limit
defaults to zero; E2B_TEMPLATE_READ_CACHE_BYTES can enable uniform, bounded reads
of regular files for every image, with actual bytes and preparation time recorded.
It does **not** run ENTRYPOINT/CMD, an interpreter, or application code.
It then asks
`sandbox-snapshot-capture` to freeze the guest agent, pause the VM, save CPU,
device and memory state, and take independently owned disk copies (reflinks
where available). Files are sealed and synchronized before a manifest is
published and its directory synchronized. The builder is resumed and destroyed.
A template is a set of files, not a parked VM or an image-specific VM pool.
Cloud Hypervisor calls the memory file `memory-ranges`; it has the same role as
a memory image. Template generation and host-cache warming are preparation costs.
By default they run on the first request and are included in its latency.
`E2B_EAGER_IMAGE_TEMPLATES=1` explicitly moves this work to service startup;
report it separately. No application is started during either preparation mode.

The existing single pool prepares network namespaces, CNI, endpoint identities,
and VMM processes without selecting an application image. The
`sandbox-snapshot-prepare` action retires the temporary guest before a slot
becomes Ready. Thus Ready resource slots have an API process and network
resources and a fresh, never-run KVM VM with parked vCPU objects, but no
running application guest or connected guest agent. `vmm.prepare` creates
these objects without an image or RAM mapping. Restore requires an exact CPU
and hypervisor profile match and consumes the prepared objects once. Delete
and failed restore release them. This preparation currently supports x86_64
KVM without confidential-computing extensions.

A create request durably leases one private slot, validates prepared restore
metadata (or stages it on a cache miss), and invokes
`vm.restore` with `CopyOnWrite` and no full-memory prefault. Guest RAM is a
private writable mapping of the immutable memory file. Restore, first guest
faults, transport connection, entropy/network reset and final durable publication
remain inside the measured create-to-command path. OCI ENTRYPOINT/CMD executes
after binding, before public lease publication; every fresh create uses the
same ENV/WORKDIR/USER and startup semantics. Live-parent fork instead inherits
the already-running processes and never restarts the entrypoint. The required ForkControl
response checks guest responsiveness and its barrier token; an extra empty
Check RPC is unnecessary. Event polling starts after identity reset. Network forwarding stays
blocked until the restored guest has the slot's independent identity. Failure
quarantines and destroys the slot; it is never returned to Ready. The normal
replacement worker replenishes the same generic pool.

The immutable template files remain pinned on disk after user sandboxes are
destroyed. This initial implementation rebuilds templates on adapter startup;
there is no automatic template-cache eviction. Do not remove template files
while any restored VM or descendant might still reference them. New image IDs
can be imported through the normal catalog route; their first use includes
preparation unless explicitly prepared beforehand. It does not allocate a
second VM pool. Explicit forks inherit the running process/filesystem state
through the original live-fork path, rather than restoring an older template.

With file snapshots enabled, generic pool slots use `sandbox-pool-prepare`:
CNI and a fresh network identity are prepared first, then an API-only VMM calls
`vmm.prepare` to create KVM/vCPU resources. No temporary guest is booted, forked,
or deleted, and no application disk is attached to this empty VMM. The durable
slot configuration retains independent vsock/serial endpoints and MAC; network
forwarding stays blocked through template restore and identity reset. This path
requires the matching sandboxer with direct preparation support; failures are
cleaned up rather than silently falling back to a live guest.

Template capture and explicit running-parent forks keep their existing guest
and device readiness barriers. Live-memory pools retain their existing device
preparation path. Capacity and persistence rules are unchanged.

Preparation metrics are in `STATE.snapshot-preparation.json`; refill and total
replacement latency remain in `STATE.pool-events.jsonl`. Snapshot binding logs
include staging, KVM/device restore, resume, connection and identity phases.
Compare the complete client-observed request-to-READY interval, not those
internal phases alone. File templates are not automatically faster than the
running generic pool: cache faults and guest reconnection can offset saved
mount work.

Snapshot restore keeps a pending hvsock CONNECT handshake for up to 100 ms,
within the unchanged 45-second overall connection deadline. This avoids
throwing away a late guest response after 10 ms and then sleeping another
10 ms before reconnecting. `KUASAR_SNAPSHOT_HANDSHAKE_MS` (bounded to 10–100)
is a diagnostic override for this path only; live-fork reconnect is unchanged.
The normal ForkControl token, entropy and network checks are still required.
`KUASAR_SNAPSHOT_PROFILE` adds per-process CPU/fault sampling to phase logs;
leave it unset during performance comparisons.

### Durable pool metadata and restore-file synchronization

The SQLite store requires SQLite 3.38+ with JSON support. It serializes the
state document once, then updates only changed `state_records` rows in one
transaction. Deletion and replacement are atomic together. `synchronous=EXTRA`
and normal WAL auto-checkpoint settings are retained. A compatibility `state`
view exposes the current document to readers; the legacy single-row database
is migrated transactionally. Old code can read the view but cannot write it;
do not downgrade the writer without an explicit schema conversion.

Metadata cache misses write both configuration files before synchronizing them concurrently
on blocking workers. It awaits both barriers, then synchronizes the restore
directory and its parent before `vm.restore`. No successful restore/publication
can bypass a failed barrier. Parallel sync permits filesystem flush coalescing
and avoids blocking the async executor; it does not guarantee bounded storage
latency on a shared host.

`E2B_DURABLE_SQLITE_ROOT` optionally places newly created SQLite databases on a
separate **persistent** filesystem from bulk image/CoW data. The administrator
must provision and durably create this private directory first. Every new state
generation creates a unique 0600 database and a durable symlink at the original
`.sqlite3` path. SQLite opens the canonical target, keeping WAL/SHM beside it.
Existing databases are not automatically moved. Reopening follows the symlink
even if the option is later unset; a missing target fails closed instead of
reconstructing an authoritative DB from a potentially stale JSON projection.

Backups/archives must preserve the symlink target and its SQLite sidecars, not
only the symlink or JSON projection. Archived generations keep distinct targets;
do not remove a target still referenced by live or archived state. This option
retains persistence; tmpfs is inappropriate. It does not relocate image disks,
memory templates or guest RAM and does not create per-image VM pools.

### Preparing restore metadata before a lease

For each known immutable image template, the generic pool can now prepare the
small CPU/device-state and configuration files for a slot's existing MAC, CID
and transport paths. `sandbox-snapshot-stage` creates these files under a unique
temporary directory, synchronizes file contents and the directory, then renames
it atomically and synchronizes the parent. A binding document records the exact
template directory and generic-slot configuration. A hit validates that binding
and re-synchronizes the parent (covering a crash after rename but before its
barrier). Published files are never rewritten by this path.

Existing Ready slots are privately withdrawn while staging, and become Ready
only after the stage operation and durable state update succeed. A failed stage
quarantines/destroys the slot. Refills stage known templates before Ready. Newly
imported images can miss the cache during concurrent pool changes; the same safe
staging path then runs inside the measured create request. Mismatches fail closed.
All metadata is under the sandbox directory and removed by normal destruction.

This creates small per-image metadata files, NOT per-image VM pools or RAM
copies. The single generic slot pool and running-parent fork semantics remain.
Template preparation reports `metadata_stage_ms` and `metadata_staged_slots`;
refill events include staging. The request phase `stage_cache_validate` identifies
hits; misses retain write/sync/publish phases in request accounting.

Lease/publication code uses `save_record(sid)` under the existing lifecycle lock:
only that record is serialized and committed. The JSON projection is rebuilt
from committed database rows before return, so other uncommitted in-memory
records cannot leak into it. Full save remains available for reconciliation and
multi-record changes. No transaction durability or guest identity barrier is
removed, and the projection stays current for existing administration tools.

### Empty-slot startup notification

`KUASAR_POOL_BOOTSTRAP=1` opts the native sandboxer into the matching Cloud
Hypervisor staging build's `--prepare-config` and `--prepare-ready-fd` interface.
Leave it unset with older VMM binaries. A mismatched binary fails preparation;
it does not silently fall back or publish a partially prepared slot.

The native sandboxer first durably writes the generic slot configuration, then
starts Cloud Hypervisor with that configuration and a one-shot inherited Unix
socket. Cloud Hypervisor prepares the KVM VM and vCPUs without guest RAM and
writes byte `1` only after preparation succeeds. Both ends close the notification
FD; EOF, an invalid byte, or the existing ten-second deadline fails preparation.
This replaces API-readiness polling plus the external `vmm.prepare` RPC. The
existing network-cleanup join, template staging, and durable Ready publication
remain inside measured refill work. It does not change running-parent fork or
create a VM pool per image.

### Restore immutable templates using the prepared slot identity

`KUASAR_PREPARED_IDENTITY=1` requires a matching Cloud Hypervisor build advertising
`prepared_identity_v1` in `vmm.ping`. It avoids creating a per-slot restore bundle:
the VMM reads the sealed original template and applies the already prepared slot's
NIC, vsock, serial and console identity to its private in-memory configuration.
Only the matching NIC's snapshot MAC is changed. CPU state, disk paths and memory
contents remain those of the template; guest RAM uses private file CoW.

The slot's `generic-slot.json` is synchronized before the prepared slot is
published. The selected
`snapshot_template` key is committed with the private lease before restoration;
the existing identity/network barriers still precede public Ready publication.
Unsupported VMMs and incompatible templates fail preparation. This path requires
plain private RAM, read-only template disks, and one matching NIC plus vsock. It
does not change running-parent fork or allocate a separate VM pool for each image.

The startup-notification experiment above is independent and remains disabled by
default: measurements did not establish an advantage over the existing API path.

### Native network preparation for generic slots

Direct generic-slot preparation now requests `network_identity="fresh"` from
`sandbox-pool-prepare`. The sandboxer changes the owned veth MAC through netlink
before discovering the guest NIC configuration. The adapter requires the matching
acknowledgement; older native binaries fail closed before a slot can be published.
Running-parent fork retains its existing identity path.

Ingress qdiscs, bidirectional redirect filters and the priority-1 drop barrier
are installed directly through netlink, with kernel acknowledgements and exclusive
creation. Removing the barrier preserves the priority-100 redirects. No `tc` or
`nsenter + ip` subprocess is needed for this preparation path. Cleanup still joins
before restoration; the old VM is reaped, stale connections are cleared, and the
slot configuration and ownership records remain durable before publication.

`CH_FORK_TIMING=1` additionally reports `VM_POOL_PREPARE` stages (validation,
MSR profile, KVM VM, each vCPU and parking). These are nested within the host's
`POOL_VMM_PHASE` and `POOL_PREPARE_PHASE` timers, not additional elapsed work.

### Parallel preparation and optional background WAL checkpoints

The API-based empty-slot path overlaps host VMM startup with writing and syncing
`generic-slot.json` and its directory. Both operations are joined, including on
error, before returning or cleaning up the slot. This starts no guest and exposes
no Ready slot before durability succeeds. The optional startup-notification path
still persists the file first because that VMM reads it during startup.
`persist_config` overlaps `spawn_monitor`/`api_prepare`; do not sum their durations.

`E2B_SQLITE_CHECKPOINT=background` opts WAL deployments into an independent
PASSIVE checkpointer. A non-inherited owner lock retains single-adapter ownership
while SQLite NORMAL locking permits the checkpoint connection. Every commit still uses `synchronous=EXTRA` and synchronizes
the WAL before acknowledgement. Only checkpoint/reclamation work is asynchronous;
this is not asynchronous persistence. PERSIST mode and the default WAL mode retain
the existing behavior. The worker wakes after 64 changed commits or a 250 ms idle
interval. At 1024 uncheckpointed commits, writers wait for progress and fail after
30 seconds if progress is impossible. Worker errors fail subsequent writes. This
bounds pending commit count, not record size; large individual records still need
application-level limits. `DurableState.close()` joins the worker before closing the writer; process
termination can leave WAL recovery work, as before.

`wal_checkpoint` timing events report elapsed work and log/checkpointed frame
counts separately. Report their cost alongside preparation/refill latency: work
performed by the worker must not be presented as eliminated work. Readiness still
requires durable ownership, durable slot configuration and the guest/network
identity barriers. WAL/SHM sidecars remain part of crash recovery.

Background checkpointing remains opt-in: the measured comparison did not establish
a consistent advantage over parallel preparation with automatic checkpointing.

### Prepared OCI launcher and readiness

File templates include an idle, single-threaded OCI supervisor and a launcher
child blocked on a private pipe. Preparation opens runtime logs and creates that
launcher child, but does not chroot into the application image, resolve its user,
read its files or execute ENTRYPOINT/CMD. The optional uniform image cache is a
separate preparation step. VM slots remain image-independent. The launcher PID
exists in the template; the application executable starts only after the request.

After identity release, the adapter calls `StartOCIProcess` while the lease is
private. The agent takes the same execution/freeze gate as normal exec. The
supervisor sends the validated OCI argv to its waiting child, which resolves
USER/GROUP, ENV and WORKDIR and calls exec. The reply requires the close-on-exec
handshake; a failed exec cannot publish the sandbox. Client cancellation or the
RPC deadline leaves the gate owned by the launch task until this handshake
finishes. Failed leases are quarantined and destroyed by the adapter.

The supervisor reaps the application and retains the fsync/rename/fsync protocol
for its exit status. Running-parent fork inherits the existing application and
supervisor rather than starting ENTRYPOINT again. Non-snapshot creation keeps the
legacy launcher. Deploy the matching guest agent and launcher, then rebuild file
templates; old templates do not advertise `prepared_oci` and use the legacy RPC.

`oci_entrypoint_start` reports request-time startup. Snapshot preparation records
`runtime_preparation_ms` separately. API-ready means the application exec handshake
and durable publication have completed. Benchmark `native_ready_ms` additionally
includes the first command's shell marker; interpreter-print additionally includes
interpreter startup and its random marker. None of these three endpoints are
interchangeable. Do not compare an old template containing an already-running
ENTRYPOINT to fresh application startup without identifying that difference.

Native worker tests must run in a private PID/mount namespace (PID 1), with
`OCI_WORKER_TEST_BINARY` and `OCI_WORKER_TEST_PROBE` set. The probe is a statically
linked fixture that reports uid/gid/cwd/environment/groups and exits with code 7.

The runtime preparation timer is included in template preparation, not an
additional cost to sum. Reproduce the isolated native tests from the repository:

```sh
test_dir=$(mktemp -d)
gcc -O2 -static -o "$test_dir/oci-launch" integrations/e2b/oci-launch.c
gcc -O2 -static -o "$test_dir/probe" integrations/e2b/testdata/oci-worker-probe.c
sudo unshare --mount --pid --fork --mount-proc env \
  OCI_WORKER_TEST_BINARY="$test_dir/oci-launch" \
  OCI_WORKER_TEST_PROBE="$test_dir/probe" \
  python3 integrations/e2b/test_oci_worker.py -v
```


## Manifest-backed snapshot creation

Set `E2B_MANIFEST_RAW` to the accelerator `manifest-raw` binary and
`E2B_MANIFEST_CONFIG` to a private Manifest configuration file. Existing native
image templates are published through the existing chunking, compression,
encryption and deduplication pipeline during preparation. Request restoration
with `snapshotMode: "manifest"` on HTTP create, or SDK metadata
`{"kuasar.snapshot.mode": "manifest"}`. Native creation remains the default.

This initial backend eagerly reconstructs the entire sparse template into a
private directory before VM binding. It verifies fetched content and syncs files
and directories before atomic publication. These costs are inside create timing;
this is not UFFD demand paging. Live-parent HTTP fork still inherits running
state. Descendants retain references to a materialized template until its last
owner is destroyed. Do not compare this path to lazy restoration without stating
this implementation difference.

For chunk-source measurements, enable the accelerator cache's optional RocksDB
statistics. Ordinary Block Cache hits are not proof of chunk payload residency.
The report records successful payload reads classified as memory, RocksDB file,
origin or unknown, both by request count and encoded/encrypted returned bytes.
RocksDB file reads do not necessarily imply physical device IO.

### Initial reserve preparation

`E2B_TEMPLATE_POOL_WORKERS` bounds both initial preparation and later refill.
Startup prepares reserves in parallel waves and waits for every wave to settle
before publishing completion. A pending reservation can conservatively overlap
its newly persisted record, so startup fills any remaining vacancy in a later
wave instead of assuming each submitted job created a slot. Each slot still
requires successful device/network readiness and a durable Ready commit.
Running instances do not consume the reserve budget after durable assignment.
