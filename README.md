# ReliablePool

ReliablePool was introduced in **2022** as part of the **BrandMeister** and **TetraPack** projects.

## Background

ReliablePool started as an internal building block inside BrandMeister and TetraPack and
remains actively used by both projects. The standalone repository tracks the same
implementation used there rather than a detached experimental fork. Over time it evolved
into a standalone subsystem with its own API and supporting components such as tracking,
monitoring and event integration.

## What it is

ReliablePool is not a conventional memory allocator. It is a persistent shared-memory
object model built around stable mappings, explicit ownership, restart recovery, change
tracking and replication. It is intended for systems that need:

- stable addresses over time (even as the pool grows),
- explicit lifetime management and controlled ownership,
- integration points for replication/monitoring and idempotent processing,
- capability to recover data on restart (through using `memfd` or an opened file as backends and systemd's **FDSTORE** feature),
- capability for inter-process sharing.

FastRing provides asynchronous event integration for ReliablePool through
`ReliableWaiter` and `InstantWaiter`.

## Related Components

ReliablePool is commonly used together with:

- **Reliable components**: `ReliableMonitor`, `ReliableIndexer`, `ReliableTracker`, `ReliableWaiter`
- **Instant components**: `InstantReplicator`, `InstantWaiter`, `InstantDiscovery`
- **Restart tools**: `Rescue`, `Collapse`, `Epoch` (in `Tools/`)

## Reliable Components

### ReliableMonitor

Role:

- Linked monitor chain (`next`) attached to `ReliablePool`.
- Entry point for lifecycle and block events.
- A single monitor chain can be shared across multiple pools when this is intentional and lifetime/synchronization are controlled by the application.

Callback:

- `ReliableMonitorFunction(event, pool, share, block, closure)`

Core events:

- `RELIABLE_MONITOR_POOL_CREATE` / `RELIABLE_MONITOR_POOL_RELEASE`
- `RELIABLE_MONITOR_SHARE_CREATE` / `RELIABLE_MONITOR_SHARE_DESTROY`
- `RELIABLE_MONITOR_BLOCK_ALLOCATE` / `RELIABLE_MONITOR_BLOCK_ATTACH`
- `RELIABLE_MONITOR_BLOCK_RELEASE` / `RELIABLE_MONITOR_BLOCK_FREE`
- `RELIABLE_MONITOR_BLOCK_RESERVE` / `RELIABLE_MONITOR_BLOCK_RECOVER`

`RELIABLE_MONITOR_POOL_RELEASE` is delivered as the last event for a pool monitor chain.

### ReliableIndexer

Role:

- Monitor implementation that maintains a pool index: `name -> ReliablePool*`.
- Monitor implementation that maintains a block index: `(name, block_uuid) -> block_number`.

Main API:

- `CreateReliableIndexer(next)` / `ReleaseReliableIndexer(indexer)`
- `GetReliableIndexer(pool)`
- `FindReliablePool(indexer, name, acquire)`
- `FindReliableBlockNumber(indexer, name, identifier)`
- `CollectReliableBlockList(indexer, pool, time, flags)`
- `RemoveUnusedReliableBlockList(indexer, pool, time)`

Used by replication logic to resolve remote identifiers into local block numbers and to collect candidate block sets.

### ReliableTracker

Role:

- Monitor + background worker based on `userfaultfd` write-protection.
- Tracks dirty pages, calculates block CRC32C, updates replication marks/hints.
- Emits replication-oriented monitor events.

Main API:

- `CreateReliableTracker(flags, next)` / `ReleaseReliableTracker(tracker)`
- `FlushReliableTracker(tracker)` (must be called in idempotent/safe state)
- `LockReliableShare(share)` / `UnlockReliableShare(share)`
- `GetReliableTrackerClockVector(remote_timespec)`
- `VerifyReliableBlockIntegrity(block)` — returns non-zero when the block is neither `NULL` nor free and its data matches the CRC32C stored by the tracker in `block->control`; intended for validation in application and recovery code

Tracker-specific events:

- `RELIABLE_MONITOR_SHARE_CHANGE` (page-level dirty signal from tracking thread; also serves as fallback wake signal when waiter/futex activation path is not used)
- `RELIABLE_MONITOR_BLOCK_CHANGE` (block changed after flush analysis)
- `RELIABLE_MONITOR_FLUSH_COMMIT` (flush commit barrier for downstream monitors)

### ReliableFlusher

Role:

- Optional thin consumer of `ReliableTracker` in the `ReliableMonitor` chain.
- Adds an explicit durability step for file-backed pools by collecting `RELIABLE_MONITOR_BLOCK_CHANGE` reports and synchronizing each completed share with `msync(MS_SYNC)`.
- On `msync()` failure raises the sticky `RELIABLE_FLUSHER_STATE_FAILURE` flag.

Main API:

- `CreateReliableFlusher(next)` / `ReleaseReliableFlusher(flusher)`

Notes:

- Useful only for pools backed by a real file; `msync()` is a no-op on `memfd` (tmpfs).
- Coalesces consecutive block-change reports for the same share and synchronizes it when another share begins or `RELIABLE_MONITOR_FLUSH_COMMIT` closes the cycle.
- Deferring `msync()` until the share is complete ensures that tracker metadata updates for all reported blocks are included.
- Reports synchronization failures without blocking delivery of monitor events to downstream consumers.

### ReliableWaiter

Role:

- `FastRing` adapter for `ReliableTracker`.
- Waits on tracker state via io_uring futex and calls `FlushReliableTracker(...)` on wake.

Main API:

- `SubmitReliableWaiter(ring, tracker)`
- `CancelReliableWaiter(descriptor)`

## Instant Components

### InstantReplicator

Role:

- RDMA replication monitor in the `ReliableMonitor` chain.
- Synchronizes pool changes between peers.

Main API:

- `CreateInstantReplicator(port, identifier, name, secret, options, timeout, function, closure, next)` (`timeout` in milliseconds: how long a peer may go without progress before its connection is closed, 0 = 1 000 ms, rounded up to 200 ms ticks)
- `ReleaseInstantReplicator(replicator)`
- `RegisterRemoteInstantReplicator(replicator, identifier, address, length)`
- `TransmitInstantReplicatorUserMessage(replicator, data, length, wait)` (queued and delivered in order to every connected peer, lost only with a broken connection; with `wait` it sleeps until the queue has a place and a buffer is free, otherwise returns `-EBUSY`; `-EFAULT` when the replicator has stopped or failed; event handlers run on the replicator thread and never wait)

Options:

- `INSTANT_REPLICATOR_OPTION_OPTIMISTIC_MODE`: under the receiver barrier, offered blocks are first read by RDMA READ straight into the locked local blocks, without involving the sender's barrier, and the source `mark` is read after the data. A block is accepted when the source still shows the offered `mark`, the identifier matches and the CRC32C matches the control; the rest go through the synchronous CAS/WRITE transfer. The synchronous transfer stays the default: the option trades a part of the parking of the main thread for DAMAGE on copies overwritten by a rejected read, mostly of objects already released at the source. The design, its correctness argument and the measurements are in [REPLICATION.md](REPLICATION.md#optimistic-mode).

Buffers:

- The application thread never waits for replication buffers, except in `TransmitInstantReplicatorUserMessage()` with `wait`, and cannot take the last `INSTANT_RESERVE_COUNT` of them, which stay for the replicator thread. A block change notification that does not fit is counted as lost and the affected peers are synchronized again; removals and user messages are built once in a shared buffer, queued and sent in order to every peer.
- Every peer grants a window of messages that consume its receiving buffers, so a stopped or slow peer never gets more than it can receive: a SEND never waits for `RNR` and never holds up the reads and writes posted after it on the same connection. The count of received messages returns in `imm_data` of every SEND, windows are announced and confirmed by `INSTANT_TYPE_CREDIT`.
- A peer that stops answering without breaking the connection is disconnected by the replicator after about 1 s by default: when a transfer waiting for it does not advance or its credit window stays used up while it sends nothing. The barrier and the queued messages never wait longer for a stopped peer; it is caught up after it connects again. The time is the `timeout` of `CreateInstantReplicator()`, a failure budget of the instance, see [Timeouts](REPLICATION.md#timeouts).

Requirements:

- `InstantReplicator` requires RDMA remote atomic support for its compare-and-swap transfer path.
- The HCA must expose atomic capabilities. Up to `INSTANT_ATOMIC_COUNT` (16) RDMA READ and atomic operations are kept in flight per connection, limited by the weakest local card and by the depths the peer requests.
- Adapters that do not satisfy these capabilities are rejected during card setup and are treated as unavailable; they will not be considered connected peers.

Protocol format:

- Header: `InstantHeaderData`
- Payload (type-specific):
  - `INSTANT_TYPE_CLOCK`: `struct timespec`
  - `INSTANT_TYPE_NOTIFY` / `INSTANT_TYPE_RETRIEVE`: transfer metadata and registered keys
  - `INSTANT_TYPE_COMPLETE`: task completion marker
  - `INSTANT_TYPE_REMOVE`: `InstantRemovalData`
  - `INSTANT_TYPE_CREDIT`: `InstantCreditData` (window granted to the peer, window of the peer being applied)
  - `INSTANT_TYPE_USER`: arbitrary user payload

`INSTANT_TYPE_REMOVE` message format:

- `InstantHeaderData + InstantRemovalData`

Callback events (`HandleInstantEventFunction`):

- `INSTANT_REPLICATOR_EVENT_FLUSH` - requests external flush/ready handshake.
  Contract: call `FlushInstantReplicator(replicator)` from another thread/event-loop context; do not block by calling it re-entrantly from the same replicator callback thread.
  While the caller is parked in `FlushInstantReplicator()`, no other thread may modify pool objects: parking the event-loop thread alone does not protect against unrelated writers.
- `INSTANT_REPLICATOR_EVENT_CONNECTED`
- `INSTANT_REPLICATOR_EVENT_DISCONNECTED`
- `INSTANT_REPLICATOR_EVENT_USER_MESSAGE`

Additional `ReliableMonitor` events emitted by `InstantReplicator`:

- `RELIABLE_MONITOR_BLOCK_DAMAGE` - emitted after transfer retries are exhausted and block validation still fails.
- `RELIABLE_MONITOR_BLOCK_ARRIVAL` - emitted when transferred block data is validated and accepted.
- `RELIABLE_MONITOR_BLOCK_REMOVAL` - emitted when deferred remote removal reaches a busy local block and requires application-level handling.

### InstantWaiter

Role:

- `FastRing` adapter for waiting on `InstantReplicator` state transitions through io_uring futex operations.

Main API:

- `SubmitInstantWaiter(ring, replicator)` - creates and submits a waiter descriptor.
- `CancelInstantWaiter(descriptor)` - cancels the waiter and releases callback linkage.

Use when the application already runs a `FastRing` loop and needs non-blocking integration with replicator wakeups.

### InstantDiscovery

Role:

- Avahi/mDNS helper for automatic peer discovery and local service publication for `InstantReplicator`.

Main API:

- `CreateInstantDiscovery(poll, replicator)`
- `ReleaseInstantDiscovery(discovery)`

Behavior:

- Publishes local service as `_replicator._tcp` with TXT key `instance=<uuid>`.
- Browses matching services and resolves endpoints.
- Calls `RegisterRemoteInstantReplicator(...)` for discovered remote instances.
- Restarts Avahi client on transient daemon/DBus failures using delayed retry.

## Durability and Crash Consistency

There is no WAL, so `msync()` does not make blocks atomically persistent. A flush cycle is the durability boundary: `FlushReliableTracker()` runs in an idempotent state and updates `control` CRC32C plus `mark`/`hint` for the consistent pool state observed by that cycle. CRC allows recovery to detect a block whose data and metadata were persisted inconsistently.

Persistence behavior:

- `ReliableFlusher` gives the lower bound: everything confirmed by a completed flush cycle survives a crash, provided it did not set `RELIABLE_FLUSHER_STATE_FAILURE`.
- There is no upper bound: kernel background writeback persists dirty pages between cycles at arbitrary moments, so after a crash the file may additionally contain partial state of later, unconfirmed changes ("torn" blocks).
- A `memfd`-backed pool has no filesystem writeback tearing. With **FDSTORE** it retains the exact in-memory state across a service restart, including an update interrupted by the dying process; it does not survive a host reboot at all.

Torn blocks come in two kinds:

- Data newer than metadata: the block looks stale to peers and replication re-fetches it — heals itself.
- Metadata newer than data (or `mark` still carrying the in-flight low bit of an interrupted transfer): the block looks fresh while its data is stale — this kind must be handled explicitly.

Healing is deliberately left to the application. Whether a pool is tracked and replicated is the application's choice, and `block->control` is maintained only under tracking, so no component can decide validity on its own. The right hook is `ReliableRecoveryFunction`: when an existing pool is opened with `RELIABLE_FLAG_RESET`, it runs once for every recoverable block before that block is published through `RELIABLE_MONITOR_BLOCK_RECOVER`. This keeps crash validation on the restart path instead of adding runtime cost.

Recipe for a tracked (and optionally replicated) pool, inside the recovery callback:

- Keep the block as is when `hint & 1` is clear and `VerifyReliableBlockIntegrity(block)` returns non-zero. Opening the pool already turns the lock of a transfer interrupted by the crash (`mark & 1`) into the damaged state: `mark` 0 and a pending `hint` (`hint & 1`).
- Otherwise pick one of two outcomes:
  - return `RELIABLE_TYPE_FREE` — discard the block when the data model does not tolerate partial writes;
  - keep the allocation but zero `mark` and `hint` — the block is declared stale, and startup synchronization can re-fetch it from a peer with a newer valid copy.

Torn blocks cannot poison other nodes either way: receivers validate CRC on every arrival and reject mismatching transfers.

A transfer that ended after it may have overwritten a block reports `RELIABLE_MONITOR_BLOCK_DAMAGE` and leaves the block damaged: `mark` 0 and a pending `hint`. Neither the tracker nor the replicator publishes such a block. It leaves this state when a later transfer from a peer installs a version, or when the application rewrites the content and calls `RepairReliableBlock(pool, block)`, which clears the pending `hint` and inverts `control`, so the next tracker flush sees a changed checksum and publishes the block even when the content was restored to the bytes that matched the old `control`.

## Restart Recovery Tools

A `memfd`-backed pool survives a service restart only while its descriptor is kept by systemd in the unit's fd store. Three helpers in `Tools/` cover this.

### Rescue

`Rescue` owns the fd store of the process:

- at startup (a constructor, when running under systemd with `NOTIFY_SOCKET`) it takes over `LISTEN_FDS` / `LISTEN_FDNAMES`, relocates the descriptors above `FD_SETSIZE` and indexes them by name;
- `GetRescuedHandle(name)` returns a descriptor restored from the previous run, or `-1`;
- `AddRescuedHandle(handle, name)` puts a descriptor into the fd store (`FDSTORE=1`, `FDNAME=name`);
- `RemoveRescuedHandle(handle, RESCUE_REMOVE_CLOSE)` removes it from the fd store (`FDSTOREREMOVE=1`);
- `CloseUnusedRescuedHandleList()` closes and removes restored descriptors that nobody claimed.

Names must not contain `:` or control characters. A name without `%` is stored by pointer, not copied.

### Collapse

On `SIGTERM` the service has to decide whether to keep its state (and not tear down connections) or to perform a regular destructive stop. systemd does not tell this to the service ([systemd#43880](https://github.com/systemd/systemd/issues/43880)), so `Collapse` asks PID 1 (`ListJobs` over sd-bus) why the unit is being stopped:

- `GetCollapseCause()` returns a bit field of `COLLAPSE_RESTART` (restart job of the unit itself), `COLLAPSE_KEXEC`, `COLLAPSE_SOFT_REBOOT`, `COLLAPSE_REBOOT`, `COLLAPSE_POWEROFF`, `COLLAPSE_HALT`, or a negative errno;
- `IsLiveUpdateAvailable()` reports whether LUO (Live Update Orchestrator) is active in the running kernel, by checking only that `/dev/liveupdate` exists;
- `CanSurvive()` returns `1` when the state survives (unit restart, soft-reboot, kexec with LUO), `0` when it does not, or a negative errno — treat an error as "does not survive".

```c
int cause = CanSurvive();

if (cause > 0)
  /* keep connections, leave descriptors in the fd store */;
else
  /* regular stop, RemoveRescuedHandle(..., RESCUE_REMOVE_CLOSE) */;
```

Unit requirements (systemd 254 or newer):

```ini
[Unit]
After=dbus.service

[Service]
Type=notify
FileDescriptorStoreMax=4096
FileDescriptorStorePreserve=yes
```

`After=dbus.service` makes the service stop before dbus-daemon, so the query still works during shutdown. `FileDescriptorStorePreserve=yes` keeps the fd store across `stop` + `start` and `soft-reboot`.

The fd store survives kexec only through LUO, which requires systemd 262 or newer, both kernels built with `CONFIG_KEXEC_HANDOVER`, `CONFIG_LIVEUPDATE` and `CONFIG_LIVEUPDATE_MEMFD` (disabled in stock Debian kernels), `liveupdate=on` on the kernel command line, and the new kernel loaded with `kexec -s` (`kexec_file_load`). `IsLiveUpdateAvailable()` sees only the running kernel.

`GetCollapseCause()` was verified on systemd 257 and 262 in arm64 and x86 virtual machines for restart, stop, kill, soft-reboot, kexec, reboot, halt, poweroff and forced reboot, and for restart, stop and kill on x86 bare metal; memfd survival across kexec was verified with a LUO kernel on systemd 262. `IsLiveUpdateAvailable()` and `CanSurvive()` were added after these runs.

### Epoch

Blocks often keep `CLOCK_MONOTONIC` timestamps (last access, expiration). Within one boot they stay valid across restarts and soft-reboot, since the kernel and its clock are the same. After kexec the new kernel starts its own clock: whether it continues the old one depends on the platform and its clock source, and is not guaranteed. A recovered timestamp can then lie far in the future (the entry never expires) or in the past (everything expires at once).

`Epoch` keeps a small record in a memfd named `Epoch`, held by `Rescue`: the boot identifier (`sd_id128_get_boot()`), `CLOCK_MONOTONIC` and `CLOCK_REALTIME` taken as a pair.

- at startup (a constructor, after `Rescue`) it reads the record of the previous instance, computes the correction and immediately stores its own record;
- at exit (a destructor) it stores the record again, which only narrows the error: any pair of the previous boot is enough;
- within the same boot the correction is exactly zero; after a boot change it is `monotonic_now − (monotonic_saved + max(0, realtime_now − realtime_saved))`, so the downtime is measured by `CLOCK_REALTIME` and a clock step backwards counts as zero;
- `GetEpochState()` returns `EPOCH_SAME_BOOT`, `EPOCH_NEW_BOOT`, `EPOCH_UNKNOWN` (no previous record) or a negative errno (the correction stays zero then);
- `FixEpochTime(time_t)`, `FixEpochCertainTime(struct timeval*)` and `FixEpochPreciseTime(struct timespec*)` apply the correction to a stored value. Zero means "not set" and is kept as is, a result before the start of the current boot is clamped to the smallest non-zero value, so unsigned fields do not wrap around.

The correction has to be applied inside the recovery function, before a recovered timestamp is indexed or compared:

```c
static int RecoverSession(struct ReliablePool* pool, struct ReliableBlock* block, void* closure)
{
  struct SessionData* data = (struct SessionData*)block->data;

  FixEpochPreciseTime(&data->time);
  data->expires = FixEpochTime(data->expires);

  /* index the block */
  return RELIABLE_TYPE_RECOVERABLE;
}
```

The record of the new instance is stored before any pool is recovered, so a crash inside recovery never applies the correction twice; the price is that blocks left unrecovered by such a crash keep the old time base, and lifetime checks should bound them. The first start of a version that links `Epoch` sees no record and applies no correction.

## Replication Model Boundaries

`InstantReplicator` is a consensus-free monotonic version-selection replication with per-block granularity. Its guarantees end at well-defined boundaries; they are design choices, not defects.

Trust boundary:

- The HMAC handshake authenticates a peer at connect time, but the handshake blob is static: the nonce and the digest are generated once per replicator instance and resent on every connect — there is no challenge/response and no replay cache. A captured blob is sufficient to authenticate as that peer while the real peer is disconnected.
- After the handshake the data plane is raw RC verbs, and every authenticated peer holds RDMA write access to entire shares.
- The effective trust boundary is therefore the fabric itself: the protocol is intended for a closed RDMA fabric (a single network segment) where the ability to capture or inject traffic already implies full compromise.

Convergence boundary:

- Version selection is monotone per block: a node never accepts a version older than the one it committed to. Delivery of the selected version is a separate matter — see the next point.
- The offered version is recorded before the transfer completes; when the transfer is abandoned (peer death, disconnect), the block lock is rolled back but the recorded version is not, so re-offers of the same and older versions are pruned until the block changes again anywhere. This is an accepted tradeoff, not a fundamental limit: local bookkeeping could allow retrying an equal version after a failure, at the cost of extra state in the selector invariant.
- At runtime the application-visible signal is `RELIABLE_MONITOR_BLOCK_DAMAGE` (transfer validation retries exhausted); a fetch abandoned by disconnect is silent and heals with the next change. Across restarts, stale-data decisions belong to the recovery callback.

Clocks:

- A deployment using `InstantReplicator` should provide every node with a stable,
  well-synchronized `CLOCK_REALTIME`. PTP is preferred; NTP is suitable only when
  its worst-case offset and jitter stay comfortably below the 16.7 ms epoch
  quantum. Bring the clocks into agreement before starting the replicators and
  avoid backward wall-clock steps while they are running.
- For KVM guests, synchronize the physical hosts and carry each host clock into its
  guests through the `ptp_kvm` PHC (commonly `/dev/ptp0`), using `chronyd` or
  `phc2sys` to discipline the guest `CLOCK_REALTIME`. `kvm-clock` by itself is a
  clocksource, not wall-clock synchronization. Guests on different physical hosts
  remain only as well synchronized as those hosts are.
- Cross-node version comparison relies on a one-way CLOCK exchange driven by the periodic 200 ms timer; there is no RTT correction, so the measured vector includes transport and queueing jitter.
- The ideal clock-offset component of the normalization telescopes across relay chains, but the one-way measurement error does not: it accumulates per hop, so the same version delivered via different routes carries different jitter. Comparisons between versions authored by different nodes additionally see the static clock offset doubled rather than cancelled. The vector is the measured offset rounded to the epoch (16.7 ms), so offsets and jitter well below half an epoch (8.3 ms) yield a stable vector; an offset near half an epoch still flips the vector by one epoch between measurements, and a flip between two close versions of the same author can make the newer one look older, so it is skipped until the block changes again. Larger offsets skew cross-author freshness decisions until the clocks are fixed.
- After a backward wall-clock step, the epoch counter keeps ratcheting forward with flush activity, so normalization of that node's versions stays skewed until its wall clock overtakes the counter — a window at least as long as the step, extended by the minting rate.

Design and testing:

- The design of the replication, its load testing on an InfiniBand testbed, the measured cost of the synchronous transfer and the open issues are described in [REPLICATION.md](REPLICATION.md); the test tool is `Tests/Replication`.

## Examples

All examples are self-contained and have their own `Makefile`.

Build and run pattern:

- `make -C Examples/<Name>`
- `./Examples/<Name>/test`

### Basic (`Examples/Basic`)

- Minimal `ReliablePool` lifecycle.
- Uses file-backed pool (`test.dat`), recovery callback, allocation/release flow.
- Good first step to verify persistence and recovery semantics.

### CPP (`Examples/CPP`)

- C++ wrapper usage through `ReliableHolder<T>`.
- Demonstrates RAII-style block ownership and recovery callback integration.
- Good first step for C++ API consumers.

### Advanced (`Examples/Advanced`)

- Local tracking pipeline without RDMA.
- Combines `ReliableTracker` + `ReliableFlusher` + `ReliableIndexer` + `ReliableWaiter` on a `FastRing` loop.
- Generates random block activity on a file-backed pool (`test.dat`), prints monitor events and reports `ReliableFlusher` confirmation status on shutdown.
- Requires FastRing: https://github.com/cyanide-burnout/FastRing

### RDMA (`Examples/RDMA`)

- Full replication stack example.
- Combines `ReliableTracker`/`ReliableIndexer` with `InstantReplicator`, `InstantWaiter`, and `InstantDiscovery` (`avahi`).
- Use to validate peer discovery and block replication behavior across nodes.
- Requires an RDMA adapter with remote atomic support; adapters without the required atomic capabilities are rejected before the peer is considered connected.
- Requires FastRing: https://github.com/cyanide-burnout/FastRing

### UV (`Examples/UV`)

- Event-loop integration variant based on `libuv`.
- Uses `uv_async_send` bridge callbacks for:
  - `RELIABLE_MONITOR_SHARE_CHANGE -> FlushReliableTracker(...)`
  - `INSTANT_REPLICATOR_EVENT_FLUSH -> FlushInstantReplicator(...)`
- Use when embedding ReliablePool/InstantReplicator into a `libuv` runtime.

## Lua Module

Location:

- `Lua/Module.c`
- `Lua/Test.lua`

Build:

- `make -C Lua`

Run example:

- `cd Lua && ./Test.lua`

Lua API:

- `local module = require("ReliablePool")`
- `pool = module.open(path_or_fd, name, length[, recover])`
- `block = pool:allocate([type])`
- `block = pool:attach(number[, tag])`
- `result = pool:update()`
- `pool:close()`
- `block:release([type])`
- `valid = block:verify()`

Open semantics:

- If `path_or_fd` is string, module opens file with `O_RDWR | O_CREAT` and mode `0660`.
- If `path_or_fd` is number, it is treated as file descriptor.
- Pool owns descriptor lifetime and closes it on `pool:close()` / `__gc`.
- If `recover` callback is provided, module automatically sets `RELIABLE_FLAG_RESET`.

Recover callback:

- Signature: `recover(block)`.
- Use `block:verify()` to check the CRC32C previously stored by `ReliableTracker` before keeping a recovered block.
- Return value is ignored.
- C callback always returns `RELIABLE_TYPE_RECOVERABLE`.
- Callback errors are swallowed (do not abort `open`).
- Keep `block` in Lua scope/table if it must survive callback scope.

Block properties:

- Read-only: `type`, `number`, `count`, `mark`, `tag`, `identifier`
- Read/write: `length`, `data` (binary Lua string)

Pool properties:

- Read-only: `size`, `length`

Defaults:

- `pool:allocate()` default type: `RELIABLE_TYPE_NON_RECOVERABLE`
- `pool:attach(number)` default tag: `UINT32_MAX`
- `block:release()` default type: `RELIABLE_TYPE_FREE`
