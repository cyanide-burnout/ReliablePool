# ReliablePool

ReliablePool was introduced in **2022** as part of the **BrandMeister** and **TetraPack** projects.

## Background

ReliablePool started as an internal building block to support high-reliability, high-throughput components in BrandMeister and TetraPack. Over time it evolved into a standalone subsystem with its own API and supporting components (such as tracking/monitoring and event integration).

## What it is

ReliablePool is a memory/pool subsystem designed around long-lived, stable memory mappings ("shares") and predictable ownership semantics. It is intended for systems that need:

- stable addresses over time (even as the pool grows),
- explicit lifetime management and controlled ownership,
- integration points for replication/monitoring and idempotent processing,
- capability to recover data on restart (through using `memfd` or an opened file as backends and systemd's **FDSTORE** feature),
- capability for inter-process sharing.

## Related Components

ReliablePool is commonly used together with:

- **Reliable components**: `ReliableMonitor`, `ReliableIndexer`, `ReliableTracker`, `ReliableWaiter`
- **Instant components**: `InstantReplicator`, `InstantWaiter`, `InstantDiscovery`

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

- `CreateInstantReplicator(port, identifier, name, secret, function, closure, next)`
- `ReleaseInstantReplicator(replicator)`
- `RegisterRemoteInstantReplicator(replicator, identifier, address, length)`
- `TransmitInstantReplicatorUserMessage(replicator, data, length, wait)`

Requirements:

- `InstantReplicator` requires RDMA remote atomic support for its compare-and-swap transfer path.
- The HCA must expose atomic capabilities sufficient for the configured initiator/responder RDMA depths.
- Adapters that do not satisfy these capabilities are rejected during card setup and are treated as unavailable; they will not be considered connected peers.

Protocol format:

- Header: `InstantHeaderData`
- Payload (type-specific):
  - `INSTANT_TYPE_CLOCK`: `struct timespec`
  - `INSTANT_TYPE_NOTIFY` / `INSTANT_TYPE_RETRIEVE`: transfer metadata and registered keys
  - `INSTANT_TYPE_COMPLETE`: task completion marker
  - `INSTANT_TYPE_REMOVE`: `InstantRemovalData`
  - `INSTANT_TYPE_USER`: arbitrary user payload

`INSTANT_TYPE_REMOVE` message format:

- `InstantHeaderData + InstantRemovalData`

Callback events (`HandleInstantEventFunction`):

- `INSTANT_REPLICATOR_EVENT_FLUSH` - requests external flush/ready handshake.
  Contract: call `FlushInstantReplicator(replicator)` from another thread/event-loop context; do not block by calling it re-entrantly from the same replicator callback thread.
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

- Keep the block as is when `mark & 1` is clear and `VerifyReliableBlockIntegrity(block)` returns non-zero.
- Otherwise pick one of two outcomes:
  - return `RELIABLE_TYPE_FREE` — discard the block when the data model does not tolerate partial writes;
  - keep the allocation but zero `mark` and `hint` — the block is declared stale, and startup synchronization can re-fetch it from a peer with a newer valid copy.

Torn blocks cannot poison other nodes either way: receivers validate CRC on every arrival and reject mismatching transfers.

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

- Cross-node version comparison relies on a one-way CLOCK exchange driven by the periodic 200 ms timer; there is no RTT correction, so the measured vector includes transport and queueing jitter.
- The ideal clock-offset component of the normalization telescopes across relay chains, but the one-way measurement error does not: it accumulates per hop, so the same version delivered via different routes carries different jitter. Comparisons between versions authored by different nodes additionally see the static clock offset doubled rather than cancelled. Epoch quantization (16.7 ms) keeps NTP-grade offsets and typical jitter below the noise floor; larger offsets skew cross-author freshness decisions until the clocks are fixed.
- After a backward wall-clock step, the epoch counter keeps ratcheting forward with flush activity, so normalization of that node's versions stays skewed until its wall clock overtakes the counter — a window at least as long as the step, extended by the minting rate.

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
