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

Tracker-specific events:

- `RELIABLE_MONITOR_SHARE_CHANGE` (page-level dirty signal from tracking thread; also serves as fallback wake signal when waiter/futex activation path is not used)
- `RELIABLE_MONITOR_BLOCK_CHANGE` (block changed after flush analysis)
- `RELIABLE_MONITOR_FLUSH_COMMIT` (flush commit barrier for downstream monitors)

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
- Combines `ReliableTracker` + `ReliableIndexer` + `ReliableWaiter` on a `FastRing` loop.
- Generates random block activity and prints monitor events.
- Requires FastRing: https://github.com/cyanide-burnout/FastRing

### RDMA (`Examples/RDMA`)

- Full replication stack example.
- Combines `ReliableTracker`/`ReliableIndexer` with `InstantReplicator`, `InstantWaiter`, and `InstantDiscovery` (`avahi`).
- Use to validate peer discovery and block replication behavior across nodes.
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

Open semantics:

- If `path_or_fd` is string, module opens file with `O_RDWR | O_CREAT` and mode `0660`.
- If `path_or_fd` is number, it is treated as file descriptor.
- Pool owns descriptor lifetime and closes it on `pool:close()` / `__gc`.
- If `recover` callback is provided, module automatically sets `RELIABLE_FLAG_RESET`.

Recover callback:

- Signature: `recover(block)`.
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
