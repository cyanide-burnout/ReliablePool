# ReliablePool

ReliablePool is a C library for keeping the live state of a service, its objects, in shared
memory that outlives the process and can be replicated to other nodes. Objects are fixed-size
blocks of a pool mapped from a `memfd` or a file. They keep stable addresses while the pool grows,
survive a restart of the service, can be opened by several processes, and, with the optional
components, have their changes detected and copied to peers over RDMA without the application
writing update messages.

It was introduced in 2022 as part of the BrandMeister and TetraPack projects and is still used by
both; this repository carries the same implementation.

- [The Problem](#the-problem)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Execution Contract](#execution-contract)
- [Replication Model Boundaries](#replication-model-boundaries)
- [Requirements and Build](#requirements-and-build)
- [Examples](#examples)
- [Repository Layout](#repository-layout)
- [Documentation](#documentation)

## The Problem

A service such as a radio network core holds per-connection and per-call contexts in memory. When
it is restarted for an update, or crashes, that state is lost, and every client has to reconnect
and start over. Keeping the state in a database or serializing it on shutdown costs latency on the
hot path or does not help after a crash.

ReliablePool keeps such objects directly in a shared mapping instead:

- The object is the memory. The application works with plain structures through pointers; there
  is no serialization step.
- The memory outlives the process. A `memfd` handed to the systemd fd store comes back to the next
  instance of the service; a file survives a reboot. On start, the application is called for every
  surviving object to validate it and rebuild its own indexes.
- Ownership is explicit. Blocks are reference-counted, and the type a block is released with
  decides whether the object is gone or kept for the next start.
- Changes can be observed without instrumenting the code. `ReliableTracker` detects writes with
  `userfaultfd` write protection and turns them into versioned, checksummed block changes.
- Changes can be replicated. `InstantReplicator` delivers the latest version of every changed
  block to other nodes over InfiniBand or RoCE, so a peer can continue serving an object when its
  node disappears.

ReliablePool is not a general-purpose allocator: all blocks of a pool have the same size, and the
pool is meant for long-lived state objects, not for arbitrary heap allocations.

## Architecture

### Pool, Shares and Blocks

A **pool** (`ReliablePool`) is one descriptor, a regular file or a `memfd`, divided into blocks of
one size chosen at creation. It grows by 1 024 blocks at a time; each growth maps the descriptor
again as a new **share**, and old shares stay mapped while objects in them are referenced, so a
pointer to an object never moves.

A **block** has a small header followed by the application data. The header carries the local
bookkeeping (type, number, generation tag, reference count) and the part used by tracking and
replication: a global UUID, a CRC32C of the data, and the `mark` and `hint` version fields. An
application holds a block through a **descriptor** (`ReliableDescriptor`), which is one counted
reference to the block, its share and its pool.

The pool itself is enough for restart survival and for sharing between processes; everything else
is optional.

### The Monitor Chain

Every component that extends the pool is a **monitor**: a `ReliableMonitor` linked into a chain
that is passed to `CreateReliablePool()`. The pool and the components deliver events (pool and
share creation, block allocation, release, change, arrival, ...) to every monitor of the chain in
order, on the thread that causes them. An application adds its own monitor to the chain to observe
the same events.

```
CreateReliablePool(..., &tracker->super, ...)

  ReliableTracker ──▶ ReliableFlusher ──▶ ReliableIndexer ──▶ InstantReplicator ──▶ application monitor
  change detection    msync() of files    UUID ⇄ block        RDMA replication      ARRIVAL, DAMAGE, ...
```

| Component | Role | Needs |
|---|---|---|
| `ReliablePool` | Pool, blocks, descriptors, recovery on open | — |
| `ReliableTracker` | Detects writes with `userfaultfd`, computes CRC32C, stamps versions in flush cycles | — |
| `ReliableFlusher` | Writes a file-backed pool to disk with `msync()` at the end of each flush cycle | Tracker |
| `ReliableIndexer` | Assigns block UUIDs; indexes pools by name and blocks by UUID | — |
| `ReliableWaiter` | Runs tracker flushes from a FastRing (io_uring) event loop | Tracker, FastRing |
| `InstantReplicator` | Replicates block versions, removals and user messages over RDMA | Tracker, Indexer |
| `InstantWaiter` | Answers replicator barriers from a FastRing event loop | Replicator, FastRing |
| `InstantDiscovery` | Finds and announces replicator peers with Avahi (mDNS) | Replicator, Avahi |

The order of the chain matters: the indexer assigns the UUID on allocation, so it must come before
the replicator, and the tracker comes first so that shares are protected before anyone else sees
them.

### How a Change Travels

1. The application writes to an object. The first write to a page faults into the tracker thread,
   which marks the page dirty, lifts its protection and wakes the application loop.
2. At a safe point the loop runs a **flush cycle**, `FlushReliableTracker()`. Every block on a
   dirty page whose checksum changed gets a new CRC32C and a new version, and is reported as
   `RELIABLE_MONITOR_BLOCK_CHANGE`; the cycle ends with `RELIABLE_MONITOR_FLUSH_COMMIT`.
3. The flusher, if present, synchronizes the changed file. The replicator collects the changes
   into NOTIFY messages for its peers.
4. A peer that finds the offered version newer than its own copy asks for the block. Under a
   barrier on both sides, which the application threads honour by calling
   `FlushInstantReplicator()`, the block is locked by RDMA compare-and-swap and written into the
   peer's pool, validated there, and reported as `RELIABLE_MONITOR_BLOCK_ARRIVAL`.

Repeated writes between flushes are combined: replication carries versions of state, not a log of
operations. The design and its measured cost are described in
[REPLICATION.md](Documentation/REPLICATION.md).

## Getting Started

The library is a set of C sources without a build of its own: an application compiles the
components it needs, as the examples do. The steps below follow the examples from the simplest.

### 1. A Pool That Survives a Restart

```c
#include "ReliablePool.h"

struct Session
{
  uint32_t client;
  char state[64];
};

static int Recover(struct ReliablePool* pool, struct ReliableBlock* block, void* closure)
{
  struct ReliableDescriptor descriptor;
  struct Session* session = (struct Session*)block->data;

  if (session->client == 0)
    return RELIABLE_TYPE_FREE;                         /* not worth keeping */

  RecoverReliableBlock(&descriptor, pool, block);      /* take a reference */
  /* store the descriptor in the application's own index */
  return RELIABLE_TYPE_RECOVERABLE;
}

int handle = memfd_create("Sessions", MFD_CLOEXEC);   /* or GetRescuedHandle(), or open() a file */
struct ReliablePool* pool = CreateReliablePool(handle, "Sessions", sizeof(struct Session),
                                               RELIABLE_FLAG_RESET, NULL, Recover, NULL);

struct ReliableDescriptor descriptor;
struct Session* session = AllocateReliableBlock(&descriptor, pool, RELIABLE_TYPE_RECOVERABLE);

/* ... use session ... */

ReleaseReliableBlock(&descriptor, RELIABLE_TYPE_FREE);        /* the object is gone */
/* or RELIABLE_TYPE_RECOVERABLE: keep it for the next start */

ReleaseReliablePool(pool);                                    /* the handle is closed with the last reference */
```

`RELIABLE_FLAG_RESET` makes this process the owner: when the descriptor already holds a pool, all
reference counts are reset and the recovery function is called for every recoverable block. A
`memfd` survives a restart only when it is kept by systemd; `Tools/Rescue` does that, and
`Tools/Collapse` tells on `SIGTERM` whether the state will survive. How to validate recovered
objects and how to stop is described in [RECOVERY.md](Documentation/RECOVERY.md). C++ code can
use `ReliableHolder<T>` and `ReliableAllocator<T>` ([API.md](Documentation/API.md#c-helpers)).

See `Examples/Basic` and `Examples/CPP`.

### 2. Tracking Changes

Put a tracker (and optionally an indexer and your own monitor) in the chain and let the
event loop run flush cycles:

```c
struct ReliableIndexer* indexer = CreateReliableIndexer(&monitor);
struct ReliableTracker* tracker = CreateReliableTracker(RELIABLE_TRACKER_FLAG_ID_HOST | RELIABLE_TRACKER_FLAG_ID_PROCESS, &indexer->super);
struct ReliablePool* pool       = CreateReliablePool(handle, "Test", 50, RELIABLE_FLAG_RESET, &tracker->super, NULL, NULL);

struct FastRing* ring = CreateFastRing(0);
struct FastRingDescriptor* waiter = SubmitReliableWaiter(ring, tracker);   /* calls FlushReliableTracker() */
```

`CreateReliableTracker()` returns an object even without `userfaultfd`; check that
`RELIABLE_TRACKER_STATE_ACTIVE` is set and `RELIABLE_TRACKER_STATE_FAILURE` is clear in
`tracker->state`. The pool must be backed by shmem (`memfd`, tmpfs) or hugetlbfs. Without FastRing, wake the loop from
`RELIABLE_MONITOR_SHARE_CHANGE` and call `FlushReliableTracker()` there (`Examples/UV`).

See `Examples/Advanced`.

### 3. Replicating Between Nodes

Add a replicator after the indexer, answer its barrier from the loop and let discovery find the
peers:

```c
struct InstantReplicator* replicator = CreateInstantReplicator(0, NULL, "Test", "Secret", 0, 0, HandleReplicatorEvent, NULL, &monitor);
struct ReliableIndexer* indexer      = CreateReliableIndexer(&replicator->super);
struct ReliableTracker* tracker      = CreateReliableTracker(RELIABLE_TRACKER_FLAG_ID_HOST | RELIABLE_TRACKER_FLAG_ID_PROCESS, &indexer->super);
struct ReliablePool* pool            = CreateReliablePool(handle, "Test", 50, 0, &tracker->super, NULL, NULL);

SubmitReliableWaiter(ring, tracker);
SubmitInstantWaiter(ring, replicator);                    /* calls FlushInstantReplicator() */
CreateInstantDiscovery(CreateFastAvahiPoll(ring), replicator);
```

All nodes use the same replication group name and secret. Blocks of the peers appear in the local
pool without a reference and are announced by `RELIABLE_MONITOR_BLOCK_ARRIVAL`; a node that takes
over an object takes a reference with `RecoverReliableBlock()`, as in recovery. Releasing the stack goes in reverse,
with the replicator before the indexer it uses.

See `Examples/RDMA` and, for a libuv loop, `Examples/UV`.

## Execution Contract

Change detection is transparent to the code that writes objects, but not to the code that runs
the loop. An application using the tracker or the replicator must follow these rules.

**A flush cycle is a consistency point.** `FlushReliableTracker()` reads the objects and publishes
what it reads as a version, so it must be called when the objects are in a consistent, idempotent
state: from the main loop between events, or under the application's global lock. No other thread
may modify pool objects during the call. There is no WAL and no deferred flushing: the cycle is
the checkpoint.

**Flushes are driven by the loop.** The tracker thread only signals that pages became dirty. The
loop reacts with `ReliableWaiter` or, without FastRing, with its own wake-up from
`RELIABLE_MONITOR_SHARE_CHANGE`, which runs on the tracker thread and must do nothing more than
signal. A busy loop flushes less often with larger batches, an idle one flushes often with small
batches; no tuning is needed.

**The replicator barrier is answered by the writers.** To transfer blocks the replicator raises a
barrier and delivers `INSTANT_REPLICATOR_EVENT_FLUSH` on its own thread. The application forwards
it to its loop (`InstantWaiter` does this), which calls `FlushInstantReplicator()` at a safe
point and stays parked there until the barrier is released. Calling it from the event handler
itself would deadlock. While the caller is parked, the replicator writes into pool memory and
delivers `RELIABLE_MONITOR_BLOCK_ARRIVAL` and `RELIABLE_MONITOR_BLOCK_REMOVAL` on its thread;
no other thread may modify pool objects, since parking one thread does not stop the others.

**The barrier waits for the peers.** A receiver keeps its barrier raised while the sender reaches
its own, so the parked time of one node includes the response time of the other, and a failed
peer holds the barrier until its `timeout` (1 s by default) closes the connection. An application
that cannot afford its thread waiting on that path should move the writers and the safe point to
a thread of their own; see [Timeouts](Documentation/REPLICATION.md#timeouts).

**Monitors are called synchronously.** Event handlers run on the thread that causes the event
and must be short. A monitor that keeps state for the duration of a flush cycle keeps it in
thread-local storage.

**Shutdown.** Stop the loop, run a last `FlushReliableTracker()`, cancel the waiters and release
discovery, then release the objects, the pools, the replicator, the tracker and the indexer, in
this order.

## Replication Model Boundaries

`InstantReplicator` is a consensus-free monotonic version-selection replication with per-block
granularity. It brings the latest version of each block to the peers on a best-effort basis and
protects the integrity of what a receiver accepts. Replication is asynchronous to the application:
a local write is not an acknowledgment that any peer has it, several blocks delivered together are
not a transaction, and recent changes can be lost when their node fails. The protocol does not
elect owners; which node modifies an object is up to the application and its clients. The design
is explained in [REPLICATION.md](Documentation/REPLICATION.md#design-context).

Replication and restart recovery are two ways to keep the same state. Using both for one pool is
not recommended: choose either recovery or replication.

Its guarantees end at well-defined boundaries; they are design choices, not defects.

Trust boundary:

- The HMAC handshake authenticates a peer at connect time, but the handshake blob is static: the
  nonce and the digest are generated once per replicator instance and resent on every connect.
  There is no challenge/response and no replay cache. A captured blob is sufficient to
  authenticate as that peer while the real peer is disconnected.
- After the handshake the data plane is raw RC verbs, and every authenticated peer holds RDMA
  write access to entire shares.
- The effective trust boundary is therefore the fabric itself: the protocol is intended for a
  closed RDMA fabric (a single network segment) where the ability to capture or inject traffic
  already implies full compromise.

Convergence boundary:

- Version selection is monotone per block: a node never accepts a version older than the one it
  committed to. Delivery of the selected version is a separate matter; see the next point.
- The offered version is recorded before the transfer completes; when the transfer is abandoned
  (peer death, disconnect), the block lock is rolled back but the recorded version is not, so
  re-offers of the same and older versions are pruned until the block changes again anywhere.
  This is an accepted tradeoff, not a fundamental limit: local bookkeeping could allow retrying an
  equal version after a failure, at the cost of extra state in the selector invariant.
- Removals have no tombstones: they are sent only to connected peers and the removal queue is not
  persistent, so a peer that was away keeps copies of objects released meanwhile ("zombies").
  `RemoveUnusedReliableBlockList()` can drop unreferenced copies that have not changed since a
  given time.
- At runtime the application-visible signal is `RELIABLE_MONITOR_BLOCK_DAMAGE` (transfer
  validation retries exhausted); a fetch abandoned by disconnect is silent and heals with the next
  change. Across restarts, stale-data decisions belong to the recovery function.

Peers and timeouts:

- Connections are accepted only from registered peers with the same group name and secret.
  `InstantDiscovery` is the intended way peers are found. A peer that cannot be reached for
  `CONNECTION_ATTEMPT_COUNT` (128) attempts in a row, one per 200 ms tick at most, so after at
  least about 25 s, is forgotten; discovery registers it again when it announces itself, while an
  application that registers peers statically has to register them again itself.
- A peer that stops answering without breaking the connection is disconnected after the
  `timeout` of `CreateInstantReplicator()`, 1 s by default, when a transfer waiting for it does
  not advance or its credit window stays used up while it sends nothing. It is caught up by
  syncing after it connects again. See [Timeouts](Documentation/REPLICATION.md#timeouts).

Clocks:

- A deployment using `InstantReplicator` should provide every node with a stable,
  well-synchronized `CLOCK_REALTIME`. PTP is preferred; NTP is suitable only when its worst-case
  offset and jitter stay comfortably below the 16.7 ms epoch quantum. Bring the clocks into
  agreement before starting the replicators and avoid backward wall-clock steps while they are
  running.
- For KVM guests, synchronize the physical hosts and carry each host clock into its guests through
  the `ptp_kvm` PHC (commonly `/dev/ptp0`), using `chronyd` or `phc2sys` to discipline the guest
  `CLOCK_REALTIME`. `kvm-clock` by itself is a clocksource, not wall-clock synchronization. Guests
  on different physical hosts remain only as well synchronized as those hosts are.
- Cross-node version comparison relies on a one-way CLOCK exchange driven by the periodic 200 ms
  timer; there is no RTT correction, so every measurement is lowered by its transport, queueing
  and processing delay. The receiver takes the vector from the largest of the last
  `INSTANT_CLOCK_COUNT` (8) measurements of a peer, about 1.6 s, which is the one with the smallest
  delay; a backward step of the remote clock is therefore followed only after the window has
  passed.
- The ideal clock-offset component of the normalization telescopes across relay chains, but the
  one-way measurement error does not: it accumulates per hop, so the same version delivered via
  different routes carries different jitter. Comparisons between versions authored by different
  nodes additionally see the static clock offset doubled rather than cancelled. The vector is the
  measured offset rounded to the epoch (16.7 ms), so offsets well below half an epoch (8.3 ms)
  yield a stable vector unless every measurement of the window is delayed by the rest of the half
  epoch; an offset near half an epoch still flips the vector by one epoch between measurements,
  and a flip between two close versions of the same author can make the newer one look older, so
  it is skipped until the block changes again. Larger offsets skew cross-author freshness
  decisions until the clocks are fixed.
- After a backward wall-clock step, the epoch counter keeps ratcheting forward with flush
  activity, so normalization of that node's versions stays skewed until its wall clock overtakes
  the counter: a window at least as long as the step, extended by the minting rate.

Optimistic mode:

- `INSTANT_REPLICATOR_OPTION_OPTIMISTIC_MODE` lets the receiver read offered blocks by RDMA READ
  under its own barrier, without the sender's barrier, and falls back to the synchronous transfer
  for blocks that fail validation. It reduces the parking of the application thread at the price
  of DAMAGE on copies overwritten by a rejected read. It is off by default; see
  [Optimistic Mode](Documentation/REPLICATION.md#optimistic-mode).

Testing:

- The replication was load-tested on an InfiniBand and RoCE testbed; throughput, latency, barrier
  cost, failure scenarios, known issues and fixed defects are reported in
  [REPLICATION.md](Documentation/REPLICATION.md). The test tool is `Tests/Replication`.

## Requirements and Build

| Component | Requires |
|---|---|
| `ReliablePool` | Linux (`memfd`, OFD locks), libuuid |
| `ReliableTracker` | `userfaultfd` with synchronous write protection (`UFFD_FEATURE_PAGEFAULT_FLAG_WP`), implemented by the kernel for shmem (`memfd`, tmpfs, Linux 5.19 or newer) and hugetlbfs mappings but not by disk file systems such as ext4, xfs and btrfs (see [Supported Memory](Documentation/API.md#supported-memory)); libsystemd (machine ID), `Tools/CRC32C` |
| `ReliableIndexer` | `Tools/HashMap`, `Tools/RedBlackTree` |
| `ReliableWaiter`, `InstantWaiter` | [FastRing](https://github.com/cyanide-burnout/FastRing), io_uring futex operations (Linux 6.7 or newer) |
| `InstantReplicator` | liburing (io_uring futex operations, Linux 6.7 or newer; multishot timeouts, 6.4), libibverbs, librdmacm, OpenSSL (HMAC-SHA1), an RDMA adapter with remote atomics |
| `InstantDiscovery` | avahi-client and a running avahi-daemon |
| `Tools/Rescue`, `Collapse`, `Epoch` | libsystemd; systemd 254 or newer for the fd store setup in [RECOVERY.md](Documentation/RECOVERY.md#collapse) |

Privileges:

- `userfaultfd` needs `CAP_SYS_PTRACE` when `vm.unprivileged_userfaultfd` is 0, which is the
  default. Without it the tracker stays inactive.
- RDMA memory registration of the pools and buffers needs `CAP_IPC_LOCK` or a sufficient
  `RLIMIT_MEMLOCK`.

The examples expect FastRing checked out next to this repository (`../FastRing`) and use
`pkg-config` for the other dependencies:

```bash
make -C Examples/RDMA
```

```bash
sudo setcap cap_sys_ptrace,cap_ipc_lock=ep Examples/RDMA/test
```

## Examples

The examples are didactic: they show the wiring and leave out error handling. Each has its own
`Makefile` and builds `test` in its directory.

| Example | Shows |
|---|---|
| `Examples/Basic` | Pool lifecycle on a file (`test.dat`), recovery function, allocation and release |
| `Examples/CPP` | `ReliableHolder<T>` and recovery into C++ objects |
| `Examples/Advanced` | Local tracking without RDMA: tracker, indexer and `ReliableWaiter` on a FastRing loop over a `memfd` pool, printing allocations, releases and changes |
| `Examples/RDMA` | Full replication stack on a `memfd`: tracker, indexer, replicator, both waiters and Avahi discovery |
| `Examples/UV` | The replication stack on libuv, bridging `RELIABLE_MONITOR_SHARE_CHANGE` and `INSTANT_REPLICATOR_EVENT_FLUSH` through `uv_async_send()` |

`Lua/` contains a Lua binding of the core pool, described in [LUA.md](Documentation/LUA.md).

## Repository Layout

| Path | Content |
|---|---|
| `Pool/` | The library components |
| `Tools/` | CRC32C, HashMap and RedBlackTree used by the components; Rescue, Collapse and Epoch for restarts under systemd |
| `Examples/` | Examples, see above |
| `Tests/Replication` | Load generator and verifier for replication, `Compare.py` for the dumps |
| `Lua/` | Lua module |
| `Documentation/` | Reference and design documents |

## Documentation

- [API.md](Documentation/API.md): functions, events, constants and limits of every component.
- [RECOVERY.md](Documentation/RECOVERY.md): durability, the recovery function, stop or restart,
  Rescue, Collapse and Epoch, sharing a pool between processes.
- [REPLICATION.md](Documentation/REPLICATION.md): the replication design, optimistic mode, the
  testbed results, known issues and fixed defects.
- [LUA.md](Documentation/LUA.md): the Lua module.

## License

See [LICENSE](LICENSE).
