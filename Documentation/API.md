# API Reference

This document lists the public interface of the components in `Pool/`, component by component.
The concepts behind it (the pool model, the monitor chain, the flush and barrier contract) are
explained in the [README](../README.md); crash recovery and restart helpers are in
[RECOVERY.md](RECOVERY.md), the replication design in [REPLICATION.md](REPLICATION.md).

Functions marked *internal* are exported for other components of the library and are not meant
to be called by an application.

- [ReliablePool](#reliablepool)
- [Monitor Events](#monitor-events)
- [C++ Helpers](#c-helpers)
- [ReliableTracker](#reliabletracker)
- [ReliableFlusher](#reliableflusher)
- [ReliableIndexer](#reliableindexer)
- [ReliableWaiter](#reliablewaiter)
- [InstantReplicator](#instantreplicator)
- [InstantWaiter](#instantwaiter)
- [InstantDiscovery](#instantdiscovery)
- [Limits and Constants](#limits-and-constants)

## ReliablePool

Header: `Pool/ReliablePool.h`.

### Memory Layout

A pool is a file or a `memfd` mapped with `MAP_SHARED`. The mapping starts with
`struct ReliableMemory` followed by an array of fixed-size blocks:

| Field of `ReliableMemory` | Meaning |
|---|---|
| `magic` | `RELIABLE_MEMORY_MAGIC` (7), written last when a pool is created |
| `size` | Block size including `struct ReliableBlock`, aligned to `__BIGGEST_ALIGNMENT__` |
| `length` | Number of blocks |
| `free` | Head of the lock-free free list (tag in the high 32 bits, block number in the low 32 bits) |
| `floor` | Floor of tokens reserved by `InstantReplicator` |
| `name` | Pool name, `RELIABLE_MEMORY_NAME_LENGTH` (8) bytes, not necessarily NUL-terminated |

Every block starts with a header:

| Field of `ReliableBlock` | Scope | Meaning |
|---|---|---|
| `type` | local | `RELIABLE_TYPE_*` |
| `number` | local | Block number, stable for the lifetime of the pool |
| `next` | local | Link in the free list |
| `tag` | local | Generation, incremented every time the block returns to the free list |
| `count` | local | Reference count |
| `mark` | replicated | Fencing token / version mark (see [REPLICATION.md](REPLICATION.md#roles-of-hint-and-mark)) |
| `hint` | replicated | Version of the content; odd while pending or damaged |
| `identifier` | replicated | Global block UUID, assigned by `ReliableIndexer` |
| `control` | replicated | CRC32C of `data[0 .. length)`, maintained only under `ReliableTracker` |
| `length` | replicated | Data length; set to the full capacity on allocation, may be lowered by the application |
| `data` | replicated | Application data, aligned to `__BIGGEST_ALIGNMENT__` |

The replicated part, from `mark` to the end of `data`, is what `InstantReplicator` transfers.
The application owns `data` and `length`; the other fields belong to the library.

### Types and Flags

| Constant | Value | Meaning |
|---|---|---|
| `RELIABLE_TYPE_FREE` | 0 | Block is on the free list |
| `RELIABLE_TYPE_RECOVERABLE` | 1 | Block is passed to the recovery function when the pool is opened with `RELIABLE_FLAG_RESET` |
| `RELIABLE_TYPE_NON_RECOVERABLE` | 2 | Block is freed when the pool is opened with `RELIABLE_FLAG_RESET`; replicas are reserved with this type |
| `RELIABLE_FLAG_RESET` | `1 << 0` | Opening an existing pool resets reference counts and runs recovery |

### Lifecycle

```c
struct ReliablePool* CreateReliablePool(int handle, const char* name, size_t length, uint32_t flags,
                                        struct ReliableMonitor* monitor,
                                        ReliableRecoveryFunction function, void* closure);
void ReleaseReliablePool(struct ReliablePool* pool);
int UpdateReliablePool(struct ReliablePool* pool);
```

`CreateReliablePool()` opens a pool on `handle`, a descriptor of a regular file or a `memfd` open
for reading and writing. `length` is the size of the application data in a block; the block size
is `length + sizeof(struct ReliableBlock)` rounded up to `__BIGGEST_ALIGNMENT__`. `name` is
compared with the stored name (up to 8 bytes). `monitor` is the first element of the
[monitor chain](#monitor-events), or `NULL`.

- If the descriptor already holds a pool with the same magic, block size and name, the pool is
  opened. Blocks left locked by an interrupted transfer (odd `mark`) are turned into the damaged
  state first: `mark` 0, `hint | 1`.
- Otherwise the file is truncated to the initial size and a new pool is created. The initial size
  and every expansion are 1 024 blocks, rounded up to whole pages.
- The descriptor is locked with an OFD write lock (`F_OFD_SETLKW`) while the pool is opened or
  created, so several processes can open the same pool safely.
- Returns `NULL` on failure. The pool takes ownership of `handle` and closes it when the last
  reference to the pool is gone.

With `RELIABLE_FLAG_RESET` on an existing pool, every allocated block gets `count` 0. A block of
type `RELIABLE_TYPE_RECOVERABLE` is passed to `function`, when given, which returns the new type of
the block:

- a non-zero type keeps the block, and `RELIABLE_MONITOR_BLOCK_RECOVER` is delivered for it;
- `RELIABLE_TYPE_FREE` frees it.

Blocks of other types, and recoverable blocks when `function` is `NULL`, are freed. Without
`RELIABLE_FLAG_RESET` the pool is opened as is, which is how a second process attaches to a pool
that is in use.

```c
typedef int (*ReliableRecoveryFunction)(struct ReliablePool* pool, struct ReliableBlock* block, void* closure);
```

The recovery function runs inside `CreateReliablePool()`, after `RELIABLE_MONITOR_POOL_CREATE`
has been delivered to the chain. To keep using a block it takes a reference with
`RecoverReliableBlock()`; a kept block without a reference cannot be attached later, since
`AttachReliableBlock()` requires a non-zero `count`. What the function should validate is described
in [RECOVERY.md](RECOVERY.md#recovery-function).

`ReleaseReliablePool()` delivers `RELIABLE_MONITOR_POOL_RELEASE`, detaches the monitor chain and
drops the reference of the caller. The memory stays mapped while blocks hold references to it.

`UpdateReliablePool()` remaps the pool after another process has grown the file. Returns `1` when
the mapping was replaced, `0` when it is current, a negative errno on failure. Allocation and
attachment call it as needed.

### Blocks

```c
struct ReliableDescriptor
{
  struct ReliablePool* pool;
  struct ReliableShare* share;
  struct ReliableBlock* block;
};

void* AllocateReliableBlock(struct ReliableDescriptor* descriptor, struct ReliablePool* pool, int type);
void* AttachReliableBlock(struct ReliableDescriptor* descriptor, struct ReliablePool* pool, uint32_t number, uint32_t tag, int any);
void* ShareReliableBlock(const struct ReliableDescriptor* source, struct ReliableDescriptor* destination);
void ReleaseReliableBlock(struct ReliableDescriptor* descriptor, int type);
void* RecoverReliableBlock(struct ReliableDescriptor* descriptor, struct ReliablePool* pool, struct ReliableBlock* block);
void RepairReliableBlock(struct ReliablePool* pool, struct ReliableBlock* block);
```

A descriptor is one counted reference: it holds the block, the share (mapping) the block was
reached through, and the pool. The address of the data stays valid while the descriptor is held,
also when the pool grows and is remapped. All functions return a pointer to `block->data`, or
`NULL` with the descriptor cleared.

- `AllocateReliableBlock()` takes a block from the free list, growing the pool when it is empty,
  zeroes its data, sets `length` to the full capacity and `count` to 1, and delivers
  `RELIABLE_MONITOR_BLOCK_ALLOCATE`.
- `AttachReliableBlock()` takes another reference to block `number`, which must be in use
  (`count` non-zero). Unless `any` is non-zero, `tag` must match the generation of the block, so a
  stale number of a recycled block is refused. Delivers `RELIABLE_MONITOR_BLOCK_ATTACH`.
- `ShareReliableBlock()` copies a descriptor and takes another reference. No event.
- `ReleaseReliableBlock()` drops the reference. When the last reference goes, the block gets
  `type` and `RELIABLE_MONITOR_BLOCK_RELEASE` is delivered:
  - `RELIABLE_TYPE_FREE` returns the block to the free list and clears its identifier, `mark`,
    `hint` and `control`;
  - `RELIABLE_TYPE_RECOVERABLE` keeps the block in the pool memory without a reference: the
    process is done with the object, but it is offered to the recovery function the next time
    the pool is opened with `RELIABLE_FLAG_RESET`;
  - `RELIABLE_TYPE_NON_RECOVERABLE` keeps the block until the next reset.

  `ReliableIndexer` drops the block from its index and `InstantReplicator` sends a removal to the
  peers on every release, whatever the type.
- `RecoverReliableBlock()` takes a reference to a block without checks. It is meant for the
  recovery function and for taking over a replica, which arrives without a reference.
- `RepairReliableBlock()` returns a block reported by `RELIABLE_MONITOR_BLOCK_DAMAGE` to service
  after the application has rewritten its content: it clears the pending `hint` and inverts
  `control`, so the next tracker flush publishes the block even when the content matches the old
  checksum.

### Internal Functions

```c
void CallReliableMonitor(int event, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block);
uint32_t ReserveReliableBlock(struct ReliablePool* pool, uuid_t identifier, int type);
int FreeReliableBlock(struct ReliablePool* pool, uint32_t number, uuid_t identifier);
struct ReliableShare* MakeReliableShareCopy(struct ReliablePool* pool, struct ReliableShare* share);
void RetireReliableShare(struct ReliableShare* share);
void RetireReliablePool(struct ReliablePool* pool);
```

- `ReserveReliableBlock()` allocates a block with the given identifier and `count` 0 and delivers
  `RELIABLE_MONITOR_BLOCK_RESERVE`; the replicator uses it for new replicas.
- `FreeReliableBlock()` frees an unreferenced block (`-EBUSY` while it is referenced, `-ENOENT`
  when it is free or the identifier does not match; a null identifier matches any) and delivers
  `RELIABLE_MONITOR_BLOCK_FREE`.
- `MakeReliableShareCopy()` maps a second view of a share; the tracker keeps such an unprotected
  copy for its own writes.
- `RetireReliablePool()` drops a pool reference taken by `FindReliablePool(..., 1)`.

### Shares and Closures

A `ReliableShare` is one mapping of the pool. Growing the pool creates a new share; an old share
stays mapped while descriptors reference it (`weight`: `RELIABLE_WEIGHT_STRONG` per reference,
`RELIABLE_WEIGHT_WEAK` per internal view).

`ReliablePool::closures` and `ReliableShare::closures` (`RELIABLE_MONITOR_CLOSURE_COUNT`, 4) are
slots for monitors. Slot 0 is used by `ReliableTracker` and slot 1 by `InstantReplicator`; slots 2
and 3 are free for application monitors.

## Monitor Events

```c
typedef void (*ReliableMonitorFunction)(int event, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block, void* closure);

struct ReliableMonitor
{
  struct ReliableMonitor* next;
  ReliableMonitorFunction function;
  void* closure;
  const char* name;
};
```

Events are delivered synchronously to every monitor of the chain, from the first to the last, on
the thread that causes them. A chain can be shared by several pools when the application controls
its lifetime and synchronization.

| Event | Value | Source | Thread | Arguments |
|---|---|---|---|---|
| `RELIABLE_MONITOR_POOL_CREATE` | 0 | Pool | `CreateReliablePool()` caller | pool, share |
| `RELIABLE_MONITOR_POOL_RELEASE` | 1 | Pool | `ReleaseReliablePool()` caller; the last event of the chain | pool, share |
| `RELIABLE_MONITOR_SHARE_CREATE` | 2 | Pool | Thread that grows or remaps the pool | pool, new share |
| `RELIABLE_MONITOR_SHARE_DESTROY` | 3 | Pool | Thread that drops the last reference to an old share | pool, share |
| `RELIABLE_MONITOR_BLOCK_ALLOCATE` | 4 | Pool | `AllocateReliableBlock()` caller | pool, share, block |
| `RELIABLE_MONITOR_BLOCK_RECOVER` | 5 | Pool | `CreateReliablePool()` caller, after the recovery function kept the block | pool, share, block |
| `RELIABLE_MONITOR_BLOCK_ATTACH` | 6 | Pool | `AttachReliableBlock()` caller | pool, share, block |
| `RELIABLE_MONITOR_BLOCK_RELEASE` | 7 | Pool | Caller dropping the last reference | pool, share, block |
| `RELIABLE_MONITOR_BLOCK_RESERVE` | 8 | Pool | `ReserveReliableBlock()` caller (replicator thread) | pool, share, block |
| `RELIABLE_MONITOR_BLOCK_FREE` | 9 | Pool | `FreeReliableBlock()` caller (replicator thread, `RemoveUnusedReliableBlockList()` caller) | pool, share, block |
| `RELIABLE_MONITOR_SHARE_CHANGE` | 10 | Tracker | Tracker thread, on the first write to a protected page | pool, share |
| `RELIABLE_MONITOR_BLOCK_CHANGE` | 11 | Tracker | `FlushReliableTracker()` caller | pool, shadow share, block in the shadow share |
| `RELIABLE_MONITOR_FLUSH_COMMIT` | 12 | Tracker | `FlushReliableTracker()` caller, at the end of a cycle that found changes | pool |
| `RELIABLE_MONITOR_BLOCK_DAMAGE` | 13 | Replicator | Replicator thread | pool, share, block |
| `RELIABLE_MONITOR_BLOCK_ARRIVAL` | 14 | Replicator | Replicator thread, under the barrier | pool, share, block |
| `RELIABLE_MONITOR_BLOCK_REMOVAL` | 15 | Replicator | Replicator thread, under the barrier | pool, share, block |

Notes:

- The order of the chain matters. `ReliableIndexer` assigns the block identifier on
  `RELIABLE_MONITOR_BLOCK_ALLOCATE`, so it must precede `InstantReplicator`; the tracker is
  normally first, so that the shares are protected before the other monitors see them.
- In `RELIABLE_MONITOR_BLOCK_CHANGE` the block belongs to the tracker's unprotected shadow
  mapping; writes through it are not tracked.
- A monitor that keeps state for the duration of a flush cycle (between `BLOCK_CHANGE` events and
  `FLUSH_COMMIT`) must keep it in thread-local storage, not in its instance, as
  `ReliableFlusher` and `InstantReplicator` do.

## C++ Helpers

`ReliablePool.h` provides two templates when compiled as C++.

### ReliableHolder

`ReliableHolder<Type>` owns one descriptor (RAII). `Type` must be trivially copyable and trivially
destructible.

| Constructor | Equivalent |
|---|---|
| `ReliableHolder(pool, type)` | `AllocateReliableBlock()` |
| `ReliableHolder(pool, block)` | `RecoverReliableBlock()`, for the recovery function |
| `ReliableHolder(pool, number, tag, any)` | `AttachReliableBlock()` |
| `ReliableHolder(const ReliableHolder&)`, `ReliableHolder(ReliableDescriptor&)` | `ShareReliableBlock()` |

The destructor releases the block with `RELIABLE_TYPE_FREE`; `release(type)` releases it earlier
with another type (`RELIABLE_TYPE_RECOVERABLE` keeps the object for the next start). `get()` and
`operator->` give the data, `operator bool` tells whether a block is held. Moves transfer the
descriptor.

### ReliableAllocator

`ReliableAllocator<Type>` is an allocator for node-based standard containers (one object per
allocation). Each object lives in its own block together with its descriptor and the
`typeid(Type).hash_code()` of the type, so the block size of the pool must be at least
`ReliableAllocator<Type>::size`.

- `ReliableAllocator(pool, initial, final)` allocates blocks of type `initial` (default
  `RELIABLE_TYPE_RECOVERABLE`) and releases them with `final` (default `RELIABLE_TYPE_FREE`).
- `assign(pool, block)` arms the allocator with a recovered block: the next `allocate(1)` returns
  the object in that block instead of a new one, after checking the stored length and type hash
  (`std::bad_alloc` on mismatch). This lets the recovery function rebuild a container node in
  place.

## ReliableTracker

Header: `Pool/ReliableTracker.h`.

```c
struct ReliableTracker* CreateReliableTracker(uint32_t flags, struct ReliableMonitor* next);
void ReleaseReliableTracker(struct ReliableTracker* tracker);
int FlushReliableTracker(struct ReliableTracker* tracker);

int LockReliableShare(struct ReliableShare* share);
int UnlockReliableShare(struct ReliableShare* share);

int64_t GetReliableTrackerClockVector(struct timespec* remote);
int VerifyReliableBlockIntegrity(const struct ReliableBlock* block);
```

The tracker is a monitor (`&tracker->super`) and a background thread. On
`RELIABLE_MONITOR_POOL_CREATE` and `RELIABLE_MONITOR_SHARE_CREATE` it registers the share with
`userfaultfd` in write-protect mode and maps a second, unprotected view of it. The first write to
a protected page faults into the tracker thread, which marks the page dirty, wakes the waiter,
delivers `RELIABLE_MONITOR_SHARE_CHANGE` and lifts the protection of that page.

`CreateReliableTracker()` always returns an object (or `NULL` when out of memory); check
`RELIABLE_TRACKER_STATE_ACTIVE` in `tracker->state` to see whether `userfaultfd` with
`UFFD_FEATURE_PAGEFAULT_FLAG_WP` is available. A share that cannot be registered raises
`RELIABLE_TRACKER_STATE_FAILURE` and is not tracked; `ACTIVE` stays set.

### Supported Memory

The kernel interface is generic: whether a mapping accepts write protection is decided by the
`vm_uffd_ops` of its memory type. The synchronous mode the tracker uses, in which every first
write to a page is reported to the tracker thread, is implemented today only by anonymous memory,
shmem (`memfd`, tmpfs) and hugetlbfs. The page cache of disk file systems does not implement it:
ext4, xfs, btrfs and the generic file mappings of `mm/filemap.c` provide no `vm_uffd_ops`, so
registering a pool on such a file fails and the tracker raises `RELIABLE_TRACKER_STATE_FAILURE`.
The asynchronous mode (`UFFD_FEATURE_WP_ASYNC`, Linux 6.7) accepts any memory type, but it reports
no faults to user space, so the tracker cannot use it as it is.

Checked by reading the kernel sources on 2026-10-10: `vma_can_userfault()` in `mm/userfaultfd.c`
of Linux 6.12 and of the master branch, and the providers of `vm_uffd_ops` in master (only
`mm/shmem.c` and `mm/hugetlb.c`; none in `mm/filemap.c`, `fs/ext4/file.c`, `fs/xfs/xfs_file.c`,
`fs/btrfs/file.c`). Not verified on a running kernel.

Flags:

| Flag | Meaning |
|---|---|
| `RELIABLE_TRACKER_FLAG_ID_HOST` | Node identifier includes the machine ID |
| `RELIABLE_TRACKER_FLAG_ID_PROCESS` | Node identifier includes the PID |
| `RELIABLE_TRACKER_FLAG_ID_UNIQUE` | Node identifier includes the address of the tracker |
| `RELIABLE_TRACKER_FLAG_FORCE_MARK` | Mark blocks on every flush even when the checksum is unchanged; fewer CRC checks, but it hurts replication |

The node identifier is a CRC32C over the selected sources; its low 16 bits go into every version
the tracker makes.

State bits in `tracker->state`: `RELIABLE_TRACKER_STATE_ACTIVE`, `RELIABLE_TRACKER_STATE_FAILURE`
(a `userfaultfd` operation failed; sticky), `RELIABLE_TRACKER_STATE_KICK` (changes are pending;
also the futex word the waiter sleeps on).

`FlushReliableTracker()` is one flush cycle. It must be called in an idempotent state of the
application, see [Execution Contract](../README.md#execution-contract). It returns `-ENOENT` when
nothing changed since the last cycle, otherwise:

1. takes a new version from `CLOCK_REALTIME` (see below);
2. write-protects the dirty pages again;
3. for every block on a dirty page that is allocated, not damaged (`hint` even) and not already
   marked by this cycle, computes the CRC32C of `data[0 .. length)`; when it differs from
   `control`, stores the new `control`, sets `mark` and `hint` to the version and delivers
   `RELIABLE_MONITOR_BLOCK_CHANGE`;
4. delivers `RELIABLE_MONITOR_FLUSH_COMMIT` when anything was dirty;
5. releases shares that are no longer referenced;
6. returns `0`.

A version is `CLOCK_REALTIME` in nanoseconds with the low 24 bits cleared (an epoch of 2²⁴ ns,
16.7 ms), the low 16 bits of the node identifier in bits 8–23 and a counter in bits 0–7 that
advances by 4, so the two low bits are zero. Versions of one tracker are strictly increasing,
also across a backward clock step.

`LockReliableShare()` and `UnlockReliableShare()` write-protect a whole share and later restore
the protection of the pages that were dirty; they are reserved for zero-copy hot replication.

`GetReliableTrackerClockVector()` returns the difference between a remote `CLOCK_REALTIME` and the
local one, rounded to the epoch. `InstantReplicator` uses it to normalize remote versions.

`VerifyReliableBlockIntegrity()` returns non-zero when the block is neither `NULL` nor free and
the CRC32C of its data matches `control`. It is meant for recovery and application validation of
tracked pools.

## ReliableFlusher

Header: `Pool/ReliableFlusher.h`.

```c
struct ReliableFlusher* CreateReliableFlusher(struct ReliableMonitor* next);
void ReleaseReliableFlusher(struct ReliableFlusher* flusher);
```

An optional monitor placed after the tracker. It collects `RELIABLE_MONITOR_BLOCK_CHANGE` reports
and synchronizes each share with `msync(MS_SYNC)` once the share is complete: when the reports move
to another share or `RELIABLE_MONITOR_FLUSH_COMMIT` closes the cycle. Deferring `msync()` this way
includes the tracker metadata of all reported blocks. A failed `msync()` raises the sticky
`RELIABLE_FLUSHER_STATE_FAILURE` in `flusher->state`; delivery of events to the following monitors
is not affected.

The flusher is useful only for pools backed by a real file; `msync()` does nothing for a `memfd`.
What it guarantees is described in [RECOVERY.md](RECOVERY.md#durability). With current kernels the
tracker cannot track a pool on a disk file system (see [Supported Memory](#supported-memory)), so
the flusher has no effect in practice until a file system implements the write protection the
tracker needs.

## ReliableIndexer

Header: `Pool/ReliableIndexer.h`.

```c
struct ReliableIndexer* CreateReliableIndexer(struct ReliableMonitor* next);
struct ReliableIndexer* GetReliableIndexer(struct ReliablePool* pool);
void ReleaseReliableIndexer(struct ReliableIndexer* indexer);

struct ReliablePool* FindReliablePool(struct ReliableIndexer* indexer, const char* name, int acquire);
uint32_t FindReliableBlockNumber(struct ReliableIndexer* indexer, const char* name, uuid_t identifier);
uint32_t* CollectReliableBlockList(struct ReliableIndexer* indexer, struct ReliablePool* pool, struct timespec* time, uint32_t flags);
int RemoveUnusedReliableBlockList(struct ReliableIndexer* indexer, struct ReliablePool* pool, struct timespec* time);
```

A monitor that keeps two indexes: pool name → pool, and (pool name, block UUID) → block number.
On `RELIABLE_MONITOR_BLOCK_ALLOCATE` it generates the UUID of the block (`uuid_generate()`);
recovered and reserved blocks keep the identifier they have. Released and freed blocks leave the
index. `InstantReplicator` finds the indexer through `GetReliableIndexer()`, which walks the chain
of a pool.

- `FindReliablePool()` returns the pool by name. With `acquire` 1 it takes a reference, to be
  dropped with `RetireReliablePool()`.
- `FindReliableBlockNumber()` returns the block number or `UINT32_MAX`.
- `CollectReliableBlockList()` returns a `malloc()`ed array of block numbers of the pool,
  terminated by `UINT32_MAX`, or `NULL`. `flags` select unreferenced blocks
  (`RELIABLE_COLLECT_UNUSED`), referenced blocks (`RELIABLE_COLLECT_IN_USE`) or both. With `time`,
  only blocks whose `mark` is older than that `CLOCK_REALTIME` moment are listed.
- `RemoveUnusedReliableBlockList()` frees the unreferenced blocks selected the same way and
  returns their count, or `-ENOMEM`. Replicas are unreferenced, so this also frees replicas,
  zombies included, that have not changed since the given time.

The index lives in process memory; it is rebuilt from the recovery events when a pool is
reopened.

## ReliableWaiter

Header: `Pool/ReliableWaiter.h`. Requires [FastRing](https://github.com/cyanide-burnout/FastRing).

```c
struct FastRingDescriptor* SubmitReliableWaiter(struct FastRing* ring, struct ReliableTracker* tracker);
void CancelReliableWaiter(struct FastRingDescriptor* descriptor);
```

Submits an `IORING_OP_FUTEX_WAIT` on the tracker state. When the tracker reports changes, the
waiter calls `FlushReliableTracker()` on the thread of the ring and submits itself again. Returns
`NULL` when the kernel does not support io_uring futex operations or the tracker is not active;
the application then drives flushes from `RELIABLE_MONITOR_SHARE_CHANGE`, as `Examples/UV` does.

## InstantReplicator

Header: `Pool/InstantReplicator.h`.

```c
struct InstantReplicator* CreateInstantReplicator(int port, uuid_t identifier, const char* name, const char* secret,
                                                  uint32_t options, uint32_t timeout,
                                                  HandleInstantEventFunction function, void* closure,
                                                  struct ReliableMonitor* next);
void ReleaseInstantReplicator(struct InstantReplicator* replicator);

int FlushInstantReplicator(struct InstantReplicator* replicator);
int RegisterRemoteInstantReplicator(struct InstantReplicator* replicator, uuid_t identifier, struct sockaddr* address, socklen_t length);
int TransmitInstantReplicatorUserMessage(struct InstantReplicator* replicator, const char* data, uint32_t length, int wait);
```

### Creation

`CreateInstantReplicator()` creates the monitor (`&replicator->super`) and its thread, and listens
for RDMA connections (`rdma_cm`, `RDMA_PS_TCP`, RC queue pairs) on all addresses.

| Parameter | Meaning |
|---|---|
| `port` | Listening port; 0 lets `rdma_cm` choose one (`InstantDiscovery` announces the actual port) |
| `identifier` | UUID of this instance; `NULL` generates a random one. A stable identifier lets peers recognize a restarted node |
| `name` | Replication group name; peers must use the same name. Up to `INSTANT_SERVICE_NAME_LENGTH` (16) bytes go into the handshake |
| `secret` | Shared secret for the HMAC-SHA1 of the handshake |
| `options` | `INSTANT_REPLICATOR_OPTION_*` |
| `timeout` | Milliseconds a peer may go without progress before its connection is closed; 0 selects 1 000 ms. Rounded up to 200 ms ticks, see [Timeouts](REPLICATION.md#timeouts) |
| `function`, `closure` | Event handler, see below |
| `next` | Next monitor of the chain |

It always returns an object (or `NULL` when out of memory); check `INSTANT_REPLICATOR_STATE_ACTIVE`
in `replicator->state`. The replicator needs `ReliableTracker` (it reads the tracker's shadow
mapping and versions) and `ReliableIndexer` in the same chain, before it.

`ReleaseInstantReplicator()` stops the thread and frees everything. After
`INSTANT_REPLICATOR_STATE_FAILURE` (a queue pair could not be destroyed, so DMA may still reach the
memory) nothing is freed and the barrier stays raised until the process exits. The replicator
thread uses the indexer until it is joined, so release the replicator before the indexer.

Options:

- `INSTANT_REPLICATOR_OPTION_OPTIMISTIC_MODE`: under the receiver barrier, offered blocks are first
  read by RDMA READ straight into the locked local blocks, without involving the sender's barrier,
  and the source `mark` is read after the data. A block is accepted when the source still shows
  the offered `mark`, the identifier matches and the CRC32C matches `control`; the rest go through
  the synchronous CAS/WRITE transfer. The synchronous transfer stays the default: the option
  trades a part of the parking of the main thread for DAMAGE on copies overwritten by a rejected
  read, mostly of objects already released at the source. See
  [Optimistic Mode](REPLICATION.md#optimistic-mode).

State bits in `replicator->state`: `INSTANT_REPLICATOR_STATE_ACTIVE`,
`INSTANT_REPLICATOR_STATE_FAILURE`, `INSTANT_REPLICATOR_STATE_LOCK` (barrier raised),
`INSTANT_REPLICATOR_STATE_READY` (application parked in the barrier).

### Events

```c
typedef void (*HandleInstantEventFunction)(int event, struct InstantPeer* peer, const char* data, int parameter, void* closure);
```

The handler runs on the replicator thread and must not block.

| Event | `peer` | `data`, `parameter` | Meaning |
|---|---|---|---|
| `INSTANT_REPLICATOR_EVENT_FLUSH` | `NULL` | — | The replicator raised its barrier and needs the application to call `FlushInstantReplicator()` |
| `INSTANT_REPLICATOR_EVENT_CONNECTED` | peer | — | A connection is established; syncing of the pools starts |
| `INSTANT_REPLICATOR_EVENT_DISCONNECTED` | peer | `parameter`: `rdma_cm` event that closed it | A connected peer is gone |
| `INSTANT_REPLICATOR_EVENT_USER_MESSAGE` | sender | payload and its length | A user message arrived |

`INSTANT_REPLICATOR_EVENT_FLUSH` must not be answered by calling `FlushInstantReplicator()` from the
handler: the handler runs on the replicator thread, which is the one that releases the barrier.
Forward it to the application thread (a waiter, `uv_async_send()`, an eventfd).

The replicator also delivers to the monitor chain:

- `RELIABLE_MONITOR_BLOCK_ARRIVAL`: transferred data passed validation and was installed.
- `RELIABLE_MONITOR_BLOCK_DAMAGE`: a transfer ended after it may have overwritten the block
  (validation retries exhausted, a rejected optimistic read, a disconnect during a transfer). The
  block is left with `mark` 0 and a pending `hint` and is not published until a later transfer
  installs a version or the application calls `RepairReliableBlock()`.
- `RELIABLE_MONITOR_BLOCK_REMOVAL`: a removal from a peer reached a block that is referenced
  locally; the block is kept and the application decides. Unreferenced blocks are freed silently.
  Removals are applied 10 s after they arrive, under the barrier.

### Barrier

```c
int FlushInstantReplicator(struct InstantReplicator* replicator);
```

Called by the application thread at a safe point. When the barrier is raised, it marks the
application as parked (`READY`) and sleeps until the replicator releases the barrier; otherwise it
returns at once. Returns `0`, `-EFAULT` when the replicator has stopped or failed, `-EINVAL` for a
`NULL` replicator. While the caller is parked, no other thread may modify pool objects. See
[Execution Contract](../README.md#execution-contract).

### Peers

`RegisterRemoteInstantReplicator()` adds a peer, or another address of a known peer (up to
`INSTANT_POINT_COUNT`, 8). Returns `0`, `-EEXIST` when the address is known, `-ENOMEM`. Connections
are accepted only from registered peers that pass the handshake (same magic, same `name`, valid
HMAC), so both sides must know each other; `InstantDiscovery` registers peers automatically.

The replicator connects to disconnected peers on its 200 ms tick, rotating over their addresses.
A peer that cannot be reached for `CONNECTION_ATTEMPT_COUNT` (128) attempts in a row, at most one
per tick, so after at least about 25 s, is forgotten: this is the regular way dead peers are
retired. The intended setup is `InstantDiscovery`, which registers a peer again as soon as it
announces itself; an application that registers peers statically re-registers them itself when it
wants them back.

A peer that stops answering without breaking the connection is disconnected after `timeout`: when
a transfer waiting for it does not advance, or its credit window stays used up while it sends
nothing. The barrier and the queued messages never wait longer for a stopped peer; it is caught up
by syncing after it connects again.

### User Messages

`TransmitInstantReplicatorUserMessage()` queues a message of up to
`INSTANT_BUFFER_LENGTH - sizeof(struct InstantHeaderData) - 1` bytes (4 073) for every connected
peer. Messages are delivered in order and lost only with a broken connection. With `wait` the
caller sleeps until the queue has a place and a buffer is free; without it the call returns
`-EBUSY` instead. Returns `-EFAULT` when the replicator has stopped or failed, `-EINVAL` for bad
arguments. Called from an event handler, it never waits, since the replicator thread is the one
that empties the queue.

### Buffers and Credits

- The application thread never waits for replication buffers, except in
  `TransmitInstantReplicatorUserMessage()` with `wait`, and cannot take the last
  `INSTANT_RESERVE_COUNT` of them, which stay for the replicator thread. A block change
  notification that does not fit is counted as lost and the affected peers are synchronized again;
  removals and user messages are built once in a shared buffer, queued and sent in order to every
  peer.
- Every peer grants a window of messages that consume its receiving buffers, so a stopped or slow
  peer never gets more than it can receive: a SEND never waits for `RNR` and never holds up the
  reads and writes posted after it on the same connection. The count of received messages returns
  in `imm_data` of every SEND; windows are announced and confirmed by `INSTANT_TYPE_CREDIT`.

### Adapter Requirements

The synchronous transfer locks remote blocks by RDMA compare-and-swap, so the HCA must support
remote atomics. Up to `INSTANT_ATOMIC_COUNT` (16) RDMA READ and atomic operations are kept in flight
per connection, limited by the weakest local card and by the depths the peer requests. Adapters
without these capabilities are rejected during card setup and their peers never become connected.

### Protocol

Every SEND starts with `InstantHeaderData` (instance UUID, type, task number); the payload depends
on the type:

| Type | Payload |
|---|---|
| `INSTANT_TYPE_CLOCK` | `struct timespec`, the sender's `CLOCK_REALTIME` |
| `INSTANT_TYPE_NOTIFY` | `InstantCookieData` (pool name, memory keys) and `InstantBlockData` entries: offered versions |
| `INSTANT_TYPE_RETRIEVE` | The same layout: blocks the receiver asks for, with the tokens that complete the transfer |
| `INSTANT_TYPE_COMPLETE` | None: the task is complete |
| `INSTANT_TYPE_REMOVE` | `InstantRemovalData` (pool name, block UUID) |
| `INSTANT_TYPE_CREDIT` | `InstantCreditData` (window granted to the peer, window of the peer being applied) |
| `INSTANT_TYPE_USER` | Arbitrary user payload |

The connection request carries `InstantHandshakeData` (magic `INSTANT_MAGIC`, nonce, instance UUID,
group name, HMAC-SHA1 digest) as private data. The format is internal and versioned only by the
magic; all nodes must run compatible revisions.

## InstantWaiter

Header: `Pool/InstantWaiter.h`. Requires FastRing.

```c
struct FastRingDescriptor* SubmitInstantWaiter(struct FastRing* ring, struct InstantReplicator* replicator);
void CancelInstantWaiter(struct FastRingDescriptor* descriptor);
```

Submits an `IORING_OP_FUTEX_WAIT` on the replicator state. When the barrier is raised, the waiter
calls `FlushInstantReplicator()` on the thread of the ring, which parks that thread until the
barrier is released, and submits itself again. It stops when `FlushInstantReplicator()` reports a
fault. Returns `NULL` when io_uring futex operations are not supported or the replicator is not
active.

## InstantDiscovery

Header: `Pool/InstantDiscovery.h`. Requires Avahi.

```c
struct InstantDiscovery* CreateInstantDiscovery(AvahiPoll* poll, struct InstantReplicator* replicator);
void ReleaseInstantDiscovery(struct InstantDiscovery* discovery);
```

- Publishes the local replicator as a `_replicator._tcp` service named after the replicator
  `name`, on its RDMA port, with the TXT record `instance=<uuid>`. A name collision is resolved with
  an Avahi alternative name (`name #2`).
- Browses `_replicator._tcp`, resolves services with the same name (or its alternatives), skips
  its own service and loopback addresses, and calls `RegisterRemoteInstantReplicator()` for each.
- Restarts the Avahi client after 200 ms on transient daemon or D-Bus failures.

Returns `NULL` when the replicator is not active or the Avahi client cannot be created. `poll` is
any `AvahiPoll`; the examples use `FastAvahiPoll` from FastRing.

## Limits and Constants

| Constant | Value | Meaning |
|---|---|---|
| `RELIABLE_MEMORY_NAME_LENGTH` | 8 | Bytes of a pool name |
| `RELIABLE_MONITOR_CLOSURE_COUNT` | 4 | Closure slots in a pool and a share |
| Pool growth | 1 024 blocks | Initial size and step of expansion, rounded up to pages |
| `INSTANT_SERVICE_NAME_LENGTH` | 16 | Bytes of the replication group name in the handshake |
| `INSTANT_POINT_COUNT` | 8 | Addresses per peer |
| `INSTANT_CARD_COUNT` | 64 | RDMA devices |
| `INSTANT_QUEUE_LENGTH` | 2048 | Shared buffers, sending queue and receiving buffers per card |
| `INSTANT_BUFFER_LENGTH` | 4096 | Size of a message buffer (InfiniBand MTU) |
| `INSTANT_RESERVE_COUNT` | 256 | Shared buffers reserved for the replicator thread |
| `INSTANT_ATOMIC_COUNT` | 16 | RDMA READ and atomic operations in flight per connection |
| `INSTANT_BARRIER_COUNT` | 1024 | Block entries served by one barrier; a backlog is split |
| `INSTANT_CLOCK_COUNT` | 8 | Clock measurements a peer's vector is taken from |
| `INSTANT_MESSAGE_COUNT` | 512 | Queued removals and user messages |
| `INSTANT_CREDIT_RESERVE` | 64 | Receiving buffers kept for credit reports |
| Replicator tick | 200 ms | Timers, reconnects, clock exchange |
| Default peer timeout | 1 000 ms | `timeout` 0 |
| Connection attempts | 128 | Before an unreachable peer is forgotten |
| Reading attempts | 3 | Validation retries of a transfer before DAMAGE |
| Removal delay | 10 s | Before a received removal is applied |

A NOTIFY message carries as many block entries as fit into one `INSTANT_BUFFER_LENGTH` buffer.
Block data itself goes by RDMA READ and WRITE, so the block size is not limited by the buffers.
