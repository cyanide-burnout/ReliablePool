# Recovery and Restart

This document describes how the state kept in a `ReliablePool` survives a restart of the service:
what is durable and when, how an application validates and rebuilds its objects in the recovery
function, and the helpers in `Tools/` that keep a `memfd` alive across restarts under systemd.
The API is listed in [API.md](API.md), the overall model in the [README](../README.md).

- [Backing Choices](#backing-choices)
- [Durability](#durability)
- [Recovery Function](#recovery-function)
- [Stop or Restart](#stop-or-restart)
- [Rescue](#rescue)
- [Collapse](#collapse)
- [Epoch](#epoch)
- [Sharing a Pool With Another Process](#sharing-a-pool-with-another-process)

## Backing Choices

A pool lives in whatever its descriptor refers to:

- **`memfd` kept in the systemd fd store.** The memory survives a restart of the service, a
  `soft-reboot` and, with LUO, a kexec, but not a reboot. It holds the exact state of the dying
  process, including an update it was in the middle of. This is how BrandMeister and TetraPack use
  the pool.
- **Regular file.** The content survives a reboot, but only as far as the kernel has written it
  back; see the next section. Such a pool works without tracking; with current kernels
  `ReliableTracker` cannot track a file on a disk file system
  (see [Supported Memory](API.md#supported-memory)).

## Durability

The tracked file-backed case below describes the design. With current kernels it cannot be reached
on a disk file system, since the tracker cannot register such a file
(see [Supported Memory](API.md#supported-memory)); a `memfd` pool is covered by the last point of
the list.

There is no WAL, so `msync()` does not make blocks atomically persistent. A flush cycle is the
durability boundary: `FlushReliableTracker()` runs in an idempotent state and updates `control`
(CRC32C) together with `mark` and `hint` for the consistent pool state observed by that cycle. The
CRC allows recovery to detect a block whose data and metadata were persisted inconsistently.

Persistence behavior:

- `ReliableFlusher` gives the lower bound: everything confirmed by a completed flush cycle survives
  a crash, provided it did not set `RELIABLE_FLUSHER_STATE_FAILURE`.
- There is no upper bound: kernel background writeback persists dirty pages between cycles at
  arbitrary moments, so after a crash the file may additionally contain partial state of later,
  unconfirmed changes ("torn" blocks).
- A `memfd`-backed pool has no filesystem writeback tearing. With **FDSTORE** it retains the exact
  in-memory state across a service restart, including an update interrupted by the dying process;
  it does not survive a host reboot at all.

Torn blocks come in two kinds:

- Data newer than metadata: the block looks stale to peers and replication fetches it again; it
  heals itself.
- Metadata newer than data (or `mark` still carrying the in-flight low bit of an interrupted
  transfer): the block looks fresh while its data is stale. This kind must be handled explicitly.

Healing is deliberately left to the application. Whether a pool is tracked and replicated is the
application's choice, and `block->control` is maintained only under tracking, so no component can
decide validity on its own. The hook is the recovery function, which keeps crash validation on the
restart path instead of adding runtime cost.

Torn blocks cannot poison other nodes: receivers validate the CRC of every arrival and reject
mismatching transfers.

## Recovery Function

When an existing pool is opened with `RELIABLE_FLAG_RESET`, `CreateReliablePool()` calls the
recovery function once for every block of type `RELIABLE_TYPE_RECOVERABLE`, before the block is
published through `RELIABLE_MONITOR_BLOCK_RECOVER`. Blocks of type `RELIABLE_TYPE_NON_RECOVERABLE`,
replicas among them, are freed without a call. The function returns the type the block keeps, or
`RELIABLE_TYPE_FREE` to discard it.

A function that keeps a block has to:

1. **Validate it.** Reject records whose owner no longer exists, that have expired, or that
   duplicate a record already recovered.
2. **Take a reference** with `RecoverReliableBlock()` (or `ReliableHolder(pool, block)`); a kept
   block without a reference is never released and cannot be attached.
3. **Repair the content.** Raw pointers stored in a block point into the previous process: clear
   them or bind them to the new objects. `CLOCK_MONOTONIC` timestamps need the [Epoch](#epoch)
   correction.
4. **Index the object** in the application's own maps and trees, since nothing else knows it yet.

A typical shape, close to the recovery of message cache entries in BrandMeister:

```c
static int RecoverMessage(struct ReliablePool* pool, struct ReliableBlock* block, void* closure)
{
  struct MessageData* data = (struct MessageData*)block->data;
  struct Cache* cache      = (struct Cache*)closure;
  time_t now               = GetMonotonicTime();

  data->expires = FixEpochTime(data->expires);

  if ((data->expires <= now) ||                         /* expired */
      ((data->expires - now) > data->interval))         /* the clock is not continuous, the record is suspect */
    return RELIABLE_TYPE_FREE;

  if (FindStore(cache, data->owner) == NULL)            /* owner is not present in this run */
    return RELIABLE_TYPE_FREE;

  CreateMessageEntry(cache, pool, block);               /* RecoverReliableBlock(), reindex */
  return RELIABLE_TYPE_RECOVERABLE;
}
```

Patterns that work well:

- **Parents first.** Pools are recovered in the order they are opened. Open the pool of parent
  objects first; the recovery function of a child pool looks its parent up by an identifier
  stored in the child and frees the child when the parent is gone.
- **Open late.** Recovery calls into the application. Create the pool only when every component
  that owns records is ready to accept them.
- **Defer what needs the rest of the system.** Decisions that depend on configuration or
  connections can run after all pools are open: keep the objects, then delete those that turn out
  to be unwanted.

### Tracked and Replicated Pools

Recovery of a replicated pool is not recommended: a pool should use either recovery or
replication (see [Replication Model Boundaries](../README.md#replication-model-boundaries)).

For a pool under `ReliableTracker`, and optionally `InstantReplicator`, the function also checks
the integrity of a block:

- Keep the block as is when `hint & 1` is clear and `VerifyReliableBlockIntegrity(block)` returns
  non-zero. Opening the pool already turns the lock of a transfer interrupted by the crash
  (`mark & 1`) into the damaged state: `mark` 0 and a pending `hint` (`hint & 1`).
- Otherwise pick one of two outcomes:
  - return `RELIABLE_TYPE_FREE`: discard the block when the data model does not tolerate partial
    writes;
  - keep the allocation but zero `mark` and `hint`: the block is declared stale, and startup
    synchronization can fetch it again from a peer with a newer valid copy.

At runtime, a transfer that ended after it may have overwritten a block reports
`RELIABLE_MONITOR_BLOCK_DAMAGE` and leaves the block damaged: `mark` 0 and a pending `hint`. Neither
the tracker nor the replicator publishes such a block. It leaves this state when a later transfer
from a peer installs a version, or when the application rewrites the content and calls
`RepairReliableBlock(pool, block)`, which clears the pending `hint` and inverts `control`, so the
next tracker flush sees a changed checksum and publishes the block even when the content was
restored to the bytes that matched the old `control`.

## Stop or Restart

What happens to a block is decided when it is released:

| Release type | Effect |
|---|---|
| `RELIABLE_TYPE_FREE` | Block returns to the free list; the object is gone |
| `RELIABLE_TYPE_RECOVERABLE` | Block stays; the next start offers it to the recovery function |
| `RELIABLE_TYPE_NON_RECOVERABLE` | Block stays until the next reset, then it is freed |

The type given at allocation is the default intention for the object; the type given at release
is the final word. A service therefore decides on `SIGTERM` whether its state will survive
(see [Collapse](#collapse)) and shuts down accordingly:

- **State survives** (unit restart, soft-reboot, kexec with LUO): release the objects that must
  come back with `RELIABLE_TYPE_RECOVERABLE` (or with their current type), keep connections up as
  far as the protocol allows, and leave the descriptors in the fd store.
- **State does not survive**: release objects with `RELIABLE_TYPE_FREE` and remove the descriptors
  from the fd store with `RemoveRescuedHandle(handle, RESCUE_REMOVE_CLOSE)`, so systemd does not
  keep memory nobody will use.

`ReleaseReliablePool()` comes after the objects that hold blocks. The pool owns its descriptor and
closes it when the last reference is gone, which may be a block descriptor released after the pool.

## Rescue

A `memfd`-backed pool survives a service restart only while its descriptor is kept by systemd in
the unit's fd store. `Tools/Rescue` owns the fd store of the process:

- at startup (a constructor, when running under systemd: parent PID 1, `sd_booted()` and
  `NOTIFY_SOCKET`) it takes over `LISTEN_FDS` / `LISTEN_FDNAMES`, relocates the descriptors above
  `FD_SETSIZE` and indexes them by name; outside systemd it stays inactive, `GetRescuedHandle()`
  returns `-1` and `AddRescuedHandle()` does nothing;
- `GetRescuedHandle(name)` returns a descriptor restored from the previous run, or `-1`;
- `AddRescuedHandle(handle, name)` puts a descriptor into the fd store (`FDSTORE=1`,
  `FDNAME=name`);
- `RemoveRescuedHandle(handle, RESCUE_REMOVE_CLOSE)` removes it from the fd store
  (`FDSTOREREMOVE=1`); `RESCUE_REMOVE_SAVE` forgets the record and leaves the descriptor in the
  fd store;
- `CloseUnusedRescuedHandleList()` closes and removes restored descriptors that nobody claimed;
  call it once start-up is complete.

Names must not contain `:` or control characters. A name without `%` is stored by pointer, not
copied.

The usual start-up:

```c
int handle = GetRescuedHandle("ContextData");

if (handle < 0)
{
  handle = memfd_create("ContextData", MFD_CLOEXEC);
  AddRescuedHandle(handle, "ContextData");
}

pool = CreateReliablePool(handle, "CTX", sizeof(struct ContextData), RELIABLE_FLAG_RESET, NULL, RecoverContext, manager);
```

When pools depend on each other, create all their descriptors anew if any of them is missing, so
children never outlive their parents.

## Collapse

On `SIGTERM` the service has to decide whether to keep its state (and not tear down connections)
or to perform a regular destructive stop. systemd does not tell this to the service
([systemd#43880](https://github.com/systemd/systemd/issues/43880)), so `Tools/Collapse` asks PID 1
(`ListJobs` over sd-bus) why the unit is being stopped:

- `GetCollapseCause()` returns a bit field of `COLLAPSE_RESTART` (restart job of the unit itself),
  `COLLAPSE_KEXEC`, `COLLAPSE_SOFT_REBOOT`, `COLLAPSE_REBOOT`, `COLLAPSE_POWEROFF`,
  `COLLAPSE_HALT`, or a negative errno;
- `IsLiveUpdateAvailable()` reports whether LUO (Live Update Orchestrator) is active in the running
  kernel, by checking only that `/dev/liveupdate` exists;
- `CanSurvive()` returns `1` when the state survives (unit restart, soft-reboot, kexec with LUO),
  `0` when it does not, or a negative errno. Treat an error as "does not survive".

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

`After=dbus.service` makes the service stop before dbus-daemon, so the query still works during
shutdown. `FileDescriptorStorePreserve=yes` keeps the fd store across `stop` + `start` and
`soft-reboot`.

The fd store survives kexec only through LUO, which requires systemd 262 or newer, both kernels
built with `CONFIG_KEXEC_HANDOVER`, `CONFIG_LIVEUPDATE` and `CONFIG_LIVEUPDATE_MEMFD` (disabled in
stock Debian kernels), `liveupdate=on` on the kernel command line, and the new kernel loaded with
`kexec -s` (`kexec_file_load`). `IsLiveUpdateAvailable()` sees only the running kernel.

`GetCollapseCause()` was verified on systemd 257 and 262 in arm64 and x86 virtual machines for
restart, stop, kill, soft-reboot, kexec, reboot, halt, poweroff and forced reboot, and for restart,
stop and kill on x86 bare metal; memfd survival across kexec was verified with a LUO kernel on
systemd 262. `IsLiveUpdateAvailable()` and `CanSurvive()` were added after these runs.

## Epoch

Blocks often keep `CLOCK_MONOTONIC` timestamps (last access, expiration). Within one boot they stay
valid across restarts and soft-reboot, since the kernel and its clock are the same. After kexec the
new kernel starts its own clock: whether it continues the old one depends on the platform and its
clock source, and is not guaranteed. A recovered timestamp can then lie far in the future (the
entry never expires) or in the past (everything expires at once).

`Tools/Epoch` keeps a small record in a memfd named `Epoch`, held by `Rescue`: the boot identifier
(`sd_id128_get_boot()`), `CLOCK_MONOTONIC` and `CLOCK_REALTIME` taken as a pair.

- At startup (a constructor, after `Rescue`) it reads the record of the previous instance, computes
  the correction and immediately stores its own record.
- At exit (a destructor) it stores the record again, which only narrows the error: any pair of the
  previous boot is enough.
- Within the same boot the correction is exactly zero. After a boot change it is
  `monotonic_now − (monotonic_saved + max(0, realtime_now − realtime_saved))`, so the downtime is
  measured by `CLOCK_REALTIME` and a clock step backwards counts as zero.
- `GetEpochState()` returns `EPOCH_SAME_BOOT`, `EPOCH_NEW_BOOT`, `EPOCH_UNKNOWN` (no previous
  record) or a negative errno (the correction stays zero then). `GetEpochCorrection(struct timespec*)`
  stores the correction itself into the given structure.
- `FixEpochTime(time_t)`, `FixEpochCertainTime(struct timeval*)` and
  `FixEpochPreciseTime(struct timespec*)` apply the correction to a stored value. Zero means "not
  set" and is kept as is; a result before the start of the current boot is clamped to the smallest
  non-zero value, so unsigned fields do not wrap around.

The correction has to be applied inside the recovery function, before a recovered timestamp is
indexed or compared:

```c
static int RecoverSession(struct ReliablePool* pool, struct ReliableBlock* block, void* closure)
{
  struct SessionData* data = (struct SessionData*)block->data;

  FixEpochPreciseTime(&data->time);
  data->expires = FixEpochTime(data->expires);

  /* take a reference, index the block */
  return RELIABLE_TYPE_RECOVERABLE;
}
```

The record of the new instance is stored before any pool is recovered, so a crash inside recovery
never applies the correction twice; the price is that blocks left unrecovered by such a crash keep
the old time base, and lifetime checks should bound them. The first start of a version that links
`Epoch` sees no record and applies no correction.

## Sharing a Pool With Another Process

A pool can be opened by several processes at once. The owner opens it with
`RELIABLE_FLAG_RESET`; another process receives the descriptor (for example over a Unix socket or
D-Bus as `UNIX_FD`) and opens the pool with flags 0 and no recovery function, which maps it as it
is. That process can attach to live blocks by number with `AttachReliableBlock()`, either with the
tag of a known generation or with `any`. Releasing such a reference only drops the count; when it
happens to be the last one, the type passed decides the fate of the block, as for the owner. Growth by one process is picked up by the others through
`UpdateReliablePool()`, which allocation and attachment call when needed; creation and growth are
serialized with OFD locks on the descriptor.

TetraPack uses this to give a Lua extension direct read access to the call state of its core.
