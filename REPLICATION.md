# InstantReplicator Replication

This document describes the replication concept implemented in `InstantReplicator` and how it
behaves under load. It explains the design choices behind the protocol and backs them with
measurements on a two-node InfiniBand testbed: replication throughput, delivery latency, the cost
of the replicator barrier, convergence of pool contents and behavior under peer failures.

Testing dates: 2026-10-08 to 2026-10-10.
Tested revisions: `d39b85d` (replication barrier fixes) for the load sweep, latency and failure
scenarios; `eacacff` (session recovery fixes) and `eacacff` with defect 8 fixed for the
[20 000 ops/s](#20-000-opss) results; the revision that adds the [Optimistic Mode](#optimistic-mode)
and the fixes 10–15 for the final series of 2026-10-09 in [Latency](#latency) and
[Failure Scenarios](#failure-scenarios); the same revision with fix 16 for the
[RoCE](#roce) runs of 2026-10-10.
The fixes are listed in [Defects Found and Fixed](#defects-found-and-fixed).

## Design Context

### Transparent Replication of Object State

`InstantReplicator` propagates the state of objects stored in a `ReliablePool`. Application code
changes their memory through ordinary writes; `ReliableTracker` discovers the changes and the
replication stack delivers versions to other nodes. The application does not have to construct an
update message or explicitly commit each change to a replica. It still manages object lifetimes
through the pool and integrates the tracker and replicator with its execution loop: transparency
of change detection depends on honoring the flush and barrier contract.

The unit of replication is a block identified within a named pool. Repeated writes to an object
can be combined before delivery, so replication carries versions of state rather than a log of
every application operation. Several blocks delivered in a batch do not form an atomic
transaction or a consistent snapshot of the whole application.

One possible use is connection context replication. While a client is served by node A, A is the
main writer of that connection object and B holds a copy. If A disappears and the client moves to
B, B can continue from the context it received and become the main writer of that object. Other
connections can have the opposite arrangement at the same time. These are roles of individual
objects in the application, not fixed primary and standby roles of entire nodes. The protocol
does not elect or enforce an exclusive owner; client routing and the application determine which
node actually modifies the object.

### Freshness and Integrity

The aim is to bring the latest version to all peers on a best-effort basis while protecting the
integrity of a version accepted at the receiver. Freshness and integrity are separate concerns:
an intact but older copy is possible, whereas a mixture of partially transferred versions must
not be reported as a successful arrival. Validation failures and the implementation defects
found during testing are discussed below; this is the intended contract, not a claim that every
failure path has already been verified.

Replication is asynchronous from the application's point of view: a local write is not an
acknowledgment that any peer has received it. There is no consensus, quorum commit or globally
agreed order of application writes. Per-block version metadata selects newer offers, with clock
normalization between peers; it provides best-effort preference for newer state, not a guarantee
that the last physical writer wins under arbitrary concurrent writes or clock skew.

Peers can lag, intermediate versions can be skipped, and recent changes can be lost when their
source fails. Reconnection provides an opportunity to synchronize live state, but does not imply
unconditional eventual delivery: the current selector can suppress a repeated offer after an
abandoned transfer. Removals also have no persistent tombstones. The precise convergence, clock
and trust boundaries are documented in [Replication Model Boundaries](README.md#replication-model-boundaries).

### Synchronous Transfer Within Asynchronous Replication

A block is transferred by the sender: after the receiver asks for it, the sender locks the block of
the receiver by compare-and-swap on `mark` and writes the data with RDMA, while the application of
the sender is parked in the LOCK/READY barrier. The source cannot change during the transfer, and
the receiver accepts a version only when it passes validation; data that fails validation can still
land in the memory of the receiver. An asynchronous mode in which the receiver reads the sender with
RDMA READ was the original plan (`INSTANT_REPLICATOR_OPTION_OPTIMISTIC_MODE` is its remnant); it was
dropped because a read races with the application of the sender, does not guarantee the integrity of
the received data and produces extra retries and errors. It returns as an opt-in first attempt in
front of this transfer, see [Optimistic Mode](#optimistic-mode). The price of the synchronous
transfer is that the receiver keeps its own barrier raised while it waits for the sender, so the
parked time of one node includes the time the other node needs to reach its barrier.

The transfer follows this sequence:

1. A tracker flush identifies a changed block, updates its version and checksum metadata, and
   the sender advertises it in a NOTIFY batch.
2. The receiver selects an offered version and prepares its destination under its local barrier,
   then requests the data with RETRIEVE.
3. Once the sender's application reaches its own barrier, the sender checks the source against
   its checksum, attempts the remote CAS, and writes the data for a successful exchange. A source
   changed since the last tracker flush is skipped until the tracker publishes its updated state.
4. The receiver checks the resulting mark, length and checksum before reporting an ARRIVAL.
   Retryable validation failures are retried up to the configured limit and then reported as
   DAMAGE. This does not make every interrupted transfer a retried or acknowledged operation.

The barrier coordinates access to live pool memory with the application and tracker. It does not
commit a version across nodes. A synchronous transfer here means participation and coordination
of both endpoints during that exchange; the overall replication service remains asynchronous.
The application must arrange that its writers respect these safe points. Parking the event-loop
thread alone cannot protect against unrelated threads that continue modifying the same objects.

### Reactive Batching and Its Cost

Flushes are driven by the load of the main thread. A busy thread
flushes less often, so each flush and each barrier carries a larger batch; an idle thread flushes
often with small batches, which costs resources but keeps the latency low while the thread has
capacity to spare. The batch size and the barrier rate therefore balance themselves against the
application load without tuning.

This cadence amortizes coordination over more work under load without adding a fixed collection
delay when the application has spare capacity. It is not a bound on latency: a long application
callback can delay a flush or the response to a barrier request. Because the receiver waits for
the sender, application load on one node can extend the parking of another. Batching is regulated
locally while transfer waiting couples the peers.

The useful measurements are therefore the rate of delivered versions, their age on arrival,
the cost of tracking changes, and the time taken away from the application's execution by the
barrier. Fewer arrivals than writes can reflect useful coalescing. Final-state comparison checks
whether the live versions reached the replicas in a particular run; throughput and arrival
latencies alone cannot establish that. The measurements below separate these effects where the
instrumentation allows it.

### Timeouts

The replicator bounds the wait for a failed peer by one failure budget per instance, the
`timeout` of `CreateInstantReplicator()` in milliseconds (0 selects the default of 1 000 ms). It
closes the connection of a peer when either of two waits shows no progress for that long:

- a reading or writing task waits for data, a buffer or a completion of the peer and does not
  advance;
- the credit window of the peer is used up, a zero window included, and the peer sends nothing.

Both checks run in ticks of the replicator thread (`GENERIC_POLL_TIMEOUT`, 200 ms): the timeout is
rounded up to whole ticks and the actual bound is up to one tick longer, 1.0–1.2 s by default; on
the testbed the connection was closed 1.0–1.3 s after the peer stopped. The closed peer is caught
up by the initial syncing after it connects again. Everything that waits for a peer is released by
its disconnect: a barrier held by a transfer, the queued messages and a
`TransmitInstantReplicatorUserMessage()` waiting with `wait`.

There is one parameter and not one per mechanism, since to the application they mean the same: the
peer does not advance. It sets how long a node tolerates a peer without any progress before it
declares the peer broken, not the latency of individual operations. A live peer answers a transfer
in milliseconds, announces its window right after the connect and reports its credit at the latest
when half of the window is used, so an idle peer never reaches the timeout and a slow one reaches
it only when it stalls for longer than the budget; then its connection is closed as for a stopped
peer, and the budget has to be raised if such stalls are expected.
The default suits ordinary deployments. A node with a dedicated replication thread or a near
real-time event loop can lower it to 400–600 ms for a faster failover; a virtual machine, a lossy
RoCE fabric or an overloaded host can raise it to 2–3 s. Values below a few ticks turn scheduling
hiccups and load peaks into disconnects, resyncs and zombies; tens of seconds bring back the
stalls of the barrier and the application thread that the timeouts removed (defect 15).

An application that cannot afford its thread waiting for the timeout on the failure path does not
fit the execution model of `InstantReplicator`, in which the application thread takes part in the
barrier, and lowering the timeout does not change that. Such an application should isolate access
to the replicated memory instead: move the writers and the replication safe point to a thread or
event domain of their own, synchronize the objects locally by other means, or transfer the state
in a way that keeps the application path out of the barrier.

### Why RDMA

The protocol is built on one-sided operations, so it cannot be moved to a socket transport by
replacing the transport; an IP version would be a different protocol:

- **No copies.** Block data goes by RDMA READ and WRITE from one pool directly into the other.
  Through sockets a transfer is usually copied twice, from the pool into the kernel and from the
  kernel into the pool, with system calls for every message or batch; `MSG_ZEROCOPY` removes the
  copy on the sending side for large buffers, but not the system calls or the work of the remote
  CPU below.
- **No remote CPU on the data path.** The HCA executes READ and CAS without the sender's
  processor. Through sockets a thread of the remote node has to receive each request, find and
  check the block and answer, so the latency follows the load and the scheduling of that node.
- **Locking in memory.** The fencing by CAS on `mark` with a token would become an exchange of
  messages with its own locking on the remote side and an extra round trip.
- **Optimistic mode** reads a block without the source taking part and validates it afterwards;
  this exists only with one-sided reads.

Most of the median latency measured below is spent outside the network, in change detection, the
queues and the barrier of the receiver. A socket version would pay for the same stages plus its
copies and system calls, so RDMA is expected to win, most of all in the tail and under load. No IP
variant was built or measured on the testbed, and this document reports only the absolute values.

## Optimistic Mode

Status: implemented; load runs on the testbed are in [Measurements](#measurements), failure
scenarios in [Failure Scenarios](#failure-scenarios). The other measurements in this document were
taken without this mode and before the token scheme.

With `INSTANT_REPLICATOR_OPTION_OPTIMISTIC_MODE` the receiver makes one attempt per offered block
to read the source by RDMA READ under its own barrier, without involving the barrier of the sender.
Blocks that fail the attempt continue through the synchronous transfer described above. The aim is
to remove the coupling of the parked time of the receiver to the barrier of the sender: in the
[barrier breakdown](#barrier-breakdown) most of the held time of the receiver is spent waiting
for the peer.

### Contract

The source is not locked during an optimistic read, so the application of the sender can change
the block and flush it while the HCA reads it.

- An optimistic read may accept an intact state of the source that is newer than the offered
  version. In this case the receiver installs the label of the offer.
- Accepting a state older than the label is not allowed.

A label that lags behind the data is therefore a permitted outcome. A newer offer allows the label
to be realigned; the [waiting rule](#ownership-and-conflicts) ensures that such an offer is not
dropped while the block is busy.

### Roles of `hint` and `mark`

The two fields have separate roles:

- **`hint`** is the version of the state: the offer selected for a block, or the version installed
  in it. Versions come from `MakeEpoch()` of the author and are normalized to the local clock.
  A version may legitimately be installed again.
- **`mark`** is the fencing token of the content of this local block, and its lock. It never
  carries the version or the mark of another node.

| Field | Value | Meaning |
|---|---|---|
| `hint` | even | Label of the installed version |
| `hint` | odd | Pending: an offer is selected but not installed, or the copy is damaged. Not publishable |
| `mark` | `& 3 == 0`, non-zero | Epoch of a local tracker flush |
| `mark` | `& 3 == 2` | Token T of an install by the replicator |
| `mark` | odd | Locked by a transfer owned by a local task: `T \| 1` |
| `mark` | 0 | No published content: a fresh or freed block, or a copy that may be damaged |

Invariants:

1. **I1.** The content of a block is never older than its `hint` when the `hint` is even.
2. **I2.** A token (`& 3 == 2`) appears in a block at most once: the replicator leaves a value
   only for a fresh token or for 0. An epoch can appear again, but only written by a tracker
   flush, which stores the epoch together with the current content of the application (or, with
   `RELIABLE_TRACKER_FLAG_FORCE_MARK`, over unchanged content). Such content is never older than an
   earlier offer of the block. The tracker runs on the application thread, which is parked while a
   task owns a block, and it skips damaged blocks (I4), so it never writes `mark` of an owned or
   damaged block.
3. **I3.** Every odd `mark` has a local owner task. A pool opened after a crash converts leftover
   odd marks into the damaged state (`mark` 0, pending `hint`).
4. **I4.** Nothing publishes a block with an odd `hint`, an odd `mark`, `mark` 0 or a local owner:
   neither the syncing task, nor the source of a WRITE, nor the tracker. Ownership is checked in
   addition to the bit because a transfer in progress overwrites `hint`.

### Tokens

The replicator owns the generator of T; it does not depend on an instance of `ReliableTracker`.
The arithmetic shared with `MakeEpoch()` can move to a common helper.

1. **Classes.** The generator only issues values with `T & 3 == 2`. `MakeEpoch()` steps its counter
   by 4, so new tracker epochs have `epoch & 3 == 0` and never equal a token.
2. **Persistent floor.** Each pool stores a floor of tokens in its header
   (`ReliableMemory::floor`). The generator keeps its next value in memory and reserves ranges of
   2²⁰ tokens: when the range is used up, it raises the floor by the range first and only then
   issues values from it. After a restart it starts at the floor, so the unused rest of the last
   range is skipped; the space of 2⁶² tokens leaves room for 2⁴² restarts. The header is written
   once per range, not once per install.
3. **Durability of the floor** matches that of the blocks. A pool backed by a file can be
   recovered after a machine failure, and the page cache writes pages back in any order, so a block
   carrying a token could reach the disk while the header with the raised floor does not.
   The generator therefore calls `msync(MS_SYNC)` on the header page after raising the floor and
   before the first value of the range is used. The call is unconditional: on a `memfd` or another
   tmpfs-backed pool it does nothing, and the backend is not detected. On a file the write runs on
   the replicator thread under the barrier once per range, which is about once a minute at 20 000
   installs per second.
4. **Initial floor without clocks.** A pool of the old format is recreated (see
   [Compatibility](#compatibility)), so a new pool starts with zero marks and any initial floor
   of the class is above them. Old epochs with bit 1 set therefore never meet tokens. If a
   migration in place is ever added, its initial floor must be strictly above every existing `mark`
   of the pool, lock bit included.
5. **Overflow** stops issuing tokens: optimistic reads and synchronous transfers into the
   pool fail, and the counter never wraps.

### Receiver Procedure

1. **Collect**, under READY. The task acquires all its blocks in one pass or none of them. A block
   owned by another task makes the whole task wait in `WAIT_LOCK`; busyness never drops an entry.
   An entry is dropped only when the local version is not older than the offer. A damaged block
   (`mark` 0, pending `hint`, no owner) accepts any offer. For an acquired block the task saves the
   current `hint` and writes `hint = offer | 1`; `mark` does not change.
2. **Clean exit.** An entry that leaves the task while its content is untouched (`mark` is still the
   value C seen in Collect, for example because the sender skipped the block) gets the saved `hint`
   back. For a block that was damaged before Collect the saved value is the pending `hint`, so the
   block stays damaged.
3. **Fetch**, once per task. For every entry the task takes a fresh T and writes `mark = T | 1`,
   then posts an RDMA READ of the replicable part from `hint` to the end of the data straight into
   the local block. The local `mark` is not overwritten. After all data reads it posts one RDMA
   READ of the source `mark` per entry into the registered task buffer; the first of them carries
   `IBV_SEND_FENCE`, so every mark is read after every data read has completed. Entries whose length
   does not fit go straight to the synchronous transfer.
4. **Acceptance**, per entry, all of:
   - the completion status is success;
   - the source `mark` read after the data equals the offered `mark`, which is even and non-zero;
   - the identifier equals the offered one;
   - the length fits the read range, checked before the checksum;
   - CRC32C of the data equals the control.

   On acceptance the receiver writes `hint` = the offered label, then `mark = T` last (release),
   touches the pages and reports ARRIVAL.
5. **Rejection.** The data was overwritten, so the entry is flagged as damaged-by-read. The read
   also overwrote `hint` and the identifier, so the receiver restores the identifier, writes the
   pending `hint = offer | 1` explicitly, then `mark` 0, and keeps the ownership. From the start of
   the read until this point the pending bit is gone and only the ownership keeps the block from
   being published. The entry continues with the synchronous transfer. An ARRIVAL clears the flag;
   any other exit of a flagged entry reports DAMAGE regardless of the checksum and leaves the block
   damaged.

### Synchronous Transfer With Tokens

Attempt k of an entry takes a fresh token Tk. RETRIEVE carries for every entry, besides the
destination, `compare` = the current `mark` C of the destination and Tk in `hint`, which the sender
does not need otherwise.

| Step | Destination `mark` | Written by |
|---|---|---|
| RETRIEVE sent | C (unchanged) | — |
| CAS of the sender | `C → Tk \| 1` | HCA of the sender |
| WRITE of the data | `Tk \| 1` | HCA of the sender |
| Final WRITE | `Tk`, taken from `hint` of RETRIEVE, never the `mark` of the source | HCA of the sender |

The sender copies the token into `buffer->values[index]`, where the result of its CAS was read, and
writes the final mark from there. The value and its registered buffer stay unchanged until the WR
completes or the QP is confirmed to be stopped.

Validation of the receiver per entry:

| Destination after the transfer | Meaning | Action |
|---|---|---|
| `mark == Tk`, length in range, checksum holds, `hint` even | Complete | ARRIVAL |
| `mark == Tk`, but length out of range, checksum fails or `hint` pending | Written, not valid | Touched |
| `mark == Tk \| 1` | Locked, the data may be written in part | Touched |
| `mark == C` | The CAS did not succeed | Untouched: clean exit |
| Anything else | Unexpected | Touched |

Touched means: `mark` 0, the entry is flagged and retried with a fresh T until the attempt limit,
then DAMAGE.

Further requirements of the synchronous transfer:

- The sender never waits for a block: a block with a pending `hint` or owned by a local task is
  skipped like an unflushed one.
- A WRITE that lands a pending `hint` is a validation failure.
- On a disconnect, the entries of the task are abandoned after the QP is destroyed: an entry
  locked by `Tk | 1` is touched, `mark` 0, DAMAGE.

### Why the Accepted State Is Not Older Than the Label

The offered state of the source B carries the version V1 in `hint` and the content token M1 in
`mark`. B publishes only blocks without an owner, with an even `hint` and a non-zero even `mark`
(I4), and its content is not older than V1 (I1). The receiver accepts only when B still shows M1
after the data has been read.

By I2, `mark` of B equal to M1 after the read means that no install, lock or invalidation happened
to that block between the offer and the check: each of them leaves M1 for good. The changes that
keep M1 are only those of the application of B before its next flush:

| Change of the source during the read | What the receiver sees | Outcome |
|---|---|---|
| Write and flush of V2 | The flush writes `control`, then a new `mark`, then `hint`. With M1 still visible after the data, the data is V1 or V2 | V1 or label lag |
| Write without a flush | Newer data with the old control | Rejected by the checksum (probabilistic, 2⁻³²) |
| Mark refresh with an unchanged checksum (`RELIABLE_TRACKER_FLAG_FORCE_MARK`) | A new epoch | Rejected, conservative |
| Install of any version, by WRITE or by READ | `T \| 1`, then a fresh T | Rejected |
| Interrupted transfer | `T \| 1`, then 0 | Rejected |
| Release or reuse of the block | 0, then a new epoch or token; the identifier is checked | Rejected |

The data accepted is therefore V1 or a later state written by the application of B, never an
earlier one. If M1 is an epoch, a tracker restart can write M1 again (I2), but only with a flush of
content written by the application after the offer: the same outcome as the first row, label lag.
A token never comes back, because the persistent floor guards it.

### Damaged Blocks

A block is damaged when a transfer ended after its content may have been overwritten: `mark` 0 and
a pending `hint`, without an owner. Such a block is reported by DAMAGE and is not published (I4),
so content damaged by DMA never leaves the node as a new version. The tracker cannot tell a repair
by the application from damage: DMA does not mark the page table entries dirty, but a write to a
neighboring block of the same page does, so a dirty page says nothing about this block.

A damaged block leaves this state in one of two ways:

- **An install** by a later transfer from a peer, which repairs the copy (see the conflict table).
- **`RepairReliableBlock(pool, block)`**, a function of `ReliablePool`. The application calls it
  after it has rewritten the content, on the application thread outside the barrier, where no task
  owns the block. It writes `hint` 0, an even label older than any version, and inverts `control`.
  Content restored to the bytes that matched the old `control` now differs from it for certain;
  other content matches the inverted value only by chance (2⁻³²), which is accepted as residual
  risk: computing the checksum in `ReliablePool` would pull CRC32C into the pool. The store also
  marks the page dirty, so the next tracker flush checks the block even when the application did
  not rewrite it, sees a changed checksum, writes a new epoch to `mark` and `hint` and publishes
  the block. Whether and how to repair stays the decision of the application; the alternative of
  releasing the block and reserving a new one needs no call.

### Ownership and Conflicts

Rules:

1. **R1.** Acquisition is all or nothing, so a waiting task holds no block and tasks of one node
   cannot form a cycle.
2. **R2.** Only a reading task waits, only in Collect and only for a local owner.
3. **R3.** A writing task never waits: it skips a busy block, and sends COMPLETE when it skipped
   everything.
4. **R4.** A block that a writing task holds is busy for Collect, so a pending `hint` cannot be
   raised between the source check of the sender and its WRITE.

| Holder of X on the node | New reading task for X | New writing task for X (incoming RETRIEVE) |
|---|---|---|
| None | Acquires | Serves |
| Reading task | Waits in `WAIT_LOCK` | Skips |
| Writing task | Waits | Skips |
| Nobody, damaged | Acquires, the install repairs the copy | Skips |

When nodes A and B both hold X for reading and request it from each other, the writing task on
each side skips the block. Without a preceding optimistic read both reading tasks end with
untouched content: concurrent writers of one block, resolved by the next change, as with a failed
CAS before. If both had already overwritten their copies by a rejected optimistic read, both
report DAMAGE and the previous copies are not restored. This is permitted by the contract and is
the cost of reading into the live block.

Local acquisition cannot form a cycle: a reading task waits for a local owner, which waits either
for its own DMA or for a remote writing task, which waits for nothing. The wait for a network
response is bounded by the timeouts of a stopped peer (defect 15): a transfer that does not advance
for about 1 s, or a credit window that a peer leaves exhausted for about 1 s, closes the
connection.

### Connection Teardown

All reads and writes of a task must have stopped before its blocks are restored, unlocked,
reported or its buffers are released. On a disconnect:

1. The peer stops accepting new work (`INSTANT_PEER_STATE_DISCONNECTED`).
2. `ibv_destroy_qp()` destroys the QP and its result is checked. `rdma_destroy_qp()` cannot be
   used because it does not report errors.
3. Only after a successful destruction are the pending requests and the tasks of the peer cleared
   by the rules above and their memory released.
4. If the destruction fails, DMA into pool memory may still be in progress. Nothing of the peer is
   cleared or released, `DestroyDescriptor()` is not called, the peer is not reconnected, the
   memory regions stay registered and the replicator enters `INSTANT_REPLICATOR_STATE_FAILURE`.
   The barrier is never released in this state: LOCK and READY stay raised, including when the
   replicator thread exits, which today clears them. `FlushInstantReplicator()` returns `-EFAULT`
   without releasing anything. The application must treat it as fatal and stop without touching
   pool memory; the pool is recovered by the next process.

`ReleaseInstantReplicator()` follows the same order. The replicator thread, before it leaves,
passes every connection that has not failed through the disconnect path: the QP is destroyed, the
unfinished transfers are abandoned and reported, and only then the barrier is released. After a
failure `ReleaseInstantReplicator()` releases nothing, since DMA may still reach the buffers and
the pools; the resources are kept until the process exits.

A peer that stops answering without breaking the connection, for example a stopped process whose
HCA still acknowledges the transport, is closed by the replicator itself (defect 15). Such a peer
does not answer DREQ either, so `rdma_disconnect()` would leave the connection open until the peer
resumes; instead the replicator passes it through the same disconnect path as
`ReleaseInstantReplicator()` and destroys the CM identifier. The peer finds the connection broken
when it resumes and connects again.

This relies on the provider removing the completions of a destroyed QP from the shared completion
queue, as mlx5 does. The rule is not claimed for other providers.

### Compatibility

- **Network protocol.** RETRIEVE carries the token in `hint` and the lock value changes to `Tk | 1`;
  the size of `InstantBlockData` does not change.
  `INSTANT_MAGIC` changes, so nodes of different versions refuse each other in the handshake.
- **Pool format.** The floor is `ReliableMemory::floor`, a 64-bit field of the pool header, which
  makes `RELIABLE_MEMORY_MAGIC` 7. The header grows to 64 bytes with `flags` and room for future
  fields, and `data` starts on a cache line boundary.
  Memory of the old layout is never interpreted with the new one: on a magic mismatch
  `CreateReliablePool()` recreates the pool, as for any format change, and its content returns
  from the peers by synchronization. A recreated pool has only zero marks, so the
  initial floor needs no scan; a migration of old pools in place is not planned.
- **Tracker epochs.** The counter step of `MakeEpoch()` changes from 2 to 4.

### Assumptions

- `node & UINT16_MAX` differs between nodes. Otherwise two authors in one epoch produce equal
  versions for different content.
- After a tracker restart, `CLOCK_REALTIME` does not step back by more than one epoch slot. This
  concerns how version labels are ordered. Correctness of `mark` does not depend on clocks:
  tokens rely on the floor, and a repeated epoch is covered by I2.
- Stores of the tracker become visible to the HCA in program order (x86 TSO with coherent DMA).
- A CRC32C collision (2⁻³²) is accepted as residual risk, as in the synchronous transfer.

### Measurements

Two nodes as in [Testbed](#testbed), 20 s of writes with 30 % frees and payloads up to 256 B, then
15 s of quiescence, one run per row, the same token revision in both modes, two RDMA READ and
atomic operations in flight per connection (see [Read Depth and Peak](#read-depth-and-peak) for
16). A debug build (not part of the tree) counted the barriers, the time the main thread stayed in
`FlushInstantReplicator()` and the outcome of every optimistic read. Both modes converged fully in
every run, with no corrupt or stale arrivals and no damaged block left at the end.

| Requested | Mode | Barriers per node | Parked, ms (scull / sandbox) | Reads accepted | DAMAGE (scull / sandbox) |
|---|---|---|---|---|---|
| 5 000 ops/s | synchronous | ~35 200 | 4 763 / 4 918 | — | 0 / 0 |
| 5 000 ops/s | optimistic | ~19 100 | 2 617 / 3 002 | 99.99 % | 4 / 1 |
| 20 000 ops/s | synchronous | ~15 850 | 8 933 / 8 367 | — | 0 / 0 |
| 20 000 ops/s | optimistic | ~16 850 | 5 695 / 6 409 | 99.7 % | 346 / 267 |

The optimistic mode takes 21–45 % of the parking away from the main thread; at 5 000 ops/s it also
halves the number of barriers. Most rejected reads see a changed source `mark`, a few a length or
checksum torn by a concurrent write of the source.

DAMAGE comes from rejected reads whose synchronous fallback found nothing to transfer. Two further
optimistic runs at 20 000 ops/s counted the cause at the moment it happened: every DAMAGE on one
node matched a block that the sender on the other node skipped when it served RETRIEVE.

| Run | DAMAGE (scull / sandbox) | Skipped as missing | Skipped as not flushed | Repaired by a later install | Released by a removal |
|---|---|---|---|---|---|
| 1 | 323 / 289 | 306 / 282 | 17 / 7 | 17 / 7 | 302 / 275 |
| 2 | 320 / 225 | 289 / 216 | 31 / 9 | 31 / 8 | 284 / 214 |

Skipped counts are listed under the receiver that reported the DAMAGE. In 90–98 % of the cases the
source had released the block by the time RETRIEVE was served: `FreeReliableBlock()` had cleared
it, often after the read, which then saw a changed `mark` rather than 0 (the read saw 0 in 21–40 %
of the DAMAGE). In the remaining 2–10 % the source block was live but changed again without a flush,
4–7 such cases per 100 000 installs. Copies of released objects stayed damaged until the delayed
removal released them, which is why the count of damaged blocks grows during the writes, up to
~190 per node, and returns to zero after the removals. The repaired copies match the live cases:
63 of the 64 were repaired by a later install, and one was released by a removal. The synchronous
mode skips the same blocks with the content untouched (466 and 368 skips in a run at
20 000 ops/s), so its old copies stay intact until the removal.

### Read Depth and Peak

`initiator_depth` and `responder_resources` limit the RDMA READ and atomic operations in flight per
connection. They were 2, which serializes the reads of a batch pairwise. Both ConnectX-4 functions
of the testbed allow 16, which is now `INSTANT_ATOMIC_COUNT`: the replicator lowers it to the
weakest local card, and the accepting side does not exceed the depths requested by the initiator.

Parking at 20 000 ops/s, ms (scull / sandbox), one run per cell unless a range is given:

| Mode | Depth 2 | Depth 16 |
|---|---|---|
| synchronous | 8 933–9 118 / 8 227–8 560 | 9 098 / 8 311 |
| optimistic | 5 652–5 924 / 6 357–6 827 | 4 718–4 803 / 4 540–4 584 |

Delivered versions per second in both directions together, 20 s of writes, one run per cell. Above
the capacity of a node its main thread saturates and writes less, so one direction takes most of
the transfer; the sum is the comparable figure:

| Requested | Synchronous, depth 2 | Synchronous, depth 16 | Optimistic, depth 2 | Optimistic, depth 16 |
|---|---|---|---|---|
| 40 000 ops/s | ~34 000 | ~36 000 | ~41 000 | ~45 000 |
| 60 000 ops/s | ~35 000 | — | ~42 000 | ~44 000 |

All runs converged for the live objects, with no corrupt or stale arrivals and no damaged block
left. The optimistic mode raises the ceiling by about 20 %. Depth 16 takes about another fifth of
the parking away from the optimistic mode and adds 5–10 % at the peak; the synchronous mode, whose
transfer needs one atomic operation per block, stays within the spread of the runs.

### When to Enable It

The synchronous transfer stays the default; the optimistic mode is an option:

- It trades a part of the parking of the main thread for DAMAGE reported on copies the receiver
  overwrote by a rejected read. In the runs above 90–98 % of them belonged to objects the source had
  released by the time it served RETRIEVE; of the live ones, 63 of 64 were repaired by a later
  install and one was released, all before the end of the run. An application that wants DAMAGE
  to mean a real transfer failure keeps the synchronous mode.
- The correctness argument above does not require a single writer per block. A damaged replica is
  not published (I4), and a reader holding an older offer of it sees a lock, 0 or a fresh token
  after its read, never the offered `mark` again (I2). A single writer is still advisable: blocks
  written on two nodes at once can end in DAMAGE on both sides (see
  [Ownership and Conflicts](#ownership-and-conflicts)).

### Open Questions

- **Released source.** A read rejected because the source released the block could be handled as
  the removal of the object instead of DAMAGE. It needs a criterion that proves the release of the
  offered object from a rejected read, including the reuse of the block; none is established yet.

## Testbed

| | Node A | Node B |
|---|---|---|
| Platform | Intel NUC (Hades Canyon), bare metal | KVM guest |
| CPU | Intel Core i7-8705G, 4 cores / 8 threads | Intel Core i5-1235U host, 4 vCPU in the guest |
| Memory | 16 GB | 2 GB (guest) |
| OS | Ubuntu 26.04 LTS, kernel 7.0.0 | Debian 13, kernel 6.12 |
| HCA | Mellanox ConnectX-4 (MT27700), firmware 12.23.1020 | ConnectX-4 virtual function (SR-IOV), passed through to the guest |
| HCA attachment | Thunderbolt 3 enclosure (OWC Mercury Helios 3S), PCIe 3.0 x4 | Thunderbolt 4 on the host |

Fabric:

- InfiniBand 4X FDR (56 Gb/s), active MTU 4096, subnet manager on the fabric.
- IPoIB is used only for RDMA CM addressing; peers are registered explicitly by address.
- Remote atomics: `ATOMIC_HCA` on both the physical function and the virtual function,
  `max_qp_rd_atom` / `max_qp_init_rd_atom` = 16.
- The Thunderbolt 3 link limits the PCIe bandwidth of node A to PCIe 3.0 x4. The tested payload
  volumes stay far below that limit.

Clocks: both nodes run `systemd-timesyncd`, there is no PTP and no `ptp_kvm` in the guest.
The measured offset between the nodes was 1–4 ms during the runs reported below
(it was up to 19 ms right after the testbed was brought up). One-way latencies below therefore
include the clock offset; see [Latency](#latency) for how it is compensated.

## Test Tool

`Tests/Replication` is a load generator and verifier built on the same stack as `Examples/RDMA`:
`ReliableTracker` → `ReliableIndexer` → `InstantReplicator`, driven by a `FastRing` loop with
`ReliableWaiter` and `InstantWaiter`.

Each node:

- owns a set of block slots (`-k`, default 4096) and performs `-r` operations per second;
  an operation either frees an occupied slot (probability `-f` percent) or allocates/rewrites it;
- writes a self-verifying payload: magic, length, seed, author-wide monotonic sequence number,
  `CLOCK_REALTIME` timestamp, author identifier, a pseudo-random fill derived from the seed and
  CRC32C over the whole payload; the payload length is random up to `-s` bytes;
- verifies every `RELIABLE_MONITOR_BLOCK_ARRIVAL`: payload CRC and fill (`corrupts`), sequence
  regression against the last version seen for the same block (`stales`), equal sequence
  (`repeats`); counts `RELIABLE_MONITOR_BLOCK_DAMAGE` and `RELIABLE_MONITOR_BLOCK_REMOVAL`;
  prints the failing check (magic, length, control, fill) of the first corrupt arrivals;
- measures one-way delivery latency as arrival time minus the payload timestamp;
- starts writing only after the first peer connects, so writes made before the connection
  (delivered later by the initial syncing) do not distort the latency;
- generates writes from a 1 ms timer with a budget of 0.5 ms per tick and drops the backlog when
  it cannot keep up, so the main loop stays responsive for the tracker flushes and the replicator
  barrier; above the capacity of the node the actual rate is therefore lower than `-r`;
- after `-t` seconds stops writing, waits `-q` seconds (removals are applied 10 s after they are
  received, so the quiescence must be longer), prints the totals and an order-independent digest
  of the pool, and dumps every surviving block with a flag for a damaged copy, followed by the
  identifiers of the own blocks it released (`-o`);
- keeps its own blocks allocated at exit, because releasing a block of any type sends
  `INSTANT_TYPE_REMOVE` to the peers that may still be collecting their dumps;
- with `-u` sends that many user messages per second from the application thread with `wait`,
  each carrying a random incarnation of the process and a sequence number; the receiver counts
  messages out of order (`disorders`), missing before the first connection to their author or
  across its disconnect (`skipped`) and missing within a connection (`lost`), and the sender the
  longest wait in
  `TransmitInstantReplicatorUserMessage()` (`wait_max_ms`); the dump lists the count of sent
  messages and the last message received from every author, so `Compare.py` detects a lost tail,
  which leaves no gap to count.

Node identifiers are derived from the node name (`uuid_generate_sha1()` in the OID namespace),
so a restarted process keeps its identity.

Exit status: `0` passed, `1` setup or runtime failure, `2` verification failure
(corrupt or stale arrivals, DAMAGE without `-O` in a run without a disconnect, user messages out
of order or lost within a connection, or no peer connected). With `-O` a rejected optimistic read
overwrites the copy, and in either mode a disconnect leaves a block locked by the sender written
in part, so DAMAGE is expected there; the blocks left damaged are judged by `Compare.py`, which
knows whether the object is still alive at its author.

Example (node A, node B is symmetric):

```bash
~/rp-run ./replication -n nodeA -p nodeB@192.168.129.2 -r 5000 -s 256 -f 30 -t 30 -q 15 -o nodeA.dump
```

`rp-run` is a small launcher that grants `CAP_SYS_PTRACE` (required by `userfaultfd` when
`vm.unprivileged_userfaultfd = 0`) and `CAP_IPC_LOCK` (memory registration above `RLIMIT_MEMLOCK`)
as ambient capabilities, so the test binary can be rebuilt without re-applying file capabilities.

### Convergence Criterion

The dumps of both nodes are compared per author:

- every block that the author still holds must be present on the peer with the same identifier,
  length, CRC32C and sequence — otherwise it is reported as `missing` or `mismatch` (a defect);
- blocks of an author that exist on the peer but not on the author are `zombies` — removals that
  were lost while the nodes were disconnected. Without tombstones this is expected by design;
- own blocks are the ones the author allocated itself; a block that carries the author's payload
  but is held as a received copy on the author is `resurrected` — the author freed it and then
  received it back from a peer (see [Known Issues](#known-issues)).

Zombies and resurrected blocks are reported but do not fail the comparison.

A damaged block is judged by its identifier, since its payload cannot be trusted:

- a damaged copy of an object that its author still holds fails the comparison;
- a damaged copy of an object that its author listed as released is a damaged zombie, reported
  only: a lost removal left it, as for an intact zombie;
- otherwise the fate of the object is unknown, for example after a restart of the author, which
  loses its list of released objects; the result is unconfirmed.

When both nodes stop writing at the same time and nothing was lost, the pool digests of the two
nodes are identical.

With user messages (`-u`), every peer must have received the last message of the incarnation
of an author that wrote the dump; gaps inside the stream are judged by the test itself, which
allows them only before the first connection to the author and across its disconnect.

`Tests/Replication/Compare.py` performs this comparison. It takes pairs of node name and dump
file and exits with `0` when the nodes converged, `1` when blocks are missing or mismatched, a
live object is damaged or the tail of user messages was lost, `2` on a usage error and `3` when
damaged blocks of unknown objects are left:

```bash
Tests/Replication/Compare.py nodeA nodeA.dump nodeB nodeB.dump
```

## Results

### Load Sweep

Both nodes write simultaneously for 30 s with 30 % frees, then quiesce for 15 s.
"Operations" counts writes and frees together; every run sustained the requested rate.

| Rate per node | Payload | Writes per node | One-way p50 | One-way p99 | Max (raw, A→B / B→A) | Convergence |
|---|---|---|---|---|---|---|
| 1 000 ops/s | ≤ 256 B | ~23 800 | ~0.2 ms | ~1.2 ms | 14 / 12 ms | identical digests |
| 5 000 ops/s | ≤ 256 B | ~116 000 | ~0.3 ms | ~3.2 ms | 13 / 12 ms | identical digests |
| 10 000 ops/s | ≤ 256 B | ~231 000 | ~0.5 ms | ~24 ms | 79 / 393 ms | live blocks identical, 1 resurrected block |
| 20 000 ops/s | ≤ 256 B | — | — | — | — | hangs on `d39b85d`, see [20 000 ops/s](#20-000-opss) |
| 2 000 ops/s | ≤ 4000 B | ~46 900 | ~0.4 ms | ~44 ms | 101 / 89 ms | identical digests |

Additional 60 s runs on the same revision:

| Rate per node | Payload | Frees | One-way p50 | One-way p99 | Max (raw) | Convergence |
|---|---|---|---|---|---|---|
| 100 ops/s | ≤ 256 B | 30 % | ~1.5 ms | ~3.3 ms | 9 ms | identical digests |
| 2 000 ops/s | ≤ 256 B | 30 % | ~0.25 ms | ~2.9 ms | 27 ms | identical digests |
| 2 000 ops/s | ≤ 256 B | 30 % | — | — | — | identical digests in 3 of 3 consecutive runs |

In every run up to 10 000 ops/s per node: `corrupts = 0`, `stales = 0`, `damages = 0`,
no disconnects.

### 20 000 ops/s

On `d39b85d`, 4 of 6 runs at 20 000 ops/s per node hung both nodes: an `IBV_WC_REM_ACCESS_ERR`
within the first seconds left the QP in the error state and the barrier raised forever
(defects 5–7 below). The unpatched revision `f5a94e2` hangs at this rate as well.

On `eacacff`, three runs of 20 s with 30 % frees:

| Run | Disconnects | Exit status | Live blocks | Damages | Corrupts | Zombies | Resurrected |
|---|---|---|---|---|---|---|---|
| 1 | 0 | 0 / 0 | identical | 0 | 0 | 0 | 15 / 6 |
| 2 | 4 | 2 / 2 | identical | 3 / 3 | 0 / 1 | 1 266 / 1 236 | 36 / 31 |
| 3 | 0 | 0 / 0 | 1 stale version on node B | 0 | 0 | 0 | 13 / 8 |

The remote access error still occurs; the session is now dropped and re-established and the
application keeps running. Transfers that keep failing end in `RELIABLE_MONITOR_BLOCK_DAMAGE`.

With defect 8 fixed, seven runs of 20 s with 30 % frees, none of them with disconnects:

| Result | Runs |
|---|---|
| Live blocks identical, no damages, corrupts or resurrected blocks | 6 of 7 |
| 2 stale versions on node B (defect 9, fixed since), otherwise as above | 1 of 7 |

| | One-way p50 | One-way p99 | Max (raw) |
|---|---|---|---|
| `eacacff` | ~0.87 ms | ~34 ms | 2.9–9.6 s |
| `eacacff` with defect 8 fixed | ~0.85–0.92 ms | ~32–39 ms | 55–180 ms |

At 5 000 ops/s the maximum dropped from 541 ms to 25–38 ms with p50 and p99 unchanged.

### Peak Throughput

Revision `c8b1ad7` with the write budget in the test tool, 20 s runs with 30 % frees and payloads
up to 256 B. The time of the main thread was measured with debug wrappers around
`FlushReliableTracker()` and `FlushInstantReplicator()` (not part of the tested revision).

| Requested | Actual writes/s per node | Delivered versions/s per direction | One-way p50 | One-way p99 | Max (raw) |
|---|---|---|---|---|---|
| 20 000 ops/s | ~14 700 | ~14 700 | ~0.8 ms | ~14 ms | 47 ms |
| 40 000 ops/s | ~21 000 | ~20 000 | ~6 ms | ~42 ms | 87 ms |
| 80 000 ops/s | ~22 000–27 000 | ~18 000–22 000 | ~21 ms | ~80 ms | 0.15 s |
| 160 000 ops/s | ~36 000–42 000 | ~17 000–19 000 | ~58 ms | ~150 ms | 0.24 s |
| 40 000 ops/s, 65 536 slots | ~15 000–17 000 | ~15 000–16 500 | ~14 ms | ~100 ms | 0.18 s |

Delivered versions are fewer than writes at high rates because a block rewritten during a
transfer is delivered only in its latest version. The runs in this table used a profiling build. A
repeated sweep on a clean build of the same revision with block dumps converged in all five runs:
live blocks identical on both nodes (about 50 300 per node with 65 536 slots), no damages, corrupt,
stale or resurrected blocks. Its latencies agree with the table, except a maximum of 2.2–2.4 s at
160 000 ops/s.

Main thread of a node, share of wall time in a single one-second sample per run:

| Requested | Tracker flush | Parked in the replicator barrier | Writing | Flushes per second |
|---|---|---|---|---|
| 20 000 ops/s | 15–21 % | 32–34 % | 29–32 % | ~1 700 |
| 40 000 ops/s | 18–21 % | 35–43 % | 34–46 % | ~200 |
| 80 000 ops/s | 14–20 % | 39–45 % | 38–40 % | ~50 |
| 160 000 ops/s | 14–18 % | 33–40 % | 39–50 % | ~25 |

The observed plateau of this testbed and test tool is about 20 000 delivered versions per second
per direction with small payloads; beyond it the delivery stays flat and only the latency grows.
In the samples the main thread spends 32–54 % of the wall time parked in the LOCK/READY barrier and
11–21 % in tracker flushes. Parking is waiting, not CPU time; the profile is one sample per run, not
a statistic. What the barrier waits for is broken down in [Barrier Breakdown](#barrier-breakdown). With the earlier test tool, payloads up to 4 000 B reached about 13 MB/s per direction,
far below the link capacity.

With the earlier test tool, which caught up the whole backlog in one timer tick, the generator
starved the flushes at 80 000 ops/s and above, flushes became rare and huge, and the delivered
rate fell to 5 000 versions per second instead of reaching the plateau.

### Barrier Breakdown

Every barrier of a run was timed with a debug build (not part of the tested revision), 20 s runs
with 30 % frees and payloads up to 256 B, statistics over all barriers of a node.
*Reaction* is the time from raising LOCK to READY, i.e. until the main thread of the node reaches
`FlushInstantReplicator()`. *Held* is the time from READY to the release, i.e. how long the main
thread stays parked.

| Requested | Barriers/s | Tasks per barrier | Reaction p50 / p99 | Held p50 / p99 / max | Parked per delivered version |
|---|---|---|---|---|---|
| 5 000 ops/s | ~1 560 | 1.2 | 0.01 / 0.2–1.1 ms | 0.10 / 0.8–1.0 / 11–26 ms | ~64 µs |
| 20 000 ops/s | ~750 | 2.2 | 0.05–0.06 / 1.1–1.8 ms | 0.35–0.37 / 4.7 / 37–41 ms | ~27 µs |
| 40 000 ops/s | ~160 | 4.0 | 1.5–1.7 / 9–30 ms | 1.9 / 22–36 / 46–47 ms | ~24 µs |

Share of the held time by what the pending tasks were waiting for:

| Requested | Data from the peer (`WAIT_DATA`) | Own RDMA completions | Shared buffers | Other task processing |
|---|---|---|---|---|
| 5 000 ops/s | 67–75 % | 15–22 % | 0 % | 9–11 % |
| 20 000 ops/s | 76–80 % | 7–9 % | 0 % | 11–17 % |
| 40 000 ops/s | 79–83 % | 4–8 % | 0 % | 9–17 % |

The reactive cadence is visible directly: with growing load the barrier rate drops tenfold, the
batch per barrier grows, and the parked time per delivered version falls from ~64 to ~24 µs. This is
the total parking of the main thread divided by the delivered versions, an efficiency measure, not
the latency of an individual version.

Most of the held time is the receiver waiting for the data of the peer, the expected cost of the
synchronous transfer. At low and medium load the reaction of the sender is negligible and the held
time stays in fractions of a millisecond; it consists of the transfer (compare-and-swap, then
write) and the processing of requests and completions, which the breakdown above does not separate.
At 40 000 ops/s the main thread of the sender is
saturated, its reaction grows to 1.5 ms at the median, and the receiver stays parked for that time:
the busyness of one node is paid by the main thread of the other. The self-balancing is local to a
node, while the wait couples the two nodes. This explains part of the cost of the protocol and is
consistent with the observed plateau of about 20 000 delivered versions per second, but the
measurements do not prove that it is the final limit. It follows from choosing integrity over
latency and is not a defect; within the synchronous model the lever that keeps the contract is a
faster reaction of the application to `INSTANT_REPLICATOR_EVENT_FLUSH`.

### Latency

The latency is measured across nodes with unsynchronized clocks, so each direction includes
the clock offset with opposite signs. The one-way values in the tables are the mean of the two
directions for the same percentile, which cancels the static offset; the raw maxima are shown
per direction.

The final series of 2026-10-09 measured the latency in both modes, 30 s of writes with 30 % frees
and 2 000 user messages per second on both nodes at once (offset between the nodes about 0.4–0.8
ms):

| Rate per node | Mode | One-way p50 | One-way p99 | Max (raw, A→B / B→A) |
|---|---|---|---|---|
| 5 000 ops/s | optimistic | ~0.27 ms | ~7.5 ms | 25 / 35 ms |
| 5 000 ops/s | synchronous | ~0.28 ms | ~4.8 ms | 27 / 25 ms |
| 20 000 ops/s | optimistic | ~0.66 ms | ~21 ms | 43 / 41 ms |
| 20 000 ops/s | synchronous | ~1.2 ms | ~22 ms | 66 / 66 ms |

The latency covers the whole path from the write to the version seen by the application of the
peer: change detection by the tracker, the queues, the network, the read or the transfer and the
barrier of the receiver. The median at 5 000 ops/s is the same as on `d39b85d` (~0.3 ms), while the
p99 is higher (~3.2 ms there) and varied between 4.8 and 7.5 ms in the runs of 2026-10-09; the
cause was not analyzed. At 20 000 ops/s the
optimistic mode halves the median, since most blocks arrive without waiting for the barrier of the
sender.

Per-stage instrumentation at 100 and 2 000 ops/s (debug builds, not part of the tested revision)
showed where the time goes:

| Stage | Maximum observed |
|---|---|
| Write → `RELIABLE_MONITOR_BLOCK_CHANGE` (tracker flush) | 2–23 ms |
| NOTIFY buffer fill | ≤ 0.2 ms |
| Sending queue → `ibv_post_send` | ≤ 32 ms |
| On the wire (NOTIFY) | ≤ 2 ms |
| Receive → reading task created | ≤ 0.2 ms |
| Barrier wait (reader / writer) | ≤ 6–39 ms |
| RETRIEVE → data arrived | ≤ 26 ms |

The steady-state tail is the sum of ordinary stages; no single stage dominates. Delays of
100–250 ms seen in early runs were caused by writes made before the peers connected, which are
delivered by the initial syncing task; the test tool now starts writing after the first
connection.

### Failure Scenarios

1 000 ops/s per node, 30 % frees. Node B is disturbed while node A keeps running.

| Scenario | Detection | Live blocks after recovery | Zombies on node A | Result |
|---|---|---|---|---|
| `kill -9` of node B, restart after 5 s | CM `DISCONNECTED` | all present, no mismatches | 3 113 | passed |
| 5 × `kill -9` / restart, 2 s down, 4 s up | CM `DISCONNECTED` each time | all present, no mismatches | 12 240 | passed |
| `SIGSTOP` of node B for 15 s, then `SIGCONT` | `RNR retry counter exceeded` after ~10 s, then reconnect after `SIGCONT` | all present, no mismatches | 18–50 (867–892 on node B) | passed on `ec029f4` (2 runs); failed on `d39b85d` |

After a restart, the initial syncing task restores all live blocks of the surviving node on the
restarted one. Blocks of the killed incarnation remain on the survivor as zombies: their owner is
gone and nobody sends removals for them. After a freeze, the removals sent while the peer was
stopped are lost in the same way, mostly on the frozen node.

The token revision repeated the scenarios in both modes at 5 000 ops/s with 30 % frees, node B
disturbed after 8 s: `kill -9` with a restart after 2 s, `SIGSTOP` for 2 s (flap) and for 15 s
(freeze). Every run converged for the live objects, with no corrupt or stale arrivals; the
connection teardown that destroys the QP before abandoning the transfers recovered all
disconnects. Synchronous runs had no DAMAGE. Optimistic runs had 4–51 DAMAGE after a kill or a
freeze and 1 222 on the node that resumed after the flap, which processed a backlog of offers for
objects that its peer had released meanwhile (1 194 blocks skipped as missing at the source).
One optimistic freeze run left a single damaged copy of a released object on the frozen node: its
removal was lost with the disconnect, as for the 202 intact zombies of that run. A repeated run
left none.

Before defect 14 was fixed, whether a freeze dropped the connection depended on the moment it hit,
not on the mode; a debug build logged the failed completions. If the survivor had tasks waiting
for data of the frozen node (`tasks=9`, LOCK held in one synchronous run), its main thread stayed
parked in the barrier for the whole stop, sent nothing, and the connection survived. Otherwise the
survivor kept writing, its NOTIFY messages filled the shared receive queue of the frozen node, and
the send failed with `RNR retry counter exceeded` (vendor error 0x87) after about 10 s; the pair
reconnected after `SIGCONT`. Until defect 12 was fixed, the main thread of the survivor also
stalled for about 9 s of these retries waiting for shared buffers. Since the receiver grants the
credit (defect 14), the survivor stops sending once the window of the frozen node is used, the
connection survives the freeze in both modes, the survivor keeps writing with its parking under
25 ms, and syncing tasks catch the frozen node up after `SIGCONT`.

Since the timeouts of defect 15, a stop longer than about 1 s closes the connection: the window of
the frozen node is used within a fraction of a second at 5 000 ops/s, the survivor closes the
connection about 1 s later and connects again after `SIGCONT`, when the initial syncing catches the
frozen node up. The removals of the survivor during the stop are lost with the connection, so a
15 s freeze leaves about 2 200 zombies of the survivor on the frozen node (Known Issue 4), where a
connection kept open had left none.

The final series of 2026-10-09 repeated the scenarios in both modes at 5 000 ops/s with 30 % frees
and 2 000 user messages per second sent with `wait`, node B disturbed after 8 s: `kill -9` with a
restart after 2 s and `SIGSTOP` for 2 s (flap), 7 s and 15 s (freeze), with the default timeout of
1 s. Every run converged for the live objects with no corrupt or stale arrivals and no damaged
blocks left; synchronous runs had no DAMAGE, optimistic runs 1–94 per node. Each scenario
disconnected once, the stops included, since even the 2 s flap outlasts the timeout. No user
message arrived out of order. The test of this series allowed gaps of user messages in any run
with a disconnect and did not compare the ends of the streams; the stricter check described with
[defect 15](#defects-found-and-fixed) showed that messages were missing only across the
disconnect. The
application thread of node A waited in `TransmitInstantReplicatorUserMessage()` for at most
1.0–1.2 s while node B was stopped, until the timeout closed the connection, and kept writing
after it. The zombies followed the length of the disconnect: those of the killed incarnation on
node A (3 125–3 142), and those of node A on node B after a stop, 483–1 130 after 2 s,
2 280–2 287 after 7 s and 2 622–2 632 after 15 s. The longest one-way latency on node B, up to
7.2 s after the 7 s stop and 15.2 s after the freeze, is the age of the versions node A wrote while
node B was stopped, delivered by the syncing after the reconnect.

### RoCE

On 2026-10-10 the same hosts, adapters and switch were also run over RoCE v2: the second ports of
the ConnectX-4 cards, in Ethernet mode, on the Ethernet half of the switch (active MTU 1 024 instead
of 4 096), with the peers given by their Ethernet addresses; the queue pairs were created on the
RoCE devices. The first run found [defect 16](#defects-found-and-fixed): the RoCE virtual function
of node B had a zero node GUID until an administrative MAC was assigned to it.

At 5 000 ops/s with 30 % frees and 2 000 user messages per second both modes converged without a
disconnect, every message arrived, and the one-way median was about 0.22 ms optimistic and
0.36 ms synchronous. A 15 s freeze in the optimistic mode closed the connection after the timeout,
the sender waited 1.21 s, and the pair converged after `SIGCONT`. Five `kill -9` runs with a
restart, four synchronous and one optimistic, converged for the live objects with no corrupt or
stale arrivals; user messages were missing only across the disconnect and every stream ended with
the last message sent.

One synchronous kill left 5 DAMAGE on node A, all at the moment of the disconnect: transfers whose
blocks node B had already locked by CAS when it was killed. The specification requires this (an
entry locked by `Tk | 1` is touched on a disconnect), so the test now accepts DAMAGE without `-O`
in a run with a disconnect and leaves the damaged blocks to `Compare.py`. All 5 were copies of
objects of the killed incarnation that the restarted node no longer held, so `Compare.py` reported
them as unknown (exit `3`); the other synchronous kills had no DAMAGE. Synchronous DAMAGE was not
seen on InfiniBand, likely because the window between the CAS and the end of the WRITE is shorter
there; it was not measured.

## Known Issues

These issues were reproduced on the testbed and are open.

1. **Rare corrupt arrival after a reconnect.** At most one per run, only in runs with
   disconnects: a zero-filled reserved block is accepted as an arrival. Observed before defect 8
   was fixed; the kill, freeze and timeout runs with disconnects since then had no corrupt
   arrivals. Not enough to call it fixed.
2. **`IBV_WC_REM_ACCESS_ERR` at 20 000 ops/s.** Before defect 8 was fixed, a remote access error
   occurred within the first seconds in most runs at 20 000 ops/s and in every run with payloads
   up to 4 000 B; the session recovers from it since defect 5. Since defect 8 was fixed it has not
   occurred in 20 runs: 12 at 20 000 ops/s, 4 at 10 000 ops/s with payloads up to 4 000 B and 4 at
   higher rates. The cause was not determined, so it is only likely, not
   shown, that defect 8 was the trigger.
3. **A stale copy in a run without failures.** One optimistic load run at 5 000 ops/s on
   2026-10-10 left one block on node B with an older version than its author held: the author
   wrote the newer version about 0.4 s before it stopped writing, and it did not arrive during the
   15 s quiescence, with no task left, no DAMAGE and no disconnect. Five repeated runs, and about
   25 load runs before, converged. The cause was not determined.
4. **Zombies after a peer restart or reconnect** are expected: there are no tombstones, removals
   are sent only to connected peers and the removal queue is not persistent
   (see [Replication Model Boundaries](README.md#replication-model-boundaries)). A zombie can
   also be damaged: a transfer abandoned by the disconnect leaves its copy damaged in either mode,
   and when the object is gone at its author no later version repairs it. After a restart the
   author has lost its list of released objects, so `Compare.py` cannot confirm that the object
   was released and reports such a copy as unknown (exit `3`), as in a synchronous kill over
   [RoCE](#roce).

## Defects Found and Fixed

The following defects were found by this test: 1–4 are fixed in `d39b85d`, 5–7 in `eacacff`,
8 and 9 after `eacacff`. Defects 10 and 11 were found by code review while specifying the
[Optimistic Mode](#optimistic-mode) and are fixed in both modes by the token revision, defects
12–15 after it, defect 16 was found by the first run over RoCE and defect 17 by a count of free
shared buffers added to the test afterwards.

1. **Removals raced the tracker flush.** `ApplyRemoval()` freed blocks on the replicator tick
   outside the LOCK/READY barrier. A concurrent `FlushReliableTracker()` could see the block as
   still in use with an already cleared control word, emit `RELIABLE_MONITOR_BLOCK_CHANGE` with a
   cleared identifier, and the peer reserved a permanent block with a nil UUID. Removals are now
   applied in `ExecuteTaskList()` only while READY is held; due removals raise the barrier.
2. **Stale READY.** `FlushInstantReplicator()` checked LOCK and then set READY in a separate
   operation. When the barrier was released in between, READY remained set without LOCK and the
   replicator believed the application was parked while it was running. READY is now raised by
   compare-and-swap only while LOCK is held, and the wait loop rechecks the state on every
   wake-up.
3. **Silent LOCK re-raise.** A late `RING_TAG_READY_STATE` completion after a barrier release
   raised LOCK again without `INSTANT_REPLICATOR_EVENT_FLUSH` and without a wake-up, which
   deadlocked the replicator and the application. It was masked by defect 2. Only
   `ExecuteTaskList()` raises LOCK now.
4. **Application parked until an unrelated completion.** When the application had already
   raised READY by the time the barrier was raised, no wait was armed and the replicator noticed
   READY only with the next unrelated completion (up to 200 ms). The wait is now armed
   unconditionally on raise and completes at once. The maximum time the application thread spent
   parked dropped from 98 ms to 16–22 ms at 2 000 ops/s.

Validation of fixes 1–4: three consecutive runs at 2 000 ops/s per node with 30 % frees produced
identical digests, no nil-UUID blocks and no hangs, while the unpatched revision produced a
nil-UUID block in every run at 1 000 ops/s and above.

5. **Fatal work completions were ignored.** `EnsureWorkOperationCode()` mapped an error completion
   to an ordinary opcode. An error such as `IBV_WC_RNR_RETRY_EXC_ERR` or `IBV_WC_REM_ACCESS_ERR`
   moved the QP to the error state and every later work request was flushed, but the peer stayed
   `CONNECTED` until a process restart. Any completion error other than `IBV_WC_WR_FLUSH_ERR` now
   disconnects the QP; the regular disconnect path reconnects and resyncs.
6. **Tasks of a broken session kept the barrier raised.** On disconnect, tasks waiting for the
   completion of work already posted were kept, but the QP was destroyed right after and the
   completions never arrived. All tasks of the broken session are dropped now; queued work
   completes its tasks with `IBV_WC_WR_FLUSH_ERR` first.
7. **An abandoned exchange was accepted as an arrival.** The sender locks the block of the
   receiver by compare-and-swap (`mark | 1`) and then writes the data. When the sender abandoned
   the exchange after a successful compare-and-swap, the receiver saw a changed mark and validated
   the old content: CRC32C of zero data equals the cleared `control` of a reserved block, so a
   zero-filled block was delivered as an arrival, and an existing block stayed locked. An odd mark
   is never accepted now: the lock is released and the transfer is retried up to
   `READING_ATTEMPT_COUNT` times, then `RELIABLE_MONITOR_BLOCK_DAMAGE` is reported.

Validation of fixes 5–7: no hangs in 7 runs at 20 000 ops/s per node (3 on `eacacff`, 4 with
fixes 5 and 6 only) with up to 4 disconnects per run; corrupt arrivals dropped from 1–3 per run to
at most one, only in runs with disconnects.

8. **Unflushed blocks were sent with a stale `control`.** The barrier parks the application but
   does not flush the tracker first, so a block written after the last flush was transferred
   with new data and the old `control`: 0.2–0.5 % of the RDMA writes at 20 000 ops/s. The receiver
   rejected the transfer, but the data stayed in its copy, and once the task ended, the tracker of
   the receiver published the copy as its own change (260–750 copies per 20 s). When the author had
   freed the block in the meantime, it received the block back and kept it forever (1–36
   resurrected blocks per run). The sender now skips a block whose data does not match `control`
   before the compare-and-swap; the next flush offers the new version.

Validation of fix 8: no resurrected blocks and no damages in 7 runs at 20 000 ops/s per node
(1–36 resurrected blocks per run before); the maximum one-way latency dropped from seconds to
55–180 ms with p50 and p99 unchanged.

9. **The clock vector flipped by an epoch.** `GetReliableTrackerClockVector()` subtracted two
   timestamps truncated to the epoch (16.7 ms). Whenever an epoch boundary fell between the send
   time of the CLOCK message and its receipt, the vector became one epoch smaller, so with an
   offset of 1–4 ms it toggled between 0 and −1 epoch every few measurements. When it changed
   between two close versions of the same author, the newer version was normalized below the
   stored hint of the older one, rejected as older and never offered again, leaving the peer on a
   stale version without `RELIABLE_MONITOR_BLOCK_DAMAGE`. The vector is now the whole difference
   rounded to the epoch.

Validation of fix 9: no stale versions in 17 runs at 20 000 ops/s per node (3 of 15 runs before),
the vector at connection was 0 in all of them.

10. **A selected but not installed version leaked into offers.** `CollectReadingBlockList()`
    raised `hint` to the offered version before the data arrived. A block in this state could be
    offered by the syncing task with its old `mark` and the raised `hint`, and could serve a
    RETRIEVE: the sender checked only the checksum, and the WRITE carried the raised `hint` with
    the old data. The peer then held the old data under the newer label and dropped the real newer
    version until the next change. `Collect` now writes the offer with the pending bit
    (`hint | 1`); the syncing task and the sender skip a block with a pending `hint` or a local
    owner, and a WRITE that lands a pending `hint` is rejected.
11. **An offer was dropped while the block was in transfer.** `CollectReadingBlockList()` dropped
    an entry whose `mark` was odd, so a newer offer that arrived during a transfer of the same block
    was lost until the next change. A reading task now acquires all its blocks or none and waits
    while another task owns one of them.

Validation of fixes 10 and 11: by code; neither defect was reproduced directly. The load and
failure runs of the token revision in both modes had no stale arrivals and converged for the live
objects.

12. **A stalled peer blocked the application thread.** The application thread built NOTIFY and
    REMOVE messages in shared buffers and waited for a free one. A buffer returns to the pool only
    when its SEND has completed at every peer, so a frozen peer whose sends were retried after
    `RNR` held all 2 048 buffers, and the main thread of the survivor stalled for 8.5–8.9 s until
    the session dropped; node B once hung this way right after `SIGCONT` on `d39b85d`. Now:
    - the application thread never waits and cannot take the last `INSTANT_RESERVE_COUNT` (256)
      buffers, which stay for RETRIEVE, COMPLETE, CLOCK and syncing;
    - every connected peer may hold an equal share of the remaining buffers in unfinished sends;
      a NOTIFY beyond it, or one that found no buffer, is counted as lost for the peer;
    - a peer whose count of lost notifications differs from the value at the start of its last
      syncing (`lost` and `last`) gets a new syncing task once its unfinished sends have dropped
      below half of its share; the syncing takes at most that half, so notifications keep the
      rest, and losses during a syncing start the next one; a block skipped by a syncing because
      a transfer holds it counts as lost;
    - REMOVE and user messages are built once in a shared buffer and go through the lock-free
      sending queue, like NOTIFY, so the application thread takes no lock; the replicator thread
      moves them to an ordered list of its own and sends them to every connected peer within its
      credit, returning the buffer when every peer has it, so a busy peer delays them instead of
      losing them; a peer that connects later starts at the end of the list, as before. At most
      `INSTANT_MESSAGE_COUNT` (512) messages are queued: beyond it, or without a free buffer, a
      removal is dropped, which leaves a zombie on the peers like a removal lost with a
      disconnect, and a user message is refused with `-EBUSY`, or with `wait` the application
      thread waits until the peers take the queued messages (`-EFAULT` when the replicator has
      stopped or failed); the replicator thread, including the event handlers, never waits, since
      it is the one that empties the queue. A stopped peer delays the waiting thread for about 1 s
      (defect 15).

Validation of fix 12: in freeze runs of both modes the survivor kept writing about 3 800 versions
per second through the whole stop, where it had stalled for about 9 s; the send still failed with
`RNR retry counter exceeded` and the pair reconnected. Load runs at 20 000 ops/s converged in both
modes, and in a kill run the zombies were only those of the killed incarnation. With node B stopped
for 7 s at 20 000 ops/s, short of the RNR timeout, node A counted the notifications it could not
send and caught node B up with 1–2 syncing tasks after the stop (11 in a row before the syncing
yielded half of the share to the notifications); the live objects converged.

13. **A removal released a block owned by a transfer.** `ApplyRemoval()` freed the block of an
    expired removal under the barrier without checking whether a reading task had acquired it. The
    transfer then wrote into a free block: a READ or WRITE could still land there, and the abandoned
    entry stored its pending `hint` and identifier into it. When the application allocated the
    block for a new object of its own, the object kept the pending bit, and the tracker never
    published it. A removal of a block that a transfer owns now waits at the head of the queue
    until the transfer ends.

Validation of fix 13: one run with node B stopped for 7 s left three own objects of node B with a
pending `hint`, found by `Compare.py`, which now fails on a damaged own block; the repeated runs
after the fix left none.

14. **A SEND stuck in `RNR` held up the transfers of the connection.** Messages and transfers share
    one RC connection per peer. When the shared receive queue of a peer was full, because its
    process was stopped or behind, a SEND was retried after `RNR`, and the READ, CAS and WRITE work
    requests posted after it on the same send queue waited too. The survivor of a 7 s stop stayed
    parked for 1–2 s per barrier on optimistic reads that could not complete, the resumed node for
    up to 10 s on reads and on data from the survivor, and a 15 s freeze ended with
    `RNR retry counter exceeded` and a reconnect. The local credit of fix 12 limits unfinished
    sends, but the HCA of the peer completes a SEND as soon as it places it into a receiving buffer,
    so the queue of a stopped peer still filled up. Now the receiver grants the credit:
    - every peer gets a window of messages that consume a receiving buffer: SEND and
      `RDMA_WRITE_WITH_IMM`; the windows of all connected peers together stay within the shared
      receive queue less `INSTANT_CREDIT_RESERVE` (64) buffers kept for the credit reports;
    - every SEND returns in its `imm_data` the count of messages received from the peer (the card
      number keeps the low `INSTANT_CREDIT_SHIFT` bits); `INSTANT_TYPE_CREDIT` announces a window
      and confirms the window applied to the peer, and is sent when the window changes, when the
      peer must be confirmed or when half of the window was used without another message to carry
      the count; at most one report per peer is in flight;
    - a window starts at 0, so nothing but reports goes to a peer before it has announced one; a
      peer may use any window announced to it until it confirms the last one, so the receiver
      counts the largest of them as used and a new peer gets only what the others leave free;
      a new window is announced only after the previous change is confirmed, so a confirmation
      cannot be taken for a later change of the same size;
    - a writing task takes its credit before the CAS, for the `RDMA_WRITE_WITH_IMM` or COMPLETE
      that ends it; a message without a credit waits (RETRIEVE, syncing, queued messages) or counts
      as lost (NOTIFY).

    Buffers are returned to the receive queue on the replicator thread at once, so credits come
    back while the application is parked and the barriers of two nodes do not wait for each other.

    A reading batch of a barrier is also bounded now. A barrier serves the reading tasks known when
    it was raised, at most `INSTANT_BARRIER_COUNT` (1 024) entries, and the next barrier is raised
    only after the application has returned from the previous one; writing tasks are always served,
    since deferring them would make two barriers wait for each other. This keeps a backlog from
    holding one barrier, but alone it shortened the stall after a 7 s stop only from about 16 s to
    4–10 s: the rest was this defect.

Validation of fix 14 at 20 000 ops/s with 30 % frees: with node B stopped for 7 s, the longest
single parking was 35–52 ms on both nodes (4–10 s before), with 2 syncing tasks and no disconnect;
a 15 s freeze in both modes no longer dropped the connection, the survivor was parked for at most
14–23 ms, and 3 syncing tasks caught the frozen node up; a kill with a restart reconnected from
windows of 0.
All runs converged for the live objects. Load runs without failures changed within the spread of
the runs: parking 5.0–5.5 s optimistic and 9.5–10.2 s synchronous per node over 20 s, the same
delivered versions.

15. **A stopped peer held the barrier and the queues without a limit.** The barrier waited for its
    transfers without a timeout (formerly a known issue): a RETRIEVE lost without a QP error, or a
    peer that stopped while its HCA still acknowledged the transport, kept LOCK and READY raised
    until the peer resumed. With the credit of defect 14 a stopped peer no longer breaks the
    connection by `RNR`, so the queued messages for it, and a user message waiting for a place in the
    queue, also waited for it. Now:
    - a reading or writing task that waits for data, a buffer or a completion and has not advanced
      for the timeout of the replicator (1 s by default, see [Timeouts](#timeouts)) closes the
      connection of its peer; a change of the state of the task and a completed CAS count as
      progress;
    - a peer whose credit window is used up, a zero window included, and which has sent nothing for
      the same timeout is closed too: a live peer announces its first window right after the
      connect and reports at the latest when half of the window is used, so this covers a peer
      stopped before its first announcement;
    - the connection is closed by the replicator itself through the disconnect path, since a stopped
      peer does not answer DREQ: with `rdma_disconnect()` alone the disconnect came only after
      `SIGCONT`, 13 s after the timeout of a 15 s freeze.

Validation of fix 15 at 5 000 ops/s with 30 % frees: a RETRIEVE dropped on purpose by node B kept a
synchronous reading task of node A waiting until the task timeout closed the connection about 1 s
later; the pair reconnected 1.2 s after and converged. In a 15 s freeze of node B in both modes, node
A closed the connection about 1.3 s after the stop began and reconnected after `SIGCONT`; a kill
with a restart behaved as before. Load runs without failures had no timeouts and no disconnects.
All runs converged for the live objects, with no corrupt or stale arrivals.

Further checks with 2 000 user messages per second sent with `wait` from the application thread:
under load without failures every message arrived in order and the longest wait was 0.1 ms; in a
15 s freeze the sender waited 1.15–1.21 s until the connection was closed, and the messages missing
at the receiver were exactly those sent while the connection was down. A RETRIEVE dropped in the
optimistic mode closed the connection about 1 s later as well. Node B stopped right after
`RDMA_CM_EVENT_ESTABLISHED`, before it announced a window, was closed by node A about 1 s later in
both modes, connected again after `SIGCONT` and converged. With the timeout set to 400 ms and
3 000 ms the sender waited 0.58 s and 3.06 s in a freeze, the bound rounded up to whole ticks.

The stricter check of the test allows a gap of user messages only before the first connection to
the author and across its disconnect, and `Compare.py` compares the last message received from
every author with the count it sent, since a lost tail leaves no gap. Load, `kill -9` with a restart
in both modes, flaps in both modes, a 7 s stop and a freeze all passed it: no message was lost
within a connection, every stream ended with the last message sent, and the messages missing
across a disconnect were those sent while the connection was down (1 040–23 320 per run); a
restarted node received the stream of its peer from the point of its connection. A dump with one
more sent message than was received was reported by `Compare.py` as a lost tail.

16. **A connection request without a device context crashed the replicator.** `EnsureCard()`
    passed `descriptor->verbs` to `ibv_query_device()` without a check. librdmacm leaves it `NULL`
    when it cannot map the request to a local device; on the testbed the RoCE function of node B, a
    virtual function without an administrative MAC, had a zero node GUID, and the replicator thread
    of node B crashed with `SIGSEGV` on the first request from node A. Such a request is rejected
    now, and an outgoing connection resolved to such a device fails the same way. With a MAC
    assigned to the virtual function the node GUID is derived from it and the connection works.


17. **Shared buffers of posted SENDs leaked on a disconnect.** A SEND holds a reference to its
    shared buffer until its completion, and the request item was released right after
    `ibv_post_send()`. `ibv_destroy_qp()` removes the completions of the QP from the shared CQ, so
    every SEND still in flight when a connection was closed kept its buffer for good: a `kill -9`
    of node B cost node A 51 of its 2 048 buffers in one run, and repeated disconnects would have
    drained the pool. The posted SENDs of a peer are now kept in order (`submitted`) until their
    completions, and `HandleDisconnected()` releases the rest after the QP is destroyed. With the
    fix all 2 048 buffers were free again after three kills in both modes and five 3 s stops in a
    row, and load runs were unchanged.
