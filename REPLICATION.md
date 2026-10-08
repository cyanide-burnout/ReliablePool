# InstantReplicator Replication

This document describes the replication concept implemented in `InstantReplicator` and how it
behaves under load. It explains the design choices behind the protocol and backs them with
measurements on a two-node InfiniBand testbed: replication throughput, delivery latency, the cost
of the replicator barrier, convergence of pool contents and behavior under peer failures.

Testing date: 2026-10-08.
Tested revisions: `d39b85d` (replication barrier fixes) for the load sweep, latency and failure
scenarios; `eacacff` (session recovery fixes) and `eacacff` with defect 8 fixed for the
[20 000 ops/s](#20-000-opss) results.
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
the received data and produces extra retries and errors. The price of the synchronous transfer is
that the receiver keeps its own barrier raised while it waits for the sender, so the parked time of
one node includes the time the other node needs to reach its barrier.

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
  of the pool, and dumps every surviving block (`-o`);
- keeps its own blocks allocated at exit, because releasing a block of any type sends
  `INSTANT_TYPE_REMOVE` to the peers that may still be collecting their dumps.

Node identifiers are derived from the node name (`uuid_generate_sha1()` in the OID namespace),
so a restarted process keeps its identity.

Exit status: `0` passed, `1` setup or runtime failure, `2` verification failure
(corrupt, damaged or stale arrivals, or no peer connected).

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

When both nodes stop writing at the same time and nothing was lost, the pool digests of the two
nodes are identical.

`Tests/Replication/Compare.py` performs this comparison. It takes pairs of node name and dump
file and exits with `0` when the nodes converged, `1` when blocks are missing or mismatched and
`2` on a usage error:

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
| 2 stale versions on node B, otherwise as above | 1 of 7 |

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

## Known Issues

These issues were reproduced on the testbed and are open.

1. **Rare corrupt arrival after a reconnect.** At most one per run, only in runs with
   disconnects: a zero-filled reserved block is accepted as an arrival. Observed before defect 8
   was fixed; since then disconnects occurred only in the two freeze runs, which had no corrupt
   arrivals. Not enough to call it fixed.
2. **Stale version without `RELIABLE_MONITOR_BLOCK_DAMAGE`.** In 3 of 15 runs at 20 000 ops/s
   without disconnects 1–2 blocks remained on the previous version on the peer; none in the runs
   at other rates. The last versions were written shortly before the writing stopped; their
   transfer was dropped by a path that does not retry.
3. **`IBV_WC_REM_ACCESS_ERR` at 20 000 ops/s.** Before defect 8 was fixed, a remote access error
   occurred within the first seconds in most runs at 20 000 ops/s and in every run with payloads
   up to 4 000 B; the session recovers from it since defect 5. Since defect 8 was fixed it has not
   occurred in 20 runs: 12 at 20 000 ops/s, 4 at 10 000 ops/s with payloads up to 4 000 B and 4 at
   higher rates. The cause was not determined, so it is only likely, not
   shown, that defect 8 was the trigger.
4. **A frozen peer stalls the survivor.** While node B is stopped, its HCA keeps acknowledging
   messages until its shared receive queue (2 048 buffers) is drained. Node A then waits for
   `RNR retry counter exceeded` (about 10 s); during the RNR retries its main thread stalls for
   8.5–8.9 s waiting for shared buffers (issue 5). Since defect 5 is fixed, the error drops the
   session, node A continues and the pair reconnects after `SIGCONT`.
5. **The application thread can block indefinitely.** `AllocateSharedBuffer(..., 1)` is called
   from the application thread (block change and release notifications). When all shared
   buffers are held by traffic towards a peer that no longer completes it, the application main
   loop blocks and does not react to `SIGINT`. Node B hung this way right after `SIGCONT` on
   `d39b85d`. Since the session recovery fixes the wait ends when the failed session is dropped,
   but it still lasts as long as the RDMA retries (issue 4).
6. **Tasks under the barrier wait without a timeout.** Tasks of a broken session are dropped now,
   but a message lost without a QP error would still keep the LOCK/READY barrier raised.
7. **Zombies after a peer restart or reconnect** are expected: there are no tombstones, removals
   are sent only to connected peers and the removal queue is not persistent
   (see [Replication Model Boundaries](README.md#replication-model-boundaries)).

## Defects Found and Fixed

The following defects were found by this test: 1–4 are fixed in `d39b85d`, 5–7 in `eacacff`,
8 after `eacacff`.

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
