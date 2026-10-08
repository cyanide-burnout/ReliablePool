# InstantReplicator Load Testing

This document describes how `InstantReplicator` was load-tested on a two-node InfiniBand
testbed and what the tests showed. It covers replication throughput, delivery latency,
convergence of pool contents and behavior under peer failures.

Testing date: 2026-10-08.
Tested revision: `f5a94e2` plus the replication barrier fixes listed in
[Defects Found and Fixed](#defects-found-and-fixed).

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
- measures one-way delivery latency as arrival time minus the payload timestamp;
- starts writing only after the first peer connects, so writes made before the connection
  (delivered later by the initial syncing) do not distort the latency;
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
  were lost while the nodes were disconnected. Without tombstones this is expected by design.

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
| 10 000 ops/s | ≤ 256 B | ~231 000 | ~0.5 ms | ~24 ms | 79 / 393 ms | **1 block missing** |
| 20 000 ops/s | ≤ 256 B | — | — | — | — | **fails**, see [Known Issues](#known-issues) |
| 2 000 ops/s | ≤ 4000 B | ~46 900 | ~0.4 ms | ~44 ms | 101 / 89 ms | identical digests |

Additional 60 s runs on the same revision:

| Rate per node | Payload | Frees | One-way p50 | One-way p99 | Max (raw) | Convergence |
|---|---|---|---|---|---|---|
| 100 ops/s | ≤ 256 B | 30 % | ~1.5 ms | ~3.3 ms | 9 ms | identical digests |
| 2 000 ops/s | ≤ 256 B | 30 % | ~0.25 ms | ~2.9 ms | 27 ms | identical digests |
| 2 000 ops/s | ≤ 256 B | 30 % | — | — | — | identical digests in 3 of 3 consecutive runs |

In every run up to 5 000 ops/s per node: `corrupts = 0`, `stales = 0`, `damages = 0`,
no disconnects.

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
| `SIGSTOP` of node B for 15 s, then `SIGCONT` | not detected | replication stopped | — | **failed**, see below |

After a restart, the initial syncing task restores all live blocks of the surviving node on the
restarted one. Blocks of the killed incarnation remain on the survivor as zombies: their owner is
gone and nobody sends removals for them.

## Known Issues

These issues were reproduced on the testbed and are open.

1. **Fatal work completions are not handled.** `EnsureWorkOperationCode()` maps an error
   completion to an ordinary opcode, so the buffer is released and nothing else happens.
   An error such as `IBV_WC_RNR_RETRY_EXC_ERR` or `IBV_WC_REM_ACCESS_ERR` moves the QP to the
   error state, every later work request is flushed, but the peer stays `CONNECTED`: there is no
   disconnect, no reconnect and no resync until a process restarts. Asynchronous events
   (`ibv_get_async_event()`) are not processed either.
2. **A frozen peer breaks the pair.** While node B was stopped, its HCA kept acknowledging
   messages until its shared receive queue (2 048 buffers) was drained. Node A then received
   `RNR retry counter exceeded` after ~12 s, its QP went to the error state, and issue 1 left
   it there. During the RNR retries the main thread of node A stalled for 9–12 s waiting for
   shared buffers.
3. **The application thread can block indefinitely.** `AllocateSharedBuffer(..., 1)` is called
   from the application thread (block change and release notifications). When all shared
   buffers are held by traffic towards a peer that no longer completes it, the application main
   loop blocks forever and does not react to `SIGINT`. Node B hung this way right after
   `SIGCONT`.
4. **Tasks under the barrier wait without a timeout.** Reading tasks in `WAIT_DATA` keep the
   LOCK/READY barrier raised until the data arrives. If the QP has died (issue 1), the data
   never arrives, the barrier is never released and the application thread stays parked in
   `FlushInstantReplicator()`.
5. **`IBV_WC_REM_ACCESS_ERR` at 20 000 ops/s.** In 4 of 6 runs at 20 000 ops/s per node, a
   remote access error occurred within the first 0.2–2.6 s, while the pools were still growing.
   Combined with issues 1 and 4 this ends either in a permanent hang of both nodes or in
   massive loss of later changes. The unpatched revision `f5a94e2` hangs at this rate as well.
   The suspected cause is a mismatch between a block address and the memory region key during
   pool expansion; this is not confirmed yet.
6. **A block lost at 10 000 ops/s.** One live block of node A was missing on node B, without
   disconnects or error counters. The cause is not determined yet.
7. **Zombies after a peer restart** are expected: there are no tombstones, removals are sent
   only to connected peers and the removal queue is not persistent
   (see [Replication Model Boundaries](README.md#replication-model-boundaries)).

## Defects Found and Fixed

The following defects were found by this test and are fixed in the tested revision.

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

Validation of the fixes: three consecutive runs at 2 000 ops/s per node with 30 % frees produced
identical digests, no nil-UUID blocks and no hangs, while the unpatched revision produced a
nil-UUID block in every run at 1 000 ops/s and above.
