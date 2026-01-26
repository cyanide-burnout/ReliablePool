# ReliablePool

ReliablePool was introduced in **2022** as part of the **BrandMeister** and **TetraPack** projects.

## Background

ReliablePool started as an internal building block to support high-reliability, high-throughput components in BrandMeister and TetraPack. Over time it evolved into a standalone subsystem with its own API and supporting components (such as tracking/monitoring and event integration).

## What it is

ReliablePool is a memory/pool subsystem designed around long-lived, stable memory mappings (“shares”) and predictable ownership semantics. It is intended for systems that need:

- stable addresses over time (even as the pool grows),
- explicit lifetime management and controlled ownership,
- integration points for replication/monitoring and idempotent processing,
- capability to recover data on restart (through using `memfd` or an opened file as backends and systemd’s **FDSTORE** feature),
- capability for inter-process sharing.

## Related components

ReliablePool is commonly used together with:

- **ReliableMonitor** — a pluggable monitor/replication chain (supports multiple providers):
  - **ReliableIndexer** — indexes blocks and pools for cross-host replication providers.
  - **ReliableTracker** — tracks modified pages via `userfaultfd` write-protection (WP) and maintains a dirty bitmap; flushes in an idempotent state; can notify monitors about changes:
    - **ReliableWaiter** — an external adapter for the **FastRing** event loop, kept outside the tracker to avoid coupling ReliableTracker to a specific runtime.


---
