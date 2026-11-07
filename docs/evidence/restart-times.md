# How long a node refuses while an owner's routes are unknown

**Host:** `ai`, AMD Ryzen 9 7950X3D (16 cores, 32 threads), 93 GiB RAM, NVMe SSD, Linux 6.8.0 (the exact kernel on
this date is not recorded).
**Date:** 2026-09-07.

## The claim

docs/development.md, ["Simulation harness"](../development.md#simulation-harness) (an owner whose routes are unknown): "Measured on the simulation's seed 5: 0.111–0.202 s
at a cold cluster start, which is the transport connecting and is over long before the readiness gate's five seconds
of required stability; 0.102 s for a restart whose peers are reachable; and 1.92–1.99 s for a restart beside a
partitioned peer, ending when the partition does."

## What was run

- `TestCorpus` in `sim`, seed 5, at the corpus's own timings (the harness's, not the product's): three nodes in one
  process with a seeded workload of publishers and durable subscribers and a seeded schedule of faults. In seed 5
  that schedule drops and delays frames between nodes, partitions one node for about 3.8 s, and crashes another,
  restarting it about 1.3 s later with its disk intact (no torn tail or corruption).
- 3 runs with node logs on. The window is read from each node's log lines for entering and leaving the state in which
  an owner's routes are unknown. In runs 1 and 3 the restart fell beside the partitioned peer; in run 2 its peers
  were reachable.

## The build

The build of 2026-09-07 that makes a node refuse a QoS 1 or 2 publish, rather than acknowledge it, while an owner's
route set has not arrived (`routes-unknown`).

## Results

| Occasion | Window |
|---|---|
| Cold cluster start, per node | 202 ms, 166 ms, 111 ms |
| A restart beside a partitioned peer (runs 1 and 3) | 1.985 s, 1.916 s |
| A restart with its peers reachable (run 2) | 0.102 s |

The refusals this cost, from the nodes' counters: `routes-unknown` refused 73, 43 and 50 publishes in three runs.
Over a batch of 40 seed-5 runs of the same build it refused 22–87 a run (median 51), against about 384 publish
attempts and 126 acknowledged a run. Seed 4, the other seed that restarts a node, showed 22; every other seed showed none.

## What this does not show

- One seed, three runs, in one process on one host, at the harness's timings rather than the product's.
- The restart beside a partition ends with the partition, so its length is the schedule's, not the broker's.
- The cold-start figure is one reading a node.
