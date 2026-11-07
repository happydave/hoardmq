# The QoS 2 exchange's latency on a three-node cluster

**Host:** `ai`, AMD Ryzen 9 7950X3D (16 cores, 32 threads), 93 GiB RAM, NVMe SSD (an fsync there takes about 5 ms),
Linux 6.8.0-139.
**Dates:** 2026-10-01 and 2026-10-02.

## The claims

- docs/development.md, ["Simulation harness"](../development.md#simulation-harness) (PUBCOMP waits for the release): "It costs one more fsync per inbound QoS 2 exchange,
  shared by the group commit, and one replica round trip: about 12 ms between PUBREL and PUBCOMP on a three-node
  cluster on one host."
- docs/development.md, ["Simulation harness"](../development.md#simulation-harness) (the accepted record): "a client that sends PUBREL as soon as it reads PUBREC meets it
  on the replica: the whole exchange went from 15.9 ms to 20.4 ms at the median on a three-node cluster on one
  host."

## What was run

- `TestMeasureInboundQoS2Exchange` in `internal/broker`, run with `HOARDMQ_MEASURE_PUBCOMP` set: a three-node
  in-process cluster with each node's data on disk, one publisher with a persistent session on the first node, 20
  warm-up exchanges and then 200 timed QoS 2 exchanges a run, back to back.
- Each run reports the median and 95th percentile of PUBLISH → PUBREC, PUBREL → PUBCOMP and the whole exchange.
- Two pairs of builds, each interleaved base and change, 5 runs each.

## The builds

- **2026-10-01, PUBCOMP after the release is durable:** PUBCOMP goes out only once the packet identifier's release is
  in the node's journal and on the replicas. The base is the same build with PUBCOMP sent at once, the behaviour
  before it.
- **2026-10-02, the accepted record:** the broker records that a QoS 2 publish was accepted, and absorbs a
  retransmission as a duplicate only when that record is there. The base is the build before it.

## Results

PUBCOMP after the release is durable (2026-10-01), each run's value, 5 runs an arm:

| Figure | Base | Change |
|---|---|---|
| PUBREL → PUBCOMP, median | 0.04–0.05 ms | 11.67–11.99 ms |
| PUBREL → PUBCOMP, 95th percentile | 0.07–0.08 ms | 13.08–13.58 ms |
| PUBLISH → PUBREC, median | 21.44–23.04 ms | 11.80–11.98 ms |
| Whole exchange, median | 21.52–23.08 ms | 23.43–24.01 ms |
| Whole exchange, 95th percentile | 24.41–26.96 ms | 25.09–27.74 ms |

The PUBREL → PUBCOMP medians and the whole exchange's medians separate completely, 5 against 5 (one-sided exact
Mann–Whitney p = 0.004); the whole exchange's 95th percentiles do not (p = 0.11).

The accepted record (2026-10-02), the median of the five run medians, with the runs' range:

| Figure | Base | Change |
|---|---|---|
| PUBLISH → PUBREC | 10.11 ms (5.75–10.16) | 10.11 ms (5.74–10.16) |
| PUBREL → PUBCOMP | 7.41 ms (5.74–10.09) | 10.29 ms (10.27–10.32) |
| Whole exchange, median | 15.90 ms (14.96–15.98) | 20.40 ms (20.08–20.41) |
| Whole exchange, 95th percentile | 22.59 ms (21.34–25.43) | 28.03 ms (27.61–28.76) |

Every change run's whole-exchange median is above every base run's, 5 against 5. With the client pausing 20 ms
between reading the PUBREC and sending the PUBREL (3 runs each), the PUBREL → PUBCOMP medians were 5.88, 6.12 and
5.79 ms at the base and 5.85, 5.84 and 6.05 ms with the change. The difference goes when the pause is the only
change, which supports the reading that the acceptance is still being synced on the replica when an immediate PUBREL
arrives, and the release then waits for a second fsync.

## What this does not show

- One publisher sending back to back, in one process on one host. Many publishers, whose fsyncs the group commit
  would share, and nodes on separate hosts were not measured.
- The two sets ran on different days on different bases, so their absolute figures differ (the whole exchange's
  median was 23.43–24.01 ms after the first change and 14.96–15.98 ms at the second's base).
- Five runs an arm.
