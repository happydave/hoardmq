# The group commit's default delay

**Host:** `ai2`, AMD Ryzen 9 9950X3D (16 cores, 32 threads), 91 GiB RAM, NVMe SSD, Linux 7.0.0-28.
**Date:** 2026-09-05.

## The claim

docs/operations.md, ["Durability and group commit"](../operations.md#durability-and-group-commit): "The default delay of 0 was chosen by measurement (one 100,000-client failover
run at 0, one at 1 ms); the size matters only with a delay, and was not measured."

## What was run

- A failover measurement: three in-process nodes over loopback, no TLS, a lease ttl of 2 s (the harness's; the
  product's is 30 s), 2 replicas, 100,000 durable clients each with one QoS 1 subscription, connected round-robin
  with Clean Start 0 and a 3,600 s expiry. The node holding the most clients was crashed with no DISCONNECT and no
  checkpoint; each client reconnected to the next node in its order with a jittered backoff from 50 ms doubling to
  1 s, and the run watched for ten minutes.
- One run with `walGroupMaxDelayMs` at 0 and one at 1 ms, on the same build, in one sweep.

## The build

The build of 2026-09-05 that added the group commit: a keyed, coalescing fsync outside the store lock, with
`walGroupMaxDelayMs` and `walGroupMaxSize` (256).

## Results

| | Delay 0 | Delay 1 ms |
|---|---|---|
| Node crashed (clients it held) | n2 (33,411) | n3 (33,399) |
| Time to connect and subscribe all | 1m45.72s | 1m40.399s |
| Clients accepted back after losing a connection | 100,000 of 100,000 | 71,639 of 100,000 |
| Per-client reconnect time p50 / p90 / p99 / max | 55.397 / 61.68 / 64.177 / 66.051 s | 75.924 / 79.089 / 86.368 / 89.789 s |
| Connect refusals during the failover | 121,712 | 101,276 |
| Established connections lost | 208,791 | 188,068 |
| Every client back within ten minutes | not reached: 0 online at the end | not reached: 159 online at the end |

- With a delay of 1 ms the clients came back more slowly (p50 76 s against 55 s), so the default is 0: under a
  storm, arrivals overlap every fsync anyway, and a hold only adds latency.
- In both runs the cluster then lost almost every client again within the ten-minute window (0 and 159 online at the
  end).
  A third run, of the same build with lease-liveness counters added, traced that to follower lease renewals timing
  out under the 2 s lease ttl, after which both survivors fenced themselves: a defect of that build's lease plane,
  not of the group commit.

## What this does not show

- One run at each delay, with different nodes crashed, on one host. The difference in recovery rests on two runs.
- Both runs collapsed later from the lease defect above, so neither is a clean measurement of a full recovery.
- `walGroupMaxSize` was not measured; it bounds a hold only when the delay is non-zero.
- The harness's lease ttl of 2 s, not the product's 30 s.
