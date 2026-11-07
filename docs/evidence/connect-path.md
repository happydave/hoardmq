# A node lost at 100,000 clients: the connect path

**Host:** `ai2`, AMD Ryzen 9 9950X3D (16 cores, 32 threads), 91 GiB RAM, NVMe SSD, Linux 7.0.0-28.
**Date:** 2026-09-26.

## The claims

- docs/security-and-access.md, ["The connect path's cost"](../security-and-access.md#the-connect-paths-cost): "When a node of a three-node cluster serving 100,000 clients is lost, the
  survivors take its clients back within about three seconds of the loss being declared, 12,017 to 16,438 in the
  busiest second, and every client is back 31–33 s after the crash. At 500 a second a listener … the two survivors
  admitted at most 1,033–1,148 in any one second and recovery took 56.0–65.2 s; a host of 32 threads derives about
  373 keys a second".
- Chart README, "Limits": "the two survivors take about 16,700 clients each, at most 12,017–16,438 a second across
  the cluster … against about 3 s unlimited, while every client was back 31–33 s after the crash".

## What was run

- In-process: three nodes, 100,000 anonymous clients, product timings, a 60 s hold. The node holding the most
  clients was crashed: the leader in all 8 default runs and in 5 of 8 at 500 a second (a follower in runs 2, 5 and
  8). The probe published throughout.
- Two arms, interleaved, 8 runs each (16 runs, every one completed):
  - **defaults:** no connect-rate limit, the password-verification cache on (60 s TTL);
  - **500 a second:** each node's client listener admits 500 CONNECTs a second with a burst of 1,000.
- About 33,340 clients were dropped by the crash in each run, so each survivor took about 16,700.
- The clients were anonymous, so no password was verified in these runs. The derivation rate is a separate
  measurement on the same host, after the batch (see [auth-cost.md](auth-cost.md)).

## The build

One build, of 2026-09-26: the one that added connect-rate limits with a burst, and the password-verification cache.
The arms differ only in their environment.

## Results

| | defaults (8 runs) | 500 a second a listener (8 runs) |
|---|---|---|
| Node crashed (the one holding the most clients) | the leader in 8 of 8 | the leader in 5 of 8, a follower in 3 |
| Every client back after the crash | 31.35–32.57 s | 56.00–65.21 s |
| Most clients back in one second | 12,017–16,438 | 1,033–1,148 |
| CONNECTs refused for rate, a run | — | 893,012–1,077,749 |
| Loss declared (applied on the survivors), after the crash | 28.50–29.61 s | 23.68–33.15 s |
| Lost node out of the shard map, before every client was back | 2.73–2.84 s | 31.82–32.25 s |
| Probe refusals after every client was back | 0 in 7 runs; 1,156 in 1 | 0 in 8 runs |
| Probe messages acknowledged and not delivered | 0 in 8 | 0 in 8 |

Each run, at the defaults:

| Run | Every client back | Most back in one second | Probe refusals after |
|---|---|---|---|
| 1 | 31.72 s | 16,438 | 0 |
| 2 | 32.15 s | 12,017 | 0 |
| 3 | 32.57 s | 15,492 | 1,156 |
| 4 | 32.23 s | 14,495 | 0 |
| 5 | 31.62 s | 15,931 | 0 |
| 6 | 31.35 s | 12,377 | 0 |
| 7 | 31.73 s | 16,136 | 0 |
| 8 | 31.90 s | 13,133 | 0 |

The key derivation on the same host, measured after the batch: one PBKDF2 derivation at 600,000 iterations took a
median of 60.7 ms over 21 serial runs (60.6–70.2 ms), and with every one of the 32 threads deriving the host
completed 372.8 a second (256 in 0.687 s).

## What this does not show

- One host: the clients ran in the same process as the nodes and shared its CPU.
- Anonymous clients: no run here derived a key. That the default rate of no limit holds no derivation back is
  arithmetic on the separate derivation figure, not a run with password clients at 100,000.
- The runs were not balanced by which survivor led the cluster after the crash: 6 one way and 2 the other in each
  arm.
- In run 3 at the defaults both survivors forgot the lost node 33.8 s after every client was back, and 1,156 probe
  publishes were refused in that time; 30 sessions handed on in transit by one survivor were never served and
  expired. That run is not attributed here.
- "About three seconds" is the time from the lost node leaving the shard map to the last client back. The crash to
  the loss being declared took 28.50–29.61 s of the 31–33 s.
