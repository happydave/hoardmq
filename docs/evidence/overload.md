# The overload tier in a 100,000-client failover

**Host:** `ai2`, AMD Ryzen 9 9950X3D (16 cores, 32 threads), 91 GiB RAM, NVMe SSD, Linux 7.0.0-28.
**Date:** 2026-09-26.

## The claims

- docs/operations.md, ["Overload and bans"](../operations.md#overload-and-bans) (the stress level): "In a failover storm at 100,000 clients the replication backlog
  peaked at 84,314 frames, 0.42 of the 200,000 limit that build ran with (about a fifth of today's 400,000), and the
  stress level stayed 0".
- docs/operations.md, ["Overload and bans"](../operations.md#overload-and-bans) (what cluster-wide costs): "in seven failover runs at 100,000 clients the survivors saw
  two or three abnormal ends a run and tripped none".

## What was run

- In-process: three nodes, 100,000 anonymous clients, product timings, a 60 s hold. The cluster's leader was crashed.
  The probe published throughout.
- 7 runs, every one completed, balanced by which survivor led after the crash (4 one way, 3 the other).

## The build

The build of 2026-09-26 that added per-client PUBLISH rates, the cluster stress level, the ban list and flapping
detection, and non-blocking delivery, run at its defaults of the time: no per-client rate; the stress limits at
1,000,000 queued messages, 200,000 frames of replication backlog and no memory limit; flapping detection on at 15
abnormal ends in 60 s. It was a measurement branch run from its own worktree, not yet merged into `main` at the
time. The backlog limit's default was raised to 400,000 after this batch; the batch was not repeated at the new
limit.

## Results

| Run | Every client back | Peak backlog (frames), each survivor | Stress level (max) | Flapping trips | Abnormal ends | Probe refusals after every client was back |
|---|---|---|---|---|---|---|
| 1 | 31.56 s | 23,260 / 47,758 | 0 | 0 | 2 | 0 |
| 2 | 32.04 s | 15,019 / 84,314 | 0 | 0 | 2 | 0 |
| 3 | 32.36 s | 11,646 / 44,171 | 0 | 0 | 2 | 0 |
| 4 | 31.64 s | 8,359 / 83,204 | 0 | 0 | 2 | 0 |
| 5 | 32.27 s | 13,780 / 45,114 | 0 | 0 | 3 | 0 |
| 6 | 30.54 s | 21,217 / 46,029 | 0 | 0 | 2 | 0 |
| 7 | 31.92 s | 6,042 / 83,856 | 0 | 0 | 2 | 33 |

- Every client was back in 30.54–32.36 s.
- The deepest backlog sample in each run was 44,171–84,314 frames: 0.22–0.42 of the 200,000 limit, against level 1
  at 0.6. The 84,314 peak is 0.21 of today's 400,000.
- The stress level stayed 0 on both survivors in every run, at the node and the cluster level. No CONNECT was refused
  for overload, no connection was paused, no delivery was deferred and no client was throttled.
- The flapping detector saw 2–3 abnormal ends a run (summed over the survivors), tripped 0 times and wrote 0 bans.
- The backlog is sampled once a second: it counts the frames sent to peers and not yet acknowledged, summed over a
  node's peers.

## What this does not show

- One host, one batch of 7 runs, anonymous clients, no per-client rate set.
- The backlog's headroom at today's default of 400,000 is arithmetic on these peaks, not a run at that limit.
- Run 7 refused 33 probe publishes after every client was back, with 34 replication barrier timeouts on one
  survivor's stream just before. The same signature had been recorded once before, at a rate of about 1 run in 76.
  It is not attributed to this build.
- Takeovers of the lost node's sessions end no connection, so a node loss gives the detector little to count; a
  reconnect flood of clients that disconnect abnormally was not part of this run.
