# The chart's rolling restart on kind

**Host:** `ai2`, AMD Ryzen 9 9950X3D (16 cores, 32 threads), 91 GiB RAM, NVMe SSD, Linux 7.0.0-28.
**Dates:** 2026-09-23, 2026-10-04, 2026-10-05 and 2026-11-03.

## The claims

- Chart README, "What a rolling restart refuses": "a restart of all three pods refused 3–23 of 600 publishes in 5
  runs, against 103–209 in 5 interleaved runs of the build before it, where the replication barrier waited on the
  restarting pod. Every message was delivered in all 10", and "as all rolling restarts did before (99–281 of 600 in the 26 recorded runs from 2026-09-23 to 2026-10-05)".
- Chart README, "4. Meanwhile, the rolling restart": "3–23 of 600 in the 5 runs of 2026-10-05 with each draining pod
  leaving the shard map, all of them the routing plane's around a client's move".
- Chart README, "Recorded runs": the five runs of 2026-11-03, the eleven of 2026-10-05 (evening), the three of
  2026-10-05, the two of 2026-10-04 and the sixteen of 2026-09-23, each with its refusals and deliveries.
- `docs/upgrade.md`, "On Kubernetes, with the chart": "in the chart's eleven recorded rolling-restart runs of
  the evening of 2026-10-05, a once-a-second sample during a rollout never found two pods not ready at once".

## What was run

- The chart's runbook, recorded by `deploy/helm/rolling-restart/run.sh`: a fresh namespace on an ephemeral one-node
  kind cluster (kind v0.33.0; node v1.37.0 and the podman provider in the sets through 2026-10-05, as each set's
  record states), the chart's three pods with default values plus the
  runbook's anonymous access and allow-all ACL, one durable QoS 1 subscriber and one QoS 1 publisher of 600 messages
  (Paho), and `kubectl rollout restart` of the StatefulSet.
- Each run keeps the clients' events, each pod's refusal counters once a second, the pods' readiness once a second,
  every pod incarnation's log and the namespace's events; `deploy/helm/rolling-restart/attribute.py` classifies each
  refused publish by the path that refused it.
- In the 2026-10-04 and 2026-10-05 sets the kind node's processes were pinned to some of the host's cores (cores
  24–31 on 2026-10-04 and in the first 2026-10-05 set, cores 16–31 in the 2026-10-05 evening set).

## The builds and the results

### 2026-09-23: 16 runs

The build of that day, and the same with the replication barrier narrowed to wait only on the targets of the keys it
names; 6 runs of the first, then 5 pairs interleaved.

| Build | Runs | Refused of 600 | Delivered |
|---|---|---|---|
| that day's build | 11 | 99–277 (median 199) | all 600 in each |
| barrier narrowed to its keys' targets | 5 | 102–194 | all 600 in each |

93.5% of the first build's refusals were the replication barrier on the publisher's pod waiting on the restarting
pod.

### 2026-10-04: 2 runs

The build of that day, which records the admin API's audit and the namespace's events: one run with the chart's
`admin.enabled`, one without.

| Run | Refused of 600 | Delivered | `FailedPreStopHook` during the rollout |
|---|---|---|---|
| admin API on | 187 | 600 | 3, each hook killed 25–30 ms after its drain completed |
| admin API off | 208 | 600 | 3, the same |

### 2026-10-05: 3 runs

The build of that day, in which the broker holds its exit after a drain (`cluster.drain.holdExitSeconds`, 15 s by
default) until the stop signal, so the preStop hook returns first.

| Run | Refused of 600 | Delivered | `FailedPreStopHook` | Hook failures in the kubelet's journal |
|---|---|---|---|---|
| admin API on | 111 | 600 | 0 | 0 of 6 |
| admin API off | 281 | 600 | 0 | 0 of 6 |
| hold set to 0 (the control) | 118 | 600 | 3 | 6 of 6, each killed (`exited with 137`) |

With the hold, each pod's stop signal came 0.90–1.01 s after `drain complete; waiting for the stop signal` (6 of 6
in each run). No run refused a publish after its rollout completed.

### 2026-10-05 (evening): 11 runs

`main` of that day against the build that takes a draining pod out of the shard map, 5 runs each interleaved, then 1
more of the latter with `admin.enabled`.

| Run | Build | Refused | Replication barrier | Routing: `subscriber-node-down` | Routing: other | Not attributed | Rollout |
|---|---|---|---|---|---|---|---|
| 1 | `main` | 123 of 600 | 59 | 14 | 47 | 3 | 29.4 s |
| 2 | `main` | 109 of 600 | 69 | 2 | 37 | 1 | 31.4 s |
| 3 | `main` | 103 of 600 | 70 | 3 | 29 | 1 | 28.4 s |
| 4 | `main` | 209 of 600 | 129 | 11 | 66 | 3 | 38.4 s |
| 5 | `main` | 145 of 600 | 137 | 4 | 2 | 2 | 28.4 s |
| 1 | leaving the map | 3 of 600 | 0 | 3 | 0 | 0 | 27.4 s |
| 2 | leaving the map | 7 of 600 | 0 | 6 | 1 | 0 | 34.3 s |
| 3 | leaving the map | 9 of 600 | 0 | 7 | 2 | 0 | 33.4 s |
| 4 | leaving the map | 12 of 600 | 0 | 2 | 10 | 0 | 33.4 s |
| 5 | leaving the map | 23 of 598 | 0 | 13 | 10 | 0 | 33.4 s |
| admin | leaving the map, admin API on | 21 of 599 | 0 | 11 | 10 | 0 | 32.4 s |

- The two arms do not overlap: 3–23 against 103–209 (5 runs each; one-sided exact p = 1/252, about 0.004).
- Every subscriber received all 600 messages in all 11 runs. Two runs published 598 and 599: the client library
  returned "no connection" for publishes made while the publisher was disconnected.
- In every run of the change, each restarting pod's drain left the shard map (3 of 3 a run). "Routing: other" in
  those runs is a receiver's refusal of a forward to the subscriber's old pod just after the subscriber moved.
- No preStop hook failed.
- Readiness: at no once-a-second sample during any rollout were two broker pods not ready at once, in either arm.

### 2026-11-03: 5 runs

`main` of that day against the hardened chart (a restrictive security context and a read-only root filesystem by
default): 1 run of the hardened chart on one cluster, then 2 of each interleaved on a second.

| Run | Chart and image | Refused of 600 | Delivered | preStop failures |
|---|---|---|---|---|
| 1 | hardened | 39 | 600 | 0 |
| 2 | `main` | 1 | 600 | 0 |
| 3 | hardened | 22 | 600 | 0 |
| 4 | `main` | 33 | 600 | 0 |
| 5 | hardened | 5 | 600 | 0 |

Every refusal was a dispatch to a pod going down or the routing barrier around the subscriber's move, except one
replication-barrier refusal in a run of `main`. The arms overlap (1–33 against 5–39): these runs show the hardened
chart rolls without loss as `main` does, not that the counts differ.

## What this does not show

- Two clients on three pods: not a load test. The share of sessions moved, and what that costs, at 100,000 clients is
  in [rolling-restart-in-process.md](rolling-restart-in-process.md).
- Placement was recorded, not balanced: where the publisher and the subscriber land is the Service's choice, so the
  arms' counts are descriptive; the attribution of each refusal carries the claims.
- 2 to 16 runs a set, on one host and a one-node kind cluster.
- The 99–281 range "before" the shard-map change spans the 26 runs of the four sets before it: 99–277 on 2026-09-23
  (16 runs), 187 and 208 on 2026-10-04, 111, 281 and 118 on 2026-10-05, and 103–209 for `main` that evening.
- The readiness reading is from the 2026-10-05 evening set; the other sets were not read for it.
