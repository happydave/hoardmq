# Rolling restarts in the in-process harness

**Host:** `ai2`, AMD Ryzen 9 9950X3D (16 cores, 32 threads), 91 GiB RAM, NVMe SSD, Linux 7.0.0-28. The 2026-10-09
and 2026-10-21 batches ran with the whole host reserved.
**Dates:** 2026-10-06 to 2026-10-21, per set below.

## The claims

Chart README, "What a rolling restart refuses":

- "at 10,000 clients the drains took 25.2–25.3 s and a roll 92 s, refusing 68–137 probe publishes against 287–360
  for the build before, which cut its last drain each time; at 100,000 clients the drains took 25.8–59.7 s (the last
  pod 3,067 clients a second) and a roll 94.7–128.2 s, against 1,676 s at a fixed 100 a second".
- "At 100,000 clients the last pod's step once refused or failed 18.9–45.1% of the probe's publishes for about 20 s
  (in 6 of 6 rolls with node logs on; 41% in 1 of 3 without)".
- "the last step refused or failed 1.6–3.9% of the probe against 35.6–41.3%, a roll 2.30–2.83% against 11.66–13.35%,
  and rolls took 95.1–95.2 s against 97.2–98.0 s. The second pod's step refused or failed 568–1,016 of 32,000 around
  the first pod's restart, against 498 before."
- "The build before the redirect deadline, its drains cut at the grace, refused 4,280–5,827, failed 4,693–5,743 and
  dropped 95,723–115,049 connections a roll at 100,000 clients".
- "28.4–30.8% of retained publishes were refused over a roll in 5 runs, an eighth to about a quarter of them for a
  moving shard, against 37.0–39.8% on the build before … 22.5–28.7% were refused in 3 runs, against 27.3–30.5% on the
  build before it, and none was lost".

Chart README, "Limits" (derived): "In a rolling restart at 100,000 clients the last pod redirects 3,067 clients a
second, about 1,534 a second to each of the other two (3,067 ÷ 2)".

## What was run

- `TestRollingRestart` in `sim`: three nodes in one process, product timings, a hold of 60 s, restarted in the order
  n3, n2, n1 as a StatefulSet restarts its pods. Each node drains before it stops and rejoins after.
- Where a set says the drains were cut, a drain still running 75 s after it began was cut and its node stopped, as
  the kubelet does at the chart's grace period (`HOARDMQ_SIM_ROLL_CUT_SEC=75`).
- The probe: QoS 1 publishers and subscribers in pairs, one publish a second each, plus 6 shared-subscription
  groups; 100 pairs at 10,000 clients and in the 100,000-client sets of 2026-10-06 and 2026-10-07, 1,000 pairs at
  100,000 clients from 2026-10-09. "Refused" is a publish answered with an error;
  "failed" is one that got no acknowledgement. The probe's attempts grow with a roll's length, so compare counts
  within a step, or a step's share.
- Arms within a set were interleaved, one roll an arm a round, except where a set below says otherwise. Every run
  completed (exit 0).
- "Nothing lost" in every set below means: 0 probe messages acknowledged and not delivered, 0 QoS 2 duplicates, 0
  sessions rebuilt empty, and every client back.

## The builds

- **2026-10-06:** a new owner's confirmation reaches the drained node and its old replica targets. Each drain then
  redirected its clients at a fixed 100 a second.
- **`main` of 2026-10-09:** drains at a fixed 100 a second (unless a set overrides the pace).
- **The redirect deadline** (2026-10-08): a drain's redirect is due by its deadline, the drain's 30 s timeout less the
  5 s takeover timeout, so a pod paces its redirects to its client count.
- **A whole-timeout variant** (2026-10-09, measurement only, not shipped): the same deadline at the whole 30 s.
- **A trace build** (2026-10-20, measurement only): the redirect deadline with per-key refusal, barrier and
  confirmation-queue records added; and the same with the redirect paced at a fixed rate, no deadline.
- **`main` of 2026-10-21**, and **the targeted wakes** (2026-10-21): the replication stream wakes only the waits an
  acknowledgement or a refusal can decide.
- **Retained, 2026-10-08:** `main` of that day with the retained probe added; and the change that makes a shard's
  leader refuse a retained write while the shard moves (`moving`).
- **Retained hand-overs, 2026-10-08:** every node that held a shard hands its copies to the shard's new leader at any
  leader change; measured against `main` of the same day.

## Results

### 10,000 clients: the redirect deadline against `main` (2026-10-09)

3 rolls an arm, drains cut at 75 s, the probe at 100 pairs.

| | `main` (3) | redirect deadline (3) |
|---|---|---|
| Roll | 178.2–178.3 s | 91.4–92.0 s |
| Drains n3 / n2 / n1 | 34.2–34.3 / 53.2 / 75.0, cut | 25.2 / 25.2–25.3 / 25.2–25.3 s |
| Redirect pace n3 / n2 / n1 (clients a second) | 100 / 100 / 98 until the cut | 137 / 205 / 308 |
| Drains cut at 75 s | 3 of 9 (every n1) | 0 of 9 |
| Probe refused / failed, a roll | 287–360 / 84–261 | 68–137 / 0–7 |
| CONNECTs refused | 11–14 | 0 |
| Lost | nothing | nothing |

### 100,000 clients: the redirect deadline against `main` (2026-10-09)

3 rolls an arm, the probe at 1,000 pairs. `main` and the deadline arm had drains cut at 75 s, as did the variant; the
fixed-rate arm ran `main` at 1,050 redirects a second with no cut. `main`, the deadline arm and the fixed-rate arm were
interleaved; the variant ran its 3 rolls after them.

| | `main`, cut (3) | redirect deadline (3) | whole-timeout variant (3) | `main` at 1,050 a second (3) |
|---|---|---|---|---|
| Roll | 244.9–245.2 s | 94.7–128.2 s | 109.0–110.2 s | 174.4–207.6 s |
| Drains n3 / n2 / n1 | 75.0 cut, each | 25.8–26.1 / 26.1–26.3 / 26.1–59.7 s | 30.8–30.9 / 31.2–31.3 / 30.9–32.0 s | 33.3 / 50.8–83.3 / 74.0 s |
| Redirect pace n3 / n2 / n1 | cut after about 7,500 clients | 1,363 / 2,040–2,045 / 3,067 | 1,136 / 1,703 / 2,555 | 1,050 |
| Probe refused, by roll | 5,827; 4,855; 4,280 | 11,542; 4,163; 3,370 | 11,208; 1,620; 1,709 | 2,484; 2,521; 2,965 |
| Probe failed, by roll | 4,693; 5,743; 4,929 | 718; 207; 248 | 1,390; 0; 203 | 252; 269; 247 |
| Last step's share refused or failed, by roll | — | 40.9%; 2.4%; 2.6% | 40.8%; 0.5%; 2.2% | 1.2–1.8% |
| Drains cut at 75 s / run past the 75 s grace | 9 of 9 / 9 of 9 | 0 / 0 | 0 / 0 | not cut / 2 of 9 (n2) |
| Drains ending `copies-expired` | — | 1 of 9 | 0 of 9 | 2 of 9 |
| Connections dropped / CONNECTs refused | 95,723–115,049 / 147,515–207,149 | 0 / 0 | 0 / 0 | 0 / 0 |
| Lost | nothing | nothing | nothing | nothing |

In the deadline arm one drain of 9 ended `copies-expired` with 3 sessions unconfirmed (n1, roll 2). In the first
roll of the deadline arm and of the variant, the last step refused or failed about 41% of the probe; in the deadline
arm's, that step counted 6,877 replication barrier timeouts on n2. It came in 1 of 3 rolls in each arm and was not
attributed in this set. At a fixed 1,050 a second, 2 of 9 drains (both n2) ran past the chart's 75 s grace and ended
`copies-expired`.

The pace behind the derived figure in "Limits": the last pod redirected 3,067 clients a second; split between the
other two pods, that is about 1,534 a second each (3,067 ÷ 2), and only in the last pod's step.

### 100,000 clients at a fixed 100 a second (2026-10-06)

The 1,676 s comparison comes from 2 rolls of the build of 2026-10-06, drains at the then-default 100 redirects a
second and not cut, the probe at 100 pairs: rolls of 1,676.4–1,676.6 s, drains of 349.7–350.3 / 524.0–524.5 /
786.1–786.3 s, the probe refused or failed 0.11–0.16% of a roll, nothing lost. Every drain ran far past the chart's
75 s grace.

The set of 2026-10-07 ran the same broker with the harness's drain cut and pace settings, 3 rolls an arm. Rolls
whose drains were cut at 75 s took 244.4–244.8 s and lost 4 acknowledged probe messages in 1 of 3 rolls, with
94,496–99,251 connections dropped a roll; a redirect at about 1,050 a second, not cut, took 169.3–170.5 s and lost
nothing.

### The last step's spike, with node logs on (2026-10-21)

6 rolls an arm, interleaved, node logs on in both, the probe at 1,000 pairs, no cut: the trace build at the
deadline's pace against the same at a fixed 1,050 a second.

| Roll | Deadline pace: last step refused + failed of attempts | Barrier timeouts | Fixed 1,050 a second: last step | Barrier timeouts |
|---|---|---|---|---|
| 1 | 9,516 + 1,273 of 56,970 (18.9%) | 6,443 | 1,709 + 79 of 78,197 (2.3%) | 644 |
| 2 | 11,347 + 817 of 26,970 (45.1%) | 6,916 | 1,056 + 35 of 80,000 (1.4%) | 0 |
| 3 | 8,675 + 1,272 of 27,606 (36.0%) | 5,807 | 2,066 + 130 of 109,000 (2.0%) | 0 |
| 4 | 8,072 + 1,272 of 25,970 (36.0%) | 5,808 | 924 + 0 of 80,000 (1.2%) | 0 |
| 5 | 8,522 + 1,238 of 24,990 (39.1%) | 6,213 | 2,116 + 122 of 77,395 (2.9%) | 644 |
| 6 | 10,590 + 1,223 of 57,962 (20.4%) | 6,447 | 1,692 + 124 of 78,197 (2.3%) | 645 |

- The spike came in 6 of 6 rolls at the deadline's pace and 0 of 6 at the fixed pace. It is the draining node's
  replication stream to the node that rejoined during its drain staying 10,000–35,000 frames unacknowledged for
  about 20 s, so every barrier on it timed out at 2 s.
- Without node logs (the 2026-10-09 set above) it came in 2 of 6 rolls at the deadline's pace (the deadline arm and
  the variant) and 0 of 3 at 1,050 a second.
- Nothing was lost in any roll.

### The targeted wakes against `main` (2026-10-21)

3 rolls an arm, interleaved, node logs on, the probe at 1,000 pairs, the deadline's pace.

| Arm | Roll | Second pod's (n2) step refused + failed | Last step: barrier timeouts | Last step refused + failed of attempts | Over the roll | Roll |
|---|---|---|---|---|---|---|
| `main` | 1 | 498 + 0 (1.6%) | 6,426 | 9,451 + 1,278 of 25,990 (41.3%) | 13.35% | 98.0 s |
| `main` | 2 | 498 + 0 (1.6%) | 6,450 | 8,727 + 1,288 of 25,972 (38.6%) | 12.56% | 97.7 s |
| `main` | 3 | 498 + 0 (1.6%) | 5,797 | 8,199 + 999 of 25,806 (35.6%) | 11.66% | 97.2 s |
| targeted wakes | 1 | 498 + 70 (1.8%) | 0 | 918 + 317 of 32,000 (3.9%) | 2.60% | 95.2 s |
| targeted wakes | 2 | 748 + 251 (3.1%) | 0 | 167 + 352 of 32,000 (1.6%) | 2.30% | 95.1 s |
| targeted wakes | 3 | 823 + 193 (3.2%) | 0 | 918 + 211 of 32,000 (3.5%) | 2.83% | 95.2 s |

Every roll: 0 acknowledged and undelivered, 0 group messages delivered to no member, 0 QoS 2 duplicates, 0 sessions
rebuilt empty, 100,000 of 100,000 back. The change's extra refusals in the n2 step (568–1,016 of 32,000 against 498)
are `send-failed`, `subscriber-node-down` and failures with no acknowledgement around n3's restart; they are not
attributed.

### Retained publishes across a roll (2026-10-08 and 2026-10-09)

10,000 clients. A retained probe of 64 topics over every shard, one QoS 1 retained publisher a node publishing once a
second, ran beside the pair probe. 5 rolls an arm (2026-10-08), then 3 rolls an arm (2026-10-09).

| Set | Arm | Retained refused over a roll | `moving` refusals | Retained lost |
|---|---|---|---|---|
| 2026-10-08 | before | 37.0–39.8% | 0 | 0 |
| 2026-10-08 | refuse while moving | 28.4–30.8% | 27–60 | 0 |
| 2026-10-09 | `main` | 27.3–30.5% | 48–57 | 0 |
| 2026-10-09 | hand-overs at a leader change | 22.5–28.7% | 5–34 | 0 |

Each roll of the "refuse while moving" arm, and its `moving` share of the retained refusals:

| Roll | Retained refused | `moving` | Share |
|---|---|---|---|
| 1 | 227 of 737 | 49 | 21.6% |
| 2 | 232 of 757 | 60 | 25.9% |
| 3 | 209 of 728 | 27 | 12.9% |
| 4 | 214 of 751 | 57 | 26.6% |
| 5 | 205 of 721 | 33 | 16.1% |

- The "before" arm's refusals were barrier timeouts: 6,499–6,799 a roll, against 1,182–3,270 with the change.
- In the 2026-10-09 set the rolls took 276.3–280.3 s on `main` and 268.7–276.3 s with the hand-overs.
- The retained probe loads the cluster: the pairs' refusals in these rolls (8.15–24.52% over a roll) are far above
  the pairs' share in rolls without it.

## What this does not show

- One host, in one process: the pods' network, the kubelet and the image pull are not modelled; the 75 s cut stands
  in for the kubelet's grace.
- Three rolls an arm in most sets; two in the 2026-10-06 set, five in the 2026-10-08 set and six in the 2026-10-21
  spike set. The spike at the deadline's pace appeared in 2 of 6 rolls in the 2026-10-09 set and 6 of 6 in the
  2026-10-21 set; the sets differ in node logging, build and date, and the cause of the difference is not
  established.
- "Nothing lost" is the probe's and the clients' reading, not a check of every message the population sent.
- The retained figures are at 10,000 clients only. The `moving` share counts a refusal at the shard's leader; its
  publisher's own barrier failure is counted elsewhere.
