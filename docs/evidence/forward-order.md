# Forwards leave in sequence order

**Host:** `ai`, AMD Ryzen 9 7950X3D (16 cores, 32 threads), 93 GiB RAM, NVMe SSD, Linux 6.8.0-139.
**Date:** 2026-09-22.

## The claim

docs/development.md, ["Simulation harness"](../development.md#simulation-harness): "A sender admits its forwards to a peer in sequence order, and two 10,000-client
leader-crash failover runs reported neither a gap nor a late frame".

## What was run

- In-process: three nodes, 10,000 clients, product timings. The leader was crashed and the run watched 180 s past
  the point every client was back. The probe published from the crash to the end of the hold.
- 2 runs of the change, and before them 1 run of the previous build on the same shape, the same day.
- Each survivor's counters were read from the run's snapshot: forwards sent, `sequence-gap` failures, late frames
  (`hoardmq_forward_late_frames_total`) and stream restarts.

## The builds

- **Before:** the build of 2026-09-22 before the change, with the late-frame counter added.
- **The change**, of 2026-09-22: the routing plane holds a per-peer send lock from taking a forward's sequence number
  until the transport admits the frame, so frames leave in the order of their numbers.

## Results

| Run | Survivor | Forwards sent | Sequence gaps | Late frames | Stream restarts |
|---|---|---|---|---|---|
| before | first | not captured | 170 | 87 | not captured |
| before | second | not captured | 136 | 70 | not captured |
| change, run 1 | first | 1,827 | 0 | 0 | 0 |
| change, run 1 | second | 1,640 | 0 | 0 | 0 |
| change, run 2 | first | 1,458 | 0 | 0 | 0 |
| change, run 2 | second | 1,455 | 0 | 0 | 0 |

- The forwards count shows the zeros are not from an idle stream: each survivor forwarded 1,455–1,827 frames.
- A counter that never moved is absent from the snapshot; from the change on, the snapshot captures the late,
  restart and forwards families, so an absent entry is a zero.
- The mechanism has its own test: `TestFramesAreAdmittedInSequenceOrder` (`internal/cluster/routing`) failed in 20 of
  20 runs with the send lock removed, and passes with it.

## What this does not show

- Two runs of the change against one before, at 10,000 clients on one host. The zero rests on the counts and on the
  test above, not on a large sample.
- The probe was still refused around the failover in both runs (2,971 and 1,897 publishes; 400 and 0 of them after
  every client was back). Those refusals are the failover's, not a gap's, and are not this claim.
- Run 1 also rebuilt 441 sessions empty on a reconnect, a defect of that build outside this claim.
