# A checkpoint at 100,000 and 1,000,000 sessions

**Host:** `ai2`, AMD Ryzen 9 9950X3D (16 cores, 32 threads), 91 GiB RAM, NVMe SSD, Linux 7.0.0-28, the whole host
reserved.
**Date:** 2026-11-03; the encoder's change measured 2026-11-07 (below).

## The claims

docs/operations.md, ["Persistence"](../operations.md#persistence): "Measured on one node (16 cores, 32 threads) … at 100,000 sessions a checkpoint takes 0.33–0.34
s and adds about 0.37 GiB of heap at its peak over 0.23 GiB live, writes 69 MiB, and a restart reads, decodes and
installs it in 0.46–0.47 s; at 1,000,000 sessions it takes 3.20–3.91 s and adds 3.3 GiB over 2.4 GiB live, writes 696
MiB, and a restart takes 5.58–5.67 s (3.12–3.23 s to read and decode, 2.30–2.40 s to install)", and "size memory for
about 2.4 times the sessions' live state at 1,000,000 sessions and 2.5–2.6 times at 100,000".

## What was run

- `TestCheckpointScale` in `internal/session`, with `HOARDMQ_CHECKPOINT_SCALE=100000,1000000`: it fills a session
  manager with offline persistent sessions, checkpoints them into a new store while sampling the live heap every
  millisecond, then drops the manager and restarts (opens the store, recovers, and loads a new manager).
- Each session holds one subscription and one queued QoS 1 message of 31 bytes.
- 3 runs at each size, in one process, no cluster and no clients.

## The build

`main` of 2026-11-03 with the measurement test added; the checkpoint itself unchanged.

## Results

| Sessions | Checkpoint | Heap added at its peak (live heap) | Files written | Restart: read and decode | Restart: install | Restart: total | Heap held after |
|---|---|---|---|---|---|---|---|
| 100,000 | 335, 330, 329 ms | 354, 378, 378 MiB (237 MiB) | 69 MiB | 279, 284, 286 ms | 169, 167, 172 ms | 459, 462, 470 ms | 153 MiB |
| 1,000,000 | 3.91, 3.32, 3.20 s | 3,372 MiB in each (2,437 MiB) | 696 MiB | 3.12, 3.15, 3.23 s | 2.40, 2.30, 2.31 s | 5.62, 5.58, 5.67 s | 1,658 MiB |

The heap a checkpoint adds is about 1.4 times the live state at 1,000,000 sessions (3,372 ÷ 2,437) and 1.5 to 1.6
times at 100,000 (354–378 ÷ 237), so the live state and the checkpoint together peak at about 2.4 times the live
state at 1,000,000 ((2,437 + 3,372) ÷ 2,437 = 2.38) and 2.5–2.6 times at 100,000 ((237 + 354–378) ÷ 237).

## 2026-11-07: the payload encoded a session at a time

The claims above now read, in docs/operations.md: a first checkpoint "0.34–0.36 s … about 0.32 GiB" at 100,000 and
"3.26–3.32 s and adds 3.0 GiB over 2.4 GiB live" at 1,000,000, a later one "0.24–0.26 s … 0.16–0.21 GiB" and
"2.76–2.86 s and 2.1 GiB", a restart "0.46–0.50 s" and "5.65–5.76 s", and "about 2.3 times … 2.4 times at 100,000 …
1.7–1.9 times for later ones".

- **What changed:** the store encodes the checkpoint session by session into a buffer sized from the last payload,
  the same bytes as `json.Marshal`; the measurement collects twice before each baseline, so `encoding/json`'s pooled
  buffer is not counted in the live heap, and measures a second checkpoint in the same store.
- **What was run:** the same test, `main`'s store code (with the new measurement) and the change interleaved, 3 runs
  each at both sizes, then the change 3 more times on the final commit.

| Build, 1,000,000 sessions | First checkpoint | Second checkpoint | Live heap | Restart total |
|---|---|---|---|---|
| `main`'s store code | +3,372 MiB, 3.04–3.08 s | +3,372 MiB, 3.01–3.02 s | 2,436–2,437 MiB | 5.58–5.62 s |
| the change (6 runs) | +3,063 MiB, 3.26–3.32 s | +2,119–2,120 MiB, 2.76–2.86 s | 2,436 MiB | 5.65–5.76 s |

The cost: a store's first checkpoint takes about 0.25 s longer at 1,000,000 sessions (3.26–3.32 s against
3.04–3.08 s), since its buffer has no earlier payload to be sized from; every later checkpoint takes about 0.2 s less
and about 1.25 GiB less heap.

At 100,000: `main` +354 MiB for each checkpoint; the change +321–343 MiB for the first and +164–211 MiB for the
second, 0.34–0.36 s and 0.24–0.26 s, over 237 MiB live. The ratios: (2,436 + 3,063) ÷ 2,436 = 2.26 and
(2,436 + 2,120) ÷ 2,436 = 1.87 at 1,000,000; (237 + 321–343) ÷ 237 = 2.35–2.45 and (237 + 164–211) ÷ 237 =
1.69–1.89 at 100,000.

## What this does not show

- One session shape (one subscription, one small queued message, every session offline); sessions holding more were
  not measured.
- One process with no cluster beside it: a node's replication, leases and clients were not running during the
  checkpoint.
- Three runs a size, one host.
