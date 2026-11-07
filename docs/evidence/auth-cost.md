# What verifying a password costs, and the `auth.db` store

**Hosts:** `ai`, AMD Ryzen 9 7950X3D (16 cores, 32 threads), 93 GiB RAM, NVMe SSD, Linux 6.8.0-139; `ai2`, AMD
Ryzen 9 9950X3D (16 cores, 32 threads), 91 GiB RAM, NVMe SSD, Linux 7.0.0-28.
**Dates:** 2026-09-26, 2026-09-28 and 2026-11-02.

## The claims

- docs/security-and-access.md, ["The connect path's cost"](../security-and-access.md#the-connect-paths-cost): "Verifying a password derives a key: 600,000 iterations of PBKDF2, about 62 ms
  of one core, so a 32-thread host derives at most 320–373 a second with every thread busy."
- docs/security-and-access.md, ["The connect path's cost"](../security-and-access.md#the-connect-paths-cost): "An Argon2id verification at the default cost holds 64 MiB and took 45–47 ms
  here (its four lanes run at once; a 32-thread host, against about 30 ms for the reference Go implementation)."
- docs/security-and-access.md, ["The in-broker database (`auth.db`)"](../security-and-access.md#the-in-broker-database-authdb): the table of heap, snapshot, a follower's staging and the consensus
  loop's hold at 100,000 and 1,000,000 credentials.

The figure of about 85 Argon2id verifications a second in a reconnect storm, in
docs/security-and-access.md, ["The connect path's cost"](../security-and-access.md#the-connect-paths-cost), is arithmetic on the second
claim (4 at once at about 47 ms each), not a measurement.

## PBKDF2 (2026-09-26)

A small program, not part of the repository, ran 21 serial PBKDF2 derivations at 600,000 iterations, then derived on
every hardware thread at once and timed the throughput.

| Host | One derivation (21 serial) | Every thread deriving |
|---|---|---|
| `ai` (32 threads) | median 62.2 ms (61.5–75.3 ms) | 320 a second |
| `ai2` (32 threads), after a 100,000-client batch on it | median 60.7 ms (60.6–70.2 ms) | 372.8 a second (256 in 0.687 s) |

## Argon2id (2026-09-28)

`BenchmarkIDKeyDefault` in `internal/argon2` (64 MiB, 3 passes, 4 lanes) on `ai`, 3 runs of 20 derivations each, on
the build of 2026-09-28 that accepts Argon2id hashes with an in-house implementation. The reference
`golang.org/x/crypto` implementation, with its assembly, ran the same benchmark in a separate module on the same host,
minutes apart. The host's load average was 6.3 at the start.

| Implementation | Per derivation, each run |
|---|---|
| in-house (the broker's) | 44.96, 46.06, 46.70 ms (64 MiB and about 410 allocations) |
| `golang.org/x/crypto` | 28.80, 29.41, 31.70 ms |

## The `auth.db` store (2026-11-02)

`TestCredentialSnapshotMeasure` in `internal/cluster/consensus/etcdraft`, run with `HOARDMQ_CREDENTIALS_MEASURE` set,
on `ai2` with the whole host reserved and nothing else running, on the build of 2026-11-02 that added the
measurement. Each credential is a PBKDF2 PHC string, 105 bytes with its JSON, under a 36-byte username. Heap is
`HeapInuse` after a GC, by phase. Two runs at each size, in the order 100,000, 1,000,000, 1,000,000, 100,000.

| Figure | 100,000 (2 runs) | 1,000,000 (2 runs) |
|---|---|---|
| Heap held | 33.1–33.3 MB (331–333 bytes a credential) | 401–405 MB (401–405 bytes) |
| Applying all of them | 226–227 ms | 2.42–2.45 s |
| Snapshot file | 24.98 MB (249 bytes a credential) | 251.8 MB (251 bytes) |
| Capture, on the consensus loop | 8–9 ms | 171–174 ms |
| File written, off the loop | 113–115 ms | 1.22 s |
| The builder's heap peak over what it holds | +6–20 MB | +388–389 MB |
| A follower's staging | 110 ms | 1.27–1.29 s |
| A follower's heap peak over what it holds | +30 MB | +598–625 MB |

The two runs at each size agree within 5% on every figure but the builder's peak at 100,000 (6 against 20 MB, GC
timing at that size). The table in docs/security-and-access.md, ["The in-broker database (`auth.db`)"](../security-and-access.md#the-in-broker-database-authdb) gives
the upper end of each range: 33.3 MB and 405 MB held, 25 and 252 MB
of snapshot, 30 and 625 MB more on a follower, 9 and 174 ms of capture.

## What this does not show

- The derivation and benchmark figures are of one host each, under whatever else the host was doing (the `ai2`
  figure was taken right after a batch; the Argon2id benchmark started at a load average of 6.3).
- The derivation program measures the key derivation alone, not a CONNECT through the broker.
- The `auth.db` figures are of credentials alone, with no leases or sessions beside them, two runs a size.
