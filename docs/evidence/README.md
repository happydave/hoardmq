# Evidence for the measured claims

The largest client load measured so far is 100,000 clients on one host, against an in-process cluster of three nodes,
and 250,000 clients in the memory sweep, with the three nodes on one host and the clients on another. The checkpoint
and credential-store measurements reach 1,000,000 sessions or credentials, in one process with no clients connected.
Measurements of 1,000,000 or more sessions across several hosts are not yet published.

Each page here summarises runs kept privately, in the project's private repository and its work records. The run files
name the hosts and network they ran on and run to hundreds of megabytes, so they are not published; a page gives what
a reader needs to judge the claim instead: the claim it backs, what was run, the host, the date, each build by its
date and what it changed, how many runs and their spread, the results, and what the runs do not show. The private
history's commit identifiers do not exist in this repository, so builds are not named by them.

## The pages

| Page | What it backs |
|---|---|
| [connect-path.md](connect-path.md) | A node lost at 100,000 clients: how fast the survivors take its clients back, with and without a connect-rate limit |
| [overload.md](overload.md) | The stress level and the flapping detector in a 100,000-client failover |
| [forward-order.md](forward-order.md) | Forwards between nodes leave in sequence order: no gap and no late frame in two failovers |
| [memory-250k.md](memory-250k.md) | The fall in the host's available memory at 250,000 clients, behind the chart's connection bound |
| [rolling-restart-in-process.md](rolling-restart-in-process.md) | Rolling restarts in the in-process harness at 10,000 and 100,000 clients: drain pace, refusals, retained publishes |
| [rolling-restart-kind.md](rolling-restart-kind.md) | The chart's rolling-restart runbook on kind: refusals of 600 publishes, delivery, preStop hooks, readiness |
| [checkpoint-scale.md](checkpoint-scale.md) | A checkpoint's time, heap and size, and the restart, at 100,000 and 1,000,000 sessions |
| [inflight.md](inflight.md) | What a client that stops acknowledging holds, by in-flight bound, and the rate a fast client keeps |
| [auth-cost.md](auth-cost.md) | PBKDF2 and Argon2id verification cost, and the `auth.db` store at 100,000 and 1,000,000 credentials |
| [qos2-latency.md](qos2-latency.md) | The QoS 2 exchange's latency on a three-node cluster, before and after two durability changes |
| [restart-times.md](restart-times.md) | How long a node refuses publishes while it does not know an owner's routes |
| [admin-client-list.md](admin-client-list.md) | How long a client-list page holds the lease table's read lock, from a probe of the design |
| [wal-group.md](wal-group.md) | The group commit's default delay of 0, from two 100,000-client failovers |

## Terms

- **In-process:** three broker nodes in one test process (the `sim` package and the packages' own tests), clients
  connecting over loopback unless a page says otherwise.
- **kind:** the Helm chart's three-pod StatefulSet on a one-node kind cluster, driven by the chart's rolling-restart
  runbook.
- **Product timings:** the cluster timings the broker ships (its lease ttl of 30 s among them), not the harness's
  own set, which is much tighter (a lease ttl of 2 s against 30 s) so that short scenarios run fast.
- **The probe:** the harness's QoS 1 publish probe, a small set of publisher and subscriber pairs (and, in the
  rolling restarts, shared-subscription groups) that keeps publishing through a failover or a roll and records what
  the broker answered and whether what it acknowledged arrived.

## The hosts

Every measurement ran on one of these hosts. Each page names its host again.

| Host | CPU | RAM | Disk | Kernel |
|---|---|---|---|---|
| `ai` | AMD Ryzen 9 7950X3D (16 cores, 32 threads) | 93 GiB | NVMe SSD (Samsung 990 PRO 4 TB) | Linux 6.8.0 (6.8.0-139 from 2026-09-08 to 2026-10-03, 6.8.0-146 after; not recorded before 2026-09-08) |
| `ai2` | AMD Ryzen 9 9950X3D (16 cores, 32 threads) | 91 GiB | NVMe SSD (Samsung 9100 PRO 4 TB) | Linux 7.0.0-28 |
| `gtr` | AMD Ryzen AI Max+ 395 (16 cores, 32 threads) | 124 GiB | NVMe SSD (Crucial P310 2 TB) | Linux 6.17.0-29 |
