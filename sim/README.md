# Simulation harness

`sim` runs real HoardMQ nodes in one process, drives a seeded workload of
publishers and durable subscribers through a seeded schedule of faults,
and checks the design's invariants as properties afterwards. It is the
cluster's acceptance gate: a build that fails a seed is not stable.

## Running

| Command | What it does |
|---|---|
| `go test -timeout 30m ./sim` | the fixed corpus (`corpus.go`), the fault-free baseline, the negative controls and a 1,000-client failover. **The `-timeout` is not optional here either**: the gate itself now runs for about ten minutes, which is Go's default, so without it the package panics or passes by luck — it panicked on 2026-09-22 |
| `HOARDMQ_SIM_SEED=n go test ./sim -run TestCorpus` | one seed; the schedule is printed. Add `-count=8` or more when a seed is suspected of being intermittent: a seed's rate is the evidence, one run is not (seed 2 was red about half the time while a single full run recorded green). After any change to the takeover or replication paths, run the seeds with `-count`, not once — and when a seed is intermittent, record its rate (failures per runs) in the work item's `test.md`, not just the last run's colour |
| `HOARDMQ_SIM_RANDOM=k go test ./sim -run TestCorpus` | k random seeds, each printed before it runs (the nightly mode) |
| `HOARDMQ_SIM_REDUCED=1 go test ./sim` | the first two seeds only |
| `go test ./sim -run TestRestartNeedsSnapshot` | the fixed scenario that crashes a node long enough to need a snapshot to rejoin (WI 1291's case) |
| `go test ./sim -run 'TestSkewed|TestLeaderChangeToASkewed'` | the three clock scenarios: followers, the leader, and a leader change to a node whose wall clock is stepped ahead three ttls at a time, five times, at a stride that walks the step across the adoption tick (a single step always fell just after the leader's tick and passed on a defective build). No lease of a node that stayed up may move and none of its clients may lose its connection |
| `HOARDMQ_SIM_REDUCED=1 go test -race -timeout 40m ./...` | the whole-module race run. Not a gate: only when a work item that tracks a race needs the whole module (`docs/dependency-policy.md`); the reduced corpus keeps `sim` inside ten minutes |
| `HOARDMQ_SIM_FAILOVER=100000 go test ./sim -run TestFailover -timeout 1h` | the failover measurement **at the product's timings**. Its report goes to the artifact directory when `HOARDMQ_SIM_ARTIFACT` is set, and only there, so the checkout stays clean; without one it is written to `results/failover-<date>.md` (WI 1483). Timings: every timing the nodes read is the broker's own default — heartbeat 1 s × 3 misses, raft tick 100 ms, lease ttl 30 s, takeover timeout 5 s (also the routing plane's move grace), replication ack timeout 2 s, join timeout 30 s, connect timeout 10 s, readiness stable 5 s, drain 100 clients/s with a 30 s timeout and a 30 s confirmation. A recorded run defaults to the product's heartbeat, misses, tick and ttl since 2026-09-14 (WI 1475), and to the rest since 2026-09-24 (WI 1805); before that every run's takeover and ack timeouts were 1 s. The report and provenance name the set as `product` and record each value. A cold start waits out the 30 s join timeout before the designated node bootstraps, so the run's start is about half a minute longer than the harness set's. **The `-timeout` is not optional**: the storm window is ten minutes and Go's default test timeout is ten minutes, so without it the run panics at the moment the window closes and the report is never written. It cost WI 1292 a hundred-thousand-client run |
| `HOARDMQ_SIM_REPEAT=N HOARDMQ_SIM_FAILOVER=<clients> HOARDMQ_SIM_ARTIFACT=<abs dir> go test ./sim -run TestFailoverRepeat -timeout 3h` | the failover measurement N times, reporting the distribution and writing `repeat-summary.json`. **One run cannot characterise a build**: rounds 13–16 of 2026-09-07 were the same commit and gave 1m39.804s, 5m36.225s and twice not-reached — a spread covering every outcome this project had read as a difference between builds. Each run is its own process with its own artifact, ended once its `complete` marker appears rather than when the process exits (the teardown of 100k clients outlives the measurement), and the driver waits between runs for the machine to settle on the measurement's own starting conditions rather than on a sleep: the socket count always, and for a remote-client batch the broker host's connection-tracking table as well, which holds the previous run's entries for about two minutes (WI 1496). Runs that did not reach are counted separately and never folded into the median. Because a run is ended by signal, `t.TempDir`'s cleanup never runs; the measurement publishes its store root as `store_dir` in `run.json` and the driver removes it between runs, refusing any path that is not under the process's temporary root (WI 1377) |
| `HOARDMQ_SIM_ARTIFACT=<dir> …` | with any run: writes a per-run evidence directory (see Run artifacts below). Off by default; the gate's behaviour and budget are unchanged without it |
| `HOARDMQ_SIM_LOG=<file> …` | with any run: the nodes' debug logs, all three interleaved, in one file. **Give it an absolute path.** The file is opened by the test binary, whose working directory is this package, so a relative path resolves under `sim/` — and a missing directory fails the run in about 7 ms, which a loop that greps the output for `VIOLATION` reads as a clean pass. That mistake cost a WI 1351 measurement twelve runs that had never started |
| `HOARDMQ_SIM_NODELOG=1 …` | with `HOARDMQ_SIM_ARTIFACT`, the failover measurement keeps its nodes' debug logs in the artifact (`nodes.log`). Off by default: at a hundred thousand clients the log is enormous, and the artifact's bounded per-node metrics snapshots are the default answer to "what were the nodes doing" |
| `HOARDMQ_SIM_TIMINGS=harness …` | with a recorded failover run: the **stress variant**, the harness's timings: heartbeat 100 ms × 3, raft tick 10 ms, lease ttl 2 s, takeover and ack timeouts 1 s, join timeout 1 s, connect timeout 5 s, readiness stable 0, drain 1,000 clients/s with a 3 s timeout and a confirmation of the ttl (at least 1 s). Labelled `harness (stress)` in the report and provenance. It applies to recorded runs only; the gate in `go test ./sim` and the corpus always use the harness's timings, so their duration is unchanged. Every repeat batch before 2026-09-14's product batch ran this set; see [results/README.md](results/README.md). Shards are not a timing and differ in both sets: 16, where the product's default is 64 |
| `HOARDMQ_SIM_HEARTBEAT_MS=<ms>`, `HOARDMQ_SIM_HEARTBEAT_MISSES=<n>`, `HOARDMQ_SIM_RAFT_TICK_MS=<ms>`, `HOARDMQ_SIM_TAKEOVER_TIMEOUT_MS=<ms>`, `HOARDMQ_SIM_ACK_TIMEOUT_MS=<ms>` … | override one timing of whichever set is in use; the label then reads, for example, `product, overridden: heartbeat 500 ms`, and `product, overridden: takeover 1000 ms, ack 1000 ms` reproduces the takeover and ack timeouts every run before WI 1805 ran. A 100 ms tick needs a ttl of at least three election timeouts (3 s), so the harness set with the product's tick fails configuration validation, and an ack timeout above the takeover timeout fails it too |
| `HOARDMQ_SIM_LEASE_TTL_MS=<ms> …` | the failover measurement's lease ttl, overriding the set's (30 s at product timings, 2 s at harness), for measuring how the liveness margin behaves under a storm |
| `HOARDMQ_SIM_WAL_GROUP_DELAY_MS=<ms> …` | with any run: the journal's group-commit hold (`node.walGroupMaxDelayMs`) for the nodes, for comparing holds in a measurement; unset = the product default (0) |
| `HOARDMQ_SIM_CLIENT_COMMAND=<command> HOARDMQ_SIM_NODE_ADDRS=<ip:port,ip:port,ip:port> HOARDMQ_SIM_CLIENT_SOURCES=<ip,…> go test ./sim -run TestFailover …` | the failover measurement with its clients in a **separate driver process** (`sim/cmd/simclients`), usually on another host (WI 1480). See **Clients on another host** below. `HOARDMQ_SIM_CLIENT_BYTES=<n>` adds the driver's measured memory per client to its starting conditions |
| `HOARDMQ_SIM_HOLD_SEC=<s> …` | with the failover measurement: H, the interval a recovered population must hold for before the instant counts. Default 60 s for a recorded run, 2 s for the 1,000-client gate. See **What "recovered" counts** below |
| `HOARDMQ_SIM_CRASH_AT_SEC=<s> …` | with the failover measurement: hold the crash until that many seconds after the run's start, and refuse a run that reaches the crash later. The survivors declare the crashed node dead on a lease tick whose schedule is set when their nodes start, so two builds whose fills take different times crash at different points of it and recover at different times for that alone; set it, past the slowest arm's crash point, whenever arms are compared (WI 2032). `crash_after_sec` in `run.json` shows where the crash fell |
| `go run ./sim/cmd/failoverbounds <run.json or folder> …` | judges failover runs against the pass bounds and prints the set's verdict, each run's, when WI 1625's forget fell against the instant, each run's `drop_timeline`, when each survivor's adoptions finished and its served fact was committed against the forget and the instant, with the served proposals, and where refusals after the instant came from. See **The pass bounds** below |
| `HOARDMQ_SIM_PROBE=<n> …` | with the failover measurement: publisher/subscriber pairs publishing QoS 1 through the crash and the hold (default 100; 0 disables). See **The publish probe** below |
| `HOARDMQ_SIM_ROLL=<clients> HOARDMQ_SIM_ARTIFACT=<abs dir> go test ./sim -run '^TestRollingRestart$' -timeout 3h` | the rolling-restart measurement at the product's timings: every node drained, stopped and restarted in turn with the probe running (WI 2208). See **Rolling restart** below. `sim/results/roll/driver.sh` runs a batch. `HOARDMQ_SIM_ROLL_DRAIN_LIMIT_SEC` bounds the wait for one drain (default 1,800 s); `HOARDMQ_SIM_NODELOG=1` writes the nodes' debug log to the run's directory as `nodes.log` (about 47 MB at 10,000 clients); `HOARDMQ_SIM_ROLL_CUT_SEC=<s>` cuts a drain still running that many seconds after it began, as the kubelet does at a pod's termination grace: the node is shut down within 2 s (SIGTERM, then SIGKILL), and restarted (WI 2251); `HOARDMQ_SIM_ROLL_RETAINED=<topics>` adds the retained probe (WI 2207), off unless set |
| `HOARDMQ_SIM_DRAIN_RATE=<n> …` | with any cluster: every node's `cluster.drain.ratePerSecond`, the least clients a drain redirects a second (the product's default is 100; a drain goes faster where that is what finishes its redirect by its deadline, WI 2251); recorded as `drain_rate` in `roll.json` (WI 2251) |
| `HOARDMQ_SIM_PROBE_GROUPS=<n> …` | with the probe: shared subscription groups beside the pairs, alternately QoS 1 and 2, each with members on every node (default 6; 0 disables). See **The probe's shared groups** below |
| `HOARDMQ_SIM_VICTIM=before-leader\|before-follower …` | with the failover measurement: crash a follower chosen so its clients reconnect first to the leader, or to the other follower, with no election (WI 1493). Unset or `most-clients` crashes the node with the most clients, which at even counts is `n1`, normally the leader. Every run records `leader_before_crash`, `leader_after`, `storm_target` and a once-a-second `online_series` in `run.json`, and the report's Leader placement section names them |
| `HOARDMQ_SIM_PROFILE_AT_SEC=<s,s,…> …` | with the failover measurement and an artifact directory: goroutine, mutex and block profiles tagged `at<s>` that many seconds after the crash. The mutex and block profiles are cumulative from process start, so compare two with `go tool pprof -diff_base`; the block profile is where goroutines waited on channels, conditions and selects, which a mutex profile does not show |
| `HOARDMQ_SIM_AUTH=jwt\|ldap\|file …` | with any cluster: how the clients authenticate. Unset is anonymous. `jwt`: every client connects with an access token the nodes verify against an in-process JWKS. `ldap`: every client connects with a password the nodes bind with to an in-process directory. `file`: the population's clients (`f000000` upwards, as many as the run has) connect with passwords the nodes verify against a users file of PBKDF2 hashes at the product's 600,000 iterations, made once on every CPU before the fill (about 20 s for 10,000 clients on a 32-thread host), while probes and every other client connect anonymously beside them. `run.json`'s provenance records the mode (`auth`) |
| `HOARDMQ_SIM_CONNECT_RATE=<rate>[,<burst>] …` | with any cluster: every node's client listener's `connectRate` and `connectBurst` (the product's default is no limit); recorded as `connect_rate` and `connect_burst` in the provenance (WI 1283) |
| `HOARDMQ_SIM_AUTH_CACHE_TTL=<s> …` | with any cluster: `auth.cache.ttlSeconds` on every node; 0 turns the password-verification cache off. Recorded as `auth_cache_ttl_sec`. `storm.<node>.password_cache` in `run.json` says what the cache did (derivations, hits, entries) and `storm.<node>.connect_rate_refused` what a connect-rate limit refused |
| `HOARDMQ_SIM_DUMP=<prefix> …` | with any run: `SIGUSR1` writes the full goroutine profile to `<prefix>-<time>.txt` and the run continues; the completion line on stderr says how long the stop-the-world took, which at a hundred thousand connections is over a minute and perturbs the measurement — take the dump from a run whose numbers you are not keeping. `pprof` caps the profile at 64 MiB. That cost belongs to this *text* dump (`debug=2`), which is per-goroutine and unaggregated and remains the only source of per-goroutine wait durations; for "which stacks is everyone piled on", the artifact's `debug=0` profiles answer it with a bounded pause — largely superseding hand-run dumps |

Every sim binary runs with contention profiling on
(`SetMutexProfileFraction(100)`, `SetBlockProfileRate(10000)`) — rates the
WI 1329 research found negligible even under extreme contention. The
product does not import this package, so nothing leaks into it.

Each run's artifact directory ends with a `complete` marker, written after
every other file. It is the only signal that a measurement has finished: a
watcher that infers completion from the last file it happens to know about is
guessing, and the throwaway driver that did so carried a race where a run
starting before its own directory existed would be killed on sight of its
predecessor's report.

## Provenance

A failover measurement records how it was produced, in its `run.json` under `provenance` and at the
end of its `report.md`: the command line, every `HOARDMQ_SIM_*` variable that was set plus `TMPDIR`,
`GOTMPDIR`, `GOMAXPROCS`, `GOGC`, `GOMEMLIMIT` and `GODEBUG`, the effective lease ttl, heartbeat,
misses, raft tick and group-commit hold the nodes were built with, the host, the Go version, and the
source tree's commit with whether tracked files were modified. The tree is read again when the
artifact is written, and a change during the run is reported. Only those named variables are
recorded, because results are committed. A value the harness could not read is stated as unavailable
with the reason, never filled in. The repeat driver records its own invocation the same way in
`repeat-summary.json`.

So a script that only launches a measurement is scaffolding, and a result is published with its
`run.json` or its report, not with the script. This refines WI 1374's note that the throwaway batch
scripts were a sketch rather than reversing it: before this record existed, one of those scripts was
the only statement of how rounds 14–16 ran. Rounds published before 2026-09-14, their surviving
drivers and what cannot be traced are in [results/README.md](results/README.md).

A measurement tree deployed to another host must be a git checkout, not a copied directory: a copy
without `.git` records its source tree as unavailable.

## Run artifacts

With `HOARDMQ_SIM_ARTIFACT=<dir>` set, each `sim.Run` writes
`<dir>/<test>-seed<seed>-<time>/` containing `run.json` (seed, schedule,
cluster shape, stats, violations with evidence, the full ledger, owner
samples and probes, a runtime-metrics delta including mutex-wait-per-op,
and a coverage block saying what the run actually exercised — a vacuous
run is visible as vacuous), `timeline.html` (a self-contained static page:
the run's data is embedded, no server, no build step — faults, per-client
connection events and deliveries on one time axis, busiest 40 clients
shown), and end-of-run `goroutine`/`mutex`/`block` profiles (`.pb.gz`, `debug=0`).
Read profiles with `go tool pprof -top <file>`; diff two runs with
`pprof -diff_base`.

The failover measurement writes its own artifact: `report.md`, a
`run.json` of phases with runtime deltas and the lease counters (and
`store_dir`, the root the nodes' stores are under, for a driver that ends
the run before the test's own cleanup), profile
snapshots at end-of-fill, **mid-storm** (when reconnects stop making
progress for 30 s, or 2 minutes after the crash, whichever first), and
window end, plus flight-recorder execution-trace windows (`trace-*.out`,
`go tool trace`, each covering up to 45 s): `trace-fence.out` when the
first survivor fences, `trace-storm.out` at the stall, and
`trace-end.out` only if neither was taken. The mid-storm mutex profile is
the first thing to read after a bad run: it ranks locks by waiters × wait
and names the convoy directly.

Three more pieces of evidence come from the crash onward (WI 1474).

- **`cpu-storm.pb.gz`** is a CPU profile of the storm's first minute, or of
  the window if that is shorter. Every node's goroutines carry the pprof
  labels `sim=node` and `node=<name>`, and every simulated client's carry
  `sim=client`, so `go tool pprof -tagfocus=sim=client` against
  `-tagfocus=sim=node` says how much of the CPU is the harness's own
  clients; `go tool pprof -tags` prints the split. Unlabelled samples, which
  `-tagignore=sim=.` shows, are mostly the garbage collector's mark workers,
  plus broker work run from timer callbacks (`time.AfterFunc` goroutines do
  not inherit labels). Read those broker frames as node CPU. In a
  20,000-client run on `ai`, nodes carried 66% of samples, clients 2.5%, GC
  20%, and unlabelled broker frames about 7%.
- **`peer_samples[].sockets` is capped** at 2,000 rows a sample, with `sockets_dropped` recording what the cap
  discarded. The filter asks the kernel for the cluster's own connections, which is single digits per node, so the
  cap only fires when something is wrong — and once it did: a drawn cluster port collided with the remote mode's
  fixed client listener port, the filter matched every client connection, and one 250,000-client run wrote 6,421,763
  rows into a 937 MB artifact while spending 286 ms a second reading them. The collision itself is prevented now
  (listener ports are kept out of the draw); the cap is what makes the next one cheap (WI 1511).
- **`peer_samples`** in `run.json` is one sample a second. Each sample
  holds every survivor's view of its peers: link state, read idle, and the
  frames queued for each connection's writer. It also holds `ss`'s receive
  and send queues for every socket on a cluster port. Each read carries its
  own duration, so a read that waited on a lock shows as a long read, not
  as a quiet link.
- **Each survivor's `storm.lease`** now carries `fence_records` and two
  fence counters. A fence record holds the inputs of a fence or of the
  first deferral in a streak: whether a settled leader or an unreachable
  quorum allowed it, each peer's reachability, the record's age, and the
  lease tick loop's own gap and core-status read time. The counters are
  `fences_leader_settled` and `fences_no_quorum`. Also carried are
  `loop_gap_max_ms` and `status_read_max_ms`.
- **Each survivor's `storm.overload`** is what its stress controller saw
  (WI 1314): the node and cluster level at the end, the highest of each
  and when (`max_node_level`, `max_cluster_level`), the indicators' peaks
  (`peak_indicators`: queued messages, replication backlog, live heap) and
  the samples taken. Beside it, `overload_refused`, `bans_met`,
  `flapping_trips` and `flapping_bans` count the CONNECTs refused for
  overload and for a ban and the flapping detector's trips and bans; the
  metrics record adds connections ended by how and deliveries deferred.

`storm.read_ms` times each part of the survivors' stats read. The lease
liveness, consensus status and peer reads take locks a storm can hold,
and a WI 1470 progress line's figures arrived about 25 s after its
timestamp.

### Was it this process, or the peer?

Two cluster connections falling silent at the same instant cannot be the
peer's doing, and per-connection figures cannot tell a peer's silence from
this process being stopped. WI 1340 round 8 ended on that ambiguity for a
5.8 s silence, and the answer turned out to be *in* its artifact and unread.
The failover measurement now answers it directly:

- A **sampler** sleeps 50 ms and measures how long the sleep took. The
  excess is how long this process was not running, whatever the reason —
  stop-the-world, scheduler starvation under a lock convoy, CPU steal on a
  shared host, swap. It reports how long and never why: at six seconds all
  four are consistent, and choosing one in an artifact is a conclusion
  nobody checked. The **host figures** beside it now say why (WI 1375): each
  interval carries Linux pressure stall information for cpu, io and memory
  (`some` — any task waiting — and `full` — every runnable task waiting, the
  figure directly comparable with the stall), the data directory's device
  utilisation, queue depth and write rate with the device resolved rather
  than assumed, this process's own write rate, iowait, steal, and the dirty
  and writeback levels. A stall beside io pressure is a disk wait; beside cpu
  pressure and no io it is contention; beside neither it is a finding. Each
  group is reported as unavailable, never as zero, when the host does not
  expose it, and a counter that goes backwards makes its group unavailable
  rather than an unsigned underflow. `cpu full` is structurally zero — one
  task is always running — so `cpu some` is the figure to read. Excesses
  over **250 ms** are kept as timestamped events in
  `run.json` (`stalls`), bounded at 256 with the overflow counted
  (`stalls_dropped`) so a truncated list cannot read as a quiet run.
- The runtime's own histograms — `/sched/pauses/total/gc:seconds` and
  `/sched/latencies:seconds` — are **subtracted per interval** rather than
  read cumulatively. A cumulative quantile answers "somewhere in this run",
  which is exactly what made round 8's 21.5 s scheduler latency
  uninterpretable. `run.json`'s `intervals` carries each interval's own GC
  pause total and worst scheduler latency, with `measured` distinguishing
  "nothing happened" from "could not be subtracted", and a `host` block with
  the interval's pressure, disk, process and memory figures, each group
  carrying its own availability flag (`psi_ok`, `disk_ok`, `stat_ok`,
  `mem_ok`, `self_ok`). The cumulative
  per-phase figures are still there, so older artifacts remain comparable.
- `environment` records `GOMAXPROCS`, `NumCPU` and the load average, without
  which a starvation figure means nothing, and `crash_after_sec` places the
  crash on the same scale as the intervals and the stalls.

Both approximations are labelled where they are published: the GC total is a
bucket-midpoint sum and the scheduler figure is a bucket's upper bound, so
both are magnitudes rather than measurements.

A measurement's progress prints every 15 s — but `go test` buffers the
binary's output unless run with `-v`, so **run measurements with `-v`**,
or tail `<artifact>/progress.log`, which receives the same lines
regardless of buffering. The line reads
`[sim <phase>] <elapsed> <done>/<total> refusals=… fails=… goroutines=… mutex-wait=<rate>s/s | stall=…ms gc=…ms sched-max=…ms | psi io=<some>/<full>% cpu=…% mem=…% iowait=…% steal=…% <device> util=…% qd=… w=…MB/s self-w=…MB/s dirty=…MB wb=…MB`;
the three figures after the first bar are that interval's own — how long the
process was not running at worst, and the runtime's account of why over the
same interval — and the host figures after the second bar are what the
machine was doing meanwhile (a group the host does not expose reads
`psi=unavailable` or `disk=unavailable`, and `host=unavailable` when none
could be read), so a line showing two connections idle for six seconds
beside `stall=6000ms` is a whole-process pause and the same line beside
`stall=2ms` is not, without opening a second file;
the mutex-wait rate is the whole process's contention accumulation per
second, and a jump there is the storm signature (a 6,000-client local run
went 281 → 2,238 s/s when the crash landed). In the reconnect window the
line continues with the survivors' admission picture, read without any
product lock: `| handshaking=<in flight>/<bound> paused=<accept pause so far>
connack=<accepted>/<handshakes ended> prebuffered=<CONNECTs already in the socket at accept>
abandoned=<CONNECTs refused because the client had already closed>
queued=<lease work keys> coalesced=…
fsyncs=<group commits> ops/fsync=<records per fsync> syncwait=<total caller wait>
lease: fences=… adopted=… lost=… alivefail=… alivemax=<slowest renewal>
renew=<ok>/<attempts> skipped=<launches over the in-flight bound> fallback=<renewals on the ordered lane>
streak=<longest current failure streak> fail: timeout=… noleader=… lane=… other=<failed renewals by cause>
takeovers: older=<requests whose live connection is older> stale=<of those, prebuffered>
raft: <node>=<role>/t<term>/L<leader>/lag<commit−applied>/ctl:up<peers>/idle<ms>/hmax<ms>@<class>/data:up<peers>/idle<ms>/qwait<ms>`
— per survivor, the consensus core's view and, for each of the two cluster connections
(WI 1340), how many peers are up, how long the reader has gone without a frame, the
worst handler time and its class, and the longest a frame waited in a dispatch queue.
The lease figures are the liveness timeline: a survivor that fences or whose
leases get adopted while it is healthy is the false-death cascade the round-4
100k run showed; the renewal figures say whether the urgent lane is carrying
the signal (WI 1337); `prebuffered` and the takeover figures are WI 1338's
attribution of backlog-delayed sockets taking over live sessions, and
`abandoned` counts the CONNECTs refused before their lease was acquired
because the client had already given up and closed (WI 1485).
The failover `run.json` carries the same per survivor (`storm`) plus the
kernel's `TcpExt` deltas over the run (`netstat_delta`: `ListenOverflows`,
`ListenDrops`, …) — under pause-accept (`listener.maxHandshakes`, default
512) those counters are the truth about the queue the broker deliberately
lets build in the kernel. Each survivor's `storm` block also times its
replica fetches stage by stage (`replication_fetch`: as the node that asked,
and as the holder that answered — queue wait, find, reply, pool or worker),
and `metrics_before` / `metrics_after` hold a bounded snapshot of a few metric
families from just before the crash and from the end of the watch (lease
operations proposed by kind, hidden route owners, declarations, fetch
outcomes, forwards sent and their failures by reason, late frames and forward
stream restarts; the list is `sim/stormmetrics.go`). A survivor whose snapshot timed out is named in
`metrics_timed_out_before` / `_after` and absent from the map: absent is not
zero. `storm_at_crash` is the `storm` block read just before the crash: the counters in it run from process
start, so the storm's own share is the difference, which `results/analysis/stormshare.py` prints (pause,
handshakes and their slot time, accepted and refused). `results/analysis/fetchstages.py` reads one set of runs and
`fetcharms.py` compares arms. `unansweredarms.py` compares arms on what became of a session fetch nobody
answered — by who made it (a CONNECT, the adoption worker, a takeover), beside the worker's retries and
give-ups and the takeover acknowledgements that outlived their pull. `refusalarms.py` compares arms on the
acquires refused because the owner is alive and unreachable — proposed and refused at apply, or refused locally —
beside the handshake slot time and the pause, from the crash on. `movegrace.py` prints the move grace's share of the
routing plane's refusals (`storm.<node>.routing_refusals`), over the whole run and after the recovery instant, with
the takeover timeout each run recorded (WI 1805). `movepath.py`, over a run directory with its `nodes.log`, names
the path each move-grace refusal after the instant fell on (made on the old node, forwarded from the new owner, from a
third node, or a stale route) and whether the old node had acted on the new owner's confirmation yet (WI 1834). `fetcharms.py`, `unansweredarms.py` and `refusalarms.py` group runs by **placement**
and compare arms within each as well as overall. The crashed node's clients all retry first on one node, the
storm target (`storm_target` in `run.json`); when it wins the election (`leader_after`) they land on the adopter
of their leases, and when the other survivor wins they land beside it and take every lease over from it. The
election decides which, not the harness, and the two kinds of run differ by seconds of recovery and by thousands
of takeovers, so arms that drew different mixes are said to have. `placementmix.py --need K` tells a batch driver
whether every arm has K runs of each kind; the balancing driver is beside the ai2 batch runbook in the tickets
repo.

There is no CI system in this repository yet; the gate is `go test ./sim`
in the build sequence documented in the repository README.

## Rolling restart

`TestRollingRestart` measures what a rolling upgrade costs since WI 1788, which takes a draining node out of the
shard map before it stops (WI 2208). It fills the population as the failover measurement does, starts the probe
(pairs and groups), then takes each node in the order a StatefulSet does, highest name first: it drains the node
(`Server.Drain`, waiting for `Drained`), stops it, restarts it over its data, and moves on once the node reports ready.
After the last step the probe runs for the hold and is read with the roll's end as its instant, so its refusals
"after the recovery instant" are those after the roll.

The gate runs it with 300 clients at the harness's timings, holding each step for three seconds so the probe
publishes in each, and checks the arm's structure: three steps in order, every drain complete, a map without the
drained node seen in its step, every node ready again, the probe publishing in every step, the population back.

`roll.json` holds, per step and in seconds after the first drain began:
- the drain's start, each phase's first sample (every 50 ms), its end, its final status (sessions confirmed and
  unconfirmed, moved copies kept), and the rise of its `hoardmq_drain_*` counters on the drained node;
- `beyond_chart_grace`: the drain outlasted the chart's 75 s termination grace. The harness has no kubelet, so the
  drain is not cut there unless `HOARDMQ_SIM_ROLL_CUT_SEC` asks, and a drain past it is a finding;
- stop, restart and ready times;
- every (version, basis) a running node applied, first seen when (sampled every 100 ms);
- each map change of the step (`changes`), dated by the first node to apply it, with the moved copies the running
  nodes kept until the next change: their peak, how long after the change the last was still kept, and whether
  they were all released before it. From the second step on the first change is the previous node's rejoin, and a
  change whose basis lacks the step's node is its leave;
- each node's confirmations (`confirms`): as an owner, those sent (`sent`), those to a moved session's old targets
  (`sent_moved`), and the jobs given up because the session was no longer held here (`unheld`), its durability wait
  kept failing (`undurable`) or a send failed (`unsent`); as a recipient, those received, those that ended a sealed
  copy or an offline session (`acted`, with the time from receipt to act), and the rest (`ignored`). A moved copy's
  confirmation is among `ignored`, and its release is in `copies`;
- the replica copies dropped and kept, by reason, and the checks of an owner's targets, by outcome (`copies`);
- the sessions the nodes served empty, by reason (`served_empty`), the restarted node's included from its restart;
- with a cut, whether the drain was still running at it (`cut`), its status then (`cut_status`: phase, clients and
  leases left, sessions awaiting confirmation) and the leases the node's own table said it still owned
  (`cut_leases`; the status counts none while clients are redirected);
- the probe's outcomes in the step's window (from its drain's start to the next one's), pairs and groups apart, with
  the rate refused or failed;
- the rise of the forward and barrier failure counters over the step, by reason.

With `HOARDMQ_SIM_ROLL_RETAINED=<topics>` (16 in the gate) a retained probe runs beside the pairs (WI 2207): one
publisher per node writes QoS 1 retained messages round-robin over its share of `rprobe/0000`… (the topics cover every
shard), one a second, each topic's sequence counting up. `roll.json`'s `retained` holds what it published, what was
acknowledged and refused (by reason code), its outcomes per step and over the roll, and an end check: after the hold a
fresh client subscribes `rprobe/#`, and a topic served an older message than its last acknowledged one, or none, is
listed in `lost`. The gate requires it to have published and been acknowledged, and every acknowledged topic served.
The retained probe loads the cluster the pairs share: on `main` at 10,000 clients the pairs were refused 23–25% over a
roll with it running (`results/roll2207-10k-2026-10-08/`), against 0.65–0.88% in WI 2252's rolls without it (WI 2265).

`findings` names every drain that did not complete, was cut, ended with sessions unconfirmed, placed other than
`left` or outlasted the chart's grace; a step whose nodes served a session empty (not counting `no-expiry`: the
retained probe's publishers connect with no session expiry, and a drain that redirects one ends its session); a node not ready again; a probe loss or a QoS 2 group duplicate; a session rebuilt
empty; a client not back; and a roll refusal rate above the chart runbook's worst after WI 1788, 23 of 600
(3.83%, `results/leaving-2026-10-05/`). The test fails on none of them.

## What "deterministic" means here

The workload (which clients, which topics, which QoS, the publish rate) and
the schedule (which faults, against which nodes, at which offsets, for how
long) are derived from the seed alone. Re-running a seed re-injects the
same faults at the same offsets against the same workload. Execution rides
the real Go scheduler, network stack and disks, so wall-clock interleavings
differ between runs, and a violation found under a seed is reproduced by
re-running the seed — usually within a few attempts, not guaranteed on the
first. A fully deterministic runtime (simulated clock and scheduler across
the stack) is out of scope; this is stated rather than claimed.

## Faults

Injected at the product's own seams (`broker.Hooks`, `transport.SetFault`,
`Server.Crash`, files on disk):

- **partition**: a node isolated from every other (both directions) for
  at least the lease ttl, with a connect probe against it once fenced;
- **drop / delay / reorder**: a fraction of frames from one node to another
  not written, every frame delayed, adjacent frames swapped — optionally
  restricted to named frame classes, so a scenario can silence one plane
  (replication, say) while heartbeats and lease traffic keep the node in the
  cluster. Heartbeats are never dropped, but a delay long enough to hold
  them past `heartbeatMs × heartbeatMisses` still resets the link, which is
  what a slow link does;
- **crash**: a node stopped as a power loss would (no DISCONNECT, no
  checkpoint) and restarted later, optionally with a torn tail on its state
  log or a corrupted record in it. The corruption flips one bit of the last
  record's stored checksum and leaves the payload alone, so the checksum is
  the only thing that can catch it;
- **drain**: the node's graceful leave (redirects, lease hand-off, the wait
  for its sessions to be confirmed held, bounded by the ttl at harness
  timings), then a restart;
- **skew**: an offset on the node's lease and replication wall clock, up to a
  ttl either way. The node's steady clock — what it measures its own
  elapsed time on — is left alone, as a wrong wall clock leaves a machine's
  monotonic one. Offsets beyond the ttl are the clock scenarios' (above).

At most one node is down at a time (the replica count is two), and every
fault ends before the settle period.

## What a subscription's acknowledgement means

`Client.Subscribe` reads the SUBACK's reason code and returns an error when the
broker refused the subscription. It is worth stating because the opposite —
treating any SUBACK as a grant — makes the properties below vacuous rather than
wrong: a client that believes it is subscribed when it is not receives nothing,
and `no-acknowledged-loss` and `no-double-delivery` cannot fail when nobody is
listening. The corpus would go green *because* the broker refused a subscribe
(WI 1500).

`Client.resubscribe`, which re-issues the subscriptions after a reconnect finds
no session, cannot wait for the acknowledgements: it runs on the connect
goroutine before the reader starts. So the reader inspects every SUBACK and
counts refusals into `Client.SubRefusals`, whoever sent the SUBSCRIBE, and
records a `suback-refused` event with the code. That is the path a mass
reconnect after a crash puts every displaced client through, so it is the one
worth watching.

A refused subscription during the fill counts the client as failed, so a run's
client count reflects clients that are actually subscribed.

`subscribe-granted` is the property that reads those refusals (WI 1509). It
fails on any of them, naming the client, the node, the reason code and the
time, and it **also fails a run in which nothing was granted at all** — a run
that subscribed to nothing has no refusals either, and would otherwise report
a clean bill for a check it never exercised. The two failures are worded
apart, because "nothing subscribed" and "everything was refused" both leave
the granted list empty and send a reader to different places. A corpus run's
statistics carry `subRefusals` and `subRefusedClients` when either is non-zero.

The corpus passes it today, with no exemption: zero refusals across every
seed. It is a guard against a regression rather than a description of a live
defect, as `replication-recovers` below now is too.

### What "recovered" counts in the failover measurement

**A client is recovered when it holds a live connection whose CONNACK was
Success and every filter it holds was granted on that same connection.** The
cluster's recovery instant is the start of the earliest interval of length H
throughout which every still-trying client is continuously recovered. Owner
decision, 2026-09-21, built by WI 1618.

Four things follow from it:

- **A grant belongs to one connection.** A reconnect re-earns every filter,
  either from the CONNACK's Session Present flag — the broker stating it holds
  the session, subscriptions included — or from a fresh SUBACK. A SUBACK is
  matched to its SUBSCRIBE by packet identifier *and* connection, so an answer
  arriving from a connection that has gone cannot grant anything.
- **The hold is part of the measurement, not a check after it.** A population
  that comes back and is then closed moves the instant out to the interval
  that actually held. H is 60 s for a recorded run — the lease ttl is 30 s and
  the routing plane's full sync runs every 30 s, so 60 s covers a lease
  lapsing and the sync that reveals what the lapse changed — 2 s for the
  1,000-client gate, and `HOARDMQ_SIM_HOLD_SEC` overrides both. The interval
  rule itself is tested over synthetic series (`recovered_test.go`), which is
  why the gate does not spend a minute re-exercising it.
- **A run reports an instant only if the recovered count was seen below the
  target after the crash.** A run where nothing dropped has not recovered; it
  reports "never fell". The remote driver required this from WI 1480 and the
  in-process mode did not, which was WI 1698.
- **The old reading is published beside the new one.** `connack_recovery_sec`
  is the first return of the *online* count, with no hold and no
  subscriptions, computed by the same code with H set to zero. The two agree
  whenever sessions survive the failover — a takeover that keeps the session
  answers Session Present true, so the client is recovered at its CONNACK with
  no SUBSCRIBE at all — and diverge exactly where a session was rebuilt empty
  or a subscription was refused.

Counted and reported beside it, and judged against the pass bounds below:
`session_lost` and `session_lost_clients` (a reconnecting client
told Session Present false although it held a session — its queue is gone and
the resubscribe hides the event, which is how thousands a run went unseen
before WI 1653), with `session_lost_sample` naming up to 1,000 of those clients
(in-process populations) and `served_empty` the survivors' own count of the
same event by reason (WI 1575), so each loss can be joined with the node logs
and attributed (`results/analysis/servedempty.py` does the join for runs that
kept `nodes.log`); and `sub_downgraded` (a SUBACK granting a lower QoS than was
asked for; a grant, and a change to what the subscriber is promised).

The shape is reported on the recovered count: `ninety_nine_sec`,
`ninety_nine_nine_sec` and `recovered_series`. Peak, trough and
`online_series` stay on the online count, so every batch published before this
change is still comparable.

### The publish probe

The failover measurement used to publish nothing, so a cluster in which every
client was back and every QoS 1 publish was refused read as recovered: WIs
1624 and 1625 were both invisible to it. `HOARDMQ_SIM_PROBE=<n>` pairs
(default 100, 0 disables) publish one QoS 1 message a second, each to its own
partner's topic, from just before the mark to the end of the hold. `run.json`'s
`probe` records the attempts, what the broker answered — acknowledged, No
Matching Subscribers, refused by reason code, or nothing at all — the last
refusal's time, the same figures again from the recovery instant onwards, and
how many Success-acknowledged messages the partner never received after a 5 s
drain. A storm refuses by design; the question the split asks is whether the
cluster is still refusing once it has recovered.

Because the pairs sit outside the measured population they can still be
recovering after it has recovered, so a refusal to a topic whose subscriber is
not back yet would read as the cluster refusing a recovered topic. The record
separates the two: `refused_with_subscriber_back` counts the refusals after the
instant whose own partner was recovered at that moment, and `subs_recovered`
and `last_sub_recovered_sec` say where the cohort itself got to.

Each lost message is attributed in the record itself (WI 2259): `probe.losses`
names, for up to 50 of them, the node that answered Success and when, and the
pair's connection events (refusals, drops and CONNACKs with Session Present,
with their nodes) and the subscriber's deliveries within 30 s of it;
`no_subscriber_marks` dates and places each No Matching Subscribers answer the
same way. `sim/results/analysis/lossattr.py <artifact dir>` prints them with the
lines of the run's `nodes.log`, when it kept one, that name the pairs' clients
or topics.

The cohort is small and outside the target population on purpose: the headline
must stay comparable with the batches already published, and a probe large
enough to move the measurement would be measuring itself. Its clients run in
the test process in both modes, so in the remote-client mode they are on the
broker host rather than with the population.

### The probe's shared groups

`HOARDMQ_SIM_PROBE_GROUPS=<n>` groups (default 6, 0 disables) run beside the
pairs (WI 1962). Group `k` is `$share/pg<k>/probe/group/<k>`, at QoS 1 when `k`
is even and 2 when odd. It has a member on every node and a second on node
`k mod 3`, which makes that node the group's dispatch owner, and one publisher
on node `k+1 mod 3`. So a publish goes to the owner for its decision, and the
owner's round robin gives about one message in three to the third node, which
is WI 1896's relay. The default six cover each owner node at each QoS once. The
probe waits until every node's routing table shows each group's members where
they connected, and fails to build otherwise, naming what each table showed.

A group message is delivered when one member has it, so `run.json`'s
`probe.groups` counts:
- `acked_not_delivered`: acknowledged and received by no member;
- `qos2_duplicates`: a QoS 2 message received as more than one exchange, on one member or across members (a
  retransmission under the identifier still open is one delivery);
- `qos1_repeated` and `qos1_second_member`: a QoS 1 message received more than once, and by more than one member. They
  judge nothing; the product redistributes a QoS 1 message only once its member's session ends, which the probe's
  durable members never do, so a second member is a finding to file;
- `delivered_unacked`, and deliveries by member node, overall and per group (`per_group`).

No Matching Subscribers is counted as acknowledged for a group, unlike for a
pair: the broker answers it to a QoS 1 publish whose group was decided on
another node while a member holds the message (WI 2247), so leaving those out
would hide their losses. Refusals are split at the recovery instant as the
pairs' are, with `refused_with_members_back` for those whose every member was
recovered. `failoverbounds` prints a report-only **Shared groups** section
with the survivors' group forwards and relays
(`hoardmq_group_gives_relayed_total`). The run injects no disk fault, so WI
1891's refusal of a group delivery whose record failed to append is not reached.

### The pass bounds

A run and a set of runs are judged against the bounds the owner ratified on
2026-09-22 from WI 1618's batch, applied by WI 1734.

| Figure | Bound | Gating |
|---|---|---|
| Time to every client recovered | ≤ 55 s | only for 100,000 in-process clients on ai2 at product timings, H = 60 s, with the run's placement known; any other run reports the figure and says which of those it is not. A run labelled `product` that records no takeover timeout was recorded before WI 1805, when its takeover and ack timeouts were 1 s: it is judged as before, with a note saying so |
| Subscriptions granted below the QoS asked for (`sub_downgraded`) | 0 | yes |
| Probe messages acknowledged and never delivered (`probe.acked_not_delivered`, plus `probe.groups.acked_not_delivered`) | 0 | yes; a run whose probe did not run or published nothing has no figure and fails the row |
| Sessions rebuilt empty on a reconnect (`session_lost`) | 0 | yes |
| QoS 1 refusals after the recovery instant (`probe.refused_after_recovery`, plus the groups', whose QoS 2 publishes count too) | 0 | yes, since WI 1706 closed (2026-09-24); a run whose probe did not run or published nothing has no figure and fails the row |
| Time to 99% and 99.9% recovered | — | recorded, never gating |

The verdict is in `run.json` under `bounds` (a row per figure with its bound,
figure, status and note, and the failures by name), in a **Pass bounds** section
of `report.md`, and on each run and the batch in `repeat-summary.json`. The
test's own status does not change with it: a bound that fails is a finding the
artifact states, not a harness error. A set of runs — a result folder — is
judged by

```
go run ./sim/cmd/failoverbounds <run.json or folder> ...
```

which prints the set's rows, each run's verdict and failures, when WI 1625's
forget fell against the instant, and the two report-only sections below, as
Markdown, and exits 0 when the set passes, 1 when it fails and 2
when it found nothing to read. A set passes the time row only when every run it
judged passed and they include at least three of each placement (see
`results/analysis/fetcharms.py`); a run over the bound fails the set whatever the
mix. In-process clients and a 60 s hold are part of the configuration because
the bound was measured with them.

**Counters at the recovery instant.** `metrics_before` and `metrics_after`
bracket the whole run, so a refusal counted between them cannot be dated
against the instant. When the recovered count begins a hold, the run takes one
metrics snapshot of each survivor on its own goroutine — a snapshot walks the
session table under its lock, so it is never a series and never on the sampling
loop — and takes another if that hold breaks and a new one begins. The one taken
for the hold that confirmed the instant is `metrics_at_instant`, and
`metrics_at_instant_timing` says when each read began after the instant and how
long it took, how many snapshots the run began, and whether it had to be taken
after watching ended. What rose after the instant is `metrics_after` minus it;
the report lists the forward and barrier failures that did, per survivor. The
snapshot includes `hoardmq_route_owners_forgotten_total`, which dates WI 1625's
forget against the instant: *before* when the snapshot already holds the
survivor's final count, *after* when it does not. The counter is read at the end
of the snapshot, so *before* is exact to the snapshot's read time, which is
printed beside it.

**From the crash to the shard map dropping the crashed node** (WI 1791). Each
run records `drop_timeline` in `run.json`, in seconds after the harness's own
crash mark:
- each survivor's transport seeing the victim down;
- the post-crash leader's last applied alive record from the victim;
- the leader's declaration proposal, and each survivor's apply of it;
- the first map whose basis leaves the victim out: its version, why the
  leader's driver checked (`tick`, `retry` or `declaration`), when it decided,
  and each survivor's apply and reconcile;
- each survivor's own adoptions up to the victim's emptied index seen finished
  (`adopted_sec`), and its served fact committed (`proposed_sec`) (WI 1633);
- the last commit of each survivor's cover before the forget (`cover_sec`), for a
  survivor that took sessions in transit (WI 1890); one that took none has no
  cover and is not missing;
- each survivor's forget of the victim's routes, and the reason that held it
  last;
- the instant.

The survivors' lease, replication and routing stats carry the timelines it is
read from (`storm.<node>.lease.timeline`, `replication_maps`, `routing_forgets`).
A step that could not be read is listed in `missing` and never written as zero.
`failoverbounds` prints it per run, with whether the map had applied on every
survivor before the instant. It also prints the refusals' sources after the
instant: `metrics_after` minus `metrics_at_instant` for the barrier's failures
on the crashed node's stream and on a survivor's, each by reason, and for
forward failures. Both sections are report-only.

### What "back online" counted before WI 1618

The failover measurement has no corpus and never calls `Check`, so it carries
the same two figures in `run.json` as `sub_refusals` and
`sub_refused_clients`, in `report.md` as a row, and in `repeat-summary.json`
as a batch total with `sub_refused_runs`. In the remote-client mode they are
computed in the driver process and cross the socket with the rest of the
summary.

They are there because **the recovery figure counts a CONNACK, not a working
subscription**. A client whose resubscribe was refused is online, is counted
back, and receives nothing. `sub_refused_clients` counts clients refused at
least once *since the window opened* — a client refused during the fill is not
counted again as one the storm refused — which makes it an upper bound on how
many of the clients counted back came back to nothing, not the exact number: a
client refused early and granted on a later reconnect is in it and did
recover.

**That question is now settled**: such a client is not recovered, and the
headline waits for it (above). The two fields stay because they are the only
account of what happened during the storm rather than at the end of it, and
because no batch published before WI 1509 recorded them at all — for those the
question cannot be answered either way.

## Does a stream that cannot make progress recover?

`replication-recovers` is the only property evaluated from a probe taken after the run rather than
from the ledger. Once every fault is healed, every stopped node restarted and the cluster settled, each node is asked
one question: can you still accept a QoS 1 publish? A node whose replication barrier cannot pass answers 0x83 and
keeps doing so — the outcome of every defect in WIs 1491, 1494 and 1506 — and a refused publish never enters the
ledger's publications, so no other property can see it.

Three things about it are worth knowing before reading a run:

- **It is not evaluated unless every node is running.** With a replica down the survivors refuse a QoS 1 publish
  *correctly*, because the barrier cannot reach the missing peer. `livenessProbes` in a run's statistics reads 0 when
  the property said nothing because it did not run.
- **It is a bounded retry, not a snapshot**, because the claim is that a stream stops being broken. The bound is
  eight acknowledgement timeouts, derived from the cluster's own configuration.
- **It is enforced on every run.** It was switched off while the product could not pass it: a forward that failed
  left a mark that refused every later QoS 1 publish on the node, for as long as the node was watched, on every seed.
  With the forward barrier judging a publish on its own frames, no node in the corpus refuses after the heal, and the
  switch is gone.

A violation names the node, the reason code, and that node's replication barrier failures by peer and reason, so it
says which stream could not make progress and why.

### How long, how much, and at which barrier

The property answers yes or no. Beside it every run in which the property is evaluated reports a magnitude: from the instant the cluster settled after
the heal, each node is probed on its own goroutine, every 250 ms, until it first accepts or the property's bound
passes. The run's description carries one `recovery:` line and `run.json` carries `healed_at` and a `recovery`
record per node:

- `recovered` and `after_ms` — the time to the first accepted probe. **For a node that did not recover, `after_ms`
  is how long it was watched: a lower bound, never a duration to average.** `recovery.notRecovered` counts such
  nodes and `recovery.longestMs` is taken over the recovered ones only.
- `refused` and `refused_by_code` — how many probes it refused, and with what.
- `barrier_failures_since_heal` — the node's failures at the two barriers a QoS 1 acknowledgement waits on, as the
  **difference** between the healed instant and the end of the watch: `replication:<peer>/<reason>` and
  `forward:<reason>`. The cumulative totals in a violation's text include the fault window, where refusing is
  correct; this does not. It is what showed that the node refusing after the heal is refused by the routing plane's
  forward barrier, not by the replication stream the totals point at.

A run in which no node refused anything after the heal says `no node refused a publish after the heal` and
contributes no duration: its silence is not a fast recovery. `recovery.wedgedNodes` is the non-vacuity figure.

The probe publishes to a topic nobody subscribes to, so it writes no replication record
(`TestProbeWritesNoReplicationRecord`) and, matching no subscriber on any node, has nothing to forward. That is what lets it time a stale mark without clearing it — and it is also
why a node behind such a mark refuses it indefinitely, where a publish that replicates or forwards would move the
stream past the mark.

`results/analysis/wedge.py BEFORE AFTER` compares two arms of artifacts on these figures with an exact permutation
test, and says so when neither arm observed a wedge.

Beside it, a refused publish is now counted per node: `publishErrors.<node>` in a run's statistics, against the
`publishErrors` total that was already there. One node refusing everything and every node refusing a little are
different findings under the same total.

## Properties

Evaluated over the ledger (every acknowledged publish, every subscription
with its SUBACK time, every delivery with its DUP flag) and the cluster's
tables after the settle period:

| Property | Statement | Design |
|---|---|---|
| `one-live-owner` | at no sampled instant is a client identifier online on two nodes | invariant 2 |
| `no-double-delivery` | per subscriber, each QoS 1 or 2 message is delivered once without DUP; any further delivery carries DUP — except a repeat attributable to a re-homing (the subscriber moved node at an instant, that message's first delivery fell within `ResendLag` before it, and the repeat came from the new node within `ResendWindow` after; both derived from the cluster's ack and takeover timeouts, and only at QoS 1, where at-least-once permits it). At QoS 2 the allowance does not apply: a re-send under the identifier the client still has open is one delivery, and anything else — a repeat under a fresh identifier, or any delivery after the client completed the exchange — is a violation, DUP flag or not | invariant 2, 4.3 |
| `ordering` | per subscriber, per publisher, per topic, deliveries without DUP arrive in publish order | invariant 4 |
| `no-acknowledged-loss` | every QoS 1 or 2 publish the publisher saw acknowledged is delivered to every durable subscriber that held the topic before the acknowledgement | invariant 5 |
| `minority-refuses` | a node cut from quorum answers a new connect, once its lease lapsed, with Server Unavailable or Use Another Server | invariant 3 |
| `checksum-fails-closed` | a node restarted over a corrupted log record refuses to start | invariant 5 |
| `subscribe-granted` | every SUBSCRIBE in the run was granted, and the run granted at least one | invariant 4.3 |
| `replication-recovers` | every node still running at the end can accept a QoS 1 publish, asked once every fault is healed and the cluster settled (see *Does a stream that cannot make progress recover?*) | invariant 5 |
| `publish-counted-once` | every publish the harness recorded is in the accepted ledger or the refused one, never both — a property over the harness's own accounting, because the harness is one of the suspects whenever it reports a defect | — |
| `no-refusal-without-faults` | a run that injected **no** faults refused no QoS 1 or 2 publish, and accepted at least one. A faulted run's refusals are reported and judged by nothing: it partitions, crashes and skews nodes on purpose, so a refusal there is the design working | invariant 5 |
| `no-session-served-empty` | no running node served a fresh session to a client whose lease showed it had one (`hoardmq_sessions_rebuilt_empty_total`, read at the end of the run: a crashed node contributes nothing, a restarted one only what it counted since; a count under `no-expiry`, a session that had no expiry and ended with its connection, is reported in the stats and is no violation). No corpus schedule is meant to remove every copy of a session, so any count is a finding. Its control, `TestSessionWithNoCopyLeftIsCountedServedEmptyOnce`, removes every copy (one replica, owner crashed) and must read exactly one. Every seed is judged; seed 1's exemption, for an adoption whose fetch ended while its only replica holder was down and its declared-dead previous owner unlinked, was removed when WI 1765 made both wait. An exemption names the work item that removes it (`TestCorpusExemptionsNameTheirWorkItem`), is logged `EXEMPTED` and counted in "exemptions fired over the corpus" | invariant 2 |

A renewal failure on the line names its cause, and two of those causes are
easy to confuse: `noleader` means no leader was known, `unreachable` means
one was and this node could not send to it. They used to be one number, and
18 of the 20 failures behind WI 1292 round 10b's one fence sat in it while
raft had a stable leader. `leaderlessmax` is how long the node went without
a leader at all, which is what makes a `noleader` count interpretable — a
handful across a one-second election and the same handful across a
twenty-five-second window are different diagnoses (WI 1357).

`no-acknowledged-loss` is a statement about *acknowledged* publishes only, so
a build that refuses more of its traffic satisfies it more easily. Two things
answer that. `no-refusal-without-faults` judges the one run where a refusal
has nothing to blame — the fault-free one, where zero is the design's own
number rather than a threshold the harness chose — and it fails a run that
accepted nothing at all, which is what would otherwise satisfy
`no-acknowledged-loss` most easily of all. For a faulted run, the number is
reported and not judged: the run's
statistics carry `refused.<reason>` per run — every running node's
`hoardmq_forward_failures_total` totalled by the reason the routing plane
recorded, so a change in what a build refuses, and why, is visible in the
corpus rather than only on a cluster — `subscriber-node-down`,
`routes-unknown` and `session-moving` are the three the routing plane
records. The totals cover the nodes still
running at the end, each counting from its last start, so a crashed node
contributes nothing and a restarted one contributes only what it refused
after it came back. A rise in `refused.routes-unknown`
beside a fall in `publications`, for instance, is the WI 1351 rule doing its
work; the same rise with no fall would be it over-refusing. There is no
property over the refusal *rate* yet — that is WI 1308.

A run's statistics also carry `deliveries.duplicateSkipped`: the routed
copies the running nodes skipped because the session had been installed from a
hand-over already holding the message (WI 1521). Above zero, the run reached
the late-forward path that `no-double-delivery` would otherwise report; a
`no-double-delivery` or `ordering` violation prints each delivery's packet
identifier, QoS, DUP flag and node.

## Negative controls

A property that has never failed proves nothing. `controls_test.go` runs
each property's scenario twice: once against a broker built with a known
defect in it, where the property must report a violation, and once against
a sound one, where it must not. The defects are the switches in
`broker.Defects`, each of which disables one protecting mechanism; none is
reachable from configuration.

| Property | Injected defect | Mechanism disabled |
|---|---|---|
| `minority-refuses`, `one-live-owner` | `MinorityServes` | a fenced node keeps granting leases |
| `no-double-delivery` | `ForgetInbound` | inbound QoS 2 identifiers are not remembered |
| `no-double-delivery` | `PubrelUnreplicated` | the PUBREL goes out before the PUBREC state is replicated, so a client completes an exchange a rebuild cannot know about |
| `ordering` | `ReorderForwards` | the transport swaps adjacent frames |
| `no-acknowledged-loss` | `NoBarrier` | acknowledgement after the local sync only |
| `checksum-fails-closed` | `IgnoreChecksum` | a bad checksum is read as sound |
| stale-epoch refusal | `SkipEpochCheck` | covered by `internal/cluster/replication`'s unit test |
| persist-before-send | `SendBeforeSave` | covered by `internal/cluster/consensus/etcdraft`'s join test |
| a reconnected client keeps its connection and its queue (`adoption_scenario_test.go`, a scenario of its own rather than a corpus property) | `AdmitDuringAdoption` | a CONNECT for a lease this node adopted is admitted while the adoption is unfinished, so the import that follows evicts it |
| a client keeps its session when a takeover pass crosses the adopter's install (`internal/broker/importrace_test.go`, a three-broker scenario, not a corpus property) | `ImportPastLaterGrant` | a fetched session is installed although a later grant for its client has been seen, so the install's write drops the replica copy the new owner's fallback reads |

## What the harness changed in the broker

Building it, and running it to completion, surfaced the holes below, fixed
in the same work item. The worst were found last, each only after the
previous one was fixed and the whole suite ran again:

- the broker **panicked** (`send on closed channel`) when a client connected
  while it was closing other connections: the outbound channel was closed to
  stop the write loop while senders were selecting on it. It is no longer
  closed at all; a separate signal stops the writer;
- a **durable session was lost** because the shard map could move backwards
  through the snapshot-restore path, and the rebalance dropped replicas
  against the older map. Restore is monotonic now, and a copy is only
  dropped on a map that moved forward;
- a **session was held by two nodes at once**, its live owner enqueueing
  into a copy nobody would read, because a takeover decided whether to ask
  the previous owner for its state from this node's reading of that node's
  clock. The previous owner is asked whenever it can be reached;
- the rebalance **dropped the last copy** of a session whose owner had died.
  A copy is kept while its owner is not alive to make new ones;
- **exactly-once broke across an owner loss**: the PUBREL went out as soon as
  the owner had it locally, so a client could complete an exchange no
  replica knew about and a rebuild delivered the message to the application
  a second time. The PUBREL is now held, and retried, until the PUBREC state
  is replicated;
- **a stream that had dropped frames kept acknowledging**: when a send fails
  or a link resets the pending frames are discarded, but frames pushed
  afterwards took fresh sequence numbers, so their barriers no longer saw
  the failure and the peer acknowledged them over a copy with a hole in it.
  A holed stream now fails every barrier until a whole resync is queued
  behind the gap, and a send failure triggers that resync instead of waiting
  for a transport transition that never comes;
- **a takeover adopted a copy nothing was replicating to**: a shard's
  holders are its leader and every follower, while an owner replicates to at
  most `replicas-1` of them, and a session's owner is set by its lease
  rather than by the shard map. The holder receiving nothing kept whatever
  an earlier map left it, and the fetch took the first holder that answered.
  It now asks only the nodes the previous owner was replicating to;
- a **queued message's packet identifier was not journaled**, so a session
  rebuilt from a replica re-sent under a fresh identifier that the client
  could not match to the exchange it had open. The identifier is reserved at
  enqueue and rides the same replicated record;

and the five found while building it:

- forwards between nodes were at most once, so a publish could be
  acknowledged while its copy for a remote subscriber was lost — forwards
  are now acknowledged by the receiving node after it made them durable, and
  a link reset or an unreachable subscriber's node fails the publish;
- the replication barrier skipped followers the transport reported down,
  acknowledging without a copy — it now fails;
- a live takeover exported the session before closing the old connection, so
  a message sent in between reached the client twice without DUP — the
  connection is closed first;
- a session's routes were withdrawn from the cluster the moment its state
  left a node, one round trip before its new owner published them, so a
  publish in that window matched nothing anywhere and was acknowledged with
  the message delivered nowhere — the previous owner now holds the removal
  for the takeover timeout, during which the other nodes still forward to it
  and the publish is refused rather than lost;
- a QoS 2 publish refused in the PUBREC left its packet identifier recorded
  in the session, so a PUBREL that a conformant client never sends would
  still have been answered — the identifier is released with the exchange
  (MQTT 5 §4.3.3).

Refusing rather than losing has an availability cost that the seeds make
visible: under a long partition or a link that keeps resetting, most QoS 1
and 2 publishes across the divide are answered Implementation Specific Error
until the shard map drops the dead node or the session is re-homed. Seeds 2
and 8 acknowledge only tens of publishes out of about four hundred
attempts. That is the design's stated preference — a refusal the client can
retry over a silent loss — and the corpus fails a seed that carries no
traffic at all, so a build that refuses everything cannot pass vacuously.

## Failover measurement

`TestFailover` connects N durable clients round-robin, crashes the node
holding the most of them, and measures the time to full reconnection and
the per-client reconnect distribution, with the survivors' lease operation
and proposal counts to show batching. It records and judges without failing:
a run where some client never came back within the window says so in the
report, and a figure beyond its bound is a failure the artifact states (see
**The pass bounds**), not a red test. Its reporting path takes nothing under a product lock
without a bound: the lease counters come from a metrics snapshot that walks
the session table under the session manager's mutex, and under a storm that
wait has outlived the measurement, so it is given thirty seconds and
reported as `NaN` otherwise. Above twenty thousand clients the
connections are spread over the loopback addresses 127.0.0.1–8: the
ephemeral port range is per source and destination pair, and one source
does not have enough ports for a hundred thousand clients over two
surviving nodes. On Linux each such socket sets `IP_BIND_ADDRESS_NO_PORT`,
so the kernel chooses its port at `connect()`, unique for the four-tuple,
rather than at `bind()`, unique for the address across every destination:
WI 1476 found that search, repeated for every reconnect of the storm, taking
most of the host's recorded CPU. A run records how its clients dialled, read
back from a real socket before the fill, in `provenance.effective`
(`client_sources`, `client_dial`) and in the report (WI 1477). `results/`
holds the recorded runs with their methodology and caveats, and
[results/README.md](results/README.md) says how each was produced.

### Clients on another host

In the in-process measurement the clients share the brokers' Go scheduler, CPU and socket tables, and no
per-process priority can separate goroutines in one process. With `HOARDMQ_SIM_CLIENT_COMMAND` set, the
clients run in the driver program `sim/cmd/simclients` instead. The test starts the command, usually
`ssh … user@host`, and the two talk over the driver's stdin and stdout, one JSON object per line. The
protocol is described in `sim/driver.go`.

**The nodes.** They listen on the three `HOARDMQ_SIM_NODE_ADDRS` addresses and advertise them. That is the
only configuration that differs from an in-process run (`TestRemoteNodesDifferOnlyInAddress`). It is
deliberately **not** several ports per node: the handshake bound and accept pausing are per listener, so
more listeners would change the brokers' admission as well as where the clients run.

**The clients.** They dial from every `HOARDMQ_SIM_CLIENT_SOURCES` address at every client count, so a load
sweep never changes dial mode.

**The run.**
1. The driver waits up to ten minutes for its host to meet the starting conditions, then refuses, naming
   what it saw:
   - no other driver process;
   - at most 2,000 TCP sockets in use or TIME-WAIT;
   - an open-file limit of at least clients + 1,000;
   - at least 16 GiB available;
   - a 1-minute load of at most 4;
   - every source address on the host;
   - at most half the ephemeral range of clients per source address.

   On the broker host the test checks the connection-tracking table and that every node address is present.
2. Every source connects once to every node, and the dial mode is read back from those sockets.
3. The driver fills as the in-process test does, and takes the pre-crash baseline when told to.
4. From the mark it sends a reading of the clients every 50 ms. Once the test reports the crash, it sends
   `allback` when every client is online again, after a reading below that.
   - A reading from before the crash never counts as recovery.
   - Full recovery is the time the test receives `allback`, on its own clock.

**The artifact.**
- **`run.json`** gains `client_mode` and, for a remote run, `remote_clients`:
  - the driver's build and host;
  - a per-second series of the client host: CPU, the driver's CPU and memory, TCP sockets, retransmits, and
    failed connects by class;
  - the broker host's conntrack count;
  - the `client_limited` verdict. A run is client-limited if, during the window:
    - a connect failed for want of a port or a file descriptor;
    - the client host was at least 90% busy for 3 consecutive seconds; or
    - the driver was stopped for over a second.

    Retransmits are reported but decide nothing: accept pausing makes the client host retransmit SYNs by
    design.
- **The report** gains a Client host section.
- **`reconnects.json`** holds every per-client reconnect time in both modes.

**Failures.**
- **The driver's own:** a driver that refuses, dies, or is silent for 30 s fails the test before the
  `complete` marker. A repeat batch then counts the run as failed, never as a broker that did not recover.
- **A full conntrack table:** the run refuses above `conntrackStartLimit` entries rather than measure through
  a table the previous run is still emptying. `TestFailoverRepeat` waits that condition out between remote
  runs, on the same limit, so back-to-back 100k runs start on their own.
- **Stdin closing:** the driver listens on no port, and ends every client and exits when its stdin closes.
- **The build:** a recorded run refuses a driver built from another commit or a modified tree.
