# 1,000,000 persistent sessions: memory, a node's loss and its restart

**Hosts:** the three nodes on `ai3`, AMD Ryzen 7 9800X3D (8 cores, 16 threads), 92 GiB RAM, NVMe SSD (Samsung 990
PRO 4 TB), Linux 7.0.0-42; every client on `ai2`, AMD Ryzen 9 9950X3D (16 cores, 32 threads), 91 GiB RAM, NVMe SSD
(Samsung 9100 PRO 4 TB), Linux 7.0.0-28. Each host's network card linked at 1 Gb/s, on one local network; `ai2`'s
card has one receive queue.
**Date:** 2026-11-12 and 2026-11-13.

## The claims

- docs/production-readiness.md, ["Measured"](../production-readiness.md#measured): "The largest client load measured
  so far is 1,000,000 persistent sessions, with three nodes on one host and every client on another, on the
  broker code released as `v0.2.0`: in five runs a node was killed and its 333,333 clients reconnected to the
  survivors, 99.9% of them within 152–229 s of losing their connection, and no session of 5,000,000 checked was
  lost".
- This directory's [README.md](README.md), its opening paragraph: the same load, as the largest measured.

## What was run

- **Layout.** Three broker processes (`hoardmq serve`) on the broker host, each pinned to its own cores (3, 2 and 3
  of its 8, each core's two threads to one node), each with one client listener on one address. The cluster's links
  between them stay on that host. One client program on the client host holds every client and dials from 40 source
  addresses, so that no source runs out of ephemeral ports; the test that drives it, kills and restarts a node, and
  reads every node runs beside it.
- **Configuration:** every member at the broker's default except the node's name, data directory and addresses,
  static discovery naming the other two, `mtls` cluster authentication with a certificate authority the run makes,
  the metrics module (the observability listener), a file authorizer allowing every topic, JSON logs, and
  `auth.anonymous: true`. **Product timings:** every cluster timing at its default, the lease ttl of 30 s among them.
- **Anonymous clients.** No client sent a password. With passwords every reconnect would derive a key, and at about
  373 derivations a second on `ai2`'s 32 threads ([auth-cost.md](auth-cost.md)) that would be what a storm of 333,333
  reconnects measured, about 15 minutes of derivation, not the cluster's recovery.
- **The sessions:** 1,000,000 MQTT 5 clients, each with Clean Start 0 and a session expiry of 3,600 s, subscribed to
  its own topic at QoS 1, asking for a keep alive of 60 s and pinging at it; the clients' sockets have TCP keepalive
  off. Client `i` lists the nodes from node `i mod 3`, so a lost node's clients all try the same survivor first.
  After a drop, a refused CONNACK or a failed connect (a 2 s dial, then 5 s for the CONNACK) a client waits a backoff,
  from 50 ms doubling to a 1 s cap, each wait drawn from half to one and a half times it, and tries the next node or
  the one a Server Reference names.
- **One run, in order:** the cluster started; the fill of all 1,000,000 (CONNECT and SUBSCRIBE); the connections held
  idle 600 s, then read; 100,000 sessions taken offline with a clean DISCONNECT, 10 QoS 1 messages of 64 bytes
  published to each from 64 publishers, read again, then the sessions resumed and their messages counted; a check
  that every node checkpointed after the fill; one node killed with SIGKILL (the storm, below); the delivery check;
  the killed node started again on its data directory (the rejoin); a census of every node's subscriptions against
  its advertised routes; every node stopped. A run took 48–50 minutes.
- **The storm:** the node killed was the same in every run, `n2`, a raft follower in each (`n1` led). The run reads
  every client once a second for 15 minutes from the kill. A client is recovered when it is connected with every
  filter it holds granted on that connection, and the **recovery instant** is the start of the first 60 s in which
  every client still trying stays recovered. A probe of 100 publisher and subscriber pairs, outside the measured
  clients, publishes QoS 1 once a second each through the storm and records what the cluster answered and whether
  what it acknowledged arrived.
- **The delivery check,** after the storm and before the restart: one QoS 1 message on every client's own topic.
  A client that resumed with Session Present 1 and receives nothing is a **lost session**, counted with the node
  serving it; a publish refused on every pass is classed by its reason code, not counted lost. Each run also removes
  the subscription of 10 clients itself, and fails unless the check counts all 10 lost.
- **Five runs**, one after another, each read in full before the next. The broker host's 500 ms samples, each node's
  `/metrics`, the client program's own figures each second and both hosts' TCP counters were recorded beside each
  run.
- **Host settings as run:** on the broker host, a listen queue (`somaxconn`) and SYN backlog of 65,535, room for
  2,000,000 sockets in TIME-WAIT, open files 1,048,576, and no connection tracking. On the client host, open files
  1,048,576, ephemeral ports 10000–65535, connection tracking up to 16,777,216 entries, and TIME-WAIT reuse for
  outgoing connections (`tcp_tw_reuse` 1): one host stands in for a fleet's many client hosts, which would not share
  a source address's ports, and without reuse its TIME-WAIT sockets at the reconnect storms would crowd them.

## The build

One broker build, of 2026-11-12, whose broker code was released as `v0.2.0`: the development line after `v0.1.0`,
carrying every change in the changelog's `v0.2.0` section ([CHANGELOG.md](../../CHANGELOG.md#v020--2026-11-14)), the
latest of them the replica-store checkpoint that no longer holds the node's replica fetches for up to seconds. The cluster ran at feature level 2, confirmed
before each fill. `v0.1.0` itself is not measured here: it carries the route-set limit in
[production-readiness.md](../production-readiness.md#known-limits), which this load is past.

Runs 1–4 and run 5 differ only in the test program: before run 5 it gained a record naming every client that dropped
after every client was back. The broker's code is the same in all five.

## Results

### Memory

| Run | Fill (0 failed) | Resident memory a session, cluster-wide | A node's rise, over its sessions and copies | A queued message | Broker host's lowest available memory |
|---|---|---|---|---|---|
| 1 | 380.5 s | 39,067 B | 19,080–19,769 B | 3,717 B | 40.3 GiB of 89.6 |
| 2 | 382.4 s | 38,582 B | 18,189–19,969 B | 4,553 B | 39.1 GiB |
| 3 | 379.5 s | 39,890 B | 19,806–20,104 B | 1,941 B | 41.2 GiB |
| 4 | 388.4 s | 39,189 B | 19,443–19,758 B | 4,605 B | 40.5 GiB |
| 5 | 375.8 s | 39,140 B | 18,843–20,074 B | 4,252 B | 42.9 GiB |

- **A session**, cluster-wide: the three nodes' resident memory once idle, less their resident memory before any
  client, divided by 1,000,000. Each session is held twice (`cluster.replicas` 2), by the node that owns it and as a
  replica copy on another, so a node holds about 333,333 sessions and as many replica copies, and its rise divided by
  the two together is about half the figure.
- **The host's own fall:** the broker host's available memory fell 40.7–42.0 KiB a client from the empty cluster to
  idle, the kernel's sockets included (the 250,000-client sweep on an earlier build read about 46 KiB,
  [memory-250k.md](memory-250k.md)).
- **A queued message:** the three nodes' resident rise while the 1,000,000 messages were queued, divided by the
  messages acknowledged (all 1,000,000 in every run), with each message's replica copy inside it. It spreads from
  1,941 to 4,605 B between runs: at this size the garbage collector's timing moves it. Every one of the 100,000
  resumed sessions received all 10 of its messages in every run.
- **Peaks:** each node's highest resident memory in a run was 20.8–23.6 GiB (`n1`, which ended with most of the
  lost node's sessions), 14.2–14.7 GiB (`n2`) and 17.7–22.5 GiB (`n3`); the broker host kept 43.7–47.9% of its
  available memory at its lowest. The client program peaked at 32.6–34.9 GB, about 33–35 KB a client, on its own
  host.
- **The checkpoint:** every node checkpointed 28–29 times after the fill in every run. The broker does not publish a
  checkpoint's duration; [checkpoint-scale.md](checkpoint-scale.md) stays its source.

### The storm

The kill dropped 333,333 clients in each run (333,334 in run 4). Times are from the kill, except where said.

| Run | 99% back | 99.9% back | Every client online | Recovery instant | What set the instant | Most back in one second |
|---|---|---|---|---|---|---|
| 1 | 162.32 s | 164.04 s | 169.7 s | 224.97 s | one client's socket left open to the killed node (below) | 12,807 |
| 2 | 179.58 s | 180.55 s | 184.0 s | 183.56 s | the last client back | 10,014 |
| 3 | 150.88 s | 151.78 s | 156.4 s | 167.56 s | a few clients dropped again, as Session Taken Over | 14,190 |
| 4 | 227.71 s | 229.03 s | 235.4 s | 256.16 s | a client dropped again, the cause not recorded | 15,502 |
| 5 | 161.62 s | 162.46 s | 164.5 s | 166.36 s | 7 clients dropped again, each taken over by its own earlier CONNECT | 11,376 |

"99% back" and "99.9% back" are that share of the dropped clients' times from each one's drop to its first accepted
CONNACK; "every client online" is the first one-second reading with every client connected and recovered. A client on
a dead connection counts as recovered until it learns otherwise, and a client can drop again, so the instant can come
after every client was online:

- **Run 1:** when the node was killed its host closed the node's connections, some with a reset, and in every run the
  client host's card, with one receive queue, missed at least 119,000–169,000 packets in the 10 s after the kill (a
  lower bound, sampled about every 1.2 s). A client whose close was lost stayed connected to the dead node until its
  next keep-alive ping found the connection dead. In run 1 one such client stayed to +212.9 s, then reconnected.
- **Runs 3 and 5:** a client already back was dropped with Session Taken Over. Run 5's record names them: each was
  taken over by a CONNECT of its own that it had abandoned at its 5 s deadline in the storm and that a node read
  later; each reconnected within 115–180 ms and resumed with Session Present 1. Run 3's drop is Session Taken Over by
  the drop counts; its client was not named.
- **Run 4:** a client dropped about +255 s and came back with no failed connect counted; the run's drops show two
  transport closes beyond the kill's, and which client and why is not recorded.

Each reconnecting client made a median of 35–57 connects (99th percentile 48–78, most 55–86) from its drop to its
accepted CONNACK; in all, 532,488–877,728 CONNACKs refused (Server unavailable, Use another server) and 9.7–15.3
million connects failed (a dial timeout, a CONNECT with no CONNACK in 5 s, refused). The broker host counted 11.4–17.9
million listen-queue overflows a run, each a connect delayed. From the kill to the instant the two survivors used on
average 1.2–1.9 of their 6 threads each (500 ms samples, at most 5.5–6.0).

The probe: 0 messages acknowledged and not delivered in any run, 13,881–21,567 acknowledged a run; 3,547–6,229
publishes refused a run (0x83: a node refuses a QoS 1 publish that could match routes it cannot prove), while the
survivors had not yet replaced the lost node's routes.

### Restart and rejoin

The killed node was started again on its data directory after the delivery check.

| Run | Sessions loaded, and the time to it | `/readyz` and `/servingz` | Rejoined | Replica copies before the kill → at rejoin |
|---|---|---|---|---|
| 1 | 333,399 in 4.3 s | 14.9 s | 15.9 s | 333,389 → 532,734 |
| 2 | 333,399 in 4.2 s | 14.8 s | 31.4 s | 333,389 → 519,168 |
| 3 | 333,399 in 3.4 s | 14.3 s | 31.8 s | 333,389 → 530,987 |
| 4 | 333,399 in 3.3 s | 14.2 s | 16.2 s | 333,389 → 505,878 |
| 5 | 333,399 in 4.1 s | 14.9 s | 15.9 s | 333,389 → 527,529 |

Rejoined is the start of 30 s in which the node is ready and serving, holds replica copies that do not change, and no
node is short of copies. The node loads its sessions from its own disk; they had all moved to the survivors in the
storm, and it held none at the census. Its copies came over the broker host's own links, so the 1 Gb/s
card bounds none of these times.

### Losses

| Run | Clients checked | Delivered | Lost sessions | Controls caught | Census | Sessions holding no subscription |
|---|---|---|---|---|---|---|
| 1–5, each | 1,000,000 | 999,990 | 0 | 10 of 10 | 0 differences | 0 |

- **0 lost of 5,000,000 checked.** With no loss in N checks, the 95% upper bound on the chance that a session is lost
  in a node's loss is about 3/N, here 6 in 10,000,000 (1 in about 1.7 million). The five runs are not independent
  samples: one build, one layout, the same node killed and the same clients each time; the bound is the arithmetic
  on the count, not a rate across deployments.
- **The census:** after the rejoin, each node's subscriptions equal the routes it advertises. It compares counts, so a
  missing route and a stale one on the same node would cancel.
- **Sessions without a subscription:** every node's sessions less its subscriptions, less the run's own exceptions
  (the probe's 100 publishers and the 10 controls): 0 in every run. No node counted a session served empty, and no
  node's route reconciler corrected anything.

### What the runs saw besides

Each is recorded as observed; none lost a session or an acknowledged probe message in these runs.

- **Refusals while the cluster recovers:** the refused CONNACKs and the probe's 0x83 refusals above. In runs 3 and 4
  the probe was still refused for 44.2 s and 8.4 s after the recovery instant, each time until the survivors forgot
  the lost node's routes; in run 5 its refusals ended 7.8 s after that forget, at the end of the episode below.
- **A full replica resync between the survivors** in runs 4 and 5: mid-storm, one survivor's replication to the other
  failed, and it resent every copy the other should hold (510,599 in run 4, 420,139 in run 5).
- **The restarted node's kept copies:** in runs 1–4 the restarted node counted 333,459–444,960 replica copies kept
  because their hand-over to a survivor was not acknowledged; in run 5 it counted none.
- **A raft leader change in the storm** in runs 1 and 4: the leader stepped down for lack of a quorum it could hear,
  and a leader was elected at the next term within 1.3 s.

## What this does not show

- **A cluster across hosts.** The three nodes ran on one host, so replication and the rejoin crossed no network: that
  flatters them against nodes on separate hosts.
- **A whole host's loss.** One node was killed, never the host.
- **Passwords, TLS** on the client listeners, or WebSocket clients.
- **A rolling restart** at this scale.
- **One session shape:** one subscription a client, 64-byte payloads, persistent MQTT 5 sessions, no shared
  subscriptions among the measured clients.
- **Two hosts on one local network at 1 Gb/s, and one client host.** One machine's card and kernel carried every
  client; the lost resets and listen-queue overflows above are partly that host's.
- **The leader's loss.** The killed node was a raft follower in all five runs.
- **A gate.** The 100,000-client pages ([connect-path.md](connect-path.md)) ran in one process with a different
  layout; the times here are not comparable to theirs and are reported, not judged.
- **An earlier batch, and the run before it.** Five runs of the same measurement on two earlier builds of the same
  day lost 126 sessions in one run (none in the other four). After the kill, a survivor whose search for those
  sessions' copies ran out of time behind a long hold of its replica store read the search as "nothing held" and
  served each of those clients a fresh, empty session, while the other survivor held their only copies; each client
  resumed it with Session Present 1 and no subscription. In another run of that batch a survivor served one client
  an empty session, most likely the same way, and that client still received its check message. Both causes are
  fixed in the build above: such a search now refuses the client (Server Busy) until the copy is found, and the
  checkpoint no longer holds the replica store for that long. The first run at 1,000,000 with the delivery check, on
  a build of the same day before the batch, checked 300,000 of the clients and counted 2 sessions lost to routes a
  survivor held but did not advertise: it held 17 more subscriptions than it advertised routes for, so a publish made
  on another node found no subscriber. The one order found that leaves a subscription unadvertised was a route
  withdrawal, held while a session moved, written after the session's return had announced the route again; it is
  now written only if nothing was announced for the filter since, and every node's route reconciler compares its
  subscribers with its advertised routes and corrects a difference ([CHANGELOG.md](../../CHANGELOG.md#v020--2026-11-14),
  Added and Fixed).
