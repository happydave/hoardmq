# Development

For contributors: the wire codec, the fuzz targets, the conformance suite, the simulation harness that gates the
cluster, and how to build and test.

## Codec

`internal/packet` is the wire codec: one decoder and one encoder for MQTT
V3.1 (`MQIsdp`, level 3), 3.1.1 (level 4) and 5.0 (level 5). The level is
fixed per connection from the CONNECT packet. Every 5.0 property and reason
code is typed; unknown property identifiers, bad flags and overruns are
reported as Malformed Packet, value and sequencing violations as Protocol
Error, and an unsupported protocol version as a CONNACK in the client's own
framing. Properties are stripped at encode time for level 3 and 4 peers. The
decoder holds no buffer while a connection is idle: it blocks on a one-byte
read and takes a pooled buffer only once a packet has begun. Fuzz targets
cover every level.

## Fuzzing

Every parser of bytes from a client, a peer, a directory, an identity provider or the disk has a Go native fuzz
target: the MQTT codec (`internal/packet`: `FuzzDecodeV5`, `FuzzDecodeV4`, `FuzzDecodeV3`, `FuzzConnect`), the
write-ahead log's segment reader (`internal/wal` `FuzzSegment`, which also holds `wal verify`'s survey to what a
start does), cluster frames and the hello before authentication (`internal/cluster/transport` `FuzzReadFrame`,
`FuzzDecodeHello`), LDAP messages (`internal/ldap` `FuzzReadMessage`, `FuzzParseResponse`), and JOSE tokens and key
sets (`internal/jose` `FuzzParse`, `FuzzParseKeySet`, `FuzzVerify`). Each is seeded from its package's tests; the
seeds are also in each package's `testdata/fuzz/<Target>/` (written with `HOARDMQ_FUZZ_SEEDS=write go test -run
'^Fuzz' <package>`). A plain `go test` runs every seed as a test. A campaign runs one target at a time:

```
go test -run '^$' -fuzz '^FuzzSegment$' -fuzztime 10m ./internal/wal/
```

A failure the engine finds is written under the target's `testdata/fuzz/` and fails every later `go test` until
fixed.

## Conformance

`conformance/` is a black-box suite that drives a real node over TCP with
the Eclipse Paho clients (`paho.golang` for 5.0, `paho.mqtt.golang` for
3.1.1) and the project codec for hand-crafted frames. Every test names the
normative statements it observes, by the identifiers of the specifications'
Appendix B (`conformance/statements/mqtt5.json`, 251 statements;
`mqtt311.json`, 141). The test binary writes a results file, and
`go run ./conformance/cmd/coverage` renders
[conformance/COVERAGE.md](../conformance/COVERAGE.md): every statement with
`pass`, `fail`, `not-covered` or `n/a-client` (client-only statements, each
with its reason in `statements/scope.json`). Every test names the
specification it observes, the one of the protocol level it connects at, and
its results count only there: statement numbers recur in the two
specifications with different meanings. A test that names an identifier its
specification does not have fails the run. Today MQTT 5.0 has 146 of 251
statements passing, 0 failing, 90 not covered and 15 client-only; MQTT 3.1.1
has 79 of 141 passing, 0 failing, 52 not covered and 10 client-only.

```
go test ./conformance -conformance.results=$PWD/conformance/results.json
go run ./conformance/cmd/coverage && git diff --exit-code conformance/COVERAGE.md
HOARDMQ_EXTERNAL=1 go test ./conformance/external     # Paho interoperability suites, minutes
```

The external test fetches the upstream `paho.mqtt.testing` suites into
`tmp/` at a pinned commit, runs them against an in-process node on
`localhost:1883` and compares with `conformance/external/paho/expected.json`,
where each expected failure carries its reason; the last run is recorded in
`last-run.json` and rendered at the end of `COVERAGE.md`.

## Simulation harness

`sim/` is the cluster's acceptance gate: real nodes in one process, a
seeded workload of publishers and durable subscribers, a seeded schedule of
faults (partitions with a minority probe, dropped, delayed and reordered
frames, crashes with torn or corrupted logs, drains, clock skew), and the
design's invariants checked as properties afterwards — one live owner per
client identifier, no double delivery, per-publisher ordering, no
acknowledged QoS 1 or 2 loss, minority refusal, checksum fails closed. A
seed names a schedule and a workload; re-running it re-injects the same
faults (execution rides the real scheduler, so interleavings differ — see
`sim/README.md` for what "deterministic" means here). Every property is
paired with an injected defect in `broker.Hooks` that disables its mechanism
so the negative control shows the property catches the failure. Summaries of
the measurements, with their methodology, are in
[docs/evidence/](evidence/README.md).

Building the harness changed the broker: forwards between nodes are now
acknowledged by the receiving node after the message is durable there, and
a QoS 1 or 2 publish fails (Implementation Specific Error) instead of being
acknowledged when a forward was lost to a link reset, skipped in the
receiver's sequence (the acknowledgement is cumulative, so a gap is reported
before the next frame's acknowledgement could cover it), or a matching
subscriber's node is unreachable; the replication barrier fails, rather than
skips, when a follower is down; a live takeover closes the old connection
before exporting the session so nothing is sent twice without DUP; a session
leaving a node holds its route removal for the takeover timeout, so a
publish sent while its new owner's route is still in flight is refused
rather than accepted with nowhere to deliver it; and a QoS 2 publish refused
in the PUBREC releases its packet identifier with the exchange.

Running it to completion found the six more listed here, all reachable in production: the
broker panicked (`send on closed channel`) when a client connected while it
was closing other connections; a durable session could be lost because the
shard map could move backwards through a snapshot restore while the
rebalance dropped replicas against it; a takeover decided whether to ask the
previous owner for its state from this node's reading of that node's clock,
so a skewed clock left two nodes holding one session with the queue on the
wrong one; the rebalance could drop the last copy of a session whose owner
had died; a replication stream that had dropped frames kept acknowledging,
because frames pushed after the break took fresh sequence numbers and their
barriers no longer saw it, so a barrier passed over a peer whose copy had a
gap; and a takeover could adopt a copy from a shard holder the previous
owner was not replicating to, since a shard's holders are its leader and
every follower while an owner replicates to at most `replicas-1` of them.

**Exactly-once across an owner loss.** A client completes a QoS 2 exchange
when it receives the PUBREL, and after that it is entitled to treat a
re-delivery as a new message. So the broker does not send the PUBREL until
the PUBREC state is in its own journal and replicated: while an owner cannot
reach its followers, or its journal failed to take the state, it holds the
PUBREL and retries (rewriting the session whole in the second case), and the
exchange stalls rather than completing somewhere the replicas cannot see. A
session rebuilt from a replica therefore never finds a completed exchange it
does not know about, and its
re-send arrives under the identifier the client still has open — a
retransmission the client absorbs, not a second message. The identifier is
reserved when the message is queued and travels in the same replicated
record, so a rebuild re-sends under it rather than allocating a fresh one.

The cost is latency, not duplication: a QoS 2 delivery to a subscriber whose
owner is cut off does not complete until the owner can replicate again or
the session is re-homed. `TestDefectPubrelUnreplicated` is the negative
control — with the hold disabled the harness sees the subscriber's
application take the same message twice.

The inbound side has the mirror rule. After a client receives PUBCOMP it may
reuse the packet identifier for a new message, and a node that recovered, or
a replica that took over, still holding the identifier open would answer
that message as a duplicate and drop it. So PUBCOMP goes out only once the
identifier's release is in the node's journal and on the replicas; otherwise
the session holds it and retries every 250 ms, and a PUBREL the client
resends on a new connection waits the same way. It costs one more fsync per
inbound QoS 2 exchange, shared by the group commit, and one replica round
trip: about 12 ms between PUBREL and PUBCOMP on a three-node cluster on one
host ([docs/evidence/qos2-latency.md](evidence/qos2-latency.md)).

A refused QoS 2 publish releases its identifier too: after a PUBREC with an
error the client may send a new message under it at once. So the error PUBREC
waits for the release in the same way and is held and retried otherwise. While
the session's followers are unreachable, the refusal reaches the client late:
when they return, or once the shard map replaces them. A successful publish
pays nothing for this.

An identifier's opening can reach the journal or a replica before the
publish's own copies do: a queued copy in another session, or a forward to
another node. So once a publish's acknowledgement is decided, the broker also
records that it was accepted, and a PUBLISH under an open identifier is
absorbed as a duplicate only when that record is there. An identifier that a
restart or a takeover brings back open without it is refused when the client
sends the PUBLISH again (Implementation Specific Error; at 3.1.1 and 3.1, a
close), because that publish may never have reached its subscribers. The
client's retry is then a new message. Each such refusal is counted by
`hoardmq_qos2_unconfirmed_refused_total{level}`. The record is not waited
for, but a client that sends PUBREL as soon as it reads PUBREC meets it on
the replica: the whole exchange went from 15.9 ms to 20.4 ms at the
median on a three-node cluster on one host
([docs/evidence/qos2-latency.md](evidence/qos2-latency.md)).

The cost of that honesty is refusals: while a node is unreachable and until
the shard map drops it or its sessions are re-homed, QoS 1 and 2 publishes
that cannot be placed are answered Implementation Specific Error and the
client retries. The design prefers that to a silent loss.

**An owner whose routes are unknown.** Hiding an unreachable owner's routes
only works over routes this node has. A node that has just restarted holds
none: route knowledge reaches a node from its owner and nowhere else, so a
node restarting beside a peer it cannot reach has an empty table for that
peer and nothing to hide. It cannot say a publish matches no subscriber,
because it cannot say anything at all about that peer's subscribers. So it
does not: while any owner's route set is unknown, QoS 1 and 2 publishes are
refused with Implementation Specific Error, the same answer as a hidden
match, and the publishes still go to the owners this node does know about.

An owner is unknown when no full route sync has arrived from it since this
node started **and** the replicated lease state cannot prove its routes do
not matter. An owner nothing was ever taken from, holding no lease, has no
subscribers to miss. An owner that stays unreachable has its leases adopted
after the lease ttl, but the reassignment moves the leases before the adopting
node has imported the sessions or published their routes, so holding nothing
is not enough: the refusal ends when the owner's sessions are served
elsewhere, by the same proof that ends a hidden owner's refusal (each member
that owns leases has said in the log that its adoptions are finished, and this
node holds that member's routes as of then; this node's own adoptions are
finished too). A node that restarts beside a lost owner therefore refuses
until the adoptions finish, and says in the log that its own are finished, as
a node that had the owner's routes does. `hoardmq_route_owners_unserved{reason}`
counts the owners held this way (`unsynced`, `adopting`, `unreachable`, with
the meanings of `hoardmq_route_owners_hidden`). A node that cannot read the
lease state yet — one still replaying its log — has proved nothing and
refuses.

**A node that has just restarted, as a receiver.** A restarted node loads the
sessions its store holds before its consensus core, lease service and
replication start, and its lease table names it owner of what it owned when it
stopped until its core has caught up with the leader. Until both have
happened, a forwarded QoS 1 or 2 publish into one of its sessions is refused to
its sender (`not-serving` below), and the publish with it. Taken in that
window, it would be acknowledged on this node's journal alone, and a client
that meanwhile took the session over elsewhere from a replica, because this
node could not yet answer its takeover, would never receive it. A QoS 0
forward is delivered as before.

**A session that has left this node.** When a session moves to another node,
its old owner keeps advertising its routes for a grace period, so the other
nodes keep forwarding here, find no session, and refuse — rather than
matching nothing anywhere while the new owner's route is still in flight.
That protects every node except the one doing it: a node cannot see its own
routes through the two rules above, because the match skips itself and the
hidden set holds only unreachable peers. So the node still advertising the
route was the one node blind to it, and answered its own publishes as though
the route did not exist — including with No Matching Subscribers, which is a
positive claim it was not entitled to make. It now refuses a QoS 1 or 2
publish whose topic matches a filter it is handing over, for the same window
in which it is telling everyone else to forward there.

In practice the window is the length of the ignorance and no longer.
Measured on the simulation's seed 5
([docs/evidence/restart-times.md](evidence/restart-times.md)): 0.111–0.202 s
at a cold cluster start,
which is the transport connecting and is over long before the readiness
gate's five seconds of required stability; 0.102 s for a restart whose peers
are reachable; and 1.92–1.99 s for a restart beside a partitioned peer,
ending when the partition does. `hoardmq_route_owners_unknown` reports how
many owners are unknown right now, and
`hoardmq_forward_failures_total` separates the three causes a publish is
refused for *at dispatch* by its `reason` label: `subscriber-node-down` (a
hidden owner holds a matching route), `routes-unknown` (an owner's route set
has never arrived) and `session-moving` (a session is leaving this node and
its route is still advertised). The same family carries the causes a forward
fails for once it has been dispatched, among them `sequence-gap` — the
receiver saw a sequence above the one it expected and reported the frames
between as missing — and `receiver-refused`: the receiver refused that one
forward on its own account (it reached no session there, `no-local-subscriber`
on the receiver; a session on its filter was moving away, `session-moving`; a
record the delivery wrote there failed to append, `not-journaled`; or the
receiver has just restarted and does not serve its sessions yet,
`not-serving`), which
fails that publish and no other; and `give-refused`: a group's owner handed the
message on to a third node that did not hold it, which also fails that publish
alone. A failure the
receiver cannot place on one frame (a gap, a durability wait that failed)
fails every forward to it up to that sequence. `hoardmq_forward_late_frames_total` counts the other
half of that: a frame arriving *below* the expected sequence, which is a
frame a gap already reported missing turning up late. A gap with no late
frame behind it is a loss; a gap the late frame follows was a reordering,
and the publish it refused had in fact been delivered. A sender admits its
forwards to a peer in sequence order, and two 10,000-client leader-crash
failover runs reported neither a gap nor a late frame
([docs/evidence/forward-order.md](evidence/forward-order.md)). A late
frame does not lower
the sequence expected next, so one reordering reports one gap. A frame
below the expected sequence that fills no reported gap is the sender's
count starting again after a restart, counted by
`hoardmq_forward_stream_restarts_total`.

## Building and testing

Go 1.26.4 or later (`go.mod`); no Makefile, the Go tool is the build system.

**Lint** runs golangci-lint `v2.13.3-0.20260913112253-5573d88ff782` under Go 1.27.1, the pair the reference host
uses. Install it with the Go that runs it:
`go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.3-0.20260913112253-5573d88ff782`. A
golangci-lint built with an older Go than the toolchain it runs under cannot read that toolchain's export data and
fails on every package.

**Loopback aliases:** a few simulation tests bind or dial from other loopback addresses (`127.0.0.11`, `.12`,
`.21`–`.23`). Linux routes all of 127.0.0.0/8 to `lo`. Elsewhere they skip, naming the address, unless it is added
(macOS: `sudo ifconfig lo0 alias 127.0.0.11`).

```
go test -timeout 30m ./...                                              # the sim package alone runs for about sixteen minutes, past Go's default of ten: without this the gate panics
go build ./...                                                          # the test gates all compile the test-augmented package, so only this catches a non-test file referencing a _test.go declaration
go vet ./...
golangci-lint run ./...
go run ./tools/licensecheck
go generate ./... && git diff --exit-code NOTICE
go test -run '^$' -fuzz=FuzzDecodeV5 -fuzztime=60s ./internal/packet    # and V4, V3, FuzzConnect
go test ./deploy/...                                                    # Helm chart self-check
go test ./cmd/hoardmq-build                                             # builds the example module into a binary with hoardmq-build, offline, and runs it
SINKKAFKA_LIVE_BROKERS=… SINKKAFKA_LIVE_CONTAINER=… go test -run '^TestLiveKafka$' ./modules/sinkkafka   # sink.kafka against a real Kafka (see docs/modules.md, "Kafka forwarder")
go test -timeout 30m ./sim                                              # simulation harness: seed corpus, negative controls, 1k-client failover
HOARDMQ_SIM_RANDOM=20 go test -timeout 60m ./sim -run TestCorpus        # nightly: random seeds, each printed
HOARDMQ_SLOWPEER=1 go test -timeout 10m -run '^TestSlowPeerAndTheLeadersOtherSends$' ./internal/cluster/consensus/etcdraft   # slow-peer measurement: one follower's link from the leader delayed; the other follower's send gaps and how each election went
podman build --build-arg VERSION="$(git describe --tags --always --dirty)" -t hoardmq:dev .   # the reference image (see docs/operations.md, "Kubernetes deployment")
```

`tools/gatechain/chain.sh <host> <wi> <out-dir> <steps-file> <commit>` runs a list of these, one after another,
detached. It records its PID and each step's exit and wall time, and it stops before the next step once
`<out-dir>/stop` exists. Before every step it asks `reeve claims <host> --wi <wi>` itself, and runs nothing without a
live claim of that work item on the host; then it reads the checkout the step runs in, and runs nothing unless that is
`<commit>` (the full hash) with no tracked change, so a step that changes a tracked file ends the chain at the next. With `CHAIN_REMOTE` the steps run on another host over ssh. The comment at its top
describes the files it writes.
`tools/gatechain/sync.sh <host> <wi> <base>` carries the work item's branch to that host the same way: without a
live claim it moves nothing and exits 3.

`go run ./tools/breakrun -spec <breaks.json> -out <dir>` runs a break pass. For each break it applies the edits,
runs the named tests and puts the files back, also on SIGINT or SIGTERM, and it refuses a tree whose spec'd files are
already changed. It reports each break `caught` (the log holds the break's `expect`, or any test failed when it
names none), `failed-elsewhere`, `survived`, `did-not-build`, `ran-no-test` or `anchor-miss`. A `TestMain` failing
with the expected message is caught. The comment at its top gives the spec's form.

`go run ./tools/astcmp -repo <worktree> -base <rev> <file>...` proves a change touched comments only. It parses
each Go file at `<rev>` and in the working tree (or at `-head <rev>`) without its comments, and compares the two
syntax trees with every position cleared; directive comments (`//go:`, `//nolint`, `//lint:`, `//line`,
`//export`) count as code. It prints `same` or `CHANGED` for each file and exits 1 if any changed or could not be
read. `-strings <list>` names string literals allowed to differ, one `path:baseLine:headLine` a line, and each named
one must differ.

The gates and the dependency policy behind them are in
[docs/dependency-policy.md](dependency-policy.md). The race detector
is in no gate: `-race` runs only in a work item that tracks a race, over the
package that showed it, and in a negative control whose subject is a race
(same document). The broker compiles in the adopted Raft core
`go.etcd.io/raft/v3` (Apache-2.0) and its `google.golang.org/protobuf`
(BSD-3), and the Kafka forwarder's client `github.com/twmb/franz-go` with its
`kmsg`, `klauspost/compress` and `pierrec/lz4` (BSD-3), all listed in NOTICE;
the conformance
suite imports the two Paho Go clients (EPL-2.0, test only, pinned) and what
they bring in, none of which reaches the binary.

A test that starts nodes through the broker package's fixtures or the
simulation harness writes their debug logs to `hoardmq-nodes-<test>-*.log` in
the temporary directory (`GOTMPDIR`, else `TMPDIR` or `/tmp`). A failing test
keeps the file and prints `node logs kept: <path>`; a passing one removes it.
`HOARDMQ_TEST_LOG=1` sends the broker fixtures' logs to stderr instead, and
[sim/README.md](../sim/README.md) lists the simulation's own switches.
