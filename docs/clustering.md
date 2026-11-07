# Clustering

This page is the design rationale for HoardMQ's cluster: how its nodes talk, agree, own clients, route and replicate,
and why each rule is the way it is. An operator running a cluster needs [operations.md](operations.md) first, which
says what to watch and what to do, and points into this page where a failure is explained.

## Cluster transport

Nodes talk over two authenticated, framed connections per peer pair, both
opened by the node whose name sorts lower and accepted by the other. The
**control** connection carries the liveness and coordination classes —
heartbeats, lease renewals and notifications, takeover requests, drain and
redirect signals, and raft messages (appends bounded to a fixed size) — and
the **data** connection the bulk classes: replication, forwarded publishes,
route updates, takeover acknowledgements, raft snapshots, and admin calls
forwarded to the node that holds their client, with their answers. The data
connection is the ordered stream the publish-order guarantee rests on:
forwarded publishes from one node to a peer arrive in the order they were
admitted, whichever goroutines sent them. On every connection the reader
only decodes and hands each frame to its class's own queue and drainer, so a
handler that takes long delays its own class and nothing else; a node is
never declared silent because its peer's handlers were busy. `cluster.listen` binds the peer
listener; `cluster.peers` names the other nodes; `node.name` must be set and
`cluster.id` must agree, or the peer is refused. `cluster.auth.mode` is
`mtls` (each node presents a certificate issued by the cluster CA in
`auth.tls.caFile`, the certificate's common name must be the node's
name and its extended key usages must include both client auth and server
auth, and every frame travels inside TLS) or `secret` (both sides prove a
shared secret from `auth.secretFile` with an HMAC over fresh nonces). In
`secret` mode only the handshake is authenticated: the frames after it carry
no MAC and are not encrypted, so a host on the path between two nodes can
read, alter or inject raft, replication, forwarded and admin traffic. Use
`secret` only on a network you trust; use `mtls` everywhere else. With peers
or DNS discovery configured and no mode named, the mode is `mtls`; a node
with neither needs no mode. Each node both dials and accepts, so a
certificate that lists only one of the two usages, only "any", or none (which
X.509 reads as any) is refused by its peers: logged at Warn as `cluster peer
refused` with reason `certificate` ("certificate for … lacks the client and
server auth extended key usages", or the TLS library's usage error) and
counted in `hoardmq_cluster_refusals_total{reason="certificate"}`, as is any
other certificate a peer's TLS check refuses. Issue node certificates with
both usages (`extendedKeyUsage = serverAuth, clientAuth` in OpenSSL); the Helm
chart's do. Until a
peer has authenticated, a node reads no frame larger than the largest hello
the handshake encodes (131,140 bytes), whatever `cluster.maxFrameBytes` is. The
framing is versioned; peers negotiate the highest version both support, so
a rolling upgrade to a newer framing keeps the cluster talking. Framing
version 2 added an urgent lane beside the ordered stream (heartbeats and
lease renewals written and flushed ahead of queued data); version 3 added
the second connection: a version-2 peer is served on one connection carrying
every class, with the same per-class dispatch on the receiver, until it is
upgraded. Version 4 added the forwarded admin calls; a node on a link below
it answers an admin call for that peer's client with 409 naming the peer.
Version 5 added the snapshot transfer; a peer on a link below it cannot be
sent a snapshot by a newer leader (counted `reason="peer-version"`, retried
after the backoff) until it is upgraded. Heartbeats every `cluster.heartbeatMs` on each connection carry a
stress level; `cluster.heartbeatMisses` silent intervals on the control
connection mark a peer down (the data connection allows ten times that,
since a slow data reader is back-pressure, not silence), and the dialing
side reconnects with backoff for as long as the node runs. A data connection
that resets is redialed without the peer going down; the forwards and
replication frames it carried are failed to their senders and the peer's
replicas re-synced. A node with no peers configured is a valid one-member
cluster.

## Consensus core

Every node runs a consensus core, a one-member group when it has no peers
(there is no single-node mode). The core is `go.etcd.io/raft/v3` behind
the project's own interface: propose a command and get its result once
applied, an apply callback in log order, snapshots in a format the project
owns, membership changes and a leader hint. The replicated state machine
holds the membership (names, ids, addresses, roles), a cluster-wide
configuration set and a monotone epoch, and three components: the lease
table (with the cluster feature level and each node's record), the shard map,
and the `records` component (client credentials, API keys, bans and
administered limits). Its log is `node.dataDir/logs/raft`, on the same
write-ahead log engine as the session state; hard state and entries are
written and synced before any message leaves the node, which the tests
show with an instrumented store.

A node with a log resumes from it. A node with an empty log and configured
peers asks them to admit it; the leader adds it as a learner, it catches up,
and the leader promotes it to voter once its log is within
`cluster.raft.promoteLag` entries of the commit index. If no peer answers
within `cluster.raft.joinTimeoutMs`, only the designated node bootstraps a
new group: the lowest-named node among itself and its peers, or the one
with `cluster.raft.bootstrap: true`. A member that comes back with an empty
log (a wiped volume) is removed and re-admitted as a learner rather than
starting a second group. A node whose window passes after an answer has named
a leader keeps asking to be admitted rather than bootstrapping, since a group
exists: while it waits it is not ready and admits no client,
`hoardmq_raft_join_wait_seconds` is the time it has waited (0 once it has
joined or bootstrapped), and a warning naming the wait repeats every 30
seconds. A snapshot is taken every
`cluster.raft.snapshotEntries` applied entries and the log is compacted
behind a `snapshotTrailing` window. A snapshot is a file in a directory
beside the raft log (`logs/raft-snapshots` under `node.dataDir`); the raft
log records which file. The loop captures the state at one applied index,
which costs a pointer per lease, and a builder writes the file off the loop,
so applies, heartbeats and elections carry on meanwhile. A follower that
needs a snapshot is sent the file on the data connection in 256 KiB chunks
with at most four unacknowledged, so anything else queued to that peer waits
behind at most 1 MiB of it; the follower writes it to a file, checks its size
and SHA-256, decodes it off its own loop and installs it by a swap. No frame
grows with the snapshot, so `cluster.maxFrameBytes` does not bound how many
clients or credentials a cluster holds. A transfer with no answer for ten
election timeouts fails, one that is making progress has no deadline, and a
failed one is retried after a backoff that starts at one election timeout and
doubles to thirty. While a transfer is in flight the leader keeps its log
back to that snapshot, so the follower can catch up from it; the hold ends
with the transfer (`hoardmq_raft_log_pinned_entries`). Builds and transfers
are counted (`hoardmq_raft_snapshot_builds_total{outcome}`,
`hoardmq_raft_snapshot_transfers_total{direction,outcome,reason}`, with
bytes, seconds and the transfers in flight), and a failed or refused transfer
is logged at Warn on both sides with its reason. A node holds the current
snapshot and, while it builds the next, a second: size its volume for twice
`hoardmq_raft_snapshot_bytes` beside the logs. A node that restarts, or installs a
snapshot, is **recovering** until its applied index reaches the commit index
the leader announces (or, for a node the leader has nothing to send, a few
heartbeats confirm it is current): the state-machine components apply the
replayed history silently and are told once, when recovery ends, to
reconcile against the final state — the replicator reconciles its copies
against the current shard map on one worker, the lease service starts the
timers for the offline leases it owns and seals local sessions whose lease
moved while the node was down (see "Leases and session takeover"). **A
recovering node admits no client**: a CONNECT is refused with Use Another
Server and the leader's address when it knows one, otherwise Server Unavailable
(`hoardmq_lease_ops_total{op="acquire",outcome="recovering"}`), because its
lease table is the one it had when it stopped and may name it owner of a
session the cluster has since given to another node. A single node is
its own only voter and elects itself as soon as it restarts, so there the
refusal lasts milliseconds. Acting on each replayed change would act on
states the node had already acted on before it stopped (a durable session
has been lost this way). `hoardmq_raft_recoveries_total{kind}`
counts them; a snapshot arriving at or below the applied index is installed
and counted as `hoardmq_raft_snapshots_total{direction="in-stale"}`, never
refused. `cluster.discovery.mode` is `static`
(the peer list) or `dns`: with `discovery.dns` (`service`, `name`,
`replicas`, `port`) the peers are `<name>-<i>.<service>:<port>` for every
ordinal below `replicas`, this node's ordinal coming from `node.name`, as a
StatefulSet behind a headless Service provides; a name that does not
resolve yet is retried, never treated as absence. `/readyz` reports ready
only when the core is a member that has caught up with the leader, and not
while it is behind (see ["Drain and readiness"](operations.md#drain-and-readiness)). Raft
messages arriving from the transport wait a bounded time for the core's
inbox when the consensus loop is behind — five raft ticks, one sixth of the
control connection's silence limit — and are then dropped and counted
(`hoardmq_raft_dropped_frames_total{type}`), so a stalled apply loop holds
the consensus class's drainer for at most that bound per message and never
leaves the control reader blocked behind a full class queue; a snapshot message is
never dropped while the core runs, because the leader holds the follower
until it hears that the snapshot arrived.

Going out, each peer has its own queue of raft messages (512) and its own
sender: the consensus loop saves a batch, then marshals its messages and queues
them without waiting, and each sender hands its peer's messages to the
transport in order, each waiting up to an election timeout for room. So a
follower whose link drains slowly holds up only its own messages, not the
leader's heartbeats to the others. A message that finds its peer's queue full
is dropped and the peer reported unreachable, and raft probes that follower one
append at a time until it answers. Per peer: `hoardmq_raft_send_queued{peer}`
(messages waiting), `hoardmq_raft_send_seconds_bucket{peer,le}` with `_sum` and
`_count` (the time each send waited for the transport) and
`hoardmq_raft_send_dropped_total{peer}`. Snapshots keep their own transfer.

## Leases and session takeover

Every client identifier is owned by exactly one node, recorded as a lease
with an epoch in the consensus core. A client that connects to the node
that already owns it renews locally, without a round trip — the renewal
follows in the background, names the epoch the node saw, and applies only
if the lease is still that node's at that epoch, so one that arrives after
the lease has moved changes nothing
(`hoardmq_lease_ops_total{op="renew",outcome="stale"}`); a client that
connects elsewhere takes its session over: the lease's epoch rises, the
previous owner closes that client's connection with Session Taken Over and
hands the session state to the new owner, which resumes it before the
CONNACK. The new owner writes the session whole — a delete, then its state
— so a replica that still held the old copy replaces it rather than merging;
and a peer's earlier refusal of a record for that session (a stale-epoch
write from the old owner) holds barriers only until that whole write is
sent. A client that comes back with Clean Start to a node holding no session
for it, while its lease exists, is handed nothing, so that node writes the
session's delete at its new token before the new session, persistent or not:
no replica keeps the old subscriptions or queue under the new session. Lease
operations from one node are batched into one proposal, so a reconnect storm
costs log entries rather than round trips.

**The epoch is a fencing token.** It is the log index of the command that
last changed the lease's owner, so it is the same on every node and never
goes backwards — a lease released and acquired again is granted above
anything it had before, where a counter would restart at one. (Operations
batched into one command share its index; a command carries one node's
operations, and one that would grant a client to two nodes is refused at
apply.) What matters is who compares it with what. Each node keeps the
highest token it has **seen** for each client — from its lease table, from
the result of its own acquire, from a replication frame or a takeover
request — because the table's view lags the grant: a node is told its
acquire succeeded before its own table has applied it. A follower refuses a
session record whose epoch is below the highest it has seen, whatever its
table says (`hoardmq_lease_stale_token_total{where="replication"}`). A
previous owner asked for a session by several nodes hands it to the one
holding the highest token and to no other; a requester below the highest
token seen is answered *superseded*, the session is not given up on its
account, and its CONNECT is refused (`where="takeover"`, `where="acquire"`).

A node below the cluster's feature level neither adopts a dead owner's
leases, nor raises the level, nor keeps leadership: it serves nobody, so what
it adopted nobody could reach, and the leader takes the leases it owns
(below, "Upgrades and the cluster feature level"). A node that replayed a reassignment during recovery — where no
adoption is queued, because replay acts on nothing — queues one for every
offline lease it owns and holds no session for when recovery ends
(`hoardmq_lease_adoptions_recovered_total`).

**A fetch nobody answered is not a session held nowhere.** A new owner that
cannot get a session from its previous owner fetches a replica, and the
fetch says whether every node it asked replied. When one did not and no copy
came back, the client's CONNECT is refused with `0x89 Server Busy`
(`hoardmq_lease_ops_total{op="acquire",outcome="fetch-unanswered"}`) rather
than served a fresh session: serving it would also drop this node's own
replica copy, which may be the only one. The adoption stays for the client's
retry, and the adoption worker fetches again on its own — after 500 ms,
doubling to 8 s, six times — so a client that never returns does not leave
the lost node's routes hidden. When those retries are spent the adoption is
given up and logged with the lease's evidence
(`hoardmq_lease_adoptions_abandoned_total`); the next CONNECT is then served
fresh. If the give-up's own fetch found a copy that a former holder kept, and
held back from it only because a replica source did not answer, the worker
installs that copy instead, as an adoption's import does, and the client is
served it (`hoardmq_lease_adoptions_kept_copy_total`, `adopt_gave_up_kept_copy`,
logged at Warn with the copy's holder). It loses at most what was written after
it, where a fresh session loses everything. If the silent source held a newer
copy, the client can be given again a QoS 2 message it already completed; that
needs the source up and silent through the whole retry window. Only a fetch that waited on nothing alive by the log, such as a replica
that did not answer, counts towards those six; a wait on a previous owner or a
down replica holder the log says is alive has no such end (below). A fetch that every node answered with "not found" serves a fresh
session at once — with the exceptions below for nodes that could not be asked.

**A previous owner or replica holder that could not be asked is not an answer
until it is declared dead.** The previous owner is asked whenever the new owner
has a control connection to it, the one a takeover request travels on — so a
restarted owner is asked before its data connection is back, and so is one the
leader has declared dead, whose log may still hold the session (counted as
`pulls_control_only` when the data connection is not up). With no connection
to it, the new owner cannot ask it, and until the leader declares that owner
dead it may still hold the session: a partition that heals before the
declaration gives the session back. So a takeover or adoption that finds no
copy anywhere, with such an owner, is refused with `0x89 Server Busy` and the
adoption kept, as for a fetch nobody answered. Once the owner is declared
dead, the next fetch serves a fresh session, counted as below. A previous
owner that can be asked but does not answer is judged the same way while the
log says it is alive (`adopt_owner_silent`). The adoption worker never gives
such an adoption up on time: a previous owner that gave the session away keeps
it, sealed, for as long as nothing shows the session held elsewhere, so the
wait ends only when the owner answers, is declared dead, or the lease moves. A
copy found anywhere is served at once, whatever the owner's state. Once the
adoption is older than twice `cluster.leases.ttlMs` with the owner still alive
by the log, the leader, which declares any node it has not heard from for a
ttl, can reach that owner; so a CONNECT refused for the wait from then on is
answered `0x9C Use Another Server` naming the leader, whose CONNECT takes the
lease over and asks the owner itself (`0x89 Server Busy` on the leader, or
when no leader's address is known). The wait is logged once at Warn when it
passes that point, naming the node waited on, and counted
(`hoardmq_lease_adoption_waits_total{on="owner"|"silent-owner"|"holder"}`,
`adopt_waits_past_bound`, `adopt_waiting_now`), and so are the redirects
(`hoardmq_lease_ops_total{op="acquire",outcome="adoption-redirect"}`,
`adopt_redirected`). A client that cannot follow the redirect (MQTT 3.1.1),
or reaches only this node, is refused until the owner can be asked or is
declared dead, as a CONNECT for a live owner this node cannot reach always is.
Counted in the failover artifact as `takeover_owner_undeclared` (and
`…_served`) and `adopt_owner_undeclared`. A replica holder
of the session — one of the previous owner's replica targets under the current
shard map — that the fetch skipped because it was down is judged the same way:
while the leader has not declared it dead it may come back with its copy, so a
fetch that found nothing is refused and the adoption kept, with no time end:
the wait ends when the holder answers, a copy is found, the holder is declared
dead or the lease moves. It is judged ahead of a replica that did not answer,
whose retries would otherwise end the wait while the holder still has the copy,
and past twice `cluster.leases.ttlMs` a refused CONNECT is sent to the leader as
for an owner (`takeover_holder_undeclared`, `adopt_holder_undeclared`,
`{on="holder"}` above). With no leader, as when the holder's failure leaves the
survivors without a quorum, the client is refused until the holder returns or is
declared, and meanwhile the forget of any other node lost waits for this node's
adoptions to finish. A holder declared dead is not waited for. Once the shard map
no longer holds the previous owner, as after its declaration, the owner's targets
under that map are not the nodes it replicated to, and the copy it fed may be on
any member; so a fetch under such a map judges every member it could not reach
because it was down, the previous owner aside, as such a holder (counted in the
failover artifact's `replication_fetch` as `none_peer_down`).
A holder, or another node that may keep a copy, that is back on its control
connection while its data connection — the one a fetch travels on — is not up
yet is not down: a restarted node in its first moments, or one back from a
partition, can be declared dead by a new leader and still hold the session.
The fetch cannot ask it yet and reads unanswered, so the CONNECT is refused and
the adoption kept as for a fetch nobody answered, until its data connection is
up and the next fetch asks it (`hoardmq_replication_fetches_total{outcome="none-returning"}`).

A client refused that way may retry on another node, which takes the lease from
the node that refused it. That node never held the session, so it answers the
pull with no state — and names the owner it was still waiting on. It names it
from its unfinished adoption, or, when its adoption worker has already found the
lease gone, from a note kept for twice `cluster.leases.ttlMs`. The retry can
also arrive before the first node has refused, while its own takeover is still
waiting on the previous owner, or after its import was refused for the later
grant: a CONNECT that is not a clean start notes the owner it takes the lease
from as it proposes the acquire and again on the grant, so that node names it
too. The new owner,
finding no copy anywhere, judges the named owner as it judges its own previous
owner: not declared dead in its own table, so it refuses with `0x89 Server
Busy` and its adoption waits on that owner from then on — pulling it when it has
a link, otherwise until the declaration. The same holds when a retry's adoption
fetch pulls a node that names an owner. The name is an optional member of the
takeover answer: an older build ignores it, and a newer node given an older
answer judges as before, so with an older node on either side the takeover
behaves as the older one does. Counted as `takeover_answered_pending` (of which
`…_noted` from a note, `…_taking` from a takeover's note, and
`pending_owner_notes` held, both kinds) on the node that answers, and
`takeover_pending_owner` (and `…_served`) and `adopt_pending_owner` on the node
that takes the lease.

**A session found nowhere is served empty, counted and logged.** When every
node asked has answered that it holds no copy of a session the client's lease
says existed, the client is served a fresh session: Session Present false, its
subscriptions and queued messages gone. Refusing could not bring back state
that no longer exists anywhere, and would only repeat. Each such CONNECT is
counted in `hoardmq_sessions_rebuilt_empty_total{reason}` and logged at Warn
("session served empty although its lease says one existed") with the client,
the reason, the lease's last-known owner and the token it is now served at; a
takeover among them also counts `hoardmq_lease_takeovers_total{kind="empty"}`.
On a cluster that lost fewer nodes than it keeps copies the count should stay
at zero, so any count is worth reading. The reasons: `takeover` (taken over
from an owner that had nothing and named no owner it was waiting on, or that has
been declared dead, and no replica had a copy), `adoption` (a dead owner's
lease adopted here, and no copy anywhere),
`adoption-ended` (the lease is offline here and its adoption ended earlier
without a copy — or the session expired and the lease is not yet released,
the one way a count can include a session that ended by design),
`grant-unfetched` (the lease was granted here by an acquire whose CONNECT
never got a session here, and nothing recorded that it gave up), `raced`
(the lease became this node's while the CONNECT's own acquire was in flight),
and `no-expiry` (any of these for a lease that, when the CONNECT looked it up,
carried no session expiry: such a session ends with its connection, so this
one is no loss and is counted apart). A clean start and a client's first
connect are never counted.

**A CONNECT looks for an adoption only once its node has recorded it.** A
drain's hand-off or a dead owner's reassignment records the receiving node's
adoption when that node applies it. A CONNECT on a follower can be answered by
the leader before then — its acquire applied after the hand-off — and a
CONNECT whose lookup ran mid-apply can find the lease already its node's
before the adoption is recorded. Either would be served Session Present false
for a session the adoption then imports, and a client that discards its state
on that answer, as MQTT 5 requires, can then lose a QoS 2 publish to an
identifier the restored session still holds. So such a CONNECT waits until
its node has applied that command, usually milliseconds; past
`cluster.leases.takeoverTimeoutMs` it is refused `0x89 Server Busy`
(`hoardmq_lease_hook_waits_total{outcome="served"|"refused"}`,
`hoardmq_lease_ops_total{op="acquire",outcome="hooks-behind"}`).

**A second CONNECT waits for the first's takeover on the same node.** A
CONNECT that takes a lease from another node pulls the session and imports it
after its acquire. A second CONNECT of the same client on that node — a client
that gave up on its first attempt and connected again — would otherwise find
the lease already its node's, be served Session Present false (with the QoS 2
loss above for a client that discards its state), and then be disconnected
(`0x8E`) by the first's import, which leaves the session on the connection the
client abandoned. So it waits until the first has installed the
session, and is then served it and displaces the older connection, as MQTT 5
requires of a newer connection (MQTT-3.1.4-3). Of two CONNECTs waiting on one
takeover, the later to arrive is served and the earlier refused `0x89 Server
Busy`. Past `cluster.leases.takeoverTimeoutMs` a waiting CONNECT is refused
`0x89 Server Busy` (`hoardmq_lease_takeover_waits_total{outcome="served"|"refused"|"overtaken"}`,
`hoardmq_lease_ops_total{op="acquire",outcome="takeover-in-flight"|"takeover-overtaken"}`).

**A lease granted after its CONNECT gave up is fetched before it is served.**
A CONNECT that runs out of `cluster.leases.takeoverTimeoutMs` while its
acquire is queued or proposed is refused, but the acquire can still be
granted — after a leader crash, when the proposal waits for an election —
moving the lease here with nobody fetching the session. The refused CONNECT
leaves a note, and the client's next CONNECT here treats the lease as
adopted: it asks the previous owner, then the replicas, before it is served,
with the same rules as any adoption (an unanswered fetch refuses with
`0x89 Server Busy` and is fetched again on the retry; "not found" everywhere
serves a fresh session, counted as `adoption`). A session this node already
holds is never fetched over. A refused first CONNECT, which found no lease,
leaves a mark instead: if its acquire then creates the lease here and the
client's next CONNECT here finds it still at that grant's token, the client
never had a session, so nothing is fetched and nothing is counted served
empty. If the acquire instead took the lease from another node — the client
may have connected there meanwhile and been given a session — the client's
next CONNECT here fetches from that node, then its replicas, as above, and a
takeover pull that reaches this node first is told that node's name, so the
taker asks it too. A lease that moved since is counted `grant-unfetched` as
before. Counted in the failover artifact as `acquires_abandoned` (first
CONNECTs included), `grants_unfetched_adopted`, `first_grants_unfetched` and
`first_grants_taken_adopted`.

**A session handed to a takeover is sealed, not deleted, until the new owner
confirms it.** The previous owner's pass takes the session out of reach — its
connection closed with Session Taken Over, its routes removed, so a publish
forwarded to it is reported undelivered and a CONNECT here is refused with
`0x89 Server Busy` — but writes nothing to its log, so its log and every replica
copy of the session stay as they were. The new owner confirms once it holds the
session at its grant and its records are acknowledged by its replicas; only then
does the previous owner drop its sealed copy and log the session's delete. So an
answer that is lost or late, or a previous owner that dies after answering,
leaves the replicas the new owner falls back to; a delete written at the
hand-over could reach a replica ahead of the grant and remove the only copy. The
node's own checkpoints carry its sealed copies, so a restart finds them in its log
until their deletes are written; a resync or a rebalance never sends them. A requester
asking again is answered from the sealed copy. If the lease comes back to the
previous owner, the sealed copy is what it resumes, used only when no other node
has the session. A seal nothing confirms is never dropped because time has passed:
the new owner may be alive and still unable to fetch it, and the sealed copy can be
the only one. Every twice `cluster.leases.ttlMs` the previous owner asks the lease
owner's replica targets, with the replica fetch, whether one holds the session at
the owner's grant (its own replica copy answers when it is one of them); it drops
the seal, with its delete, when one does or when the lease no longer exists, and
keeps it while the lease is back on that node. With one configured copy the owner
has no targets, so an unconfirmed seal is kept until it is confirmed, used, or its
lease ends. At most eight such checks, together with the replica copies' checks
below, are in flight on a node. The
confirmation is a message on the takeover-request class, marked by its first
byte, which an older build ignores; with an older node on either side the
hand-over behaves as the older one does, and nothing is lost. The new owner also
keeps an answer that arrives after it stopped waiting, while it has seen no
higher grant for the client — for the client's retry, for its adoption worker,
or for the node that takes the lease next — never putting it over a session it
already holds, and confirms it once imported. The deferred reclaim of a session
whose lease has left, when no pull came for it, seals it the same way; a session
whose lease no longer exists is evicted. A node's recovery seals the same way every
session its log holds whose lease another node took while it was down: that node may
not have fetched it yet, and the restarted node's log can hold the only copy (a
drained node whose receiver's pull came after it stopped, while its replica target
was down), so the copy waits for the new owner's pull and confirmation, or the
check above. Counted in the failover artifact: `sealed` (of which `sealed_by_reclaim` and `sealed_by_recovery`), `sealed_now`,
`seals_confirmed`, `seals_expired` (ended at a check: `seals_held` on a target holding the session,
`seals_lease_gone` on the lease's end; `hoardmq_lease_seal_ends_total{end}`), `seal_checks_held` and
`seal_checks_not_held` (`hoardmq_lease_seal_checks_total{outcome}`),
`reclaimed_on_confirm`, and the confirmations sent (of which
`confirms_sent_holders` went to the other live members the new owner does not
replicate to, besides the previous owner and a moved session's old targets; see
`owner-unwritten` below), received, and dropped as
never held, never durable or not delivered, and those acted on (`confirms_acted`) with their time from receipt to
the drop or eviction (`confirm_act_ms_total`, `confirm_act_ms_max`). A node acts on up to 64 confirmations at
once: each waits for the journal fsync covering its delete, and a node loss sends the adopter them in a burst,
so with fewer the moves they end would keep refusing behind the queue;
`hoardmq_lease_takeovers_total{kind="given"}` counts takeovers served from the
node's own sealed copy.

**A fetched session is installed only while no later grant for its client has
been seen, and a pass never answers across an install.** An adopted session's
state, or the state a CONNECT's takeover brought back, is installed only if
this node has seen no grant for the client above the one the state was fetched
for. The check and the start of the install are one step under the session's
lock, and a takeover pass that finds an install in progress waits for it and
seals what it installed. Without that, a pass could answer "no state" while the
adopter was still installing the session, whose write then dropped the replica
copy the new owner's fallback needed. A refused install writes nothing, so that
copy stays. A CONNECT whose state is refused this way is answered `0x89 Server
Busy`. Counted in the failover artifact as `adopt_import_refused` and
`connect_import_refused`, and, per node, `seal_waits_import` (Seals that waited
for an install) and `import_waits_import`. A trace stays beside them:
`adopt_import_after_empty_answer` (an install that followed an empty answer for
the same client, which should read zero) with a capped `import_race_sample`,
`reclaimed_sample` (offline sessions a confirmation evicted), and the worker's
widest gap from its lease check to its install and longest install.

**A session installed from a hand-over is not given again a message it
arrived holding.** A publish on a session's owner is routed into the session
there and forwarded to every other node with a subscriber for its topic. If
the session is then handed to one of those nodes, the answer carrying it is
handled on arrival while the forward waits in the routing plane's queue for
that peer, so the forward can be applied after the session is installed. The
installed session remembers the messages its state held — by the node each was
published on, that node's sequence for it and its arrival time — and a routed
copy of one of them is skipped: the session keeps the one it has, and the
forward still counts as delivered, since it was. Counted in
`hoardmq_deliveries_duplicate_total{namespace}`. A session recovered from the
node's own log remembers nothing, and a retained message sent again on a
SUBSCRIBE is not checked. A message the session was sent on the old owner and
completed there is no longer in its state, so the old owner, when it forwards,
remembers every message it sends each session for 10 s (at most the last 64)
and hands those over with the session; the new owner skips a routed copy of
them the same way. A forward later than that, or a session sent more than 64
messages in the window, can still be given one again; so can a session taken
over from a replica after its owner crashed, since the list is not replicated.

`hoardmq_lease_proposals_total{op}` counts what this node asks the log to
carry, by kind; `renew` is an acquire that names an epoch — a reconnect to
the same owner — and with `disconnected` is the traffic that changes no
ownership.

Leases are time-fenced. Each node renews all of its leases at once by
recording that it is alive every quarter of `cluster.leases.ttlMs`; a node
whose record has not been renewed for the ttl has lapsed.

Whether a node has lapsed is decided once, by the consensus leader, and
published in the log. The leader declares a node dead when it has itself
applied no alive record from that node for a ttl and a thirty-second of a
ttl more (937.5 ms at the default), measured on its own monotonic time — not
by comparing any clock with the time stamped in the record, which another
leader may have written on another clock and which a step of the wall clock
would misread. When its quarter-ttl tick finds a node past the ttl but inside
that thirty-second, it looks again as soon as the thirty-second ends rather
than at its next tick. A follower watches the same records, so a new leader
judges from what it saw itself; a node that has been watching for less than
that (after a restart, a recovery or a snapshot restore) declares nobody. The
declaration moves the node's leases to the leader, is made for a node that
owns none as well, and is cleared by that node's next
alive record. Every other reader — the connect path, the replica rebalance,
the shard map, a drain choosing its target, readiness's topology — reads
that fact and no clock: a node is alive when it has an alive record and is
not declared dead. `hoardmq_lease_nodes_declared_dead` is how many are
declared now and `hoardmq_lease_declarations_total` how many declarations
this node has proposed. A node's own fence is measured on its monotonic time
too, so a wall-clock step does not disconnect its clients, and it is counted
from when the node's last successful renewal was sent, on a timer of its own
rather than the renewal tick. The leader applied that renewal after it was
sent, so the node fences before the leader can declare it, and the
thirty-second keeps that order with a fence late by as much under load:
no client of a partitioned node is taken over on the majority side before
that node has disconnected it. The times in the
table (alive stamps, disconnect and expiry times) remain the leader's wall
clock and are not compared with another node's for liveness; after a
backward step they stay ahead until real time passes them.

A renewal is its
own proposal on the transport's urgent lane, never part of the batched
lease traffic, bounded by the ttl, with three allowed in flight, so several
attempts fit in every ttl and a storm on the data path cannot starve the
signal; three consecutive failures log a warning before the fence. Because
the fence counts from a successful renewal's send, the in-flight bound sets
how slow renewals may be: up to three quarter-ttl ticks (0.75 × ttl, 22.5 s at
the default) every launch finds a free slot, successes stay a tick apart and
the node stays unfenced; slower than that, launches are skipped and the node
fences. The lease statistics a recorded measurement keeps per node
(`storm.<node>.lease.renew_inflight_peak` in its `run.json`) give the most that
have been in flight at once. A newly elected leader waits one ttl before
judging the record of any owner it can still reach, so every live node gets
a full ttl to renew with it after an election; an owner whose link is down
is adopted as soon as the leader may declare it. The fence has one exception, the
mirror of that grace: a node whose record has lapsed does not fence while
it and the voters it can reach are a majority of the consensus voters (a
learner, or a configured peer that is not a member, counts neither way) and
the group has no leader, or has had one for less than a ttl — with no leader nothing can be adopted or
taken over, so no second owner can appear, and fencing would only
disconnect every client for the length of an election; the deferral is
counted (`hoardmq_lease_fence_deferred_total`). A node that
cannot renew (it has lost quorum) disconnects every client with Use
Another Server, or Server Busy when it knows no other node, and refuses new
connections with Server Unavailable, or Use Another Server and a Server
Reference when the leader's `cluster.advertise` address is known, until it
renews again. The majority side does not take a client over from an
unreachable owner that has not been declared dead, so that a client
identifier is not given a second live session across a partition: an acquire
lists the
members its node has no live link to, and the lease table refuses it where it
applies — against whichever node owns the lease at that point in the log — if
the owner is listed and alive
(`hoardmq_lease_ops_total{op="acquire",outcome="owner-unreachable"}`). A node
whose own lease table already shows the owner alive, and which has no live
link to it, gives that refusal itself and proposes nothing
(`outcome="owner-unreachable-local"`): after a node crash every one of its
clients retries until the declaration, and each retry would otherwise be a
consensus proposal made to be refused. Only the log grants a lease; a node
whose table is behind the declaration refuses a client once more than the log
would have. When
the leader declares an owner dead, it adopts its leases: an offline session's Will is published
after its Will Delay and its Session Expiry fires as if the owner were still
there. Both are kept as durations from the record that moved the lease, run on
the adopter's own timer, so neither the owner's clock nor the adopter's moves
them; a drain's receiver keeps them the same way. Two cases still read a
wall clock: a lease disconnected under one leader and adopted under another
lands early or late by the skew between those two leaders' clocks, and a node
that restarts holding adopted leases times them against its own clock. Keep
the nodes' clocks synchronised. A member that comes back with a wiped volume is re-admitted (see
Consensus core); a client whose owner died reconnects anywhere and resumes
what the lease carries. The queued messages of a dead owner's sessions are
recovered from the followers (see "Data-plane replication").

A takeover asks the previous owner for the session **whenever it can be
reached**, whatever the verdict on it: rebuilding the session from a replica
while an owner that was wrongly judged goes on queueing into a session it no
longer owns loses what it queued. The verdict — whether the previous owner was
alive by the log when the acquire applied — decides whether to wait when a
reachable owner does not answer (until it is declared dead, for at most a
ttl), and whether an owner that could not be asked lets "not found" everywhere
serve a fresh session (only once it is declared dead; see above). An owner that
answers with no session is holding nothing back, and the replica is asked at
once.

The pull and that wait share the CONNECT's budget,
`cluster.leases.takeoverTimeoutMs`, and a pull the owner does not answer
spends all of it. The replica is then asked on a budget of its own,
`cluster.replication.ackTimeoutMs` — the bound every replica fetch already
has — so a CONNECT whose previous owner is silent completes within the two
together (7 s at the defaults), served from the replica when a copy answers
(`hoardmq_lease_takeovers_total{kind="replica"}`). An owner that has not
answered may still hand the session over late, so when no copy answers the
CONNECT is refused with Server Busy and the adoption kept, as for a fetch
nobody answered, rather than served a fresh session.

## Routing across nodes

Each node keeps a cluster route table: which nodes hold subscribers for
which topic filters. A node publishes an entry when a filter gains its
first local subscriber and a tombstone when it loses its last, each with a
version from the node's own counter (seeded from its start time, so a
restart never loses to its own old tombstones); entries go to every peer at
once, a peer that comes up receives the node's full set, and every
`cluster.routing.syncMs` the full set goes to everyone as anti-entropy.
Tables merge by higher version, tombstone winning a tie, so they converge
after a partition heals with unsubscribes honoured.

A full set is **authoritative about absence**: it carries a snapshot version
taken when it was composed, and the receiver withdraws any entry it holds for
that node which the set does not carry and whose version is below the
snapshot. Anything the sender publishes after composing the set has a higher
version and survives it. This is how a node that has restarted retires the
entries its previous incarnation published — it no longer knows they exist,
so it can never tombstone them one at a time, but it can say what it holds
now. For the same reason the full set is sent even when it is empty: "I hold
nothing" is a message, and a node that sent nothing at all would leave its
peers routing to it forever. Which is why a restarting node sends nothing at all,
no entry and no full set, until it has loaded the sessions its store holds:
its table is empty until then, and a set sent from it would withdraw at every
peer the routes of sessions its journal still holds, so that a publish to one of
them would be answered No Matching Subscribers and dropped. A down peer's entries are hidden, not deleted,
and a QoS 1 or 2 publish matching one is refused: that peer's subscriber
cannot receive it. The entries go when the membership removes the node, when
its own next full set retires them, or — for a node that does not come back —
when its sessions are **served** elsewhere. Owned elsewhere is not enough: a
reassignment moves every lease the moment it applies, and the adopter
imports each session afterwards, from a queue; until the import advertises
the session's routes nobody else does, and a publish would be acknowledged
with no subscriber. So a node puts a **served fact** in the lease log once its
change hooks have run up to the index a lost owner was emptied at (the lease
table records it, from the log, so it is the same number on every node) and it
has no adoption unfinished: its hooked index, and how far its own route updates
had gone. A node forgets a lost owner's entries once the table shows that owner
owning nothing, it is itself in that state, and every other member that owns
leases has a fact at or past the index **and** this node holds that member's
route updates up to the fact's mark. The last condition is what keeps the
replacing routes ahead of the forget: the fact travels through consensus and
the routes on the data connection, in either order. For it, every delta batch
a node sends carries the next of a per-node sequence (seeded from its start
time, like the entry versions), and every full set the last number it holds;
a skipped number is a gap (`hoardmq_route_update_gaps_total`), and nothing is
proved from that peer again until its next full set. A member that owns
nothing is not waited for. One that is down no longer holds the refusal by
being down: once its fact and its routes are here, its own hidden entries
refuse for it. A client of the lost node can take its lease from the adopter
before the adopter imported its session; the adopter's adoption then ends
without an import, so each side counts the session as unfinished: the taker
until the session is held there, after which it puts a newer fact in the log,
and the adopter until the taker's fact reaches the grant it took the lease at,
so the adopter's own fact follows it and every node waits for the taker's
routes as of the import. Two survivors can each hand sessions to the other, so
the taker's claim does not wait for its own hand-ons: once every session it
took that way is held or its wait has ended, it puts a **cover** in the log, the grant through which
they are held with its route mark, and a hand-on ends on the taker's cover or
its fact. The forget reads a member's routes up to the higher of the two marks.
Each wait ends at twice the lease ttl at the latest, counted in the lease
statistics (`transit`, with `handed_covered` for hand-ons a cover ended); a
cover never claims a grant at or above that of a take that ended that way.
Each node proposes its fact once per lost owner, whether it holds the owner's
entries hidden or never received them (a node restarted beside the lost owner:
see ["An owner whose routes are unknown"](development.md#simulation-harness)), and again whenever sessions it
took that way are held there (`hoardmq_lease_proposals_total{op="served"}`,
covers included;
`hoardmq_route_served_proposals_total{outcome}` counts how each ended, and a
failed one is proposed again; `hoardmq_route_cover_proposals_total{outcome}`
does the same for covers). A node checks every 500 ms, and sooner when a
fact applies, its own last adoption or a take in transit ends, or a member's
routes arrive.
`hoardmq_route_owners_hidden{reason}` says why a hidden owner is still held
(`leases`; `adopting`, this node's own adoptions and sessions in transit; `unsynced`, a fact or routes
not yet here from a member it can reach, or its own hooks behind the index;
`unreachable`, the same from a member that is down; `unknown`) — a refusal that
will end from one that will not — and `hoardmq_route_owners_forgotten_total`
counts the owners forgotten this way. A lost owner that comes back and sends a
full route sync while a check is deciding its forget keeps that sync: the check
forgets only if no full sync came from the owner since it read it, and counts
the skip in `hoardmq_route_owners_forget_skipped_total{reason="synced"}`. Across a rolling upgrade from a build
without the fact, neither build forgets on the other's word: the older build
reads adoption state from full sets that no longer carry it, and the newer
reads facts the older never proposes, so a node lost mid-roll keeps its
entries hidden until the roll ends. The
tombstones of a session that is *moving* to another node — a lease takeover
or a hand-off, not an unsubscribe — are held back for
`cluster.leases.takeoverTimeoutMs`, so the other nodes keep forwarding to
the previous owner while the new one's routes are still in flight; the
message finds no session there and the publish is refused rather than
accepted with nowhere to deliver it.
The same answer is given when another session on the node still
subscribes to the filter: the forward reaches the subscriber that stayed and
is still reported to the sender as failed (`reason="session-moving"`),
because the session in transit was exported without it. The mark lasts the
grace whatever happens to the filter's count in between — the moving
session's own subscription being removed is a fall in the count, not the end
of the move — and ends early only when the count rises again, the client
coming back. A shared-subscription delivery to a member that stayed is a
complete delivery and is not refused.

The move ends early, for the publishes that can show they reached the moved
session, once the new owner has confirmed it holds the session durably (the
sealed hand-over's confirmation) and this node holds the new owner's route
for the filter. A publish made here is accepted when it was forwarded to the
new owner after the confirmation arrived. A forward from the new owner is
accepted when its publish sequence is at or above the one the confirmation
reported, the first the new owner routed with the session in place.
A forward from any other node carries no such proof — that node routes by its
own table, which may not yet hold the new owner's route — so it is refused for
the whole grace, and the route stays advertised until the grace ends so such
a node keeps forwarding here rather than matching nothing.
`hoardmq_moves_resolved_total` counts the confirmations applied to a move, and
`hoardmq_move_refusals_lifted_total{path}` the publishes accepted this way
(`local`, `forward`). A move of a shared filter, and a move whose confirmation
never comes, last the grace.

A publish is routed locally as before and then forwarded once to each remote
node with a matching subscription, over the ordered stream to that peer in
the order the publishing connection produced it; the receiving node delivers
in arrival order, so the specification's per-publisher ordering holds across
nodes. Forwards are never forwarded again. A shared subscription group that
spans nodes is dispatched by the node holding the most of its members
(lowest name on ties): a publish that matches the group on any other node is
sent to that owner, which applies the configured strategy over the member
nodes and hands the message to the chosen node for one local member, so
the group receives each message once. Forwards between nodes are
acknowledged: each frame carries a per-peer sequence, the receiving node
applies the frames in order and answers once they are durable there (its own
sync and its own followers), and a QoS 1 or 2 publish is not acknowledged
until those answers arrive — a lost frame, a reset link or a receiver that
could not make the message durable fails the publish instead. A late answer
fails it too, and the message may by then have been delivered (see Sessions).
When a group's owner hands a QoS 1 or 2 message on to a third node, its answer
to the publishing node names that frame as given on, and the publish waits
until the owner relays whether the third node held it: the third node's own
answer to the owner, after its durability wait. A give the third node refused,
a third node lost before answering, no answer within
`cluster.replication.ackTimeoutMs`, or a reset of the link the relay would come
over refuses the publish. The owner counts relays as
`hoardmq_group_gives_relayed_total{outcome}` (`held`, `refused`). A node of an
older build on either side of the pair answers and waits as before.
An MQTT 5 acknowledgement of No Matching Subscribers means no node matched
the publish: a forward sent to a subscriber's node counts as a match. Retained publishes are stored by the topic's
shard leader (see "Data-plane replication"), not by the receiving node.

## Data-plane replication

Sessions and retained messages are assigned to `cluster.shards` virtual
shards by a hash of the namespaced client identifier or topic. A shard map
in the consensus core names each shard's leader and followers; the
consensus leader computes it from the members the lease table says are alive
(see "Leases and session takeover"), with `cluster.replicas` copies (default 2, bounded by the
live member count), and proposes a new map only when that basis changes.
A member a drain has marked leaving (["Drain and readiness"](operations.md#drain-and-readiness)) is left
out of the basis while the members left still give the copy count; when they
would not, every alive member stays in, so a drain never lowers the copies.
The map version is exported as `hoardmq_shardmap_version`, the basis's size
as `hoardmq_shardmap_basis_size` and the members marked leaving as
`hoardmq_shardmap_leaving`; a map change's audit record names them.

A node that stops being a session's replica target at a map change, while the
session's owner is alive, keeps its copy until the owner confirms the session
durable at its new targets: the owner sends the takeover confirmation it
already sends a previous owner, once the whole write the map change sent its
new targets is acknowledged. Until then the copy is counted
`hoardmq_replicas_kept_total{reason="moved-target"}`; on the confirmation it
goes, `hoardmq_replicas_dropped_total{reason="not-a-target-confirmed"}`. A
confirmation can be lost (its durability wait is given up after a ttl, and a
failed send is not retried), so a copy kept this way for twice
`cluster.leases.ttlMs` asks the owner's targets, as an `owner-unwritten` copy
does, and goes once one holds the session at the owner's grant
(`not-a-target-held`). No time alone ends the keep, because an owner that died
before it was declared would otherwise leave no copy at all. The rule holds
for a copy written under the owner's current grant on a node the owner does
not replicate to, whatever map it is judged against: a later map change that
moves the shard again, or the node's restart, keeps it the same way. So does a
copy on a shard the node no longer holds whose hand-over the shard's holders
refused, as a holder that already has the session does: it is judged by the
rule of its grant, this one or `owner-unwritten`'s, and checked until it goes.
A copy from an earlier grant than the owner's is kept the same way when the
shard's only other holder is the owner itself, so the hand-over has nobody to
go to: an owner that took the lease by adoption may not have fetched the
session yet, and this copy may be the one its fetch finds. A
publish whose barrier starts before this node's rebalance has sent those whole
writes also waits on the old targets that are up, where its earlier records
went. Confirmations sent so count `hoardmq_lease_confirmations_moved_total`.

Bounded by the live member count, the copies can number fewer than
`cluster.replicas` says: with fewer live members than copies, each shard has
fewer holders, and an acknowledged publish rests on fewer copies than
configured. Two gauges, set at scrape, count the work held so on a node:
`hoardmq_replication_sessions_short`, the persistent sessions it owns with
fewer than `cluster.replicas - 1` targets, and
`hoardmq_replication_retained_shards_short`, the shards it leads with fewer
than `cluster.replicas - 1` followers. Both are counted against the current
map, so they read zero while the map's members number at least the copies,
and a node down but still named by the map is counted as a copy until the map
drops it.

A session lives where its client connects; its owner replicates every
record it journals (metadata, subscriptions, queued and in-flight entries)
to the shard's holders other than itself, up to the copy count, on the
ordered cluster stream. Each record carries the owner's lease epoch —
stamped when the record is written, from the epoch the lease service granted
this node at connect, not looked up when the record is fanned out: a node
journals for a session only while it owns that session's lease, so the
record's own epoch is the generation it was written under, where a lookup at
fan-out time can read a newer epoch belonging to a different owner. A
follower refuses a record whose epoch is behind its own lease table (or names
a different owner at the same epoch), answering rejected — a record carrying
no generation at all is judged by where it came from instead, applied from the
node the table names as owner and refused from any other — and a rejected
record fails the barrier covering it, as a lost frame does, because the
follower does not hold it. It refuses a record it cannot apply at all for the
same reason: one naming a session, or an entry, its copy does not hold says
the copy has diverged from the owner's, so the owner sends that session
whole — once per key per acknowledgement timeout — and the barrier passes once
the follower has acknowledged the repaired copy. The exception is the window between a gap the follower
reported and the resync that answers it, where a record that changes nothing
is expected and is acknowledged, the resync being what repairs the copy.
Followers persist replicas in a separate log under `node.dataDir/replica`,
keep them materialised in memory, and acknowledge after syncing; a refusal is
answered at once and acknowledges nothing, so the frames applied ahead of it
count as held only once the follower's acknowledgement after its sync
arrives. A failed replica fsync may have dropped every write not yet on the
disk, whichever peer's stream it came from, and a later fsync that succeeds
does not write them again: a follower whose batch did not sync, or saw any
fsync of its replica log fail before its own returned, acknowledges nothing
more on that stream and reports it to the owner as a gap, and the owner breaks
and resyncs the stream. The follower acknowledges again once the resync's
marker shows every frame it lost has failed at the owner, and what came since
has synced. The node keeps serving throughout. A QoS 1 or 2 PUBLISH is acknowledged only after the publisher's node
synced its own log **and** every follower its records went to acknowledged
everything sent to it so far. The barrier names the records it covers —
the publisher's session, every session the message was delivered into, and
a retained topic — and waits on their targets under the current shard map: a session's followers
with this node as owner, a retained topic's followers on its shard leader or
the leader a non-leader forwarded it to. A target that is down, one whose
stream was reset with frames in flight, and one that is up but silent for
`cluster.replication.ackTimeoutMs` all fail the publish with Implementation
Specific Error rather than acknowledge without the copies. A node that is
down but still named by the map therefore refuses the publishes whose
records it holds and no others, until the map drops it. The map follows the
declaration: the leader's map driver proposes a map without the node as soon
as the declaration applies, and otherwise checks the members every third of
`cluster.leases.ttlMs`. A record a peer refused fails every barrier naming its key while
the map names that peer, whether or not it is still the key's target, until
the key is rewritten whole or the stream resynced, and a barrier already
waiting then passes only once the peer has acknowledged the rewrite. A node the map has
dropped is waited on by nothing, and one it names again is waited on again
with its stream as it is.
A peer's acknowledgement wakes only the barriers whose records it covers, and
its refusal only those naming the refused key; a reset, a resync or a stop wakes
every barrier on the stream. A stream that carries thousands of refusals a
second during a drain therefore does not hold its own acknowledgements back.
A node that has seen a later grant for a session than the copy it holds
refuses, at once, every publish that wrote into that copy: another node owns
the session now, and a write here reaches no copy the new owner reads, whatever
a replica's lease table says yet. The refusal is counted as
`hoardmq_replication_barrier_failures_total{peer="-",reason="superseded"}`.

For QoS 2 the PUBREC state (the client has the message) is replicated to the
followers before the PUBREL goes out, so a takeover rebuilt from a replica
re-sends the PUBREL, not the PUBLISH — exactly once survives an owner loss.
A QoS 1 message the client had acknowledged can still be delivered again
after such a rebuild, because a client's PUBACK is replicated after the fact
and there is nothing to hold while it travels: QoS 1 permits it, and the
simulation harness's properties state the exception.

Retained writes go through the topic's shard leader: a node that does not
lead the shard forwards the message to the leader and its PUBACK waits for
the leader (which in turn waits for its followers); the leader stamps the
message, stores it and replicates it. A write the leader's journal fails to
append or sync is refused, and so is the publish that waits on it (0x83); the
topic's next retained write is judged on its own. Retained reads on subscribe scatter
to every shard leader (or to the one leader for an exact topic) and gather
within the acknowledgement timeout, so a wildcard subscriber sees retained
messages from every shard — but only to leaders that might hold a match. Each
node tells its peers which shards it holds retained messages for and leads,
and a subscribe whose filter falls outside a peer's current statement is
answered without asking that peer; a wildcard filter skips a peer only when
its statement is empty. Absence of a statement is never read as absence of
messages: a peer that has said nothing, or whose link has changed state since
it last spoke, is always asked. A write that makes a shard non-empty
announces it on the same ordered stream the write's barrier waits for, so the
statement reaches every live peer before the publisher's acknowledgement
does; the statement shrinks again only on a periodic recompute, which costs a
query and never a message. If the leader is down at write time the message is
stored locally and counted.

A shard-map change that moves a retained shard's holders is a move, which the
shard's leader under the new map runs to completion before the shard takes a
retained write again. It sends each new follower a release (a replication
frame naming the shard) and then the shard whole; once they acknowledge, it
sends a release to every node that held the shard and no longer does, and the
move completes when each of them alive by the log has acknowledged it. Until
then a node that stopped holding the shard keeps its copies (counted
`hoardmq_replicas_kept_total{reason="retained-moved"}`, persisted beside the
reconciled map version so a restart keeps them), and the leader refuses a
retained publish on the shard with 0x83 (`hoardmq_replication_barrier_failures_total{reason="moving"}`):
a leader that died before its new followers held the shard would otherwise
take messages it had acknowledged with it. If the leader is lost first, a
node that kept copies hands them to the shard's next leader, which installs a
handed-over message only for a topic it holds nothing for. A draining node's
leaving phase waits for its releases too.

Only a shard's leader under the map a node last acted on is taken to hold
the shard whole: a follower's push may never have landed, and a node that
held nothing has only what was handed to it. So whenever a shard's leader
changes, every node that held it hands its copies to the new leader (marked,
installed only where the leader holds nothing), and makes the hand-over
again until it is acknowledged, through later map changes too. Until then a
release drops nothing on that node, from any sender
(`hoardmq_replicas_kept_total{reason="retained-handover-owed"}`); a release
that arrived meanwhile is acted on once the hand-over is in. Only the shard's
leader under the map it has applied answers a hand-over; a node that does not
lead the shard and holds the topic, or keeps its own copies of the shard,
refuses it and the sender makes it again
(`hoardmq_retained_handovers_total{outcome="refused-not-leader"}`). An old
leader that still follows the shard keeps its own copies as a follower's until
the new leader's release and push reach it. A node never deletes its own
retained copies of a shard it leads under the map it has applied
(`{reason="retained-own-leads"}`).

When an owner dies, the lease service adopts its offline leases and, with
replication attached, fetches each session's replica from the shard's
holders — and when they have nothing, from the copy a former holder kept
because its hand-over went unacknowledged, the adopter's own included — and
imports it, so queued and in-flight messages and subscriptions survive the
node; a client reconnecting elsewhere pulls the state from the adopter as in
a live takeover, and a takeover whose previous owner cannot answer, or
answers that it holds nothing, falls back to the replica the same way. A
holder answers a fetch from a small pool of its own, apart from the worker
that applies the asking peer's replication stream: a client's CONNECT is
holding a handshake slot for the answer, and after a node loss that worker
is seconds deep in the storm's appends. A fetch reads a copy and orders
against nothing in the stream, and when the pool is full it goes to the
worker rather than being dropped. A
shard-map change moves state without closing any connection: owners send
whole snapshots to targets they gained, followers drop replicas of shards
they no longer hold — but only while the session's owner is alive to make
new copies, since otherwise the copy being dropped may be the last one and
the node taking the lease over would have nowhere to read it from. Holding a
shard is not on its own a reason to hold a copy: an owner replicates to at
most `replicas-1` of a shard's holders, so a node keeps a copy only while it
is one of that session's targets, and drops it — and refuses one handed to
it — when it is not, again only while the owner is alive, and only once the
owner has written the session where another node holds it (see
`owner-unwritten` and `moved-target` below). A copy of a session
the dropping node owns itself is from that session's previous owner, so it is
dropped rather than handed on, and a hand-over carries the copy's own epoch,
which a receiver whose lease has moved past refuses — a new
shard leader promotes its replica retained messages (or receives them from
the old holders), and new followers receive the shard's retained messages
whole. A peer that reconnects is resynced whole. A session's whole copy, at a
resync, a rebalance or a repair, is read and queued under the session's own
lock, so a record written meanwhile reaches the peer behind the copy, never
ahead of it where the older copy would replace it. A resync's retained
messages, like a new follower's, are read and queued under the lock their
writes' replication takes, so a retained write made meanwhile is never replaced
by the older message. A session's whole copy carries the epoch it is held at, as
its records do, never the lease table's token, which can be a later owner's or
lag this node's own grant. A resync refused because
one ran less than an acknowledgement timeout ago is retried when that
interval ends, and every resync ends with a marker frame the peer
acknowledges, so barriers recover once the peer is caught up, even when
there was nothing to resend.

Metrics: `hoardmq_replication_records_total{direction}`,
`hoardmq_replication_acks_total{outcome}`,
`hoardmq_replication_barrier_seconds_total`,
`hoardmq_replication_barrier_failures_total{peer,reason}` (`reason`:
`refused` — a send to the peer failed, which is how a down target usually
reads — `resync`, `down`, `timeout`, `stopped`, `not-held` for a key the peer
refused, `unscoped`, or `unnamed`, with `peer` `-`, for a wait that named no
record),
`hoardmq_replication_refusal_fallback_total{peer,kind}` (a follower's refusal
the owner could not tie to a key — `no-key` when neither the follower's
rejection nor the owner's record of recent frames names one, `mismatch` when
they disagree, `at-cap` when the set of refused keys is full: it blocks every
wait on that peer, as `unscoped`, until the stream is resynced; a follower
names the key it refused, so between current builds this stays at zero),
`hoardmq_replication_gaps_total{peer}` (a follower saw the owner's sequence
jump — frames were lost with the link up, or the owner's first push to it
never arrived; it rejects the frame that revealed it and the owner resyncs
the stream),
`hoardmq_replication_unsynced_batches_total{peer,cause}` (a follower's batch
whose writes may not be on its disk: `failed` when its own replica fsync
failed, `covered` when another fsync of the replica log failed before it
returned; the stream acknowledges nothing until the owner has resynced it),
`hoardmq_replication_unapplied_total{reason,type}` (a record that changed
nothing on the follower, `type` the record's: `reason` `no-session` or
`no-entry` is a copy that has diverged from its owner — the record is
refused and the owner repairs the key; `holed` is a record inside a gap
window, between a gap the follower reported and the resync mark that
closes it, acknowledged as the resync is what repairs it),
`hoardmq_replication_repairs_total{outcome}` (a key the owner sent whole to
a follower that refused a record for it: `snapshot`, `delete` when the
owner no longer serves the session, `repeat` for a refusal inside the
acknowledgement timeout of the last repair, which that repair answers),
`hoardmq_replicas_dropped_total{reason}` (a replica copy this node must not
hold, dropped: `owned-upsert`, `owned-delete` and `owned-rebalance` are copies
of a session this node owns — from the session's previous owner — dropped at
its own write of the session, at its own delete of it and at a rebalance;
`not-a-target-confirmed` and `not-a-target-held` are copies of a session this
node is not replicated to, from an earlier grant, or copies kept
after this node stopped being the session's target (`moved-target`), dropped
on the owner's confirmation or once a check finds one of the owner's targets
holding the session at the owner's grant — see `owner-unwritten` below),
`hoardmq_replication_stream_blocked{peer,reason}` (1 while no barrier to that
peer may pass and 0 otherwise; `reason` is `holed` — frames are missing and
the resync has not landed — or `refused` — a key the peer refused has not been
sent whole since — or `none`, which is also what a peer the shard map does
not name reads, since no barrier waits on it; with
`hoardmq_replication_stream_blocked_seconds{peer}` the age of the state. A
QoS 1 or 2 publish that needs a blocked peer is refused, so this pair is what
says whether that is happening *now* and for how long; the harness's wedged
node is a measurement of the same state from outside),
`hoardmq_replication_unstamped_total{type}` (a session record written with no
lease generation while this node's table holds one for the client — the write
is named in the log beside it; a follower applies such a record when it comes
from the node the table names as owner, and refuses it from any other),
`hoardmq_replication_resync_deferred_total`,
`hoardmq_replication_send_failures_total`,
`hoardmq_replicas_kept_total{reason}` (a session or retained copy this node
no longer holds under the shard map but kept: `owner-not-alive`;
`owned-unserved` — this node owns the session's lease and does not serve the
session yet, because the adoption that imports it has not run, so the copy
may be the only one there is; `owner-unwritten` — a copy this node still holds
the shard for but is not a target of, written under an earlier grant than the
owner's: the owner may not have fetched the session yet, so the copy stays
until the owner confirms it holds the session durably at its grant (the
confirmation a new owner sends its previous owner goes also to every other live
member it does not replicate to; one that names a grant this node's lease table
has not applied yet is judged again as the table catches up), or until the node, asking the owner's
targets every twice `cluster.leases.ttlMs`, finds one holding the session at
the owner's grant or later. No time alone ends the keep: an owner can die
before it is declared with the session written nowhere else. The checks are
counted in `hoardmq_replicas_unwritten_checks_total{outcome}` (`started`,
`held`, `not-held`), at most eight in flight per node; `moved-target` — a copy
kept after this node stopped being the session's target, or one written under
the owner's current grant on a node the owner does not replicate to, until the
owner confirms the session at its targets or a check finds one holding it; or
`map-behind` — the map is behind what the node last acted on, or the file
recording that version could not be read; `handover-unacknowledged` — a node
that stops holding a copy hands it to the shard's new holders and lets go
only once they acknowledge it, sessions and retained messages alike; a
hand-over carries the copy's own epoch, so a copy older than the lease is
refused and stays kept, and a copy of a session the node itself owns is
dropped, never handed over. A refused session copy whose owner is alive is
also counted `moved-target` or `owner-unwritten` and goes as those do;
`owner-only-holder` — a copy from an earlier grant than the owner's, on a node
that stops holding the shard while the owner is its only other holder, so no
holder took it: the owner may not hold the session yet, and the copy is also
counted `owner-unwritten` and goes as those do),
`hoardmq_replication_fetches_total{outcome}` (a takeover's fetch of a replica:
`local`, `remote`, `kept-copy` — a former holder's kept copy, taken only once
every source has answered that it holds none — `none`, and
`none-kept-unconfirmed`, a fetch that found a kept copy while a source did not
answer, and so read as unanswered: the source may hold the newer copy the owner
fed (an adoption given up after such fetches serves that copy: "A fetch nobody
answered" above), and `none-returning`, a fetch that found nothing while a node that may hold
the copy was back on its control connection and could not be asked yet, which
also reads as unanswered), `hoardmq_replicas_held`,
`hoardmq_shardmap_version`, `hoardmq_retained_forwards_total{outcome}`,
`hoardmq_retained_queries_total{direction,outcome}` (`outcome="skipped"` is a
query a peer's statement made unnecessary),
`hoardmq_retained_presence_total{direction}`,
`hoardmq_retained_promotions_total`. The audit log records shard-map changes
and replica fetches.

## Upgrades and the cluster feature level

The operator's procedure, on the chart and on hosts without Kubernetes, with
what a mixed cluster refuses and how to roll back, is
[docs/upgrade.md](upgrade.md). This section is the mechanism.

Two builds in one cluster must never apply the same replicated operation
differently, so a change to what a replicated operation *does* — a
lease-table operation every voter applies, or how a follower applies a
replication frame — is keyed to a **cluster feature level**: one integer in
the lease table's replicated state. Every node advertises the highest level
its build understands in the alive record it already sends. The consensus
leader raises the cluster's level to the lowest level any member advertises,
once every member (voters and learners) has a record; a member with no
record holds the raise back. The raise is itself a replicated operation, so
every node switches at the same position in the log, not at the moment its
own binary was replaced. The level only rises.

During a rolling upgrade the cluster therefore keeps the old semantics until
the last member is on the new build, then switches once. An alive record
carries the level the node's build understands now, so a node rolled back
reads as its older build everywhere, and a raise is a command of its own.
A cluster's first node is alone in the membership when it starts and raises
the level to its own at once.

A node whose build is **below** the cluster's level — rolled back, or added
from an older image — **holds the log**: it applies the entry that raised the
level and nothing after it until the process ends, because it would apply
those entries with semantics the cluster has left. It stays a consensus
member: it stores and acknowledges every entry and votes, so the cluster's
quorum keeps it, but it builds no snapshot and compacts nothing, so its raft
log grows with every committed entry until it is upgraded or removed. Its
snapshot on disk is therefore only ever one written by a build that
understood its level. Restarted on a build that understands the level, it
restores that snapshot and replays what it held, with the semantics each
entry was committed under; nothing is discarded or fetched. Restarted on the
same build, it holds again at the same entry.

While below the level a node serves nobody: `/readyz` and `/servingz` answer
`level` (ahead of `recovering` and `behind`; a hold is not counted as an
apply stall), a CONNECT is refused with `0x9C Use Another Server` naming the
leader (`0x88` when none is known), the clients it had are disconnected, it
takes no forwarded QoS 1 or 2 delivery into its sessions, and it proposes
nothing but its alive record. It disarms the Will and expiry timers it had
armed and starts no adoption, since its table no longer learns that a client
came back elsewhere. If it is elected leader it hands leadership to a
caught-up voter at the level. The leader leaves it out of the shard map's
basis, whatever that does to the copy count (the reduced count shows in
`hoardmq_replication_sessions_short` and `…_retained_shards_short`), hands
the leases it owns to itself with the drain's hand-off, adopting them as a
drain's receiver does, and a drain never picks it as a target. `/healthz`
stays 200: a restart on the same build would hold again. The hold logs one
Error with the entry, both levels and the raft log's size, then a Warn each
minute with its growth; size the volume, and upgrade the node before it
fills. No command removes a consensus member yet, so upgrading it is the way
out of a hold.

Replication frames carry the level their sender applied (`lvl`, absent at
level 1): the cluster's, or its build's when that is lower, because a frame is
applied off the log and the receiver's reading may differ. A frame of an unknown *kind* is still acknowledged unapplied, as
before: a kind added later must be safe for a peer that ignores it.

Raft snapshots are files of format 2 (above, "Consensus core"). A build
before them stored a snapshot inline in the raft log; a newer build still
reads that, and an older leader's inline snapshot, and writes its first file
at its next snapshot. An older build meeting format 2 on its own disk, a
rollback, refuses to start, naming the format. So does this build on a disk
whose stored snapshot is in a format it does not know, and it removes no
file from the snapshot directory first, so the newer build starts on that
disk again.

Level 1 is everything that shipped up to the first release tag. Metrics:
`hoardmq_cluster_feature_level`, `hoardmq_cluster_feature_level_build`
(this node's build), `hoardmq_cluster_feature_level_min_advertised` (what a
raise waits for; 0 while a member has no record),
`hoardmq_cluster_feature_level_raises_total{outcome}`,
`hoardmq_cluster_feature_level_refusals_total{where}` (`connect`, `frame`,
`propose`), `hoardmq_cluster_feature_level_held` (1 while this node holds),
`hoardmq_raft_held_entries` (committed entries it holds unapplied),
`hoardmq_cluster_feature_level_transfers_total{outcome}`,
`hoardmq_shardmap_members_below_level` and
`hoardmq_lease_handoffs_below_level_total{outcome}`. Raises, a node falling
below the level and a held leader's transfer are audited as `cluster.level`,
a hold as `consensus.held`, the leader's hand-off as `lease.handoff-below`.

**Making a gated change:** raise `lease.BuildLevel` in the same commit, read
`Table.Level()` where a log-applied operation is applied and the frame's
level where a frame is, keep the old behaviour below the new level, and add
the semantic to the inventory in the project's upgrade-gate design.
