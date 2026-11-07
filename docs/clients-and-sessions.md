# Clients and sessions

What a client sees: how connections are admitted, held and closed, how sessions and their queues behave, and how
subscriptions, retained messages and shared subscriptions work, including the behaviour the MQTT specifications leave
to the server.

## Connection behaviour

Keep-alive expiry closes at one and a half times the client's value, or the
server's `limits.maxKeepAlive` when that is lower (advertised as Server Keep
Alive to MQTT 5.0 clients), counted from when the broker finished handling the
client's last packet: a journal sync or a replication wait is not the client's
silence. A client that sends a keep-alive of 0 has no read deadline unless
`limits.maxKeepAlive` is set; TCP keep-alive (15 s idle, then 9 probes 15 s
apart) still closes a connection whose peer has gone, after about 150 s. The
Helm chart sets `limits.maxKeepAlive` to 3,600. `limits.receiveMaximum` bounds the client's
unacknowledged QoS 1 and 2 publishes. Outbound, the broker has at most the
client's Receive Maximum in flight, or `limits.maxInflightMessages` when that is
lower (65,535 by default, the largest a Receive Maximum can be, and what a
3.1.1 client, which sends none, gets); the rest wait in the session's queue.
It is read at start. With many clients that may stop acknowledging, set it to
1,024 (the Helm chart's value): at 256-byte payloads a client that stops holds
about 1.52 MB instead of about 52.5 MB, and a client acknowledging 1 ms after
each message still received 161–172 thousand messages a second, against
128–198 thousand without the bound, where the test's sender was the limit
(measured on loopback; [docs/evidence/inflight.md](evidence/inflight.md)). A bound of B in flight
over a round trip
of R allows at most B/R messages a second to one client, about 100,000 at 1,024
and 10 ms; raise it for clients that need more over a long round trip. `limits.maxPacketSize` bounds inbound
packets and is advertised. The client's Maximum Packet Size is honoured
outbound: a publish that does not fit is dropped silently, any other packet
is retried without its Reason String and User Properties. Topic aliases work
in both directions. A connection's inbound aliases may hold
`limits.maxTopicAliasBytes` of topics (1 MiB by default, 0 unlimited); a
PUBLISH whose alias would pass it closes the connection with Quota exceeded
(0x97), counted in `hoardmq_topic_alias_refusals_total{listener}`. An alias
grants nothing: a PUBLISH that uses one alone is authorized on its topic like
any other. Every refusal carries a reason code at MQTT 5.0; at 3.1
and 3.1.1, which have none, the connection is closed. When the server ends a
connection it flushes the DISCONNECT, half-closes the socket and keeps
reading for half a second, so a client that is still sending receives the
reason rather than a connection reset.

`limits.clientMessageRate` (PUBLISH packets a second) and
`limits.clientByteRate` (their topic and payload bytes a second), each with a
burst (`clientMessageBurst`, `clientByteBurst`; 0 is twice the rate), limit
each connection's publishing; 0, the default, is unlimited. A client over
either rate is throttled, never refused or disconnected: every PUBLISH it
sends is read and handled, and after one that leaves it in debt the broker
stops reading its connection until the debt is repaid, so TCP carries the
back-pressure to the client. The keep-alive runs from the end of the pause,
so a paused client is not closed for its silence. Throttling is counted by
`hoardmq_client_throttled_total{reason}` (`message` or `byte`) and
`hoardmq_client_throttle_seconds_total`. The rates can be changed while the
cluster runs (see ["Overload and bans"](operations.md#overload-and-bans)); a connection reads the new rates at
its next PUBLISH.

A routed message never waits for its subscriber's socket. When a
connection's outbound queue is full, the message stays in the session's
queue — within `limits.maxQueuedMessages` and its drop policy, QoS 0
included — and is sent, in order, once the connection has written half its
queue out; the publisher's own connection is not held up. Deferrals are
counted by `hoardmq_deliveries_deferred_total{listener}`. The PUBRELs a
session owes when it resumes do not wait either: a full outbound queue leaves
them for the next send, and they go out in order.

## Sessions

Session State is what MQTT 5.0 section 4.1 lists: the session's existence,
its subscriptions, the inbound QoS 2 identifiers awaiting PUBREL, the
outbound QoS 1 and 2 messages in flight, the queued messages, the Will and
its delay, and the expiry time. A client that reconnects with Clean Start 0
before its Session Expiry Interval elapses gets its in-flight messages
resent (PUBLISH with DUP, or PUBREL) in their original order and then its
queue, subject to its Receive Maximum; nothing is resent on a timer. QoS 2
inbound uses Method A: the message is routed on receipt and the packet
identifier is remembered until PUBREL, so a duplicate PUBLISH is not routed
twice; after a failure, only a publish the broker had accepted is absorbed
this way (see ["Exactly-once across an owner loss"](development.md#simulation-harness)). Level 3 and 4 clients
with Clean Session 0 never expire.

Server-defined behaviour: because routing comes first, a publish whose
acknowledgement carries an error was not accepted, but may already have
reached some subscribers. An error PUBREC ends the QoS 2 exchange and releases
its identifier (MQTT 5.0 section 4.3.3), so the publisher's retry is a new
message, whether or not it reuses the identifier, and a subscriber that
received the refused one receives the retry too, at QoS 2 as at QoS 1. A
subscriber whose queued copy could not be journaled does not: that copy was
withdrawn (see ["Durability and group commit"](operations.md#durability-and-group-commit)). QoS 2's exactly-once delivery
covers the publishes the broker accepted; a refused publish is outside it.
The error PUBREC waits until the identifier's release is durable (see
["Exactly-once across an owner loss"](development.md#simulation-harness)), so a refusal can arrive late. At 3.1.1
and 3.1, whose PUBACK and PUBREC carry no reason code, a refused QoS 1 or 2
publish is not acknowledged at all: the connection is closed, and the publish
stays unacknowledged for the client to send again.

Queues are bounded by `limits.maxQueuedMessages`; `limits.queueDrop` picks
whether a full queue drops the `oldest` message (default) or refuses the
`newest`. An authenticator can lower the bound for its principal with the
attribute `hoardmq.maxQueuedMessages`, a positive count, set by `auth.file`'s
`attributes`, the in-broker database, `auth.http`'s `client_attrs` or
`auth.sql`. It never raises `limits.maxQueuedMessages`: the lower of the two
applies, or the attribute alone when the limit is 0. An attribute that is not
a count is ignored and counted as
`hoardmq_session_queue_limit_invalid_total{namespace}`. Each CONNECT's identity
sets it for its session, so after a takeover the new connection's identity
decides. A session rebuilt from a replica or restored at start uses
`limits.maxQueuedMessages` until its client reconnects.
`limits.offlineQoS0` picks what a QoS 0 delivery to a session with
no connection does. With `queue`, the default, it is queued like QoS 1 and 2
and sent on resume. With `drop`, it is not queued, and is counted as
`hoardmq_drops_total{reason="offline_qos0"}` and reported to delivery
observers as dropped. The QoS is the delivery's, so a QoS 1 publish matched by
a QoS 0 subscription counts as QoS 0. MQTT 5.0 and 3.1.1 allow either; the
default keeps the queueing every earlier build did. For deployments that rely
on the other behaviour, such as a QoS 0 command that an offline device must
never receive later, use `drop`. It governs new deliveries only: a QoS 0
message already queued (behind a full connection, or before the setting
changed) stays and is sent on resume. A connected session's queue is
unaffected. The owner of a session applies its own node's setting, and after a
takeover the new owner's applies, so set the same value on every node. The
setting is per node; a per-namespace form waits for namespaces, and a session
with no connection has no listener to carry a per-listener one. `limits.maxSessionExpiry` caps the Session Expiry Interval a
client may ask for (0 means no cap); when it applies, MQTT 5.0 clients are
told in CONNACK. A Will is published after its Will Delay Interval when a
connection ends abnormally, at once when the delay is 0, at session end if
that comes first, never after a normal DISCONNECT, and not at all when a new
connection to the same client identifier arrives before the delay elapses.
A Will Topic must be a valid topic name: one with `+` or `#`, or an empty one,
is refused at CONNECT before authentication, with CONNACK 0x90 at MQTT 5.0
and by closing the connection at 3.1.1 and 3.1. A second connection with the
same client identifier takes the session over and the first receives
DISCONNECT 0x8E.

## Subscriptions, retained messages and shared subscriptions

Topic filters follow section 4.7: `+` matches one level, `#` the rest, and
a filter that starts with a wildcard never matches a topic that starts with
`$`. When several of a session's subscriptions match one message the
session receives it once, at the highest granted QoS, with every
Subscription Identifier attached. No Local, Retain As Published and Retain
Handling 0, 1 and 2 behave as specified; level 3 and 4 subscribers get no
properties, see RETAIN on the stored message sent at subscribe time, and
never on a forwarded message.

Retained messages are last-writer-wins per topic; an empty retained payload
removes the entry; a retained message expires with its Message Expiry
Interval (checked on access and swept every minute). Retained messages are
sent after the SUBACK and never for a shared subscription. In a cluster the
retained store is sharded by topic (see ["Data-plane replication"](clustering.md#data-plane-replication)).

A wildcard subscribe finds its retained messages through an index of topic
levels, so its cost follows what it matches, not how many topics are retained.
It holds the store's read lock only while it walks the index: lookups run
alongside each other, and a retained write waits for no more than a walk.
`limits.maxRetainedTopics` (0, the default, is unlimited) bounds the retained
topics one namespace holds on a node: the topics it stores as their shard's
leader. A retained publish that would add a topic past it is refused with
Quota exceeded (0x97; the connection is closed below MQTT 5.0 and for QoS 0)
before it is routed, so no subscriber receives it; a Will or a module's
message past it is dropped. A retained write forwarded to a leader on another
node is refused there, and the publish waiting on it is refused with 0x83; the
publisher's node has already delivered it to its own subscribers. A replacement
of a held topic, and a clear of a held topic, are never refused, and lowering
the limit removes nothing. Each refusal is counted in
`hoardmq_retained_limit_refusals_total{namespace,op}` on the node that refused it,
`op` being `write` or `clear`.

Every retained write carries a version the shard's leader stamps (the shard
map's version and a number that only rises on that node), and every node keeps
the higher of two copies of a topic, so a copy handed over late never replaces
or undoes a newer write. A retained write a node would store without a version,
because its shard has no leader or there is no shard map yet, is refused over a
topic holding a stamped message or tombstone, since a restart would put the
stamped one back: a QoS 1 or 2 publish is answered 0x83 (the connection is
closed below MQTT 5.0 and for QoS 0, as at the limit), a Will or a module's
message is dropped and logged at Warn, and each is counted `older` on its path.
A clear, a retained publish with an empty payload, is
kept as a tombstone at its version, and so is a message the leader expires. A
tombstone is never served. It counts against `limits.maxRetainedTopics` while
it is kept, so with the limit on, a clear of a topic not held is refused at the
limit once no tombstone can be dropped, and an expired message's slot stays
taken for at least `limits.retainedTombstoneHold`. With the limit off, the
default, only the drop conditions bound tombstones. The leader drops a
tombstone once `limits.retainedTombstoneHold` (3600 s by default) has passed,
its followers hold it, and every old holder of the shard has been released;
the followers drop it on the leader's word. A copy installed into an empty topic
at or below a shard's dropped tombstones is counted
`hoardmq_retained_merges_total{outcome="below-dropped"}` and logged at Warn.
`hoardmq_retained_tombstones{namespace}` is the tombstones a node holds in its
own store, and `hoardmq_retained_tombstones_dropped_total` the ones it dropped.

`hoardmq_retained_merges_total{path,outcome}` counts each retained install by
where it came from and how it ended: `newer` when it replaced what was held,
`equal` when what was held carries its version, `older` when what was held
outranks it, and `below-dropped` as above. The paths are a leader's stamped
`local` write, `admin` delete and `forward`ed write, which are `newer` when
stamped and `older` when an unversioned one is refused;
a follower's `append`, which takes the leader's stream, a new follower's push
and a resync alike; a `handover` or hand-on, a `promotion`, a promotion's
`retry`, an old leader's `own-to-replica` move, and a `reload` at start. A
copy that is not installed is logged at Debug with both versions.

A refused retained publish follows the rule for any refused publish (see
"Sessions"): it was not accepted, but it may already have taken effect. The
shard's leader applies a retained write, a clear included, before it waits for
its followers, so a refusal there can leave the topic at the new state or the
old. Sending it again reaches the state an accepted write would have.

Shared subscriptions (`$share/{name}/{filter}`) deliver each message to one
member of the group. `mqtt.sharedSubscriptionStrategy` selects
`round-robin` (default), `random`, `sticky`, `hash` (by publisher client
identifier) or `local-first` (round-robin on one node). Offline members are
skipped while any member is online; when all are offline the message queues
on one member. When a member's session ends, its QoS 1 messages still in
flight or queued are re-offered to the group; QoS 2 messages in flight are
never redistributed. Level 3 and 4 clients may use `$share/` filters.

`limits.maxSubscriptions` bounds the topic filters one client's session holds,
shared ones included; 0, the default, is unlimited. A SUBSCRIBE filter the
session does not hold, once it holds that many, is refused alone with Quota
exceeded (0x97) at MQTT 5.0 and 0x80 at 3.1.1 and 3.1; the other filters in
the packet are handled as usual, a filter the session holds is replaced, and
the client stays connected. Refusals are counted in
`hoardmq_subscription_limit_refusals_total{namespace}`. A session that holds
more, after the limit was lowered, keeps them.
