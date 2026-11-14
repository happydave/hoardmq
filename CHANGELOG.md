# Changelog

What each HoardMQ release changes, newest first, from the first release tag.
Each release has these sections where they apply: Added, Changed, Deprecated,
Removed, Fixed, Security, and **Upgrade notes**: the cluster feature level,
the cluster transport's framing version, any on-disk format change, and what
of it cannot be rolled back (see [docs/upgrade.md](docs/upgrade.md)). What
each surface may change in a patch, a minor and a major release is in
[docs/stability.md](docs/stability.md).

The extension API (`ext/`) is versioned on its own; its changes are in
[ext/CHANGELOG.md](ext/CHANGELOG.md).

## Unreleased

## v0.2.0 — 2026-11-14

A source release, as `v0.1.0` was, and the first to raise the cluster feature
level: to 2, once every node runs it. Read its upgrade notes before rolling a
cluster from `v0.1.0`.

### Added

- A route reconciler on every node: every 60 s it compares the sessions the
  node holds, its local subscribers and its advertised routes, and corrects a
  difference seen at two passes (a missing route announced, a subscriber or
  route for a session no longer held withdrawn), counted in
  `hoardmq_route_reconcile_corrections_total{direction, kind}` and logged at
  Warn per correction, with `hoardmq_route_reconcile_suspects`,
  `_cleared_total`, `_refused_total`, `_skipped_total`,
  `_skipped_persistently`, `_warns_suppressed_total`, `_passes_total`,
  `_pass_seconds`, `_hold_seconds` and `_step_seconds`
  (docs/clustering.md, ["The route reconciler"](docs/clustering.md#the-route-reconciler)).
- `hoardmq_sealed_connects_total{outcome}`: CONNECTs that resumed a session
  this node had sealed for a takeover (`resumed`), and those refused while it
  was sealed for a newer grant (`refused`).
- `hoardmq_route_send_failures_total{kind, reason}` and a Warn, at most once
  a minute per peer, kind and reason, for a route send the transport refuses
  or that misses its admission bound; `hoardmq_route_sync_age_seconds{peer}`,
  `hoardmq_route_proof_whole{peer}` and a Warn when an up peer's routes are
  not proved; `hoardmq_route_set_parts_total`,
  `hoardmq_route_sets_abandoned_total{side, reason}`,
  `hoardmq_route_set_entries`, `hoardmq_route_set_parts`,
  `hoardmq_route_set_bytes_in_progress`, `hoardmq_route_set_apply_seconds`,
  `hoardmq_route_tombstones{scope="own"}`,
  `hoardmq_route_tombstones_pruned_total`,
  `hoardmq_route_tombstones_removed_total`,
  `hoardmq_route_set_held_age_seconds{peer}`,
  `hoardmq_route_entries_unsendable_total`,
  `hoardmq_route_updates_dropped_total{reason}` and
  `hoardmq_subscription_refusals_total{reason}`
  (docs/operations.md, ["Drain and readiness"](docs/operations.md#drain-and-readiness)).
- Histograms of the follower's lock and of replica fetches, as cumulative
  `_bucket{le}` series from 1 µs to about 4.2 s with `_sum` and `_count`:
  `hoardmq_replication_lock_hold_seconds{kind}` (each holder's holds),
  `hoardmq_replication_lock_wait_seconds{kind}` (the waits of `apply`,
  `apply-other` and `find`) and `hoardmq_replication_fetch_request_seconds`
  (docs/clustering.md, ["Data-plane replication"](docs/clustering.md#data-plane-replication)).
- `hoardmq_connects_tcp_keepalive_total{listener,outcome}`, and `tcpKeepAlive`
  on the admin API's connection (`GET /clients/{id}`).
- `hoardmq_replication_whole_writes_total{cause, form}`,
  `hoardmq_session_creations_total{form}`, and a `cause` label on
  `hoardmq_replication_repairs_total` with the outcome `covered`
  (docs/clustering.md, ["Data-plane replication"](docs/clustering.md#data-plane-replication)).

### Changed

- **TCP keep-alive on client connections:** once a CONNECT asking for a
  keep-alive K other than 0 is accepted, the connection's TCP keep-alive is
  turned off; its read deadline, 1.5 × K, finds a gone client, which for K
  over 100 s is later than TCP keep-alive's about 150 s. A CONNECT asking for
  0 keeps TCP keep-alive as before, under a `limits.maxKeepAlive` too
  (docs/clients-and-sessions.md,
  ["Connection behaviour"](docs/clients-and-sessions.md#connection-behaviour)).
- **A clustered node refuses a SUBSCRIBE filter whose route entry could not
  fit in one cluster frame**, 0x97 at MQTT 5.0 and 0x80 at 3.1.1 and 3.1. At
  the default 16 MiB no filter the protocol allows is refused
  (docs/clients-and-sessions.md, ["Subscriptions, retained messages and shared subscriptions"](docs/clients-and-sessions.md#subscriptions-retained-messages-and-shared-subscriptions)).
  An existing route entry of that size is left out of route sync, counted and
  logged at Warn.
- **A session's whole write reaches its replicas as one snapshot.** An
  import, the rewrite after a failed append, a Clean Start and the creation of
  a client's first session are still journaled as records, and reach each
  target as one snapshot frame, applied there under one hold of the follower's
  lock. A session too large for one frame is sent as a snapshot without its
  queue and then each queued message; when even that does not fit, the key is
  refused at that target, every QoS 1 and 2 acknowledgement naming it refused,
  until it fits, counted and logged at Warn. The Warn for a follower's
  unapplied record is written at most once a minute a peer, with the count.
- `hoardmq_route_syncs_total{direction="out"}` counts full sets admitted to
  the transport, not sets queued; `hoardmq_route_update_gaps_total` carries a
  `peer` label.

### Fixed

- **A replica-store checkpoint no longer holds up a node's replica fetches and
  incoming replication for up to seconds.** The checkpoint, and the copy a
  shard-map change takes, copied the whole replica state through JSON while
  holding the lock that fetches and peers' replication take: about 0.65 s at
  500,000 replica copies, and up to about 2 s in a 1,000,000-session failover,
  long enough to spend a takeover fetch's budget. The state is now copied
  without encoding under the lock and encoded after it (about 25 ms at 500,000
  copies). The checkpoint's file and its form are unchanged.
- **A takeover or adoption no longer serves a fresh session because its
  replica fetch ran out of time or passed over a node it could not ask.** A
  fetch that spent its budget (`cluster.replication.ackTimeoutMs`) before
  asking the other members for a copy they kept, as when a checkpoint held
  the node's replica store for that long, read as "nothing held anywhere",
  and the client was served Session Present false while another live node
  held its only copy; a member that was down, not a source, was passed over
  the same way while the previous owner was still in the shard map. Such a
  fetch now reads unanswered, and a down member is named for the lease to
  judge by the log, so the CONNECT is refused Server Busy and the adoption
  kept until the copy is found or the member is declared dead. A takeover
  whose fetch finds nothing after its node has seen a later grant is also
  refused Server Busy rather than served a fresh session at the older grant.
  New: `hoardmq_replication_fetches_total{outcome="none-budget-spent"}`.
- **A CONNECT that reaches a node while an import of its client is installing
  waits for the install** and resumes the imported session. It was given a
  fresh session, which the install then replaced in the map without removing
  it: its connection kept it, and subscriptions made on that connection were
  routed for a session the node no longer held, lost to the client's next
  session and, once it disconnected, acknowledged and delivered nowhere. A
  CONNECT whose lease lookup found no lease now also waits for another CONNECT
  of the client still taking the lease over on the same node, as one that
  found a lease already did; a session's removal never deletes another
  session's map entry; and an install that finds another session for the
  client removes it as an eviction does, counted (`installs_replaced`) and
  logged at Error. The route reconciler never removes a subscription of a
  connected client: it counts `hoardmq_route_reconcile_refused_total{reason="connected"}`
  and logs at Warn.

- **A session that left a node no longer keeps a subscriber, or an advertised
  route, there.** A CONNECT resuming a sealed copy added its subscriptions to
  the router after releasing its locks, and a SUBSCRIBE took its session's
  lock without checking the session was still its connection's; a takeover,
  seal or eviction in between left a subscriber for a session no longer held,
  until restart. A shared subscription's messages given to it were
  acknowledged and delivered nowhere, and a publish whose only match it was
  was answered Success. Now no subscription reaches the router for a session
  once it has been removed, decided under the session's lock with the add; a
  SUBSCRIBE that loses that race is answered Unspecified Error.
- **A held route withdrawal's timer no longer ends a newer move of its
  filter.** A timer that fired as the client came back still ran, and if the
  client had left again it cleared the new move's mark, so for the rest of that
  grace a QoS 1 or 2 publish made on the node, before the new owner's route
  arrived, was acknowledged (No Matching Subscribers at MQTT 5, Success at
  3.1.1) and delivered nowhere. Now the timer acts only while it is still the
  filter's held one; one that is not is counted in
  `hoardmq_route_withdrawals_superseded_total`.
- **A client returning to a node as its move's grace ended no longer loses
  its route.** The withdrawal of a moving session's route is held for the
  grace on a timer; when the client's session was installed on the node again
  as the timer fired, the withdrawal could be written after the return's
  route, leaving the node holding the subscriber and advertising nothing for
  it. A publish made on any other node was then answered No Matching
  Subscribers. Now the withdrawal is written only if nothing was announced for
  the filter since it was held, decided under the route table's lock with the
  write; one superseded is counted in
  `hoardmq_route_withdrawals_superseded_total`.
- **A takeover no longer serves a replica copy holding a session without its
  subscriptions.** A follower judged each record of an owner's whole write
  alone, so a grant noted between them left it part of the write, and a
  session's metadata record alone made a copy wherever none was held. A
  takeover that could not reach the previous owner then served that copy with
  Session Present true, no subscriptions and no queue, logging nothing above
  Debug. Now a whole write is admitted or refused whole, and at cluster
  feature level 2 a metadata record alone that finds no copy is refused and the
  owner sends the session whole. An owner taking a lease back now repairs a
  target that refused its earlier grant's write, which it left refusing that
  client's QoS 1 and 2 publishes until a resync; and a refusal of a record sent
  before a later whole write of its key no longer lets an acknowledgement pass
  before that write is acknowledged.
- **A later owner is no longer served a session empty after a CONNECT's
  refused install.** When a CONNECT's install of a fetched session was refused
  on a later grant, its node kept no record of the owner the session came
  from, so a later owner's pull that found nothing there read the session as
  held nowhere and served it fresh while that owner still held it sealed. The
  node now names that owner to the pull, as the adoption worker's refusal
  does, and hands an adopted session on until the new owner's served fact or
  cover reaches its grant.
- **A CONNECT is no longer served a fresh session on a node whose lease
  table lags a later grant it has already seen.** After an import was refused
  on that later grant, the client's retry, or a CONNECT that had waited for
  the refused one, was served Session Present false at the older token while
  the new owner held the client's session. It is now refused Server Busy
  until the table catches up, and the retry then takes the session from the
  new owner. An owner's write below a grant its node has seen also no longer
  drops the node's replica copy of the session. New:
  `hoardmq_lease_ops_total{op="acquire",outcome="below-seen-grant"}`,
  `hoardmq_replicas_kept_total{reason="owned-superseded-write"}`.
- **A second CONNECT no longer gets a fresh session while the first holds
  the adopted session it fetched.** A CONNECT that fetched its lease's
  adoption, or whose first acquire took the lease from another node, now
  holds the client's other CONNECTs on that node until its import. Before,
  a second CONNECT was served Session Present false, and its write dropped
  the replica copy. If the first's import was then refused on a later grant,
  the client resumed elsewhere with Session Present true, an empty session,
  and its acknowledged queued messages lost. The adoption also stays
  unfinished until the import, so the served fact and the confirmation to
  the session's previous holder wait for it. New:
  `hoardmq_lease_ops_total{op="acquire",outcome="unsettled-state"}`.
- **A node's full route set larger than one cluster frame now reaches its
  peers.** It was sent as one frame and refused above `cluster.maxFrameBytes`
  (about 186,000 route entries a node at the default 16 MiB), logged only at
  Debug, so a gap in a peer's route updates was never repaired and a
  restarted node never learned its peers' routes and never served. It now
  travels in parts of at most 32 KiB, applied only when complete, paced so a
  set puts at most 32 parts ahead of other traffic on the data connection
  (docs/clustering.md, ["Routing across nodes"](docs/clustering.md#routing-across-nodes)).
- Delta route batches are cut at 32 KiB.
- A node deletes its own route tombstones once every consensus member has
  acknowledged a full set that carried them, and a full set removes the
  sender's stored tombstones it no longer carries: they were kept for the
  life of the process.
- **A session held by a live node is no longer served empty after its
  client's acquire was granted to a CONNECT that had given up.** When a
  node's lease table was behind, the refused CONNECT could note the node
  itself as the previous owner; the client's next CONNECT there then asked
  no previous owner, found no replica copy, and was served a fresh session
  (`hoardmq_sessions_rebuilt_empty_total{reason="adoption"}`) while the node
  the grant took the lease from held it. The note now records the node the
  grant took the lease from, the lease is judged again before such a CONNECT
  proposes, and an adoption that would name the node itself is refused
  `0x89 Server Busy` and kept, counted as
  `hoardmq_lease_adoption_waits_total{on="unknown-owner"}`
  (docs/clustering.md, ["Leases and session takeover"](docs/clustering.md#leases-and-session-takeover)).
- An adoption's fetch answered "superseded" by the previous owner now reads
  as unanswered, not as that owner holding nothing, so it no longer serves a
  fresh session on a lease the node's table had not yet seen move.

### Upgrade notes

- **Cluster feature level 2**, raised by the cluster once every node runs this
  release: a follower refuses a session's metadata record alone that finds no
  replica copy. Until the raise the cluster stays at level 1, and such a record
  is applied as `v0.1.0` applies it
  (docs/upgrade.md, ["By level: nothing, in a correct roll"](docs/upgrade.md#by-level-nothing-in-a-correct-roll)).
- **Once the level has risen to 2, no node can be rolled back to `v0.1.0` in
  place.** The level never lowers, and a node restarted on `v0.1.0` holds the
  log and serves nobody until it runs this release again. Roll forward with a
  fixed release instead. Before the raise, while any node still runs
  `v0.1.0`, a node can be rolled back to `v0.1.0`
  (docs/upgrade.md, ["Rolling back"](docs/upgrade.md#rolling-back)).
- **Framing version 6**, negotiated per link: it adds no class and says the
  route plane reads a full set in parts. A link to a `v0.1.0` node runs at version 5: it is sent full sets
  in one frame, refused above its frame limit as before, now counted and
  logged at Warn, and the upgraded nodes keep their route tombstones until it
  is upgraded (docs/upgrade.md, ["By build: the framing version"](docs/upgrade.md#by-build-the-framing-version)).
- **On-disk formats: none changed.** Raft snapshot format 2, write-ahead log
  segment header version 1 and configuration document `version` 1, as in
  `v0.1.0`; the store checkpoint's form is unchanged.
- **Interfaces:** the admin API at `/api/v1` (OpenAPI `info.version` 1) adds a
  response member, `tcpKeepAlive`; the extension API is 0.9.0, unchanged.
- Set `cluster.maxFrameBytes` the same on every node.

## v0.1.0 — 2026-11-07

The first release: a source release, with no image, chart package or SBOM
published, and the definition of cluster feature level 1. Every area points
at the section of the documentation under `docs/` that describes it.

### Added

- **MQTT** 5.0 and 3.1.1, with protocol level 3 (`MQIsdp`) accepted as an
  input dialect: QoS 0, 1 and 2, retained messages, wildcard and shared
  subscriptions, Wills with their delay, session expiry, topic aliases, over
  TCP, TLS and WebSocket listeners (docs/configuration.md, ["Listeners"](docs/configuration.md#listeners);
  docs/clients-and-sessions.md, ["Sessions"](docs/clients-and-sessions.md#sessions),
  ["Subscriptions, retained messages and shared subscriptions"](docs/clients-and-sessions.md#subscriptions-retained-messages-and-shared-subscriptions)).
  The conformance suite's counts per specification are in
  docs/development.md, ["Conformance"](docs/development.md#conformance).
- **Clustering** from one node up: a consensus core on `go.etcd.io/raft/v3`
  with snapshots streamed as files; a lease per client identifier with
  takeover between nodes; session and retained-message replication to
  `cluster.replicas` copies; routing of publishes between nodes; a drain that
  redirects clients and hands its leases on; readiness, serving and liveness
  endpoints (docs/clustering.md, ["Cluster transport"](docs/clustering.md#cluster-transport) through
  ["Data-plane replication"](docs/clustering.md#data-plane-replication);
  docs/operations.md, ["Drain and readiness"](docs/operations.md#drain-and-readiness)).
- **Persistence:** a write-ahead log, fsynced before a QoS 1 or 2
  acknowledgement by default, with checkpoints; a damaged or incomplete log
  stops the node with the files named; `hoardmq wal verify` reports what a
  start would repair or refuse, and `hoardmq wal repair` makes a start's own
  repairs, keeping what they drop; a running node locks its data directory
  (`LOCK`) (docs/operations.md, ["Persistence"](docs/operations.md#persistence),
  ["Durability and group commit"](docs/operations.md#durability-and-group-commit)).
- **Security:** authenticators for a users file, the in-broker database, JWT,
  LDAP, X.509 with CRL and OCSP checking, and SCRAM; file-based
  authorization with identity templates; sessions bound to the principal
  that made them; mutual TLS or a shared secret between nodes
  (docs/security-and-access.md, ["Authentication and authorization"](docs/security-and-access.md#authentication-and-authorization);
  docs/clustering.md, ["Cluster transport"](docs/clustering.md#cluster-transport)).
- **Operations:** the admin REST API with roles, API keys and OpenID Connect
  sign-in, and the `hoardmq` command as its client; Prometheus metrics,
  OpenTelemetry traces and an audit log; connect, connection, rate and
  subscription limits, overload shedding and bans; configuration reload; a
  reference Dockerfile (pinned bases, a `VERSION` argument stamped into the
  binary, `CONTRIB` modules compiled in with `hoardmq-build`) and a reference
  Helm chart to copy and adapt, with no image or chart published and the
  chart refusing to render without an image, its container hardened by default (read-only root, no
  capabilities, no privilege escalation, the runtime's seccomp profile), with an
  optional NetworkPolicy and ServiceMonitor and listeners added beside the
  built-in ones (docs/operations.md, ["Admin API"](docs/operations.md#admin-api),
  ["Observability"](docs/operations.md#observability), ["Overload and bans"](docs/operations.md#overload-and-bans),
  ["Kubernetes deployment"](docs/operations.md#kubernetes-deployment); the chart README, "Security").
- **Extensions:** the extension API 0.9.0 (with unsubscribe interceptors;
  cluster events: membership, lease moves and drains, queued for each module
  and never waited for; the module that admitted a connection on its
  identity; and versioned retained records in the experimental `ext/store`),
  `hoardmq-build` for binaries with
  other modules, the Kafka forwarder `sink.kafka` (keys from the topic, a
  topic level, the client identifier or a user property; prefixed and static
  headers), and the webhooks
  `auth.http` (authentication and authorization by an HTTP endpoint, denying
  when it cannot answer) and `events.http` (events posted to an HTTP
  endpoint through a bounded, counted buffer, or synchronously, unsubscribes
  and cluster events included), payload
  schema validation `schema.json` (a JSON Schema per topic filter, from the
  publish admission check), and, out of tree under `contrib/`, `auth.sql` (a
  PHC hash from a PostgreSQL query) and `sink.postgres` (accepted publishes
  into PostgreSQL tables, with `sink.kafka`'s policy), each with pgx in its own
  module (docs/modules.md, ["Modules"](docs/modules.md#modules),
  ["Kafka forwarder"](docs/modules.md#kafka-forwarder-sinkkafka),
  ["Postgres sink"](docs/modules.md#postgres-sink-sinkpostgres-out-of-tree),
  ["Event webhooks"](docs/modules.md#event-webhooks-eventshttp),
  ["Payload schema validation"](docs/modules.md#payload-schema-validation-schemajson);
  docs/security-and-access.md, ["HTTP authentication and authorization"](docs/security-and-access.md#http-authentication-and-authorization-authhttp),
  ["SQL (`auth.sql`, out of tree)"](docs/security-and-access.md#sql-authsql-out-of-tree)).

### Changed

- **MQTT:** a Variable Byte Integer written in more bytes than its value
  needs (a Remaining Length, Property Length, Property Identifier or
  Subscription Identifier) is refused as a malformed packet at every
  protocol level: DISCONNECT 0x81 at 5.0 after CONNECT, a closed connection
  below 5.0 and for a CONNECT (docs/development.md, ["Conformance"](docs/development.md#conformance)).
- **Documentation:** the measured claims in the documentation, the chart's README and
  `docs/upgrade.md` were corrected where their runs show less, and each now
  cites a summary of its runs in [docs/evidence/](docs/evidence/README.md).

### Security

- **`auth.http`'s superuser is its own:** its authorizer skips the endpoint
  only for a connection `auth.http` admitted with `is_superuser`. An
  `auth.http.superuser` attribute set by a users file, an `auth.db` credential,
  a mechanism or another module no longer makes a superuser. Before, anyone who
  could write a credential's attributes (`credentials:write`) could grant
  that bypass. A deployment that relied on such an attribute has the endpoint
  answer `is_superuser` instead
  (docs/security-and-access.md, ["HTTP authentication and authorization"](docs/security-and-access.md#http-authentication-and-authorization-authhttp)).
- **A re-decision that changes the identity ends the session:** when a held
  allow is made again and admits with another principal, other attributes or
  another admitting module, the connection is closed (DISCONNECT 0x98). It no
  longer keeps the identity its CONNECT built. Its Will is authorized with the
  new identity, or as the bare principal after a refusal or a changed principal
  (docs/security-and-access.md, ["Authentication and authorization"](docs/security-and-access.md#authentication-and-authorization)).

### Upgrade notes

- **Cluster feature level 1.** The first tag defines it: everything up to the
  tag is level 1, and a later change to a replicated operation raises it
  (docs/clustering.md, ["Upgrades and the cluster feature level"](docs/clustering.md#upgrades-and-the-cluster-feature-level)).
- **Framing version 5.** A newer leader cannot send a raft snapshot to a node
  on a link below version 5, which every build before this tag is; such a
  node catches up once upgraded.
- **On-disk formats:** raft snapshot format 2; write-ahead log segment header
  version 1; configuration document `version` 1.
- **Rolling back:** there is no earlier release to roll back to. A node that
  has written a format 2 raft snapshot does not start on a build from before
  this tag, which cannot read it
  (docs/upgrade.md, ["What cannot be rolled back"](docs/upgrade.md#what-cannot-be-rolled-back)).
- **Interfaces:** the admin API at `/api/v1` (OpenAPI `info.version` 1); the
  extension API 0.9.0.
