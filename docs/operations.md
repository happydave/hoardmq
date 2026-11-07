# Operations

Running HoardMQ: the operator's guide. Where to look first:

- **Sizing:** the chart's README, ["Sizing: the N-1 trade-off"](../deploy/helm/hoardmq/README.md#sizing-the-n-1-trade-off),
  and its ["Limits"](../deploy/helm/hoardmq/README.md#limits), with the measured figures behind them in
  [evidence/](evidence/README.md).
- **Limits:** the defaults in [configuration.md](configuration.md#configuration), the chart's
  ["Limits"](../deploy/helm/hoardmq/README.md#limits), and ["Overload and bans"](#overload-and-bans) below.
- **Failure modes:** a node lost, in [clustering.md](clustering.md),
  ["Leases and session takeover"](clustering.md#leases-and-session-takeover) and
  ["Data-plane replication"](clustering.md#data-plane-replication); a node leaving, in
  ["Drain and readiness"](#drain-and-readiness) below; a disk or a journal failing, in
  ["Persistence"](#persistence) and ["Durability and group commit"](#durability-and-group-commit) below.
- **Upgrades:** [upgrade.md](upgrade.md), with the mechanism in
  [clustering.md](clustering.md#upgrades-and-the-cluster-feature-level).
- **Whether to run it:** [production-readiness.md](production-readiness.md).

The rest of this page: overload and bans, observability, the admin API, drain and readiness, Kubernetes deployment,
persistence, and durability and group commit.

## Overload and bans

**The stress level.** Once a second each node measures three indicators
against a limit: messages its sessions hold for their clients, queued or in
flight (`overload.queuedMessages`, default 1,000,000), frames sent to its replicas and not yet acknowledged
(`overload.replicationLag`, default 400,000), and the live heap
(`overload.memoryBytes`; 0, the default, takes `GOMEMLIMIT` when the process
has one and otherwise leaves the heap out). The node's level is set by its
highest fraction: under 0.6 it is 0, under 0.8 1, under 0.95 2, else 3. Each
node advertises its level to its peers in its heartbeat, and each acts on the
**cluster level**, the highest of its own and its connected peers'. In a
failover storm at 100,000 clients the replication backlog peaked at 84,314
frames, 0.42 of the 200,000 limit that build ran with (about a fifth of today's
400,000), and the stress level stayed 0
([docs/evidence/overload.md](evidence/overload.md)).

**What each level sheds**, in this order and never by closing a connection:

| Level | Connections paused | Pause before each read | New CONNECTs |
|---|---|---|---|
| 0 | none | — | admitted |
| 1 | the noisiest 10% of those that published in the last second | 100 ms | admitted |
| 2 | the noisiest 50% | 250 ms | admitted |
| 3 | every one that published in the last second | 500 ms | refused |

Noisiest is by PUBLISH bytes read in the last second; a connection already
paused keeps its place while the level holds, so a pause that works does not
move to a quieter client. The pause holds the keep-alive, as a client rate's
does. At level 3 a new CONNECT is refused as soon as it is read: CONNACK 0x89
(Server busy) at MQTT 5.0, return code 3 (Server unavailable) at 3.1.1 and
3.1. At level 0 the node walks no connections. `GET /api/v1/overload`
shows the node's level, indicators and limits, and each peer's level.

**Bans.** The ban list is one list for the cluster, stored in the consensus
log, so a ban written through any node refuses on every node once applied
there. An entry has a kind and a value: `clientid`, `username`, `address` (an
IP address, written in its canonical form, `192.0.2.1`), `cidr` (a prefix,
canonical: `10.0.0.0/8`), `clientid-regex` and `username-regex` (RE2 at most
256 bytes, matched against the whole value), and `clientid-username`, one
principal's use of one client identifier (the username URL path-escaped, `/`,
then the identifier: `alice/sensor-1`), which is matched once the credentials
are authenticated rather than before. It may carry a reason and an
expiry. A ban is checked as soon as the CONNECT is read, before the
connect-rate limits and before any credential is examined, so a banned
client costs no rate token and no key derivation: CONNACK 0x8A (Banned) at
MQTT 5.0, return code 5 (Not authorized) at 3.1.1 and 3.1. It is counted by
`hoardmq_connects_banned_total{listener,kind}` and audited as an
authentication failure with the reason `ban: <kind>`. An entry at or past its
expiry refuses nothing; the node that leads the consensus core deletes lapsed
entries every ten seconds (audited as `ban.delete` by `sweeper`). A ban
refuses new connections only: a client already connected when it is written
stays connected until it next connects. The list holds at most 100,000
entries, the limit of a collection in the consensus log.

**Flapping.** A client whose established connections end abnormally on its
side — the network closed or reset, the keep-alive expired, or it broke the
protocol — `flapping.maxCount` times (default 15) within
`flapping.windowSeconds` (default 60) is banned for
`flapping.banSeconds` (default 300) by a `clientid-username` entry, so the
count and the ban are of that principal's use of that client identifier and
never refuse another principal using it, as an ordinary entry with the source
`flapping`, audited as `ban.create` by `flapping`. A client's DISCONNECT is
not a flap, and neither is any end the broker chose (a takeover, a redirect,
an administrative close, shutdown) nor any CONNECT refused before it was
established, so a storm of the broker's own refusals cannot become a storm of
bans. An existing ban, an operator's included, is left as it is. Each node
tracks at most 100,000 identifier and principal pairs, and writes at most one ban per
pair per ban time and 100 a second. `flapping.enabled: false` turns
the detector off. `hoardmq_connections_ended_total{how}` (`graceful`,
`server`, `abnormal`) counts what the detector saw;
`hoardmq_flapping_trips_total` and `hoardmq_flapping_bans_total{outcome}` what
it did.

**What cluster-wide costs.** Every ban write, an operator's or the
detector's, is one proposal to the consensus core, and each node rebuilds its
ban index when the list changes, off the connect path; a CONNECT reads the
index with one atomic load. In a failover storm the survivors' connections
end at the broker's choice (takeovers) and the lost node's end on a node that
is gone, so the detector writes nothing: in seven failover runs at 100,000
clients the survivors saw two or three abnormal ends a run and tripped none
([docs/evidence/overload.md](evidence/overload.md)).

**Administered limits.** `PUT /api/v1/limits` (or `hoardmq limits set`)
stores the per-client message and byte rates and bursts and the node's
connect rate and burst in the consensus log, for every node: a member it
sets overrides the configuration, a member it leaves out falls back to it.
A new connect rate applies to each node's gate at once, keeping each
listener's own limit; a new client rate applies to each connection from its
next PUBLISH. Limits apply to the default namespace, the only one until
namespaces are activated.

## Observability

`observability.address` (empty by default) starts an HTTP listener serving
`/metrics` in the Prometheus text format, `/healthz` (liveness: 503 naming
the cause while the node cannot recover without a restart — see "Drain and
readiness"), `/readyz` (readiness: joined, caught up, unfenced, not
draining, topology stable — see "Drain and readiness"), `/servingz` (ready
and able to acknowledge a QoS 1 or 2 publish; not a probe) and `/drain` (POST
starts the graceful leave, GET reports it, to a caller on the node's loopback
only, any other refused with 403; closed with 403 once the admin API is
configured, which then owns the drain). The host must be a loopback
address unless `observability.allowRemote` is true. Metrics are exported by the
`metrics.prometheus` module, which replaces `metrics.internal` and reads the
same counters: connections (total by listener, active, and closed by cause —
`take` when the session was taken away with a connection still attached,
`detach` when the connection was closed with the state left behind, which is
what a takeover does, `evict` when the state was discarded here because the
lease moved, and `evict-import` for the part of `evict` an adopted session's
import caused), cluster peers
connected with cluster frames, bytes, reconnects, heartbeat misses and
refusals, the consensus core (term, commit and applied index, snapshot
index, leader flag, members by role, proposals by outcome, snapshots,
snapshot builds and transfers by outcome, dropped frames, and `hoardmq_raft_proposal_seconds_total{outcome}`, the time from a
proposal to its outcome), leases (active, operations by outcome, takeovers by kind,
adoptions, fences, connect refusals), routes (active by scope, updates and
syncs by direction, forwards by kind, forward failures), replication
(records and acknowledgements, barrier time and failures, degraded
barriers, replicas held, shard map version, retained forwards and
queries, and `hoardmq_replication_backlog_frames{peer}`, the frames pushed to a
peer and not yet acknowledged: replication runs one stream a peer, so this is
the followers' lag), the drain (clients redirected, leases handed off, sessions confirmed
and unconfirmed, seconds) and
readiness (`hoardmq_topology_changes_total`), sessions and
subscriptions by namespace, publishes by namespace and QoS, deliveries by
event, drops by reason (`queue_full`, `offline_qos0`, `expired`, `refused`, `hook`),
authentication failures by listener and reason, key-set fetches
(`hoardmq_auth_jwt_key_fetches_total{outcome}`), introspections by outcome,
revocation checks by method and status and unknown statuses by policy,
enhanced-authentication exchanges by method and outcome, held decisions made
again by outcome (`hoardmq_auth_rechecks_total`), password verifications derived
and answered from the cache and the cache's size, CONNECTs refused by a
connect-rate limit by listener and limit, connections closed for their topic
aliases' byte bound by listener, LDAP decisions by outcome
(`hoardmq_auth_ldap_decisions_total`), unavailable directories by policy and
directory connections by outcome, the cluster stress level by scope
(`hoardmq_overload_level{scope}`, `node` or `cluster`) and its indicators
(`hoardmq_overload_indicator{name}`), connections paused for stress,
CONNECTs refused for overload and for a ban, bans held and invalid, lapsed
bans deleted by outcome, connections ended by how, flapping trips and bans,
clients throttled and for how long, deliveries deferred, Wills, retained messages
by namespace, the log index, hook failures by module, whether the node holds its
data directory's lock (`hoardmq_data_dir_locked`, 1 or 0; see "Persistence"), and
the journals (below).

The node's own journal and the replica log each report, labelled `store="node"` or
`store="replica"` and read without the store's lock: the last index
(`hoardmq_journal_index`), the last checkpoint's index and the seconds since this
process wrote one or, if it has written none, since it opened the log
(`hoardmq_journal_checkpoint_index`, `hoardmq_journal_checkpoint_age_seconds`),
checkpoints written (`hoardmq_journal_checkpoints_total`), every fsync of the log
and the time they took (`hoardmq_journal_fsyncs_total`,
`hoardmq_journal_fsync_seconds_total`; the second over the first is the mean
latency), failed fsyncs, rewrites after one and failed rewrites
(`hoardmq_journal_fsync_failures_total`, `hoardmq_journal_repairs_total`,
`hoardmq_journal_repair_failures_total`), and the callers that waited for a sync
under the commit policy and how long (`hoardmq_journal_sync_callers_total`,
`hoardmq_journal_sync_wait_seconds_total`). A failed fsync with no repair after it
is a disk to look at; a failed repair refuses publishes until one succeeds (see
"Durability and group commit"). `hoardmq_journal_failures_total{op}` counts what
the session layer saw fail: `append` when a record could not be written, which
refuses the QoS 1 or 2 publish that wrote it (see "Durability and group commit"),
and `sync` when a wait for durability failed, which refuses the QoS 1 or 2 publish
or state change that waited.

An append-only audit log under `node.dataDir/audit/` records authentication
failures, module enable and disable, trace start and stop, server start and
stop, the cluster's lease, membership and replication events, and events modules record
through `ext.Context.Audit` (such as `schema.json`'s `payload.invalid`) as one JSON
object per line, rotated at `observability.audit.maxBytes` keeping
`observability.audit.maxFiles` files; `observability.audit.enabled` (default
true) turns it off. Events are handed to one writer over a queue of
`observability.audit.queue` entries (default 8192) and committed in batches,
one write and one sync per batch, so no connection or administrative call
waits on the audit file. A refused CONNECT is the one event of the broker's own a reconnect
storm produces by the hundred thousand: when the queue is full it is dropped
and counted rather than queued, as is every module's event, since a module may
record from a client's path. Every other event waits for space and is
never dropped; an event accepted before shutdown is on disk before the
server exits. Drops are visible three ways — an `audit.dropped` record with
the count and the time of the previous batch, written into the log itself
when the writer next runs; `hoardmq_audit_dropped_total`; and
`hoardmq_audit_queue_depth` — alongside `hoardmq_audit_records_total`,
`hoardmq_audit_batches_total` (records over batches is the group-commit
ratio) and `hoardmq_audit_write_failures_total` (batches lost to a write or
sync error, logged once per distinct error). A trace
recording captures one client's inbound and outbound packets in a ring of
`observability.traceEvents` entries without touching other clients; the
admin API starts, fetches and stops it.

### OpenTelemetry traces

The `traces.otlp` module records spans for the publish path and exports them as
OTLP over HTTP in the JSON encoding (OTLP 1.11.0), batched by the broker itself;
it is off unless configured:

```json
"modules": {
  "traces.otlp": { "endpoint": "http://otel-collector:4318/v1/traces", "sampleRatio": 0.01 }
}
```

`endpoint` is used as given (`/v1/traces` is not appended). Sampling is decided
once, where the trace starts: a PUBLISH whose MQTT 5 user property `traceparent`
(W3C Trace Context; `tracestate` is carried with it) is valid continues that
trace, recorded if its sampled flag is set; any other publish starts a trace with
probability `sampleRatio` (default 0.01). A client that sets the sampled flag is
therefore traced whatever the ratio. A traced publish is a `publish` span (server)
on the node that took it in, a `forward` span (producer) for each node it is
forwarded to, and a `deliver` span (consumer) on each receiving node, one trace
across the cluster. The broker never adds, removes or reorders a user property —
MQTT 5 requires them forwarded unaltered — so its own span context crosses between
nodes in the forward's envelope, and subscribers receive exactly the properties
the publisher sent. Spans carry the topic, the client identifier, the namespace,
the node, the QoS, counts and the MQTT reason code of a refusal; never a payload or
a property's value.

A finished span is queued (`queueSize`, default 8192) and never waits: a full queue
drops it. One goroutine sends a batch every `flushIntervalMs` (default 1000) or at
`batchSize` spans (default 512), with a `timeoutMs` deadline (default 5000);
429, 502, 503, 504 and a failed connection are retried twice with backoff and
`Retry-After`, anything else drops the batch. Shutdown sends what is queued for up
to two seconds. The counters: `hoardmq_otel_spans_started_total`,
`hoardmq_otel_spans_exported_total`, `hoardmq_otel_spans_dropped_total{reason}`
(`queue_full`, `export_failed`, `rejected` by the collector, `shutdown`),
`hoardmq_otel_exports_total{outcome}` (`ok`, `retried`, `failed`) and
`hoardmq_otel_queue_depth`. Disabling the module through the admin API
(`modules:write`) stops sampling at once. With the module unconfigured the publish
path does no tracing work and allocates nothing for it. These spans are separate
from trace recordings (above), which keep one client's packets for the admin API.

## Admin API

`admin.address` (empty by default: nothing is bound) starts the admin REST
API on its own listener. Every request, whatever its path, is authenticated
before it is routed; one that no identity matches is answered 401 with
nothing about the resource, and audited as `admin.auth.failure` in the
droppable class the MQTT `auth.failure` uses.

```json
"admin": {
  "address": "127.0.0.1:9091",
  "roles": [
    { "name": "viewer", "scopes": ["read"] },
    { "name": "operator", "scopes": ["read", "clients:write", "retained:write", "cluster:drain"] }
  ],
  "tokens": [
    { "name": "ops", "tokenFile": "/etc/hoardmq/admin/ops.token", "scopes": ["admin"] },
    { "name": "dashboard", "tokenFile": "/etc/hoardmq/admin/dash.token", "roles": ["viewer"] }
  ],
  "tls": { "certFile": "admin.pem", "keyFile": "admin-key.pem", "clientCAFile": "ops-ca.pem" },
  "certificates": [ { "commonName": "alice", "scopes": ["clients:read", "clients:write"] } ],
  "oidc": {
    "issuer": "https://example.okta.com",
    "clientIds": ["0oa1b2c3d4"],
    "roleMappings": [ { "group": "hoardmq-operators", "roles": ["operator"] } ]
  },
  "maxBodyBytes": 1048576
}
```

Identities are bearer tokens, client certificates, API keys (["API keys"](security-and-access.md#api-keys)) and ID
tokens from an OpenID Connect provider (["Single sign-on (OIDC)"](security-and-access.md#single-sign-on-oidc)). A token is the trimmed
content of its `tokenFile`, at least 16 bytes, sent as `Authorization: Bearer
<token>`; the document names the file, never the token, and the broker keeps
only its SHA-256 digest. A client certificate counts only when
`tls.clientCAFile` verified it at the handshake (a certificate another CA
signed is refused there); its common name, looked up in `certificates`, is
the identity, and a verified certificate with no entry is refused rather than
passed over for a token. A non-loopback address requires `tls`, so a token
never crosses a network in clear text; without a client CA no certificate is
asked for. `check` refuses an admin listener with no token, certificate or
`oidc` identity, certificate identities without a client CA, a token file
that cannot be read or is shorter than 16 bytes, two identities with one
token, and an address that collides with a client listener,
`observability.address` or `cluster.listen`.

Scopes: `clients:read`, `clients:write`, `retained:read`, `retained:write`,
`cluster:read`, `cluster:drain`, `cluster:members`, `config:read`, `config:write`,
`traces:read`, `traces:write`, `modules:read`, `modules:write`,
`audit:read`, `apikeys:read`, `apikeys:write`, `credentials:read`,
`credentials:write`, `bans:read`, `bans:write`, `limits:read`,
`limits:write`, and two shorthands, `read`
(every `:read` scope) and `admin` (every scope). Each route needs one; a
principal without it gets 403 naming the scope. Every route that is not a GET
is audited with the identity (`token:<name>`, `cert:<common name>`,
`apikey:<id>` or `oidc:<user>`), the action, the target, the outcome (`ok`,
`denied`, `not-found`, `conflict`, `bad-request`, `too-large`, `error`) and
the request identifier, which the response also carries as `X-Request-Id`; a
refused request of any method is audited too, and a 401's record carries the
refusal's reason when an identity decided it (`apikey: revoked`, `oidc: token
expired`, `oidc: no role mapped`, `certificate: no identity`), never the
credential. Allowed reads are not.

Roles name scope sets once: a token or certificate may hold `roles` beside or
instead of `scopes`, and holds the union; an OIDC identity holds the roles its
groups map to. Every identity holds at least one scope: `check` refuses a
token or certificate with neither, a token, certificate or mapping that names
an undefined role, a role without scopes and two roles with one name. Roles
act on the default namespace, the only one until namespaces are activated.

The routes, under `/api/v1` (the served OpenAPI 3.1 document,
`GET /api/v1/openapi.json`, is generated from the same table that
dispatches, and describes every schema):

| Method and path | Scope | What it does |
|---|---|---|
| `GET /clients?search=&node=&after=&limit=` | clients:read | every client the cluster holds, by client identifier, paged, each naming the node that holds it; `node` keeps one node's |
| `GET /clients/{id}` | clients:read | the session, its connection (listener, address, user, protocol, age, `keepAlive` in force: the client's, or `limits.maxKeepAlive` where it replaced the client's; `requestedKeepAlive` from the CONNECT; `cleanStart`) and its lease |
| `GET /clients/{id}/subscriptions` | clients:read | the client's subscriptions |
| `POST /clients/{id}/disconnect?will=` | clients:write | close the connection with 0x98; the session stays; `will=false` publishes no Will |
| `DELETE /clients/{id}/session?will=` | clients:write | end the session (below); `will=false` publishes no Will |
| `GET /subscriptions?topic=` | clients:read | the subscriptions here a topic reaches |
| `GET /retained?filter=&after=&limit=` | retained:read | retained messages across the cluster |
| `DELETE /retained?topic=` | retained:write | remove one retained message |
| `GET /cluster` | cluster:read | readiness and reason, consensus view, members, feature level, shard map, drain, sealed copies |
| `GET /cluster/shards` | cluster:read | every shard's leader and followers |
| `GET /cluster/leases/{id}` | cluster:read | a client's lease in this node's table |
| `GET`, `POST /cluster/drain` | cluster:read, cluster:drain | the drain's status; start it |
| `DELETE /cluster/members/{node}` | cluster:members | remove a consensus member (below) |
| `GET /config` | config:read | the running document, secrets redacted |
| `POST /config/validate` | config:write | validate a document as `check` does |
| `POST /config/reload` | config:write | re-read this node's file and apply it where every change is reloadable (see ["Reloading the configuration"](configuration.md#reloading-the-configuration)) |
| `GET`, `POST`, `DELETE /traces/{id}` | traces:read, traces:write | fetch, start, stop a client's packet recording (not the OpenTelemetry spans) |
| `GET /modules`, `POST /modules/{id}/enable`, `/disable` | modules:read, modules:write | list and toggle modules |
| `GET /audit?limit=` | audit:read | the newest audit events |
| `GET /whoami` | any identity | the identity, its roles, scopes, groups and expiry |
| `GET /apikeys` | apikeys:read | the cluster's API keys, never a key |
| `POST /apikeys` | apikeys:write | create a key (shown once) |
| `DELETE /apikeys/{id}` | apikeys:write | revoke a key; 200 once every live node has applied it (["API keys"](security-and-access.md#api-keys)) |
| `GET /credentials?after=&limit=` | credentials:read | the in-broker database's client credentials: form, attributes, version; never a password or hash |
| `GET /credentials/{username}` | credentials:read | one credential |
| `POST /credentials` | credentials:write | add one: `username`, and `password` (with `scram`) or `passwordHash`, and `attributes`; 409 when it exists |
| `PUT /credentials/{username}` | credentials:write | change its password, its attributes (replaced whole) or both; `ifVersion` makes it conditional |
| `DELETE /credentials/{username}?ifVersion=` | credentials:write | remove it |
| `GET /bans?after=&limit=` | bans:read | the ban list, paged by key |
| `POST /bans` | bans:write | add one: `kind`, `value`, `reason`, and `expiresInSeconds` or `expiresAt`; 409 when it exists |
| `DELETE /bans/{key}?ifVersion=` | bans:write | remove one, by its key `<kind>:<value>`, URL-escaped |
| `GET /limits` | limits:read | the configured limits, the administered record and the limits in force |
| `PUT /limits` | limits:write | replace the administered record (see "Overload and bans") |
| `GET /overload` | cluster:read | this node's stress level, indicators and limits, and each peer's level |

A client action called on any node acts where the client is held. A node
that holds no session for the client, and whose lease table names another
node, sends the call there over the cluster transport as the caller's
identity, and answers with that node's answer and its name in the
`X-Hoardmq-Node` header. That node checks the caller's scope again and runs
the call as it would its own; both nodes audit it as the caller
(`forwardedTo`, `forwardedFrom`, one request identifier). A call is forwarded
once: one that arrives forwarded is never sent on. When it cannot be
forwarded (that node is unreachable, runs a build that takes no forwarded
calls, or has no admin API of its own), it is answered 409 with that node as
`owner` and the reason in the detail, and nothing runs there; 502 when that
node ran it but its answer is larger than a cluster frame; 504 when it gave
no answer within `cluster.replication.ackTimeoutMs` plus 5 s, so the call may
have run; 503 when that node is already running 16 forwarded calls. Each
outcome is counted (`hoardmq_admin_forwards_total{outcome}`: answered,
unreachable, unsupported, admin-off, too-large, timeout, busy). Only the
client routes are forwarded: inspect, subscriptions, disconnect, invalidate
and traces. The client list reads the lease table every node holds, so a
search on any node finds a client wherever it is; a client held on the
answering node carries its session summary (`session`). One search at a time
walks the table, holding its read lock only while it keeps one page: 1.4–2.3 ms
on average (2.7 ms at most) at 100,000 clients in a probe of the design, not a
measurement of the shipped code ([docs/evidence/admin-client-list.md](evidence/admin-client-list.md)).
A client identifier goes in its path segment
URL-escaped (`/` as `%2F`). Server-defined behaviour: a disconnect closes
the connection with `0x98 Administrative action` and the Will is published
as for any server-initiated close; an invalidation closes the connection,
discards the session and its journal records (replicated as a delete),
releases its lease, and publishes its Will, since the session has ended. With
`will=false` either call publishes no Will (a device removed on purpose), and the
audit record notes it; `true`, or no parameter, is the default, and any other
value is refused with 400. The `hoardmq clients disconnect` and `invalidate`
commands send it with `-no-will`. A
session held here whose lease has moved to another node is not discarded:
that copy is on its way to its new owner and may be the one it fetches, and
the call goes to the lease's owner. One for which this node has seen a later
grant is refused with 409. An
invalidation and a retained delete answer once their records are durable
here and on the live replicas (`"durable": true`), or after
`cluster.replication.ackTimeoutMs` with `"durable": false`. A retained delete
whose record this node's journal failed to take answers `"durable": false` at
once, and a retained delete delivers nothing to subscribers. The configuration
view replaces every member whose name contains `password`, `secret`, `token` or
`key` with `[redacted]`, module configuration included. `POST /config/validate` needs
`config:write` because it reads the files the posted document names, and it
changes nothing. `POST /config/reload` answers 200 whether or not it applied
the file, with `applied`, `restartRequired`, the changed pointers and the
problems; `hoardmq config reload` exits 1 when nothing was applied. No API call walks every session under one lock: a client
list takes one session shard's lock at a time.

The credential routes act on the cluster's database, not this node's: a write
answers once it has committed in the consensus log and applied on the node
that served it (503 when the cluster has no reachable leader; a write that
timed out may still have landed, so a retried create can answer 409). An
update or a delete with `ifVersion` is refused with 409 unless the credential
is at that version, and an update without one is made at the version it read.
Every write is audited as `credential.create`, `credential.update` or
`credential.delete` with the username, the stored form and what changed.

The ban and limits routes act on the cluster's list and record the same way
(503 without a reachable leader). A ban's key is `<kind>:<value>`
(`cidr:10.0.0.0/8` is deleted as `/bans/cidr:10.0.0.0%2F8`); an invalid ban
or limit is refused with 400 naming the problem, and a `namespace` other
than `default` with 400. Writes are audited as `ban.create`, `ban.delete` and
`limits.update`.

## Drain and readiness

A drain is the node's graceful leave, started by the admin API's `POST
/api/v1/cluster/drain`, the observability listener's `POST /drain` from the node's loopback when the
admin API is not configured, the `hoardmq drain` command or a Kubernetes
preStop hook. It closes the client
listeners (readiness turns false at once), disconnects connected clients —
MQTT 5 clients with `0x9C Use Another
Server` and a Server Reference naming a live node's advertised address,
round-robin over the members alive by lease record, older clients with a
plain close — at `cluster.drain.ratePerSecond`, or faster where that is
what disconnects them all by `cluster.drain.timeoutMs` less
`cluster.leases.takeoverTimeoutMs` (less half the timeout, if that is
smaller) from the drain's start, so the drain's length does not grow with
its clients (25 s of redirecting at the defaults), waits up to `cluster.drain.timeoutMs` for those clients'
leases to be taken over elsewhere, then hands every lease it still owns to
one live node (the consensus leader when that is another live node, else
the lowest name) with one proposal: the receiver adopts them, pulls each
session from this node (or its replicas), and runs their Will and expiry
timers, so offline sessions and clients that did not come back survive the
node. The hand-off is made even when nothing is left to hand: it is the
node's word that it is leaving, and the lease table marks it so until the
node's next incarnation is heard alive. A hand-off that fails, as one sent
to a leader that loses an election does, is made again each second until
`cluster.drain.timeoutMs` has passed, counted
`hoardmq_drain_handoff_retries_total`; one the lease table refuses is not.
From the hand-off the node proposes no alive record and does not fence. Next the drain leaves the shard map (phase
`leaving-placement`): it waits until a map without it has applied here, and
then until every replica copy it kept as a moved target has been confirmed
held at its owner's new targets, each wait at most twice
`cluster.replication.ackTimeoutMs`. A node the copy count keeps in the map
does not wait. The outcome is counted as
`hoardmq_drain_placement_total{outcome}`: `left`, `placed` (the copy count
kept it), `map-expired` or `copies-expired`; a drain that made no hand-off
counts `not-handed-off`, and stays in the map through its down window. A redirect or a hand-off never
targets a node marked leaving. Its down window then needs no session's
copy, which is what keeps a rolling restart's publishes from waiting on the
restarting pod. The drain then waits, up to `cluster.drain.confirmTimeoutMs` (default
30 s, the lease ttl's default), until every session it held and handed off
is confirmed held by its receiver — the confirmation the sealed hand-over
already sends, once the receiver holds the session and its replicas have it. A
session's owner sends its confirmation to every other live member it does not
replicate the session to, and to every member marked leaving. So a session that
the receiver loses at once to a further owner, as when a redirected client
returns, is confirmed by that owner, and so is a copy this node kept as a moved
target for a session that changed owner across the map change.
Meanwhile the node answers pulls and admits no client; it is a replica
target only if the copy count kept it in the map. Sessions still unconfirmed when the wait ends
are counted (`hoardmq_drain_sessions_unconfirmed_total`), audited and logged
at Warn with their clients: those still held here stop with the node, a
restart keeps them, and a node that never returns loses them. A session whose
copy left this node without a confirmation is waited for and counted the same
way. Confirmed ones count `hoardmq_drain_sessions_confirmed_total`, including
those whose confirmation came before the wait began. The drain then reports
complete and `serve` exits 0. With `cluster.drain.holdExitMs` above 0 (default
0), a drain that no signal started is followed by that long a wait for
SIGINT or SIGTERM, which ends it at once, before the exit; the status still
reads complete meanwhile. That is for a Kubernetes preStop hook running
`hoardmq drain -wait`: the hook runs in the broker's container, whose
processes the kernel kills when the broker exits, and the kubelet sends
SIGTERM only once the hook has returned, so the hold lets the hook read the
completion and exit 0 first. A drain started by the signal itself
(`cluster.drain.onSignal`) is not held. With no other live node the clients get Server Busy,
the leases lapse for a future leader to adopt, and nothing is awaited.
`GET /drain` (and the admin API's `GET /api/v1/cluster/drain`) returns `{"draining","phase","clientsRemaining","leasesRemaining",
"sessionsAwaiting","sessionsConfirmed","sessionsUnconfirmed","copiesKept","complete"}` (`copiesKept` while it is not zero);
each phase is audited as `server.drain`.

**Removing a member.** `DELETE /api/v1/cluster/members/{node}` (`hoardmq
cluster remove NODE`) removes a consensus member. Any node takes the call; a
follower forwards it to the leader, which decides and proposes the removal, a
raft configuration change. The leader refuses, with 409 and the reason: the last
voter (`last-voter`); a removal after which the voters left that are alive by
the log and reachable from the leader are not a majority of them (`quorum`); a
member that owns leases (`owns-leases`: drain it, or wait until it is declared
dead and its leases are adopted); and a member alive by the log that is neither
drained nor below the cluster's level (`not-drained`). An unknown name is 404.
A leader asked to remove itself hands leadership to a caught-up voter first and
the call goes on to the new leader (a call that reached it through another node
is answered 409 naming the new leader, to repeat); with no such voter, 503. The
removal drains nothing. A node that applies its own removal reports `removed`
and is never ready again; it does not wipe its storage or rejoin by itself. A
member stopped before its removal and restarted on its old volume never learns
of it: its log ends before the removal and no member answers it, so it stays
not ready (`recovering`) and admits no client, without disturbing the leader.
Clear its data directory and it joins as a new member.

Readiness (`/readyz`) is true only when the node serves, is not draining,
is not recovering,
its consensus core is a caught-up member, its leases are not fenced, its
build understands the cluster's feature level (["Upgrades and the cluster feature level"](clustering.md#upgrades-and-the-cluster-feature-level)), and
the topology — the membership with roles and the set of nodes alive by lease
record — has been unchanged for
`cluster.readiness.stableMs` (default 5 s; 0 disables the condition). A node
joining, or one lost and declared dead, therefore makes every node briefly not
ready, which is what keeps traffic off a cluster still settling. The shard
map is not part of it: a drain or a return moves only the map, and the other
nodes stay ready. A node whose consensus loop has stopped on an error (a failed
write or fsync of the consensus log, a snapshot it could not install) is not
ready either. Nor is a member that has fallen behind and is not catching up: a
follower's own commit index stops where its log ends, so once an election
timeout each follower asks its leader for the commit index (a raft read index
request, answered after one heartbeat round that concurrent requests share).
A member whose applied index stays below the answer, without advancing, for
20 election timeouts (at least 10 s; 20 s at the defaults) reports `behind`
(`/servingz` answers `not ready: behind`) until it advances again; a follower
that applies more slowly than the leader commits is advancing and stays ready.
`hoardmq_raft_apply_stalled` is 1 while the node is behind and
`hoardmq_raft_apply_stalls_total` counts the times it became so, with a warning
naming its applied index and the leader's commit. This is how a voter whose
log ends below the leader's compacted log, and which cannot receive the
snapshot it needs, shows.

Liveness (`/healthz`) fails, answering 503 with the cause, only while the node
cannot recover without a restart: its consensus loop has stopped on an error
(`consensus-stopped`), or its own or its replica journal cannot be rewritten
after a failed write or fsync because what the rewrite reads is damaged — a
checksum failure, records missing behind a checkpoint that fails, a checkpoint
or record that does not decode (`journal-unrecoverable`,
`replica-journal-unrecoverable`). A full or failing disk does not fail it: the
node refuses QoS 1 and 2 publishes and rewrites the journal once the disk
works. A restart over a damaged journal then refuses to start, naming the
files, and the node's sessions are taken over from their replicas. The
consensus log is not rewritten after a failure; the restart replays it.

A ready node can still refuse QoS 1 and 2 publishes: while it lacks the
routes of an owner that holds sessions — after restarting beside a peer it
cannot hear — any subscriber could be on that owner, so it refuses every
QoS 1 and 2 publish (see ["Routing across nodes"](clustering.md#routing-across-nodes)). That state is
**degraded**, and readiness does not report it, so a load balancer keeps the
node in service, and its connections and QoS 0 traffic go on. `/servingz`
answers 200 `serving` when the node is ready and knows every owner's routes,
503 `degraded: routes-unknown` when it is ready without them, and 503
`not ready: <reason>` when it is not ready. The gauges
`hoardmq_node_degraded{reason="routes-unknown"}` (1 while degraded) and
`hoardmq_node_degraded_seconds` (how long the routes have been unknown, 0
when not degraded) carry the same state, and `GET /api/v1/cluster` carries
`serving` and `servingReason`. The chart's readiness probe stays on
`/readyz`.

## Kubernetes deployment

HoardMQ releases are source releases: the project publishes no image, no
chart repository and no SBOM. `Dockerfile` and `deploy/helm/hoardmq` are
references to copy and adapt; each deployment builds its own image and runs
its own chart.

The reference `Dockerfile` builds a static binary on a distroless base,
running as non-root, `hoardmq serve` with the configuration at
`/etc/hoardmq/config.json`. Both bases are pinned by version and digest; its
header says how to refresh a pin. Build an image from a release tag and check
what it reports:

```
git checkout v0.1.0
podman build --build-arg VERSION="$(git describe --tags --always --dirty)" -t <registry>/hoardmq:v0.1.0 .
podman run --rm <registry>/hoardmq:v0.1.0 version
```

The image's version comes only from `VERSION`: the build context leaves
`.git` out, so an image built without it prints `devel` (`devel (/src)` with
`CONTRIB`), even from a tagged checkout. `--build-arg CONTRIB="_authsql _sinkpostgres"` compiles the named
`contrib/` modules in with `hoardmq-build`, fetching their dependencies through
the Go module proxy, and `hoardmq version` lists them under `modules:`. An
SBOM of the image you deploy is yours to make, with any generator that reads
an image (Syft and Trivy are two); it then describes what you actually run,
modules and base included. `docker` takes the same arguments as `podman`.

The reference chart is one StatefulSet with parallel pod management (the broker's
join-not-bootstrap forms the cluster), a headless Service publishing
not-ready addresses for discovery, a client Service, a PodDisruptionBudget
of one, volume claim templates, mutual TLS between nodes from a
chart-generated CA reused across upgrades (or cert-manager), a preStop
`hoardmq drain -wait` with the broker holding its exit for the SIGTERM that
follows the hook (`cluster.drain.holdExitMs`), a termination grace period of
the drain's two timeouts plus a margin, readiness on `/readyz` and liveness on `/healthz`. With `admin.enabled` the
chart configures the admin API on the pod's loopback with a generated drain
token and operator token, and the preStop drains through it. The chart sets a
connect rate, a connection bound per listener and a subscription limit sized to
its default resources. No operator and no CRD are required. The chart names
no image and refuses to render without `image.repository` and `image.tag`;
`Chart.yaml`'s `version` and `appVersion` are the release tag without its `v`.
The chart's README covers the values, the N-1
capacity trade-off and the rolling-restart runbook; `go test ./deploy/...`
keeps the templates consistent with `values.yaml` without Helm installed.

## Persistence

`node.dataDir/logs/state/` holds the write-ahead log: segmented append-only
files with a length, type and CRC-32C per record and a versioned header per
segment, plus a checksummed checkpoint of the whole state. Sessions with a
non-zero Session Expiry Interval, their subscriptions, in-flight and queued
messages, Wills and expiry times, and every retained message are journaled;
transient sessions are not, because nothing of theirs could be recovered.
A queued message leaves the log, and every replica, when it leaves the queue,
sent or expired, at every QoS, so neither a restart nor a takeover from a
replica sends a QoS 0 message again once it has gone out; a crash between the
send and the record reaching the disk, or a takeover before a replica has
applied it, still can. An in-flight message that expires before its session
resumes leaves the log and every replica too, when the resumption drops it.

Under the default `node.walSync: "commit"`, a QoS 1 or 2 PUBLISH is
acknowledged only after its records are fsynced. `"relaxed"` syncs every
`node.walSyncIntervalMs` (default 100) instead and is opt-in. Segments roll
at `node.walSegmentBytes` (default 64 MiB); a checkpoint is written at
start, at shutdown and after every 10,000 records, and the segments before
it are dropped. Sessions keep being journaled while a checkpoint reads them,
so it takes the log's position first and, for each session, the position it
read that session at; recovery replays the records after the first and skips
a session's records up to its own, so each is applied once. The replica log's
checkpoint is read under the lock its records are applied under, and is
exact at one position.

A checkpoint is the whole state as one JSON document. Measured on one node (16
cores, 32 threads) with every session offline and persistent, each holding one
subscription and one queued QoS 1 message, three runs each: at 100,000
sessions a store's first checkpoint takes 0.34–0.36 s and adds about 0.32 GiB
of heap at its peak over 0.23 GiB live, and a later one 0.24–0.26 s and about
0.16–0.21 GiB; it writes 69 MiB, and a restart reads, decodes and installs it in
0.46–0.50 s. At 1,000,000 sessions the first takes 3.26–3.32 s and adds 3.0 GiB
over 2.4 GiB live, a later one 2.76–2.86 s and 2.1 GiB; it writes 696 MiB, and a
restart takes 5.65–5.76 s. The payload is encoded a session at a time into a
buffer sized from the last checkpoint's. Only the sync, the write and the fsync
hold the store's lock; encoding does not. The broker checks every 30 s and
writes one only after 10,000 records. So the format stays: the restart is
seconds at a million sessions, and what grows with the state is the heap a
checkpoint borrows, so size memory for about 2.3 times the sessions' live
state at 1,000,000 sessions and 2.4 times at 100,000 (the first checkpoint
after a start; 1.7–1.9 times for later ones). The measurement is `TestCheckpointScale` in `internal/session`
(`HOARDMQ_CHECKPOINT_SCALE=100000,1000000`;
[docs/evidence/checkpoint-scale.md](evidence/checkpoint-scale.md)). On start, a record cut short at the end of the last segment
(a torn write) is discarded, and so is a last segment a crash left before its
header reached the disk (shorter than a header, or a header of zeros with
nothing after it), which holds no record; the start logs what it discarded.
In an earlier segment either is corruption. Corruption, a checksum failure
anywhere, and records missing from where recovery starts (see "Durability and
group commit") each stop the node with the files named, so a damaged log is
never silently read past.

`serve` locks its data directory once its listeners are bound and before it
opens any file in the directory, so a start that fails to bind still writes
nothing: an exclusive `flock` on `node.dataDir/LOCK`, held until it exits, with
its PID written into the file. A second `serve` on the same directory refuses to start, naming the
file and that PID ("data directory D is in use: D/LOCK is locked by another
process (PID N)"), and exits 1. The kernel releases the lock when the process
ends, so a crash leaves nothing to clean up; the PID is only the last holder's,
for the message, and a copy of the directory need not include the file. On a
filesystem that cannot lock (`ENOLCK` or `EOPNOTSUPP`, as some network
filesystems answer) `serve` starts anyway, logs a warning naming the directory,
and `hoardmq_data_dir_locked` reads 0: nothing then stops a second node using
the directory.

When a node refuses to start over its logs, `hoardmq wal verify -config FILE`
reports, writing nothing, what a start would do with each log the document
names: the journal, the replica log, the consensus log and its stored
snapshot. For each it gives the segments a start keeps and the checkpoint it
recovers from, then everything the start would repair (a torn tail, a last
segment cut before its header), note (a checkpoint passed over, one past the
log's end) or refuse on (a damaged record or segment, records missing from
where recovery starts, a checkpoint or record that does not decode, a stored
snapshot it cannot read). It runs the start's own checks, so the two agree.
`-json` prints the same as one object. It exits 0 when a start would proceed
and 1 when it would refuse. It refuses while another process holds the lock
(stop the node first), and on a filesystem that cannot lock runs and says so.

`hoardmq wal repair -config FILE` makes only the repairs a start would make, and
only when nothing a start refuses on is left in any log. Without `-apply` it
reports what it would do and writes nothing. With `-apply` it takes the lock,
and never runs without it. It copies what each repair drops into
`node.dataDir/wal-repair/<time>/` (a torn tail's bytes, a removed segment whole;
`-discard` keeps no copy), makes the repairs through the start's own code, and
verifies again. It does not repair a damaged record or segment, a gap, or a
consensus log a start refuses: the report names the recovery instead, which is
to restore the data directory from a backup, or to remove the member
(`hoardmq cluster remove NODE`, see "Removing a member") and start it again with
an empty data directory. Run it to see, and keep, what a start would discard;
the start makes the same repairs itself.

## Durability and group commit

Under `walSync: "commit"` (the default), a QoS 1 or 2 publish the broker
acknowledged, with the session and retained records it wrote, survives the
loss of fewer nodes than hold a copy of it: `cluster.replicas`, owner plus
followers, or every live node when there are fewer. The acknowledgement waited
for the publisher's node to fsync its records and for every node holding a
copy of them to sync its replica log (clustering.md, ["Data-plane
replication"](clustering.md#data-plane-replication)). That is any minority of
the nodes only while the copies are at least half the nodes, rounded up. The
chart's defaults are the worked case: 3 pods and 2 copies, so any one node may
be lost (the chart's README, ["Sizing: the N-1
trade-off"](../deploy/helm/hoardmq/README.md#sizing-the-n-1-trade-off)); on 5
nodes with 2 copies, losing 2 can lose what both held. A SUBACK, UNSUBACK or
CONNACK waits only for its own node's fsync (below), so what it acknowledged
is as durable as that node's disk until a copy reaches another node.

A simultaneous power loss of every node keeps acknowledged work only as well
as each host's fsync keeps what it reported written: every acknowledgement
waited for one, and a disk that reports a flush it has not done loses it. No
test cuts power, so this is untested: the tests that crash a node leave its
files as they are, or, with a fault-injecting disk, drop what its last
successful fsync did not cover, which takes the fsync's word for what reached
the disk. `walSync: "relaxed"` acknowledges before the disk has the record and
is outside what these two paragraphs state.

Under `walSync: "commit"` (the default) a QoS 1 or 2 acknowledgement, a
session change or a takeover is answered only after the record is fsynced.
Concurrent callers share fsyncs: whoever finds no fsync in flight leads one,
everyone who flushed before it started is released by it, and callers that
arrive while it runs join the next, which starts the moment the current one
finishes — so the disk sees one fsync per group, not one per operation, and
no caller waits more than two. A caller returns only after an fsync issued
after its own write reached the file.

A failed fsync may have dropped every write not yet on the disk, and a later
fsync that succeeds does not write them again, so after one the journal is not
whole and no later fsync releases a caller. The next group rewrites it instead
(so do the broker's 30 s checkpoint check and its shutdown checkpoint): the
state at the log's last index, rebuilt from the latest checkpoint, the records
after it that fsyncs covered, and the records kept in memory until one did, is
written as a checkpoint with its own fsync; appends move to a new segment and
the segments before it are deleted. The callers whose own fsync failed get the
error; the others are released once the rewrite lands. A rewrite that fails
fails its callers, and the next is tried no sooner than a second later. Every
fsync of a segment file is serialized with every other, so a failure the
kernel reports to a roll's or a checkpoint's fsync is seen by the group whose
fsync it overlapped. The replica log is rewritten the same way. The group
commit's counters in the harness's storm statistics (`sync`) include `failures`,
`repairs` and `repair_failures`, and the checkpoints written, their time and the
part of it under the store's lock (`checkpoints`, `checkpoint_sec_total`,
`checkpoint_hold_sec_total`). The same failures, repairs and checkpoints, for both
logs, are on `/metrics` as the `hoardmq_journal_*` series (see "Observability").

A node does not start over records its journal no longer holds. Recovery reads
the newest checkpoint whose checksum verifies and replays every record after it;
when one is missing — the checkpoint failed its checksum or could not be read
after the segments before it were dropped, or a segment is gone from between two
that are there — it logs an Error naming the missing records, the segment after
them, the directory and each checkpoint it passed over, and `serve` exits 1. The
files are left as they are. A checkpoint that fails its checksum while every
record from the first is still held is passed over with a warning, and the whole
log is replayed. A rewrite after a failed fsync that meets the same gap fails
rather than writing what remains as the journal, and its callers are refused.
The replica log refuses the same way.

A power cut inside a rewrite, after its checkpoint reached the disk and before
its new segment did, can leave the checkpoint past the log's end: the records
the failed fsync covered are in the checkpoint and gone from the segment. The
next start logs a warning naming both indexes, recovers from the checkpoint,
and starts a segment one past it before anything is appended, so every later
record lies where recovery replays from. The run of indexes this leaves between
the segments is below the checkpoint and is not refused; if that checkpoint
later fails its checksum, those records are held nowhere and the node refuses
to start as above. The replica log does the same.

The consensus log syncs itself before it writes a checkpoint (one fsync for each
snapshot taken or installed), so a checkpoint never names records the disk
does not hold and the durability it reports covers them. If a disk loses
records it had synced and the log ends below where its checkpoint replays from,
the next start appends from that point, logging a warning, so later records are
not skipped.

A record's length is outside its checksum, so recovery tells a torn write from a
damaged length by the checksum it has: a record in the last segment whose length
reaches past the end of the file is truncated as a torn write unless its
checksum matches its bytes up to the end of the file or up to where a whole,
sound record starts. Then the record was written whole and only its length is
wrong, and the node stops with the segment, offset and record named instead of
truncating it and every record after it. A length over 1 GiB is damage wherever
it is, and nothing is allocated for a length beyond the bytes the segment holds;
a record over 1 GiB is refused when it is written, which refuses its publish.
This cannot tell a damaged length apart from a torn write when the same record's
type, checksum or payload is damaged too, or when the damaged record is followed
only by a torn one: those are still truncated as a torn tail.

A failed write to the journal, such as a full disk, also leaves it not whole: the
log's buffered writer keeps the error, so nothing written after it would reach
the file. The callers it held are refused, and the next append, `Sync` or
checkpoint rewrites the journal as after a failed fsync, starting its new
segment with a new writer; while the disk is still full the rewrite fails and is
tried again no sooner than a second later, refusing in between. Once the disk
has room the node takes and acknowledges writes again without a restart. The
replica log does the same; the consensus log does not, and keeps a failed write
until the node restarts.

The replication layer keeps two small files beside the replica log: the last
shard map version it acted on, which a restart never drops replica copies
below, and the retained shards it keeps for their new leader. Each is replaced
as a checkpoint is: a temporary file synced, renamed into place, and the
directory synced, so a crash leaves the old content or the new. A write that
fails leaves the old file and is logged at Warn; the version in memory moves
only once the new one is on the disk. When the journal drops the segments
before a checkpoint and one cannot be removed, the segments not yet removed
stay listed and the next checkpoint tries again.

A record that fails to append refuses the QoS 1 or 2 publish that wrote it,
whichever session it was written into: the publisher's own, a subscriber's on
this node or on a node the publish reached (directly, or through a shared
group's dispatch owner), or the retained store when this node holds the topic.
The failure is the publish's, so it does not mark the subscriber's session. A
copy queued for the subscriber whose record failed is withdrawn, so the
publisher's retry is its one copy; a copy already sent to the subscriber stays
in flight and completes. A delivery nothing acknowledges (a QoS 0 publish, a
Will, a redistribution, a retained message sent on subscribe) keeps its copy.
Copies whose records were written stay where they are, so a subscriber that
received one is sent the retry too (see ["Sessions"](clients-and-sessions.md#sessions)).

A SUBACK or UNSUBACK for a session the journal records grants a filter only
once the filter's record is appended and fsynced, and a CONNACK accepts a
connection only once the fsync after the connect's records succeeded; none of
them waits for the replicas. These waits share the group commit above, so a
burst of subscribes shares its fsyncs. A filter whose record failed to append,
or whose fsync failed, is refused with 0x83 (0x80 in a 3.1.1 or 3.1 SUBACK) and
left as the session held it before the request: in memory, in routing and, by
a record that undoes the request, in the journal. Because MQTT 5.0 section
3.10.4 stops new messages for a filter as soon as its UNSUBSCRIBE arrives, a
message published while a refused UNSUBSCRIBE waited is not delivered for that
filter. At 3.1.1 and 3.1, whose UNSUBACK carries no reason code, a refused
UNSUBSCRIBE closes the connection without one. A refused CONNECT (0x83; return
code 3 at 3.1.1 and 3.1) leaves the session as an abnormal end without a Will
would: offline with its expiry, or ended when its expiry is 0. A session with
a Session Expiry Interval of 0 is not journaled, so its SUBSCRIBE and
UNSUBSCRIBE wait for nothing.

`walGroupMaxDelayMs` lets a leader hold before its fsync to collect more
callers; `0`, the default, never holds — under load arrivals overlap every
fsync anyway, and a lone writer pays nothing. `walGroupMaxSize` fires a held
group early once that many callers joined, and only matters when the delay is
non-zero. The default delay of 0 was chosen by measurement (one 100,000-client
failover run at 0, one at 1 ms); the size matters only with a delay, and was
not measured ([docs/evidence/wal-group.md](evidence/wal-group.md)).
`walSync: "relaxed"` syncs on an interval instead
and acknowledges before the disk has the record.
