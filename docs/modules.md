# Modules reference

The extension modules: how a module is compiled in, and the modules in this repository, the Kafka forwarder, the
Postgres sink, the event webhooks and payload schema validation. The authentication modules are in
[security-and-access.md](security-and-access.md).

## Modules

Everything that authenticates, authorizes, persists, measures or traces is a module
on one registry, built-in or not: `auth.file`, `auth.jwt`, `auth.x509`,
`auth.scram`, `auth.ldap`, `auth.db`, `authz.file`, `store.wal`, `metrics.internal`,
`metrics.prometheus`, `traces.otlp`, the Kafka forwarder `sink.kafka`, the webhooks `auth.http` and
`events.http`, and payload schema validation `schema.json` register the same way a third-party module would, receive
their configuration from `modules.<id>`, declare the hook families they
provide, and run provision, validate, start and stop. `store.wal` and `metrics.internal`
are instantiated by default when no module of their family is configured.
Hook families: connection lifecycle, authentication, authorization, inbound
and outbound packet interceptors (continue, modify, drop, disconnect with a
reason), subscribe and unsubscribe interceptors, delivery events, retained and
Will events, publish admission and publish accepted (below), cluster events
(below), store providers, metrics providers and trace providers. A cluster
event listener is told when this node's view of the consensus membership gains
or loses a member, when a client's lease moves here from another node, and when
this node begins its drain. The broker queues each listener's events, up to
1,024, and never waits for it: a full queue drops the event, counted in
`hoardmq_cluster_events_dropped_total{module}`. `hooks.timeoutMs` bounds every hook
call (0, the default, calls inline) and `hooks.fallback` says what a
timed-out or panicking hook counts as: `continue` (default) or
`disconnect`. `hooks.order` lists the modules called first, in that order, in
every hook family, both authentication chains (certificate authenticators
still run before the rest) and the lifecycle (stop in reverse); every other
module follows in identifier order, and a change applies at the next restart.
`hoardmq check` provisions and validates the configured modules read-only,
so a bad users-file path or an unknown module identifier is reported beside
any schema problem.

**The extension API** is the package tree `github.com/happydave/hoardmq/ext`. It
imports only the standard library:
- `ext` holds the module contract, registration (`ext.Register` from a package's
  `init`), the module context and services, and the hook families;
- `ext/mqtt` holds the packets and the message envelope;
- `ext/auth` holds authentication and authorization;
- `ext/records` holds the replicated records store's values;
- `ext/store` (experimental) holds the store provider's journal, record format and state. A store provider must
  feed the replication tap (`SetTap`), and one without it is refused;
- `ext/trace` (experimental) holds the tracer and the exporter a trace module supplies.

The API has its own version, `ext.APIVersion` (0.8.0), which `hoardmq version` prints
beside the modules compiled in. [ext/CHANGELOG.md](../ext/CHANGELOG.md) states what a
version may change. Built-in modules reach the broker only through this API, and tests
hold them to it (`internal/module/seam_test.go`).

**Building with other modules:** `hoardmq-build` compiles a binary with the built-in
modules plus each module named, from a version or a local directory:

```
go run ./cmd/hoardmq-build --hoardmq-dir . --with example.com/mymodule=../mymodule --output hoardmq
go run ./cmd/hoardmq-build --hoardmq-version v0.1.0 --with example.com/mymodule@v1.2.0
```

It writes a throwaway module whose `main` imports each named module, and runs
`go build -mod=mod` in it. `cmd/hoardmq/main.go` is the same generated `main` with
no modules (`go generate ./cmd/hoardmq`). The worked example of a module built
outside the broker's tree is
[cmd/hoardmq-build/testdata/hello](../cmd/hoardmq-build/testdata/hello/hello.go). Its
module file is kept as `go.mod.txt`: copy the directory and rename it to `go.mod`.
Its test builds it with the tool, offline, and runs its lifecycle in the binary,
including a hot disable and enable through the admin API.
Modules kept in the repository with dependencies of their own, built the same way,
are under [contrib/](../contrib/README.md): `auth.sql` (see ["Authentication and
authorization"](security-and-access.md#authentication-and-authorization)) and `sink.postgres` ("Postgres sink").

**Publish admission and publish accepted** are the two hooks a sink is built on
(API 0.2.0):
- **The admission check** (`ext.PublishAdmission`) is called once for each
  client PUBLISH. It runs after authorization, topic alias resolution and the
  QoS 2 retransmission check, and before any delivery.

  It may refuse with a reason code, which takes the path of any refused
  publish:
  - an error PUBACK or PUBREC at MQTT 5;
  - the connection closed at 3.1.1 and 3.1, and for QoS 0.

  It is counted as `hoardmq_drops_total{reason="admission"}`.
- **The accepted listener** (`ext.PublishAcceptedListener`) is told of each
  publish the node accepts, once, on that node. It is called after the step
  that decides acceptance and before the acknowledgement:
  - QoS 0 after dispatch;
  - QoS 1 after the journal sync and the replication barrier;
  - QoS 2 after those and the packet identifier's acceptance.

  It also hears Wills and module publishes. A refused publish, a copy from
  another node, a shared-group give or a retained hand-over is never reported.

An inbound interceptor runs earlier, before authorization, on the packet with its
topic alias resolved; it sees retransmissions, and its refusal of a QoS 2 one the
session holds open is not honoured (counted in
`hoardmq_inbound_refusals_ignored_total{namespace}`), so a sink refuses from the
admission check, not from an interceptor.

## Kafka forwarder (`sink.kafka`)

Every binary carries `sink.kafka`: the default one, the container image and
`hoardmq-build`'s. It runs only when configured. It produces each publish the
node accepts on a topic a route matches to that route's Kafka topic, MQTT to
Kafka only, with the franz-go client. It is built on the public extension API
alone, and `internal/module/seam_test.go` holds it to that.

```json
"modules": { "sink.kafka": {
  "brokers": ["kafka-0.kafka:9092"],
  "routes": [
    { "filter": "sensors/+/temperature", "topic": "sensor-readings", "key": "topic" },
    { "filter": "alarms/#", "topic": "alarms", "key": "userProperty", "keyProperty": "site" }
  ],
  "clientId": "hoardmq-<node>",
  "tls": { "caFile": "/etc/kafka/ca.pem", "certFile": "/etc/kafka/client.pem", "keyFile": "/etc/kafka/client.key" },
  "sasl": { "mechanism": "scram-sha-512", "username": "hoardmq", "passwordFile": "/run/secrets/kafka" },
  "headers": { "metadata": true, "userProperties": true },
  "acks": "all", "compression": "none", "lingerMs": 5,
  "buffer": { "maxRecords": 100000, "maxBytes": 67108864, "whenFull": "refuse", "waitMs": 1000 },
  "flushTimeoutMs": 5000
} }
```

**Configuration.** Only `brokers` and `routes` are required; the other values
shown are the defaults.
- `acks`: `all` (the idempotent producer) or `leader`.
- `compression`: `none`, `gzip`, `snappy`, `lz4` or `zstd`.
- SASL: `plain`, `scram-sha-256` or `scram-sha-512`. The password comes from a
  file and is never logged.
- TLS always verifies the server.
- `hoardmq check` reports every problem at `/modules/sink.kafka`, naming the
  field, and connects to nothing.

**Mapping.** A route matches the node's default namespace unless it names a
`namespace`, and a message is produced once to each distinct Kafka topic its
routes name (the first matching route naming a topic decides). A Kafka topic is
1 to 249 of `A-Z a-z 0-9 . _ -`, and a shared filter is refused.
- **Key:** the MQTT topic (`topic`, the default), the publisher's client
  identifier (`clientId`), the value of a named user property (`userProperty`
  with `keyProperty`), or `none`. A missing identifier or property gives no key.
- **Key from a topic level:** `"key": {"segment": n}` takes level `n` of the
  topic, counting from 0 (`devices/d7/telemetry`, segment 1, keys `d7`). An empty
  level is an empty key.
  - A route whose filter has no `#` and too few levels is refused at `check`.
  - When a topic has fewer levels (possible under `#`), `fallback` decides:
    `topic` (the default), `clientId` or `none`. Each fallback is counted in
    `hoardmq_sink_kafka_key_fallbacks_total{topic}`.
- **Value:** the payload, empty rather than null.
- **Timestamp:** when the node accepted the publish.
- **Headers, in order:**
  - first `mqtt.topic`, `mqtt.qos` and `mqtt.source` (`client`, `will` or
    `module`);
  - then, when set, `mqtt.clientId`, `mqtt.retain`, `mqtt.contentType`, `mqtt.payloadFormat`,
    `mqtt.responseTopic`, `mqtt.correlationData`, and `mqtt.namespace` outside
    the default namespace;
  - then every MQTT 5 user property, in order, repeats kept;
  - then the route's static `headers`, in name order.

  `headers.metadata` and `headers.userProperties` turn each group off.
  `headers.prefix` (default `mqtt.`, may be empty) replaces `mqtt.` in every
  metadata header's name, and `headers.userPropertyPrefix` (default empty) goes
  before each user property's name; each is at most 64 bytes.
  - A route's `headers` is an object of names to values: at most 32, names 1 to
    128 bytes, values at most 1,024.
  - A static header may repeat a metadata header's name; Kafka keeps both, the
    static one last.

  The username is never sent, and message expiry is not mapped.

**Delivery.** At least once into Kafka while the accepting node runs. The node
that accepted a publish forwards it, once; a copy another node forwards to its
subscribers is not forwarded again, and a Will goes from the node that
publishes it.
- Every node needs the same `modules.sink.kafka`: a node without it forwards
  nothing.
- Messages one connection publishes on one topic reach one partition in order
  when keyed by topic or client identifier.
- A QoS 1 resend is a new message and can reach Kafka twice, as it can reach a
  subscriber twice; a QoS 2 publish is forwarded once.
- The PUBACK or PUBREC means what it always means, that the message is durable
  on this node and its replicas. It does not wait for Kafka.

**When Kafka is down or slow.** The forwarder buffers in memory, up to
`buffer.maxRecords` records or `buffer.maxBytes` bytes. While the buffer is full:
- **`whenFull: "refuse"`** (the default) refuses a client's matching QoS 1 or 2
  publish with Quota Exceeded (0x97). That publish then reaches no MQTT
  subscriber either, and a 3.1.1 publisher is disconnected, since 3.1.1 has no
  way to refuse a publish. A publish admitted while there was room, which then
  meets a full buffer, waits up to `buffer.waitMs` for room before it is
  counted `overflow`.
- **`whenFull: "drop"`** refuses nothing and counts each forward it cannot
  buffer.
- **QoS 0** is never refused: its forward is dropped and counted.

**Counted, never silent:**
- `hoardmq_sink_kafka_produced_total{topic}`;
- `hoardmq_sink_kafka_dropped_total{topic,reason}`, where reason is one of:
  - `buffer_full`: the buffer was full;
  - `overflow`: refuse's wait ran out;
  - `rejected`: Kafka refused the record for good, for example too large, an
    unknown topic or not authorized;
  - `shutdown`: abandoned at stop;
- `hoardmq_sink_kafka_refused_total{topic}`;
- the gauges `hoardmq_sink_kafka_buffered_records` and
  `hoardmq_sink_kafka_buffered_bytes`.

Drops are also logged at Warn, at most once a second per reason, with their
count.

**What is lost, plainly.** A node that crashes loses what its buffer held. Those
messages were acknowledged to their publishers and delivered to MQTT
subscribers, and no counter can count them afterwards. The buffered gauges,
scraped before the crash, are that exposure. At a stop, a drain's included, the
forwarder flushes for up to `flushTimeoutMs`, then abandons the rest, counted
`shutdown` and logged. A record already sent and still awaiting Kafka's answer
then may have been written, and is counted `shutdown` all the same. A drained
node's buffer is not handed to another node.

**A live test** runs the default binary against a real Kafka. It skips unless
both variables are set:

```
SINKKAFKA_LIVE_BROKERS=127.0.0.1:7068 SINKKAFKA_LIVE_CONTAINER=<podman container> go test -count=1 -v -run '^TestLiveKafka$' ./modules/sinkkafka
```

It pauses, stops and starts the container to show each policy. The test's
variables do not start with `HOARDMQ_`, which the broker reads as configuration
overrides.

## Postgres sink (`sink.postgres`, out of tree)

`sink.postgres` writes each publish the node accepts on a topic a route matches
into that route's PostgreSQL table, in batches. Like `auth.sql`, it lives in
[contrib/_sinkpostgres](../contrib/README.md) as a module of its own, built with
`hoardmq-build`, so its driver (pgx, MIT) is never in the broker's module or
the default binary.

```json
"sink.postgres": { "connString": "postgres://hoardmq@db.example.com/mqtt?sslmode=verify-full&sslrootcert=/etc/hoardmq/db-ca.pem",
                   "passwordFile": "/run/secrets/db",
                   "routes": [ { "filter": "sensors/#", "table": "telemetry.readings" } ] }
```

```sql
CREATE TABLE telemetry.readings (
  topic text NOT NULL, payload bytea NOT NULL, qos smallint NOT NULL, retain boolean NOT NULL,
  client_id text, namespace text NOT NULL, source text NOT NULL, accepted_at timestamptz NOT NULL);
```

**Configuration.**
- `connString` and `passwordFile` follow `auth.sql`'s rules: no password in the
  string, and a host that is not this machine reached only with
  `sslmode=verify-full` or `verify-ca`.
- `routes`, 1 to 1,000: a `filter` (not shared), a `table` (`name` or
  `schema.name`, each a letter or `_` and up to 62 letters, digits and `_`,
  quoted as identifiers), and optionally a `namespace`.
- `batch.maxRows` (1,000; 1 to 10,000) and `batch.lingerMs` (5; at most 1,000).
- `timeoutMs` (10,000; 100 to 60,000) bounds one batch's write.
- `buffer` and `flushTimeoutMs` are `sink.kafka`'s, with its defaults and bounds.

**Mapping.** One row per accepted publish per distinct table its routes name,
the first route naming a table deciding. The columns are the ones above:
- the topic and the payload (empty rather than NULL);
- the QoS and the Retain flag;
- the publisher's client identifier, NULL for a module's publish;
- the namespace;
- `source`: `client`, `will` or `module`;
- when the node accepted it.

The username, user properties and message expiry are not written. A table may
have more columns if they have defaults.

**Delivery.** At least once into Postgres while the accepting node runs. A batch
is one COPY, which the server applies whole or not at all. One writer sends one
batch at a time, so rows reach a table in the order the node accepted them.
- **A failure the database may recover from** is retried, the batch kept and
  still counted in the buffer, after a backoff from 100 ms doubling to 5 s.
  That is a broken or refused connection, a timeout, or SQLSTATE classes 08,
  40, 53 and 57. A batch whose COPY timed out after the server committed it is
  written twice.
- **Any other database error** refuses the batch for good: its rows are counted
  `rejected` and logged. That covers a missing table, a type mismatch, a
  constraint or a permission. A constraint on the table refuses a whole batch.

**When Postgres is down or slow.** The buffer and its policy are `sink.kafka`'s
exactly:
- **`whenFull: "refuse"`** (the default) refuses a client's matching QoS 1 or 2
  publish with Quota Exceeded (0x97) while the buffer holds `buffer.maxRecords`
  rows or `buffer.maxBytes` bytes. A publish admitted with room that meets a full
  buffer waits up to `buffer.waitMs` before it is counted `overflow`.
- **`whenFull: "drop"`** accepts and counts each row it cannot buffer
  `buffer_full`.
- **QoS 0** is never refused: it is dropped and counted.

**At a stop**, the buffer is written for up to `flushTimeoutMs`, and what is left
is counted `shutdown`. That includes a batch in flight whose answer has not come.

**Counted, never silent:**
- `hoardmq_sink_postgres_written_total{table}`;
- `hoardmq_sink_postgres_dropped_total{table,reason}`, where reason is
  `buffer_full`, `overflow`, `rejected` or `shutdown`;
- `hoardmq_sink_postgres_refused_total{table}`;
- `hoardmq_sink_postgres_retries_total`;
- the gauges `hoardmq_sink_postgres_buffered_records` and
  `hoardmq_sink_postgres_buffered_bytes`, which are what a node that crashes
  loses.

Drops and retries are logged at Warn, at most once a second each.

The live test, `TestSinkPostgresLive` in `cmd/hoardmq-build`, takes the same
environment as `auth.sql`'s.

## Event webhooks (`events.http`)

Every binary carries `events.http` and `auth.http` (package `modules/webhook`),
built like `sink.kafka` on the public extension API alone; the package's test
copies it into a module of its own and builds it there, offline. It posts the events the API carries to an HTTP endpoint,
one JSON object a request, or an array of up to `batch.maxEvents`:

```json
"events.http": { "url": "https://hooks.example.com/mqtt-events",
                 "events": ["client.connected", "client.disconnected", "message.publish"] }
```

Each event carries `event`, `timestamp` (Unix milliseconds), `node` and
`namespace`, and:

| `event` | Fields |
|---|---|
| `client.connected` | `clientid`, `username`, `peername`, `listener`, `proto_ver` |
| `client.disconnected` | the same, `reason` (the code), `graceful` |
| `session.subscribe` | `clientid`, `username`, `topic` (the filter), `qos`: the request, before authorization |
| `session.unsubscribe` | `clientid`, `username`, `topic` (the filter): the request, before authorization |
| `message.publish` | `source` (`client`, `will` or `module`), the publisher's `clientid` and `username`, `topic`, `qos`, `retain`, `payload` |
| `message.delivered`, `message.acked`, `message.queued`, `message.dropped`, `message.expired` | `clientid` (the receiving session), `topic`, `qos`, `payload` |
| `retained.changed` | `topic`, `payload` (empty: removed) |
| `will.published` | `clientid`, `topic`, `qos`, `payload` |
| `cluster.member.joined`, `cluster.member.left` | `member`: the node this node's view of the consensus membership gained or lost |
| `cluster.lease.moved` | `clientid`, `from`, `to`: a client's lease moved to this node from another, with its `namespace` |
| `cluster.drain.started` | `member`: this node began its drain |

`payload` is base64, or left out with `"payload": "omit"`.

**Cluster events** are posted by each node about what it saw, so `node` tells
the reports apart:
- a membership change is reported by every node, within about a second (each
  node reads its view once a second, and its first view at start raises
  nothing);
- a lease move once, by the node that became the owner;
- a drain by the draining node.

The broker queues them for the module, up to 1,024 at a time, and never waits
for it; one that finds the queue full is dropped and counted in
`hoardmq_cluster_events_dropped_total{module}`. They carry nothing else of
the cluster.

**Configuration.** Only `url` is required (`http://` or `https://`); the
defaults:
- `events`: all of the above;
- `mode`: `async`;
- `buffer.maxEvents`: 10,000 (1 to 1,000,000);
- `batch.maxEvents`: 1 (1 to 1,000), `batch.maxDelayMs`: 100 (0 to 10,000);
- `retry.maxAttempts`: 3 (1 to 10), `retry.backoffMs`: 200 (10 to 10,000,
  doubled up to 5 s, or up to itself when longer);
- `timeoutMs`: 5,000 (100 to 30,000);
- `caFile`, `certFile`, `keyFile`, `bearerTokenFile`, `headers` and
  `followRedirects` as `auth.http`'s.

An attempt fails on a transport error or any answer but 2xx, a 4xx included.

**Async** (the default) never blocks the broker: a hook puts its event in the
buffer and returns. While the buffer is full a new event is dropped. At a stop
the buffer is sent for up to `timeoutMs`, and what is left is dropped.

**Sync** posts on the path that raised the event and returns once the endpoint
answers 2xx or the attempts are spent, so it slows exactly that path:
- `message.publish`: the publisher, before its acknowledgement and its next
  packet;
- the delivery events: that session's deliveries;
- connection, subscribe and unsubscribe events: that client's CONNECT, SUBSCRIBE or UNSUBSCRIBE;
- `retained.changed` and `will.published`: the path that stored or sent them;
- cluster events: only the module's own cluster event queue.

Nothing of another client waits. The wait is bounded by the attempts, their
timeouts and backoff, and by `hooks.timeoutMs` when set. A failed event is
dropped, never refused: an outage slows the path and refuses nothing.

**Counted, never silent:** `hoardmq_events_http_sent_total`,
`hoardmq_events_http_dropped_total{reason}` with reason `buffer_full`,
`failed` (attempts spent) or `stopped`, and the gauge
`hoardmq_events_http_buffered`. Drops are logged at Warn, at most once a second
per reason, with their count. Events are not durable: what the buffer holds is
lost if the node crashes.

## Payload schema validation (`schema.json`)

Every binary carries `schema.json` (package `modules/payloadschema`), built on the
public extension API alone; it runs only when configured. It checks each client
PUBLISH whose topic a rule's filter matches against that rule's JSON Schema, from
the publish admission check: after authorization, the topic alias and the QoS 2
retransmission check, and before any delivery. Wills and modules' publishes are
not checked.

```json
"schema.json": { "audit": true, "rules": [
  { "name": "readings", "filter": "sensors/+/reading", "schemaFile": "/etc/hoardmq/reading.schema.json" },
  { "name": "commands", "filter": "cmd/#", "schema": { "type": "object", "required": ["op"] },
    "onInvalid": "disconnect", "reason": 135 } ] }
```

**Rules** are tried in order; the first whose filter matches the publish's topic,
in its namespace (`namespace`, default the node's), decides, and a publish no
rule matches is admitted unchecked. Each rule has:
- `name`: 1 to 64 of `a-z 0-9 _ -`, unique; it labels the metrics and audit records;
- `filter`: not a shared filter;
- exactly one of `schema` (inline) or `schemaFile` (read at start and at a
  configuration reload; a file that no longer compiles keeps the rules held);
- `onInvalid`:
  - `drop` (the default): refuses the publish;
  - `disconnect`: refuses it and closes the connection with the reason;
  - `continue`: delivers it and counts the violation;
- `reason`: one of 0x80, 0x83, 0x87, 0x90, 0x97 and 0x99 (the default, Payload
  format invalid), the codes a PUBACK, a PUBREC and a DISCONNECT all accept.

At most 1,000 rules.

**A refused publish** takes the path of any refused publish:
- at 5.0, a PUBACK or PUBREC with the reason. A QoS 2 refusal releases its packet
  identifier, so the client's next publish under it is a new one;
- at 3.1.1 and 3.1, and for QoS 0 at any level, the connection is closed, since
  those have no way to say no.

It is counted as `hoardmq_drops_total{reason="admission"}`.

**Validation.** The payload must be exactly one JSON value; anything else (empty,
a byte-order mark, two values) is a violation. The schemas are JSON Schema draft
2020-12, a closed subset the module implements itself:
- **asserted:**
  - `type`, `const`, `enum`;
  - `properties`, `required`, `additionalProperties`, `patternProperties`,
    `propertyNames`, `minProperties`, `maxProperties`;
  - `items`, `prefixItems`, `minItems`, `maxItems`, `uniqueItems`;
  - `minimum`, `maximum`, `exclusiveMinimum`, `exclusiveMaximum`, `multipleOf`,
    compared exactly on the decimal numbers as written;
  - `minLength`, `maxLength` (in characters), `pattern` (RE2 syntax, unanchored);
  - `allOf`, `anyOf`, `oneOf`, `not`;
  - `$ref` to `#/$defs/<name>`;
  - `true` and `false`;
- **accepted and not asserted:** `$schema` (it must name draft 2020-12), `$id`,
  `$comment`, `$defs`, `title`, `description`, `default`, `examples`,
  `deprecated`, `readOnly`, `writeOnly`, `format`;
- **refused at `check` and start,** naming the rule and the keyword's path:
  anything else (`if`, `contains`, `unevaluatedProperties`, a remote `$ref`, …),
  so a schema never means less than it says.

Duplicate object keys keep the last value. A retained message's removal (an empty
payload) is not JSON, so a rule covering a retained topic refuses its removal.

**Counted:**
- `hoardmq_payload_schema_checks_total{rule,outcome}`, where `outcome` is `valid`
  or `invalid`;
- `hoardmq_payload_schema_refusals_total{rule,action}`.

**Audited:** with `audit` true, each refusal is recorded as `payload.invalid`:
the actor is the client identifier, the target the rule, the outcome the reason,
and the detail holds the topic, the QoS and what was wrong (`/temp: maximum 100`).
No value from the payload is recorded; the path names the object keys that lead
to the violation. Module events never wait for the audit log; one dropped from
a full queue is counted in `hoardmq_payload_schema_audit_dropped_total`.
