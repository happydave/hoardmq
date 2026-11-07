# Configuration reference

The configuration document and how a running node takes a change: the schema and an example with the defaults,
reloading, listeners, and the environment overrides. Who may connect and what they may do is in
[security-and-access.md](security-and-access.md), and each module's members are in [modules.md](modules.md).

## Configuration

One JSON document, validated against the published schema
[schema/hoardmq.schema.json](../schema/hoardmq.schema.json). `hoardmq check`
reports every problem in one run, as `PATH: /json/pointer: message`, sorted.

```json
{
  "version": 1,
  "node": { "name": "n1", "dataDir": "/var/lib/hoardmq", "walSync": "commit", "walGroupMaxDelayMs": 0, "walGroupMaxSize": 256 },
  "listeners": [
    { "name": "plain", "type": "tcp", "address": ":1883" },
    { "name": "secure", "type": "tls", "address": ":8883",
      "tls": { "certFile": "cert.pem", "keyFile": "key.pem", "clientCAFile": "ca.pem", "minVersion": "1.3" } },
    { "name": "web", "type": "ws", "address": ":8080", "path": "/mqtt", "maxConnections": 0, "maxHandshakes": 512 },
    { "name": "webs", "type": "wss", "address": ":8443", "tls": { "certFile": "cert.pem", "keyFile": "key.pem" } }
  ],
  "cluster": { "id": "hoardmq", "replicas": 2, "shards": 64, "listen": ":7000",
               "auth": { "mode": "mtls", "tls": { "certFile": "node.pem", "keyFile": "node-key.pem", "caFile": "cluster-ca.pem" } },
               "peers": [ { "name": "n2", "address": "n2:7000" }, { "name": "n3", "address": "n3:7000" } ],
               "heartbeatMs": 1000, "heartbeatMisses": 3, "maxFrameBytes": 16777216,
               "raft": { "tickMs": 100, "electionTicks": 10, "heartbeatTicks": 1, "snapshotEntries": 10000,
                         "snapshotTrailing": 1000, "promoteLag": 100, "joinTimeoutMs": 30000 },
               "discovery": { "mode": "static" },
               "leases": { "ttlMs": 30000, "takeoverTimeoutMs": 5000 }, "routing": { "syncMs": 30000 },
               "replication": { "ackTimeoutMs": 2000 },
               "drain": { "ratePerSecond": 100, "timeoutMs": 30000, "confirmTimeoutMs": 30000, "holdExitMs": 0 },
               "readiness": { "stableMs": 5000 },
               "advertise": "broker1.example:1883" },
  "limits": { "maxPacketSize": 1048576, "receiveMaximum": 1024, "maxInflightMessages": 65535,
              "maxQueuedMessages": 1000, "maxKeepAlive": 0, "connectTimeout": 10, "connectRate": 0, "connectBurst": 0,
              "clientMessageRate": 0, "clientMessageBurst": 0, "clientByteRate": 0, "clientByteBurst": 0,
              "maxTopicAliasBytes": 1048576, "maxSubscriptions": 0 },
  "overload": { "queuedMessages": 1000000, "replicationLag": 400000, "memoryBytes": 0 },
  "flapping": { "enabled": true, "windowSeconds": 60, "maxCount": 15, "banSeconds": 300 },
  "auth": { "anonymous": false, "sessionBinding": "principal", "decisionTtlSeconds": 3600, "argon2MemoryBytes": 268435456,
            "cache": { "ttlSeconds": 60, "maxEntries": 100000 } },
  "modules": { "auth.file": { "path": "users.json" }, "authz.file": { "path": "acl.json" } },
  "log": { "level": "info", "format": "text" },
  "mqtt": {
    "maximumQoS": 2, "retainAvailable": true, "wildcardSubscriptionAvailable": true,
    "subscriptionIdentifiersAvailable": true, "sharedSubscriptionAvailable": true,
    "topicAliasMaximum": 65535
  }
}
```

Only `version` and `node.dataDir` are required; the values shown for
`limits`, `auth`, `overload`, `flapping`, `log` and `mqtt` are the defaults, as are `cluster.id`,
`cluster.replicas`, `cluster.shards`, the heartbeat and frame limits,
`cluster.raft`, `cluster.leases`, `cluster.routing`, `cluster.replication`, `cluster.drain` and `cluster.readiness` (`cluster.listen`, `cluster.auth`,
`cluster.peers` and `cluster.advertise` are empty by default, which is a
one-member cluster). The `mqtt` object is
the operator's switch for the MQTT 5.0 capabilities a server may decline;
HoardMQ declines none unless told to. Unknown members are rejected
everywhere except inside `modules.*`, whose contents belong to the named
module; the schema also describes `modules.sink.kafka`, the Kafka
forwarder's configuration.

## Reloading the configuration

A running node re-reads its configuration file on SIGHUP, on `POST
/config/reload` or `hoardmq config reload` (see ["Admin API"](operations.md#admin-api)). It reads the
file and the environment overrides it started with, validates them as
`hoardmq check` does, and compares the result with the document in force:

- a document with any problem changes nothing, and every problem is reported
  together, as `check` reports them;
- a document that changes any member outside the table below changes
  nothing, and each such member is named by its JSON pointer: only a restart
  applies it;
- otherwise the document is put into force whole, and no connection is
  closed.

| Member | Applies from |
|---|---|
| `log.level` | at once |
| `limits.clientMessageRate`, `clientMessageBurst`, `clientByteRate`, `clientByteBurst` | each connection's next PUBLISH |
| `limits.connectRate`, `connectBurst`, and a listener's `connectRate` and `connectBurst` | the next CONNECT |
| a listener's `maxConnections` | the next accepted connection; connections over a lowered bound stay |
| `limits.maxQueuedMessages`, `limits.queueDrop` | the next message queued; a longer queue is not trimmed |
| `limits.offlineQoS0` | the next delivery; a QoS 0 message already queued stays |
| `limits.maxSessionExpiry` | the next expiry a client sets; sessions keep theirs |
| `limits.maxSubscriptions` | the next SUBSCRIBE; a session over a lowered limit keeps its filters |
| `overload.*` | the next stress sample |
| `flapping.*` | the next abnormal end; bans already written stand |

A listener's members are reloadable while the listener keeps its name, type
and address; adding or removing a listener, and every other member (the
cluster, the node, per-connection limits such as `maxPacketSize` and
`maxKeepAlive`, `mqtt`, `auth` and `modules`), needs a restart. The
administered limits (`PUT /limits`) keep their precedence over the reloaded
ones. An applied reload also has `auth.file`, `authz.file` and `auth.scram`
re-read their files; a file that cannot be read fails validation and changes
nothing, and a module whose re-read fails afterwards keeps what it had and is
reported. A reload applies to the node it runs on only. Each one is logged,
audited as `config.reload` and counted in
`hoardmq_config_reloads_total{outcome}` (`applied`, `refused`, `invalid`). The
Helm chart restarts its pods when their configuration changes (its
`checksum/config` annotation).

## Listeners

`tcp`, `tls`, `ws` (MQTT over WebSocket, path `/mqtt` by default) and `wss`
(the same over TLS). TLS listeners require `tls.certFile` and `tls.keyFile`,
accept `tls.clientCAFile` (a client certificate is then required and
verified) and `tls.minVersion` (`1.2` default, `1.3`). Certificate files are
re-read when they change, checked at most once per second at handshake time;
a pair that fails to load keeps the previous one in service. Every listener
speaks every protocol level; the level is chosen per connection from CONNECT.

`connectRate` (CONNECTs a second; 0, the default, is unlimited) and
`connectBurst` (how many it admits at once; 0 is twice the rate) limit how fast
a listener admits connections, and `limits.connectRate` and
`limits.connectBurst` do the same for the node across every listener. A CONNECT
over either limit is refused as soon as it is read, before its credentials are
examined or any session work starts: CONNACK 0x9F (Connection rate exceeded) at
MQTT 5.0; at 3.1.1 and 3.1, which have no code for it, the connection is closed
without a CONNACK. A CONNECT one limit refuses takes nothing from the other.
Refusals are counted by `hoardmq_connect_rate_refused_total{listener,limit}`
(`limit` is `listener` or `node`). Why the limits are off by default, and how to
size one, is under ["The connect path's cost"](security-and-access.md#the-connect-paths-cost).

`maxConnections` bounds the connections a listener holds at once; one more is
closed as it is accepted, since MQTT has no way to refuse a connection before
its CONNECT, and counted in `hoardmq_max_connections_refused_total{listener}`.
0, the default, is unlimited. The Helm chart sets a connect rate, a connection
bound and a subscription limit (its README, "Limits"); the broker's own
defaults stay unlimited.

## Environment overrides

Environment variables override scalar members before validation, so a bad
value is reported at the member it targets and an unrecognised variable is
reported as a problem rather than ignored:

```
HOARDMQ_NODE_NAME            HOARDMQ_NODE_DATA_DIR
HOARDMQ_NODE_WAL_SYNC        HOARDMQ_NODE_WAL_SYNC_INTERVAL_MS   HOARDMQ_NODE_WAL_SEGMENT_BYTES
HOARDMQ_NODE_WAL_GROUP_MAX_DELAY_MS     HOARDMQ_NODE_WAL_GROUP_MAX_SIZE
HOARDMQ_NODE_SESSION_SHARDS             HOARDMQ_NODE_STORE_ADMISSION
HOARDMQ_CLUSTER_ID           HOARDMQ_CLUSTER_LISTEN
HOARDMQ_CLUSTER_REPLICAS     HOARDMQ_CLUSTER_SHARDS
HOARDMQ_CLUSTER_AUTH_MODE    HOARDMQ_CLUSTER_AUTH_SECRET_FILE
HOARDMQ_CLUSTER_AUTH_TLS_CERT_FILE   HOARDMQ_CLUSTER_AUTH_TLS_KEY_FILE   HOARDMQ_CLUSTER_AUTH_TLS_CA_FILE
HOARDMQ_CLUSTER_HEARTBEAT_MS HOARDMQ_CLUSTER_HEARTBEAT_MISSES    HOARDMQ_CLUSTER_MAX_FRAME_BYTES
HOARDMQ_CLUSTER_RAFT_TICK_MS HOARDMQ_CLUSTER_RAFT_ELECTION_TICKS HOARDMQ_CLUSTER_RAFT_HEARTBEAT_TICKS
HOARDMQ_CLUSTER_RAFT_SNAPSHOT_ENTRIES   HOARDMQ_CLUSTER_RAFT_SNAPSHOT_TRAILING
HOARDMQ_CLUSTER_RAFT_PROMOTE_LAG        HOARDMQ_CLUSTER_RAFT_JOIN_TIMEOUT_MS
HOARDMQ_CLUSTER_DISCOVERY_MODE          HOARDMQ_CLUSTER_DISCOVERY_DNS_SERVICE
HOARDMQ_CLUSTER_DISCOVERY_DNS_NAME      HOARDMQ_CLUSTER_DISCOVERY_DNS_REPLICAS   HOARDMQ_CLUSTER_DISCOVERY_DNS_PORT
HOARDMQ_CLUSTER_LEASES_TTL_MS           HOARDMQ_CLUSTER_LEASES_TAKEOVER_TIMEOUT_MS   HOARDMQ_CLUSTER_ADVERTISE
HOARDMQ_CLUSTER_ROUTING_SYNC_MS         HOARDMQ_CLUSTER_REPLICATION_ACK_TIMEOUT_MS
HOARDMQ_CLUSTER_DRAIN_RATE_PER_SECOND   HOARDMQ_CLUSTER_DRAIN_TIMEOUT_MS   HOARDMQ_CLUSTER_DRAIN_CONFIRM_TIMEOUT_MS
HOARDMQ_CLUSTER_DRAIN_HOLD_EXIT_MS
HOARDMQ_CLUSTER_READINESS_STABLE_MS
HOARDMQ_LIMITS_MAX_PACKET_SIZE   HOARDMQ_LIMITS_RECEIVE_MAXIMUM
HOARDMQ_LIMITS_MAX_QUEUED_MESSAGES   HOARDMQ_LIMITS_MAX_KEEP_ALIVE
HOARDMQ_LIMITS_CONNECT_TIMEOUT       HOARDMQ_LIMITS_QUEUE_DROP
HOARDMQ_LIMITS_MAX_SESSION_EXPIRY   HOARDMQ_LIMITS_MAX_INFLIGHT_MESSAGES
HOARDMQ_LOG_LEVEL            HOARDMQ_LOG_FORMAT
HOARDMQ_OBSERVABILITY_ADDRESS
```

Arrays (`listeners`, `cluster.peers`), booleans such as
`cluster.raft.bootstrap`, and `modules` cannot be overridden from the
environment.
