# Coming from another MQTT broker

This page maps the concepts of four other brokers onto HoardMQ's: HiveMQ (4.x), EMQX (5.8), Eclipse Mosquitto (2.x)
and VerneMQ (2.x). For each concept it gives the other broker's names, from that broker's own documentation (linked),
and then the HoardMQ setting or module, with the section of HoardMQ's documentation that describes it.

Each broker is described as its documentation read in November 2026. A cell says "not documented" where the
documentation says nothing, and "source" where the behaviour was read in that project's source code instead. Check
the linked page for the version you run.

## Sessions and offline queues

| | Persistent session | Session expiry | Queue bound per offline client | Offline QoS 0 |
|---|---|---|---|---|
| HiveMQ | "persistent session"<sup>[1]</sup> | `<mqtt><session-expiry><max-interval>`<sup>[2]</sup> | `<mqtt><queued-messages><max-queue-size>` (1000), `<strategy>` `discard` or `discard-oldest`<sup>[2]</sup> | not documented |
| EMQX | a session with Clean Start false; "durable sessions" (`durable_sessions.enable`) keep it on disk<sup>[3]</sup> | `session_expiry_interval` for MQTT 3.x<sup>[4]</sup> | `max_mqueue_len` (1000)<sup>[4]</sup> | `mqueue_store_qos0` (default `true`: queued)<sup>[4]</sup> |
| Mosquitto | "persistent client"<sup>[5]</sup> | `persistent_client_expiration` (default never)<sup>[5]</sup> | `max_queued_messages` (1000), `max_queued_bytes`<sup>[5]</sup> | `queue_qos0_messages` (default `false`: not queued)<sup>[5]</sup> |
| VerneMQ | "persistent client"<sup>[6]</sup> | `persistent_client_expiration` (default never)<sup>[6]</sup> | `max_offline_messages` (1000, QoS 1 and 2), `queue_type` `fifo` or `lifo`<sup>[7]</sup> | dropped, no setting (source)<sup>[8]</sup> |
| **HoardMQ** | a session with Clean Start 0 and a Session Expiry Interval | `limits.maxSessionExpiry` caps what a client asks for | `limits.maxQueuedMessages`, `limits.queueDrop` `oldest` or `newest`; an authenticator's `hoardmq.maxQueuedMessages` attribute lowers it per principal | `limits.offlineQoS0`: `queue` (the default) or `drop` |

In HoardMQ a session is replicated to other nodes (`cluster.replicas`), so it survives the loss of the node that
holds it. clients-and-sessions.md, ["Sessions"](clients-and-sessions.md#sessions), and clustering.md, ["Data-plane replication"](clustering.md#data-plane-replication).

**Migrating from Mosquitto or VerneMQ,** where offline QoS 0 is not queued by default or at all, set
`limits.offlineQoS0` to `drop` to keep that behaviour. HoardMQ's default queues them, as EMQX's does.

## Retained messages

| | Settings |
|---|---|
| HiveMQ | `<mqtt><retained-messages><enabled>`<sup>[2]</sup> |
| EMQX | `mqtt.retain_available`; the retainer's `max_payload_size`, `msg_expiry_interval`, storage `ram` or `disc`, a maximum count (0 unlimited)<sup>[4], [9]</sup> |
| Mosquitto | `retain_available`, `retain_expiry_interval`; stored through persistence<sup>[5]</sup> |
| VerneMQ | stored and replicated by the metadata plugin; `vmq-admin retain show`<sup>[10]</sup> |
| **HoardMQ** | `mqtt.retainAvailable`; `limits.maxRetainedTopics` per namespace per node (0 unlimited; past it 0x97); sharded by topic and replicated. clients-and-sessions.md, ["Subscriptions, retained messages and shared subscriptions"](clients-and-sessions.md#subscriptions-retained-messages-and-shared-subscriptions) |

## Shared subscriptions

| | Syntax | Strategy |
|---|---|---|
| HiveMQ | `$share/SHARE_NAME/TOPIC`<sup>[11]</sup> | random, weighted to faster consumers; no setting found<sup>[11]</sup> |
| EMQX | `$share/<group>/<topic>`; `$queue/<topic>`<sup>[12]</sup> | `mqtt.shared_subscription_strategy`: `random`, `round_robin` (default), `round_robin_per_group`, `local`, `sticky`, `hash_clientid`, `hash_topic`<sup>[13]</sup> |
| Mosquitto | shared subscriptions exist<sup>[14]</sup> | not documented |
| VerneMQ | `$share/sharename/topic`, also at 3.1.1<sup>[15]</sup> | `shared_subscription_policy`: `prefer_local` (default), `local_only`, `random`, `prefer_online_before_local`<sup>[15]</sup> |
| **HoardMQ** | `$share/{name}/{filter}`, also at 3.1.1 and 3.1 | `mqtt.sharedSubscriptionStrategy`: `round-robin` (default), `random`, `sticky`, `hash` (by publisher client identifier), `local-first` |

There is no `$queue/` form. Rewrite a `$queue/topic` subscription as `$share/<name>/topic`. clients-and-sessions.md, ["Subscriptions,
retained messages and shared subscriptions"](clients-and-sessions.md#subscriptions-retained-messages-and-shared-subscriptions).

## Authentication and access control

| | Authentication | Access control |
|---|---|---|
| HiveMQ | through an extension; without one, every CONNECT is refused<sup>[16]</sup>. The Enterprise Security Extension: SQL, JWT, LDAP, File, OAuth, OIDC, X.509 realms<sup>[17]</sup> | the File RBAC extension's `credentials.xml`, `${{clientid}}` and `${{username}}`<sup>[18]</sup>; the Enterprise Security Extension's authorization managers<sup>[17]</sup> |
| EMQX | an authentication chain of backends, HTTP among them<sup>[19]</sup> | `authorization.sources` in order, `no_match` (default `allow`); the `file` source's `{allow\|deny, Who, Action, Topics}.` with `${clientid}`<sup>[20], [21]</sup> |
| Mosquitto | `password_file`, or the Dynamic Security plugin<sup>[5], [22]</sup> | `acl_file` with `topic`, `user`, `pattern` and `%c`, `%u`<sup>[5]</sup> |
| VerneMQ | `vmq_passwd` and its password file<sup>[23]</sup> | `vmq_acl` and its ACL file, `pattern` with `%c`, `%u`<sup>[23]</sup> |
| **HoardMQ** | a chain of modules: `auth.file`, `auth.db` (the in-broker database, managed through the admin API), `auth.jwt`, `auth.ldap`, `auth.x509`, `auth.scram`, `auth.http`, and `auth.sql` out of tree | `authz.file`: ordered rules, first match decides, with `${clientid}`, `${username}`, `${cn}` and `${attr.<name>}` |

A fresh HoardMQ refuses everyone: anonymous connections are off (`auth.anonymous`), and every publish and subscribe is
refused unless an authorizer allows it. EMQX's `no_match` defaults to `allow`. For a Mosquitto `pattern` rule such as
`pattern readwrite devices/%c/#`, the `authz.file` equivalent is a rule on `devices/${clientid}/#`. security-and-access.md,
["Authentication and authorization"](security-and-access.md#authentication-and-authorization).

## Bridges and forwarding

| | |
|---|---|
| HiveMQ | the Enterprise Bridge Extension (licensed)<sup>[24]</sup> |
| EMQX | "Bridge with Other MQTT Services", connectors with sinks and sources<sup>[25]</sup> |
| Mosquitto | `connection <name>` blocks with `address` and `topic … [out\|in\|both]`<sup>[5]</sup> |
| VerneMQ | the `vmq_bridge` plugin<sup>[26]</sup> |
| **HoardMQ** | no MQTT-to-MQTT bridge yet. `sink.kafka` forwards accepted publishes to Kafka (modules.md, ["Kafka forwarder"](modules.md#kafka-forwarder-sinkkafka)), and `sink.postgres` to PostgreSQL tables (modules.md, ["Postgres sink"](modules.md#postgres-sink-sinkpostgres-out-of-tree)) |

A HoardMQ cluster is one broker across nodes. Bridging between nodes of one cluster is not needed.

## Extensions and plugins

| | |
|---|---|
| HiveMQ | the Extension SDK, in Java, hot-reloaded from `extensions/`<sup>[27]</sup> |
| EMQX | Erlang plugins, and ExHook over gRPC<sup>[28], [29]</sup> |
| Mosquitto | C plugins, `plugin <path>`, `plugin_opt_*`<sup>[5], [30]</sup> |
| VerneMQ | Erlang plugins; Lua through `vmq_diversity`<sup>[31], [32]</sup> |
| **HoardMQ** | Go modules compiled in through the public API `github.com/happydave/hoardmq/ext` (connection hooks, inbound and outbound interceptors, subscribe and unsubscribe interceptors, delivery, retained and Will events, publish admission, cluster events, authenticators, authorizers, store, metrics and trace providers). `hoardmq-build` builds a binary with your modules. modules.md, ["Modules"](modules.md#modules) |

## HTTP authentication and event webhooks

| | Authentication and authorization by HTTP | Event webhooks |
|---|---|---|
| HiveMQ | not documented (an extension of your own) | not documented (an extension of your own) |
| EMQX | HTTP authentication (`result` `allow`, `deny` or `ignore`; `is_superuser`, `client_attrs`, `expire_at`) and HTTP authorization<sup>[33], [34]</sup> | the Webhook (HTTP server sink), on events such as `client.connected` and `message.publish`<sup>[35]</sup> |
| Mosquitto | not documented (a plugin) | not documented (a plugin) |
| VerneMQ | `vmq_webhooks` on `auth_on_register`, `auth_on_publish`, `auth_on_subscribe`<sup>[36]</sup> | `vmq_webhooks` on `on_publish`, `on_deliver`, `on_client_offline` and others<sup>[36]</sup> |
| **HoardMQ** | `auth.http`, in EMQX's request and answer shapes | `events.http`: `client.connected`, `message.publish`, `session.subscribe` and the rest, async or sync |

**Where `auth.http` departs from EMQX:**
- An unreachable endpoint, or any answer but 200 or 204 (a 4xx included), denies by default, where EMQX reads a 4xx
  or 5xx as `ignore`. Set `onUnavailable: pass` to pass on to the next module.
- `is_superuser` makes a superuser only on a connection `auth.http` itself admitted.

security-and-access.md, ["HTTP authentication and authorization"](security-and-access.md#http-authentication-and-authorization-authhttp), and modules.md, ["Event webhooks"](modules.md#event-webhooks-eventshttp).

## Disconnecting a client without its Will

| | |
|---|---|
| HiveMQ | the Extension SDK's `ClientService.disconnectClient(clientId, true)` (`preventWillMessage`)<sup>[37]</sup> |
| EMQX | Kick Out, or `emqx ctl clients kick <ClientId>`<sup>[38]</sup>. No option to suppress the Will found; the 5.8.6 source publishes it (source)<sup>[39]</sup> |
| Mosquitto | the plugin API's `mosquitto_kick_client_by_clientid(clientid, with_will)`<sup>[30]</sup> |
| VerneMQ | `vmq-admin session disconnect client-id=<id> [--cleanup]`<sup>[40]</sup>. No option to suppress the Will found; the source publishes it (source)<sup>[41]</sup> |
| **HoardMQ** | the admin API's `POST /clients/{id}/disconnect?will=false` and `DELETE /clients/{id}/session?will=false`, or `hoardmq`'s `-no-will`. The default publishes the Will. operations.md, ["Admin API"](operations.md#admin-api) |

## References

1. HiveMQ Control Center, clients: https://docs.hivemq.com/hivemq-control-center/latest/clients.html
2. HiveMQ configuration: https://docs.hivemq.com/hivemq/latest/user-guide/configuration.html
3. EMQX 5.8, durable sessions: https://docs.emqx.com/en/emqx/v58/durability/durability_introduction.html
4. EMQX 5.8, MQTT configuration: https://docs.emqx.com/en/emqx/v58/configuration/mqtt.html
5. mosquitto.conf(5): https://mosquitto.org/man/mosquitto-conf-5.html
6. VerneMQ, non-standard options: https://docs.vernemq.com/configuration/nonstandard
7. VerneMQ, MQTT options: https://docs.vernemq.com/configuring-vernemq/options and advanced options: https://docs.vernemq.com/configuring-vernemq/advanced_options
8. VerneMQ source, `vmq_queue.erl`: https://raw.githubusercontent.com/vernemq/vernemq/main/apps/vmq_server/src/vmq_queue.erl
9. EMQX 5.8, retained messages in the Dashboard: https://docs.emqx.com/en/emqx/v58/dashboard/retained.html
10. VerneMQ, the retained store: https://docs.vernemq.com/live-administration/retained-store.md
11. HiveMQ, shared subscriptions: https://docs.hivemq.com/hivemq/latest/user-guide/shared-subscriptions.html
12. EMQX 5.8, shared subscriptions: https://docs.emqx.com/en/emqx/v58/messaging/mqtt-shared-subscription.html
13. EMQX 5.8.6 schema text: https://raw.githubusercontent.com/emqx/emqx/v5.8.6/rel/i18n/emqx_schema.hocon
14. mosquitto(8): https://mosquitto.org/man/mosquitto-8.html
15. VerneMQ, shared subscriptions: https://docs.vernemq.com/configuring-vernemq/shared_subscriptions.md
16. HiveMQ, authentication: https://docs.hivemq.com/extensions/authentication.html
17. HiveMQ Enterprise Security Extension: https://docs.hivemq.com/hivemq-enterprise-security-extension/latest/ese.html
18. HiveMQ File RBAC extension: https://www.github.com/hivemq/hivemq-file-rbac-extension
19. EMQX 5.8, HTTP authentication, one of the chain's backends: https://docs.emqx.com/en/emqx/v58/access-control/authn/http.html
20. EMQX 5.8, authorization: https://docs.emqx.com/en/emqx/v58/access-control/authz/authz.html
21. EMQX 5.8, the file source: https://docs.emqx.com/en/emqx/v58/access-control/authz/file.html
22. Mosquitto, Dynamic Security: https://mosquitto.org/documentation/dynamic-security/
23. VerneMQ, file authentication and authorization: https://docs.vernemq.com/configuring-vernemq/file-auth.md
24. HiveMQ Enterprise Bridge Extension: https://docs.hivemq.com/bridge
25. EMQX 5.8, bridging with MQTT services: https://docs.emqx.com/en/emqx/v58/data-integration/data-bridge-mqtt.html
26. VerneMQ, bridge: https://docs.vernemq.com/configuring-vernemq/bridge.md
27. HiveMQ, extension concepts: https://docs.hivemq.com/extensions/general.html
28. EMQX 5.8, plugins: https://docs.emqx.com/en/emqx/v58/extensions/plugins.html
29. EMQX 5.8, ExHook: https://docs.emqx.com/en/emqx/v58/extensions/exhook.html
30. Mosquitto plugin API, `mosquitto_broker.h`: https://mosquitto.org/api/files/mosquitto_broker-h.html
31. VerneMQ, plugin development: https://docs.vernemq.com/plugin-development/introduction.md
32. VerneMQ, Lua plugins: https://docs.vernemq.com/plugin-development/luaplugins.md
33. EMQX 5.8, HTTP authentication: https://docs.emqx.com/en/emqx/v58/access-control/authn/http.html
34. EMQX 5.8, HTTP authorization: https://docs.emqx.com/en/emqx/v58/access-control/authz/http.html
35. EMQX 5.8, Webhook: https://docs.emqx.com/en/emqx/v58/data-integration/webhook.html
36. VerneMQ, webhooks: https://docs.vernemq.com/plugin-development/webhookplugins.md
37. HiveMQ Extension SDK, services: https://docs.hivemq.com/extensions/services.html
38. EMQX 5.8, CLI: https://docs.emqx.com/en/emqx/v58/admin/cli.html
39. EMQX 5.8.6 source, `emqx_channel.erl`: https://raw.githubusercontent.com/emqx/emqx/v5.8.6/apps/emqx/src/emqx_channel.erl
40. VerneMQ, managing sessions: https://docs.vernemq.com/live-administration/managing-sessions.md
41. VerneMQ source, `vmq_mqtt5_fsm.erl`: https://raw.githubusercontent.com/vernemq/vernemq/main/apps/vmq_server/src/vmq_mqtt5_fsm.erl
