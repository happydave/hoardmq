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
