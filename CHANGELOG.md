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

No release has been tagged. This section is what the first tag will hold,
and becomes that tag's section when it is cut. Every area points at the
README section that describes it.

### Added

- **MQTT** 5.0 and 3.1.1, with protocol level 3 (`MQIsdp`) accepted as an
  input dialect: QoS 0, 1 and 2, retained messages, wildcard and shared
  subscriptions, Wills with their delay, session expiry, topic aliases, over
  TCP, TLS and WebSocket listeners (README, "Listeners", "Sessions",
  "Subscriptions, retained messages and shared subscriptions"). The
  conformance suite's counts per specification are in the README,
  "Conformance".
- **Clustering** from one node up: a consensus core on `go.etcd.io/raft/v3`
  with snapshots streamed as files; a lease per client identifier with
  takeover between nodes; session and retained-message replication to
  `cluster.replicas` copies; routing of publishes between nodes; a drain that
  redirects clients and hands its leases on; readiness, serving and liveness
  endpoints (README, "Cluster transport" through "Drain and readiness").
- **Persistence:** a write-ahead log, fsynced before a QoS 1 or 2
  acknowledgement by default, with checkpoints; a damaged or incomplete log
  stops the node with the files named (README, "Persistence", "Durability and
  group commit").
- **Security:** authenticators for a users file, the in-broker database, JWT,
  LDAP, X.509 with CRL and OCSP checking, and SCRAM; file-based
  authorization with identity templates; sessions bound to the principal
  that made them; mutual TLS or a shared secret between nodes (README,
  "Authentication and authorization", "Cluster transport").
- **Operations:** the admin REST API with roles, API keys and OpenID Connect
  sign-in, and the `hoardmq` command as its client; Prometheus metrics,
  OpenTelemetry traces and an audit log; connect, connection, rate and
  subscription limits, overload shedding and bans; configuration reload; the
  Helm chart (README, "Admin API", "Observability", "Overload and bans",
  "Kubernetes deployment").
- **Extensions:** the extension API 0.3.0, `hoardmq-build` for binaries with
  other modules, and the Kafka forwarder `sink.kafka` (README, "Modules",
  "Kafka forwarder").

### Upgrade notes

- **Cluster feature level 1.** The first tag defines it: everything up to the
  tag is level 1, and a later change to a replicated operation raises it
  (README, "Upgrades and the cluster feature level").
- **Framing version 5.** A newer leader cannot send a raft snapshot to a node
  on a link below version 5, which every build before the first tag is; such
  a node catches up once upgraded.
- **On-disk formats:** raft snapshot format 2, which builds before the first
  tag cannot read, so a node that has written one cannot go back to such a
  build; write-ahead log segment header version 1; configuration document
  `version` 1.
- **Interfaces:** the admin API at `/api/v1` (OpenAPI `info.version` 1); the
  extension API 0.3.0.
