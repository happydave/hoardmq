# HoardMQ

HoardMQ is an MQTT broker written in Go: full MQTT 5.0 and 3.1.1 (with
protocol level 3 accepted as an input dialect), clustered from the start, free
for commercial use, and built to stand against the enterprise tiers of the
incumbents on scale, cluster resilience and security. A single node is a
one-member cluster; there is no separate single-node mode. This repository
holds the broker: listeners over TCP, TLS and WebSocket; the wire codec for
protocol levels 3, 4 and 5; sessions with QoS 0 to 2, Wills, retained messages
and wildcard and shared subscriptions; the cluster (a consensus core, a lease
per client identifier with takeover between nodes, replication of sessions
and retained messages, routing between nodes, a drain); a write-ahead log;
authentication, authorization and the admin API; metrics, traces and an audit
log; modules on a public extension API, with a Kafka forwarder; a Helm chart;
and the conformance suite and simulation harness that gate it. Releases are
source releases, tagged on the public repository; the first is `v0.1.0`
([CHANGELOG.md](CHANGELOG.md)). The design lives in the project's ticket repository.

The public repository on GitHub is published from the project's private
repository as a fresh history: each commit there is a snapshot of the private
`main` without `sim/results/`, made by `tools/publish` after a check for
site-specific names, addresses, personal email and references to the private
work items. `sim/results/` holds the
measurement runs' reports and artifacts, which name the hosts and network they
ran on and run to several hundred megabytes. They and the full history stay in
the private repository, so the paths into `sim/results/` that the code's
comments cite do not resolve on GitHub, and the test that replays published
results skips there. The measured claims in the pages listed below, the chart's README and
[docs/upgrade.md](docs/upgrade.md) cite summaries of those runs instead, in
[docs/evidence/](docs/evidence/README.md): what was run, on which host, how
many times, and what the runs do and do not show.

[CHANGELOG.md](CHANGELOG.md) records what each release changes,
[docs/stability.md](docs/stability.md) what each surface may change between
releases, [docs/upgrade.md](docs/upgrade.md) how to move a cluster from one
release to the next, and [docs/migration.md](docs/migration.md) how other brokers'
sessions, ACLs, shared subscriptions, bridges, extensions and webhooks map onto
HoardMQ's.

## Documentation

- [docs/quick-start.md](docs/quick-start.md): for a first run. Build the broker, give it a one-node
  configuration, run it, connect a client; then the `hoardmq` command and its verbs.
- [docs/configuration.md](docs/configuration.md): for whoever writes the configuration document. Its schema and an
  example with the defaults, reloading, listeners, and the environment overrides.
- [docs/security-and-access.md](docs/security-and-access.md): for whoever decides who may connect, publish, subscribe
  and administer. The authenticators, authorization, single sign-on and API keys.
- [docs/clients-and-sessions.md](docs/clients-and-sessions.md): for client developers. Connection behaviour, sessions,
  subscriptions, retained messages and shared subscriptions.
- [docs/modules.md](docs/modules.md): for integrators. The extension modules, the Kafka forwarder, the Postgres sink,
  event webhooks and payload schema validation.
- [docs/operations.md](docs/operations.md): for operators. A map to sizing, limits, failure modes and upgrades, then
  overload and bans, observability, the admin API, drain and readiness, Kubernetes, persistence and durability.
- [docs/clustering.md](docs/clustering.md): for readers who want the design behind the cluster and why it behaves as it
  does. The transport, consensus, leases and takeover, routing, replication and the feature level.
- [docs/development.md](docs/development.md): for contributors. The codec, fuzzing, the conformance suite, the
  simulation harness, building and testing.
- [docs/production-readiness.md](docs/production-readiness.md): for adopters deciding whether to run HoardMQ. What is
  stable, what is measured at what scale, and what is not yet supported.
- [docs/stability.md](docs/stability.md), [docs/upgrade.md](docs/upgrade.md) and
  [docs/migration.md](docs/migration.md): for adopters planning releases, upgrades or a move from another broker.
- [docs/evidence/](docs/evidence/README.md): for readers checking a measured claim. A summary of the runs behind each.
- [docs/dependency-policy.md](docs/dependency-policy.md) and [docs/release-checklist.md](docs/release-checklist.md):
  for maintainers. What a dependency must meet, and what a release tag needs.

## Quick start

A development broker on one node, anonymous and allowing every topic, from a checkout with Go 1.26.4 or later:

```sh
go build ./cmd/hoardmq
echo '{"version":1,"node":{"dataDir":"data"},"listeners":[{"name":"plain","type":"tcp","address":"127.0.0.1:1883"}],"auth":{"anonymous":true},"modules":{"authz.file":{"path":"acl.json"}}}' > hoardmq.json
echo '{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}' > acl.json
./hoardmq check && ./hoardmq serve
```

An MQTT client then connects to `127.0.0.1:1883`. [docs/quick-start.md](docs/quick-start.md) explains each step, and
[docs/configuration.md](docs/configuration.md) the rest of the configuration.

## Security

Report a vulnerability privately through the repository's Security tab
(**Report a vulnerability**), not in a public issue. [SECURITY.md](SECURITY.md)
gives the scope, the supported versions and what to expect.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

The HoardMQ name is covered by [TRADEMARK.md](TRADEMARK.md), not by the
software licences. Third-party attributions are generated into
[NOTICE](NOTICE).
