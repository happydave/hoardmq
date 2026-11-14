# Production readiness

What an adopter can rely on, what has been measured and at what scale, and what HoardMQ does not yet support. This page
promises nothing of its own: each statement is a summary of the page it links, and that page is the authority.

## Stable

The policy is [stability.md](stability.md). It applies from the first release tag, `v0.1.0`.

- **Release numbers** are semantic versions. A patch fixes defects and changes no surface; a minor adds, and may
  deprecate; a major may remove or change what a minor deprecated. While the major version is 0, a minor release is
  treated as a major for those removals ([stability.md](stability.md#release-numbers)).
- **Configuration:** a member the schema does not know is refused at start and by `hoardmq check`. A minor may add
  members; a member is removed, renamed or given another meaning only in a major, after at least one minor in which it
  is marked deprecated ([stability.md](stability.md#configuration)).
- **Admin API:** a minor may add routes, optional request members and response members. A route or member is removed,
  or its meaning changed, only in a major, under a new path prefix, after at least one minor in which it is marked
  deprecated ([stability.md](stability.md#admin-api)).
- **On-disk formats:** every release of a major line reads what any earlier release of that line wrote, and a major
  reads what the previous major's last minor wrote, so an upgrade across a major goes through that last minor. A new
  format is written only once the cluster's feature level shows that every member reads it
  ([stability.md](stability.md#on-disk-formats)).
- **The cluster wire:** mixed clusters are supported between adjacent releases: upgrade one release at a time
  ([stability.md](stability.md#the-cluster-wire), [upgrade.md](upgrade.md#what-a-release-can-change)).
- **MQTT:** MQTT 5.0 and 3.1.1, and protocol level 3 as an input dialect, are not removed in a minor
  ([stability.md](stability.md#mqtt)).
- **The extension API** has its own version and compatibility rules ([ext/CHANGELOG.md](../ext/CHANGELOG.md)).
- **Not covered:** metric names and labels, log messages, the details of audit records, the cluster transport's
  message contents, and everything under `internal/` may change in any release; a changed or removed metric is listed
  in the changelog ([stability.md](stability.md#not-covered)).
- **Upgrades** go one node at a time, each node draining before it stops. The leader raises the cluster feature level
  once every member advertises at least that level, and the level never lowers: after the raise there is no rollback
  in place, so roll forward with a fixed release ([upgrade.md](upgrade.md)).
- **Security fixes** go to `main` and to the latest minor release line; older lines are not patched. A vulnerability
  is reported privately through GitHub, with targets, not guarantees: an acknowledgement within 7 days, an assessment
  within 30, and the advisory published when the fix is available, by default no later than 90 days after the report
  ([SECURITY.md](../SECURITY.md)).

## Measured

The figures behind the measured claims are summarised in [evidence/](evidence/README.md), one page per claim, each
naming its host, its runs and their spread, and what the runs do not show.

- The largest client load measured so far is 1,000,000 persistent sessions, with three nodes on one host and every
  client on another, on the broker code released as `v0.2.0`: in five runs a node was killed and its 333,333 clients
  reconnected to the survivors, 99.9% of them within 152–229 s of losing their connection, and no session of 5,000,000
  checked was lost ([evidence/million-sessions.md](evidence/million-sessions.md)).
- Before it: 100,000 clients on one host, against an in-process cluster of three nodes, and 250,000 clients in the
  memory sweep, with the three nodes on one host and the clients on another.
- The checkpoint and credential-store measurements reach 1,000,000 sessions or credentials, in one process with no
  clients connected.
- No measurement at 1,000,000 sessions has the nodes on separate hosts, loses a whole host, or uses passwords.

The hosts are in [evidence/README.md](evidence/README.md#the-hosts).

## Known limits

- **`v0.1.0` sends a node's route set to a peer as one cluster frame**, refused above `cluster.maxFrameBytes`. At the
  default 16 MiB and short filters (about 90 bytes an entry) that is about 186,000 route entries a node, roughly
  560,000 clients with one subscription each on three nodes; a filter unsubscribed since the node started still
  counts, so churn brings the limit closer. Past it, routes stop resynchronising after a gap in a peer's updates or a
  node's restart: a node refuses the QoS 1 and 2 publishes that could match routes it cannot prove (Implementation
  Specific Error), and a restarted node is ready but does not serve (`/servingz` 503 `routes-unknown`). `v0.2.0` sends
  a full set in parts under the frame limit; in a roll from `v0.1.0` with every node past the limit, each upgraded node
  serves again once the last one is upgraded ([CHANGELOG.md](../CHANGELOG.md#v020--2026-11-14),
  [upgrade.md](upgrade.md#by-build-the-framing-version)).

## Not yet supported

- **No published image, chart repository or SBOM.** Releases are source releases: the `Dockerfile` and the Helm chart
  are references to copy and adapt, and each deployment builds its own image and makes its own image's SBOM
  ([operations.md](operations.md#kubernetes-deployment)).
- **Rolling back after a feature-level raise**, and starting a node whose disk holds a format 2 raft snapshot on a
  build before format 2, which every build before the first release tag is ([upgrade.md](upgrade.md#rolling-back),
  [upgrade.md](upgrade.md#what-cannot-be-rolled-back)).
- **Exactly-once for a refused publish:** QoS 2's exactly-once delivery covers the publishes the broker accepted; a
  refused publish is outside it, and may already have reached some subscribers
  ([clients-and-sessions.md](clients-and-sessions.md#sessions)).
- **SAML** for the admin API's single sign-on, which is OIDC only
  ([security-and-access.md](security-and-access.md#single-sign-on-oidc)).
- **SCRAM** channel binding (`-PLUS`) and SASLprep
  ([security-and-access.md](security-and-access.md#scram-enhanced-authentication-authscram)).
- **LDAP** SASL binds, referrals, substring filters, and group or attribute mapping
  ([security-and-access.md](security-and-access.md#ldap-authldap)).
- **Certificate revocation:** indirect CRLs, lists scoped to attribute certificates or naming their point relative to
  the issuer, delta CRLs, any other critical extension, and https endpoints under a private CA
  ([security-and-access.md](security-and-access.md#certificate-revocation-authx509)).
- **Out of scope for a security report:** a deployment that chooses a setting the documentation describes as unsafe,
  such as `secret` cluster authentication on a network it does not trust, or anonymous access in production
  ([SECURITY.md](../SECURITY.md#scope)).
- **Published measurements at 1,000,000 sessions with the nodes on separate hosts**, after a whole host's loss, or
  with password clients ([evidence/million-sessions.md](evidence/million-sessions.md#what-this-does-not-show)).
