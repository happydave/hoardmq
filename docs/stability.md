# Stability policy

> **Draft.** The promises on this page, marked *(draft)* where they are made,
> have not been decided by the project's owner. Until they are, they describe
> what the project intends, not a commitment. The statements of what the code
> does today are not drafts.

What each part of HoardMQ may change between releases, and how a change is
announced. It applies from the first release tag; before it, only the latest
commit of `main` is supported ([SECURITY.md](../SECURITY.md)). Each release's
changes are in [CHANGELOG.md](../CHANGELOG.md), and how to move a cluster
between releases is [upgrade.md](upgrade.md).

## Release numbers

*(draft)* The broker's releases are tagged with semantic versions,
`v<major>.<minor>.<patch>`:

- a **patch** fixes defects and changes no surface below;
- a **minor** adds, and may deprecate;
- a **major** may remove or change what a minor deprecated.

*(draft)* While the major version is 0, a minor release is treated as a major
for the removals below, as the extension API's own v0 rules do.

Fixes go to `main` and to the latest minor release line
([SECURITY.md](../SECURITY.md)).

## Per surface

### Configuration

The configuration document (`version` 1) and its published schema,
`schema/hoardmq.schema.json`, with the `HOARDMQ_` environment overrides.

- Today, a member the schema does not know is refused at start and by
  `hoardmq check`, naming it.
- *(draft)* A minor may add members. A changed default is listed under
  Changed in the changelog.
- *(draft)* A member is removed, renamed or given another meaning only in a
  major, after at least one minor in which it is marked deprecated.

### Admin API

The REST API under `/api/v1`, described by the OpenAPI document it serves at
`/api/v1/openapi.json` (`info.version` 1).

- *(draft)* A minor may add routes, optional request members and response
  members; a client ignores response members it does not know.
- *(draft)* A route or member is removed, or its meaning changed, only in a
  major, under a new path prefix, after at least one minor in which it is
  marked deprecated.

### On-disk formats

The write-ahead log and its checkpoints, the raft log and its snapshots, and
the replication layer's files under `node.dataDir`.

- Today, a write-ahead log segment carries a version in its header and a raft
  snapshot a format byte; a build that meets a version it does not know
  refuses to start, naming it, and removes no raft snapshot file first
  ([upgrade.md](upgrade.md), "What cannot be rolled back"). The replication
  layer's files carry no format version.
- A new format is written only once the cluster's feature level shows that
  every member reads it.
- *(draft)* Every release of a major line reads what any earlier release of
  that line wrote, and a major reads what the previous major's last minor
  wrote. An upgrade across a major therefore goes through that last minor.
- Rolling a node back to an older release is possible until the cluster has
  raised its feature level ([upgrade.md](upgrade.md)); each release's upgrade
  notes say what of its changes cannot be rolled back.

### The cluster wire

The cluster transport's framing versions and the cluster feature level.

- Today, each link runs at the lower framing version of its two ends, and
  the feature level is raised by the cluster once every member understands
  it, never lowered ([upgrade.md](upgrade.md)).
- *(draft)* A minor may add a framing version and raise the feature level.
  Mixed clusters are supported between adjacent releases: upgrade one release
  at a time.
- *(draft)* A major may stop speaking framing versions, and stop applying
  feature levels, below what the previous major's last minor ran.

### MQTT

*(draft)* MQTT 5.0 and 3.1.1, and protocol level 3 as an input dialect, are
not removed in a minor. What the specifications leave to the server is documented in the
README and changes only with a changelog entry.

### The extension API

The extension API under `ext/` has its own version, `ext.APIVersion`, and its
own compatibility rules: [ext/CHANGELOG.md](../ext/CHANGELOG.md),
"Compatibility".

### Not covered

Metric names and labels, log messages, the details of audit records, the
cluster transport's message contents, and everything under `internal/` may
change in any release. *(draft)* A changed or removed metric is listed in the
changelog.

## Deprecation

*(draft)* A deprecation is announced in the changelog's Deprecated section of
the release that makes it, naming what replaces the deprecated thing. For a
configuration member, the schema's description says so too. It
stays at least one minor release before a major removes it, and the removal
is listed in that major's Removed section.
