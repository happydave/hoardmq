# Upgrading and rolling back

How to move a HoardMQ cluster to a new release, and back: in what order to
replace nodes, what a cluster running two builds refuses while it does, how
the cluster feature level is raised, what a node below that level looks like
and what to do with it, and what cannot be rolled back. The mechanism behind
it is in clustering.md, ["Upgrades and the cluster feature level"](clustering.md#upgrades-and-the-cluster-feature-level). Each statement
of behaviour below names the test that holds it.

## What a release can change

- **The cluster feature level.** A change to what a replicated operation does
  (a lease-table operation, or how a follower applies a replication frame) is
  keyed to a level. The first release tag defines level 1, and a release stays
  at its predecessor's level until it ships such a change.
- **The framing version** of the cluster transport, negotiated on each link.
- **On-disk formats:** the write-ahead log's segment header version and the
  raft snapshot format.
- **The configuration schema.**

Mixed clusters are supported between adjacent releases: upgrade one release
at a time.

Each release's section in [CHANGELOG.md](../CHANGELOG.md) states its level,
its framing version and its on-disk changes, and
[stability.md](stability.md) what each of these may change in a patch, a
minor and a major release.

## Before you start

- Check the configuration with the new binary: `hoardmq check -config <file>`
  reports every problem in the document together (`TestCheckValid`,
  `TestCheckReportsEveryProblem`).
- Every node serving: `/servingz` answers 200 on each.
- Each volume has room for twice `hoardmq_raft_snapshot_bytes` beside the
  logs (clustering.md, ["Consensus core"](clustering.md#consensus-core)).
- A node locks its data directory (`node.dataDir/LOCK`) and refuses to start
  while another process holds it, naming that process's PID
  (`TestServeRefusesADataDirectoryInUse`). A build from before the lock takes
  none, so stop the old process before starting the new one on its directory
  (operations.md, ["Persistence"](operations.md#persistence)).

## The order: one node at a time

Replace one node, wait for it to be ready, then the next. Each node drains
before it stops, so its clients are redirected and its leases handed to a
live node. A rolling upgrade refuses what a rolling restart refuses: the
chart's README, "What a rolling restart refuses".

### On Kubernetes, with the chart

```sh
helm upgrade hoardmq deploy/helm/hoardmq -n hoardmq --reuse-values --set image.tag=<new>
kubectl -n hoardmq rollout status statefulset/hoardmq --timeout=30m
```

The StatefulSet's update strategy is `RollingUpdate`
(`TestDesignedDeploymentProperties`). Kubernetes' StatefulSet controller then
replaces one pod at a time, from the highest ordinal down, each once the one
before it is Ready; in the chart's eleven recorded rolling-restart runs of the
evening of 2026-10-05, a once-a-second sample during a rollout never found two pods not
ready at once ([evidence/rolling-restart-kind.md](evidence/rolling-restart-kind.md)).
The PodDisruptionBudget allows one pod down, each pod drains
in its preStop hook, and the termination grace covers the drain's two timeouts
(`TestDesignedDeploymentProperties`, `TestTerminationGraceHoldsTheWholeDrain`,
`TestBrokerHoldsItsExitForThePreStop`).

### On hosts without Kubernetes

For each node in turn:

1. Drain it. From the node, `hoardmq drain -address 127.0.0.1:<observability
   port> -wait` (`TestDrainVerb`); with the admin API configured, `hoardmq
   drain -admin <address> -token-file <file> -wait`
   (`TestDrainThroughTheAdminAPI`). The node exits once its drain completes.
   Under a service manager, `cluster.drain.onSignal: true` makes the stop
   signal drain before the exit (`TestServeDrainOnSignal`); give the service
   a stop timeout of at least `cluster.drain.timeoutMs` plus
   `cluster.drain.confirmTimeoutMs` plus a margin (75 s at the defaults, as
   the chart's grace), so the drain is not killed before it completes.
2. Replace the binary.
3. Start it, and wait until its `/readyz` answers 200 before draining the
   next node.

## What a mixed cluster refuses

### By build: the framing version

Each link runs at the lower framing version of its two ends
(`TestVersionNegotiation`).

- **Below version 4**, an admin call for a client the older node holds is
  answered 409 naming that node, as an older build answers it
  (`TestAdminClassesNeedVersion4`, `TestAdminForwardToAnOlderBuild`).
- **Below version 5**, a newer leader cannot send a raft snapshot: the
  transfer fails as `peer-version`, counted in
  `hoardmq_raft_snapshot_transfers_total`, logged at Warn and retried after a
  backoff, and nothing is sent inline (`TestSnapshotClassesNeedVersion5`,
  `TestTransferToAnOlderPeerIsCountedAndLogged`). An older node that falls
  behind the newer leader's compacted log therefore catches up only once
  upgraded: upgrade it next. This applies to builds from before the first
  release tag, which are all below version 5.
- A snapshot in format 1, the inline form older builds stored, is still read
  (`TestRestartFromAFormatOneCheckpoint`).
- **Below version 6**, a peer is sent each full route set in one frame, as
  before, and receives delta batches cut at 32 KiB, which it reads as any
  run of batches. A set over the link's frame limit is refused, now counted
  in `hoardmq_route_send_failures_total{reason="too-large"}` and logged at
  Warn, so past the limit routes do not resync with that peer until it is
  upgraded; between upgraded nodes sets travel in parts during the roll
  (`TestMixedVersionRouteSync`). A node on an older build sends its own set
  in one frame too, so a node restarted beside one whose set is over the
  limit is ready but degraded (`/servingz` 503 `routes-unknown`, refusing
  QoS 1 and 2 publishes made on it) until that node is upgraded as well:
  in a roll from `v0.1.0` with every node's set over the limit, each upgraded
  node serves again once the last one is upgraded
  (`TestRollFromPreviousRelease`). The roll waits on `/readyz`, as above, so
  it completes. A member on an older build never acknowledges a set, so the
  upgraded nodes keep their route tombstones until it is upgraded or
  removed. This applies to `v0.1.0`, which is below version 6.

### By level: nothing, in a correct roll

Until every member runs a build that understands a new level, the cluster
applies the old level's semantics, and no node holds
(`TestCorrectRollNeverHolds`, `TestMixedVersionClusterAppliesIdenticallyOrRefuses`).
A node below the cluster's level holds; see "A held node" below.

**Level 2** is raised by `v0.2.0`, once its roll is done.
At level 2 a follower refuses a session's metadata record alone that finds no
replica copy, and the owner sends the session whole, so a takeover never
reads a copy holding the session without its subscriptions and queue
(clustering.md, ["Data-plane replication"](clustering.md#data-plane-replication)).
During the roll the level stays 1 and every follower applies such a record as
`v0.1.0` does; the release's other replication changes work between its own
nodes from the first one upgraded, and an upgraded owner's imports reach a
`v0.1.0` follower as snapshots, which it applies as a resync's
(`TestMixedVersionWholeCopies`, `TestRollFromPreviousRelease`).

## How a raise happens

There is no command to raise the level, and none to hold a raise back. The
leader raises it, in a command of its own, once every member, voters and
learners, advertises at least that level in its latest alive record; a member
with no record holds the raise back (`TestLeaderRaisesTheLevelOnceEveryMemberUnderstandsIt`,
`TestRaiseWaitsForEveryMemberWhereverListed`, `TestRaiseIsProposedAlone`,
`TestRaiseIsACommandOfItsOwn`, `TestAliveCarriesTheLatestLevel`). So the raise
follows the last node's return on the new build. It is counted in
`hoardmq_cluster_feature_level_raises_total` (`TestARaiseIsCountedByHowItEnded`),
and `GET /api/v1/cluster` (`hoardmq cluster status`) shows the cluster's level,
this build's and each member's (`TestAdminAPIThreeNodes`). The level never
lowers (`TestClusterLevelOnlyRises`).

## A held node

A node whose build is below the cluster's level holds the log: it applies
nothing past the entry that raised the level, while it stores, acknowledges
and votes, so the quorum keeps it (`TestHeldFollowerAppliesNothingButKeepsTheLog`).

What it looks like:

- It is not ready, for the reason `level`, so `/readyz` fails and `/servingz`
  answers 503 `not ready: level`; a CONNECT is refused with `0x9C Use Another
  Server` naming the leader (`TestNodeBelowTheClusterLevelDoesNotServe`).
- `/healthz` stays 200: a restart on the same build would hold again
  (`TestNodeBelowTheClusterLevelDoesNotServe`).
- `hoardmq_cluster_feature_level_held` is 1, and `hoardmq_raft_held_entries`
  rises with every committed entry it holds
  (`TestHeldFollowerAppliesNothingButKeepsTheLog`).
- At the hold, an Error line naming the entry and the raft log's size, and an
  audit record `consensus.held`; then a Warn a minute with the log's size and
  growth (`TestHoldIsLoggedAuditedAndWarnedEachMinute`).
- On the leader, `hoardmq_shardmap_members_below_level` counts it
  (`TestNodeBelowTheClusterLevelDoesNotServe`), and the leader takes over the
  leases it owned (`TestLeaderTakesTheLeasesOfAMemberBelowTheLevel`).

What to do: restart it on a build that understands the cluster's level. It
replays what it held with the semantics each entry was committed under
(`TestBelowLevelNodeHoldsAndCatchesUpAfterUpgrade`,
`TestHeldLeaderRefusesAtOnceAndReplaysAfterRestart`). Restarted on the same
build, it holds again at the same entry (the second test). Until then its raft
log grows with every committed entry, since it compacts nothing, so upgrade it,
or remove it with `hoardmq cluster remove` (operations.md, ["Removing a member"](operations.md#drain-and-readiness)),
before its volume fills.

## Rolling back

### Before the raise

While any node still runs the older build, roll back the same way, one node
at a time, with the older binary. A node back on the older build advertises
the older level again, which holds the raise back
(`TestAliveCarriesTheLatestLevel`).

- **Configuration:** the older build refuses a configuration member it does
  not know, naming it (`TestEachRuleHasAFailingFixture`). Remove what the
  newer release added before starting the older binary. A configuration with
  peers and no `cluster.auth.mode` is read as `mtls` from the release that
  added that default (`TestClusterRules`), and refused by builds before it:
  name the mode.
- **A single node** is every member of its cluster: restarted on a build with
  a higher level, it raises the level with its first alive record, by the rule
  above. Roll it back before that, or not at all.

### After the raise

A release that raises the level can be rolled back in place only until its
last node is upgraded: the raise follows that node's return.

There is no rollback in place. The level never lowers
(`TestClusterLevelOnlyRises`), and a node restarted on an older build holds
(above). Roll forward with a fixed release.

## What cannot be rolled back

- **Level 2, once raised.** After a `v0.2.0` cluster has raised its level to
  2, no node goes back to `v0.1.0` in place: the level never lowers
  (`TestClusterLevelOnlyRises`), and a node restarted on `v0.1.0` holds
  ("A held node", above). Before the raise it can, as "Rolling back" says.
- **Snapshot format 2.** A node whose disk holds a format 2 raft snapshot does
  not start on a build before format 2, which refuses it naming the format
  (`TestDescriptorRoundTrip` holds the older reader, kept unchanged). Every
  build before the first release tag is such a build. Start the node again on
  the newer build.
- **A later snapshot format.** A build meeting a stored snapshot in a format
  it does not know refuses to start, naming the format, and removes no file
  from the snapshot directory first, so the newer build starts on that disk
  again (`TestStartOverANewerSnapshotFormatRefusesAndKeepsItsFiles`).
- The write-ahead log's segment header carries a version, and a segment of a
  version a build does not know is refused, not read (`TestHeaderValidation`).
