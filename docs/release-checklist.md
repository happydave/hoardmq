# Release checklist

What must be true before a version tag is pushed. Releases are source
releases: the tag is the release, and no image, chart package or SBOM is
published (adopters build their own from the reference `Dockerfile` and
chart; operations.md, ["Kubernetes deployment"](operations.md#kubernetes-deployment)). The first tag is special:
it defines cluster feature level 1 (clustering.md, ["Upgrades and the cluster feature
level"](clustering.md#upgrades-and-the-cluster-feature-level)), so everything that changed a replicated operation before it ships
ungated and everything after it must be gated.

## Before the first tag

- [x] The README's introduction no longer says no release has been tagged.
- [x] `CHANGELOG.md`'s `Unreleased` section becomes the first tag's section,
  and `docs/stability.md` is no longer marked draft: the owner has decided its
  promises.
- [x] **The upgrade gate exists.** A replicated cluster feature level, the
  leader's raise, the refusal below the level, the frame's level, and a
  mixed-version test (`TestMixedVersionClusterAppliesIdenticallyOrRefuses`).
- [x] **The lease's fencing token** (the epoch is the grant's log index; a holder
  is judged against the highest token seen) is in, as level 1.
- [x] **Liveness as a fact in the log** (the lease amendment's second change:
  a node declared dead by the leader's own observation, the conditional
  acquire judged at apply, no node comparing a clock with another's stamp)
  is in, as level 1.
- [x] The inventory of level 1 in the project's upgrade-gate design is
  re-read against `git log` for the replication and lease packages, so
  nothing that changed a replicated semantic is missing from the baseline
  (2026-11-07: the cluster packages, the stores and the session package,
  from `385a5b0e` through `e9dd2cd5`).
- [x] At the tag, the same re-read over the commits since `e9dd2cd5`
  (2026-11-07: through `402be303`, no row added; the release's own commits
  change no Go file).
- [x] operations.md, ["Durability and group commit"](operations.md#durability-and-group-commit) states what durability is claimed:
  under `walSync: "commit"`, acknowledged work survives the loss of fewer
  nodes than hold a copy of it (`cluster.replicas`, at most the live nodes;
  any one node on the chart's 3 pods and 2 copies), and a simultaneous power loss of every node is as durable as the
  hosts' fsync, which no test has checked by cutting power.
- [x] `TRADEMARK.md` is no longer marked a stub.
- [ ] The repository's private vulnerability reporting is on.
- [x] `SECURITY.md`'s supported versions name the release line instead of
  `main` alone.
- [x] Every gate in `docs/dependency-policy.md` passes on the tagged commit,
  the interoperability gate included.

## Before every tag

- [x] `deploy/helm/hoardmq/Chart.yaml`'s `version` and `appVersion` are the
  tag without its `v`.
- [x] The reference `Dockerfile` builds at the tagged commit, and the image's
  `hoardmq version` prints the tag given as `VERSION`. A base image pin is
  refreshed by pulling its tag and replacing the reference's version and
  digest (the Dockerfile's header).
- [x] `docs/production-readiness.md` is re-read against `docs/stability.md`,
  `SECURITY.md`, `docs/upgrade.md` and the evidence pages: it says nothing
  they do not, and its "Measured" section states the largest scale measured
  at this release.

## Before every tag after the first

- [ ] `CHANGELOG.md` has the tag's section, written from `Unreleased`, with its
  upgrade notes (level, framing version, on-disk changes and what of them
  cannot be rolled back), and any deprecation the release makes.
- [x] **Before the first gated change ships:** the design says what a node
  below the cluster's level does with log entries applied at a level above
  its build, and what happens to its state machine when it is upgraded: it
  holds the log from the entry after the raise, builds no snapshot, and
  replays what it held when restarted on a build that understands the level
  (`TestBelowLevelNodeHoldsAndCatchesUpAfterUpgrade`).
- [ ] A mixed-version test of each gated change keeps the old node on a
  build that applies the old semantics whatever the table's level, as the
  test above does with its test-only semantic.
- [ ] A change to a replicated operation, a replication or route-sync frame, or a
  state-machine snapshot field added its row to the upgrade-gate inventory in
  the same commit — not at release time.
- [ ] Every change since the previous tag to what a replicated operation
  does — a lease-table or shard-map operation, or how a follower applies a
  replication frame — raised `lease.BuildLevel`, keeps the old behaviour
  below its level, and is listed in the upgrade-gate inventory.
- [ ] A mixed-version run between the previous tag and this one: the roll
  completes, the level rises once after the last node, and no property of
  the simulation corpus fails during it.
- [ ] The extension API's changes since the previous tag, if any, raised
  `ext.APIVersion` under the compatibility rules in `ext/CHANGELOG.md` and have
  their section there; `ext/api.txt` matches the code (`TestAPIGolden`).
- [ ] `docs/upgrade.md` is true of this release: its framing version, its
  feature level and its on-disk changes, and what of them cannot be rolled
  back, each held by the test it cites.
- [ ] Every gate in `docs/dependency-policy.md` passes on the tagged commit.
