# Release checklist

What must be true before a version tag is pushed. The first tag is special:
it defines cluster feature level 1 (README, *Upgrades and the cluster feature
level*), so everything that changed a replicated operation before it ships
ungated and everything after it must be gated.

## Before the first tag

- [x] **The upgrade gate exists.** A replicated cluster feature level, the
  leader's raise, the refusal below the level, the frame's level, and a
  mixed-version test (`TestMixedVersionClusterAppliesIdenticallyOrRefuses`).
- [x] **The lease's fencing token** (the epoch is the grant's log index; a holder
  is judged against the highest token seen) is in, as level 1.
- [x] **Liveness as a fact in the log** (the lease amendment's second change:
  a node declared dead by the leader's own observation, the conditional
  acquire judged at apply, no node comparing a clock with another's stamp)
  is in, as level 1.
- [ ] The inventory of level 1 in the project's upgrade-gate design is
  re-read against `git log` for the replication and lease packages, so
  nothing that changed a replicated semantic is missing from the baseline.
- [ ] The README states what durability is claimed: acknowledged work
  survives the loss of any minority of nodes, and a simultaneous power loss
  of every node is as durable as the hosts' fsync.
- [ ] `TRADEMARK.md` is expanded from its stub.
- [ ] Every gate in `docs/dependency-policy.md` passes on the tagged commit,
  the interoperability gate included.

## Before every tag after the first

- [ ] **Before the first gated change ships:** the design says what a node
  below the cluster's level does with log entries applied at a level above
  its build — it keeps applying them today, with the semantics it has — and
  what happens to its state machine when it is upgraded.
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
- [ ] Every gate in `docs/dependency-policy.md` passes on the tagged commit.
