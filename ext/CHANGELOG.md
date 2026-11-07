# HoardMQ extension API — changelog

The extension API is the package tree `github.com/happydave/hoardmq/ext`: the
exported identifiers of its packages, their documented behaviour, and the module
configuration contract (each module's `modules.<id>` object, and `hooks`). Its
version is `ext.APIVersion`, a semantic version of its own, independent of the
broker's release tags. `hoardmq version` prints it beside the modules compiled
in. `api.txt` beside this file lists the exported API of the version it names; a
test fails when the code and that list differ.

## Compatibility

While the version is 0.x:

- A patch release never breaks the API.
- A minor release may break it. Each break is listed here under that version,
  with what a module author changes.
- In a stable package (`ext`, `ext/mqtt`, `ext/auth`, `ext/records`), an
  identifier is marked `Deprecated:` for at least one minor release before it
  is removed.
- The experimental packages (`ext/store`, `ext/trace`), and the message
  envelope's broker-set fields (`Forwarded`, `Forwards`, `Trace`), may change
  in any minor release without a deprecation period.
- Not part of the API: anything under the broker's `internal/`, the cluster
  wire, metric names, and the log format beyond what `ext/store` exposes.

Version 1.0.0 is the owner's decision. From then on Go's import compatibility
rule applies: no breaking change within v1, and a v2 takes the import path
`github.com/happydave/hoardmq/ext/v2`.

## 0.1.0

The first public version. The extension seam the broker's built-in modules use
leaves `internal`, with three additions:

- `Validator`, an optional lifecycle step after `Provision`;
- `hooks.order`, the module order;
- the store provider's `SetTap`, which is now required.

Packages:
- `ext`: registration, the module contract, the module context and services,
  the hook families;
- `ext/mqtt`: the packets and the message envelope;
- `ext/auth`: authentication and authorization;
- `ext/records`: the replicated records store's values;
- `ext/store` (experimental): the store provider's journal, record format and
  state;
- `ext/trace` (experimental): the tracer and the exporter a trace module
  supplies.

## 0.2.0

Additions only; nothing is removed or changed.

- Two hook families report a publish's acceptance on the node that accepted it:
  - `PublishAcceptedListener` (`CapPublishAccepted`) is told of each accepted
    publish once, before its acknowledgement, with its source
    (`PublishSource`: a client, a Will or a module);
  - `PublishAdmission` (`CapPublishAdmission`) may refuse a client's publish
    after authorization and the QoS 2 retransmission check, before any
    delivery.

  A sink forwards from the first and refuses from the second, so it never sees
  a publish the broker refused. The capability bits follow `CapTraces`; every
  earlier bit keeps its value.
- `ext/mqtt` holds the broker's topic rules: `ValidateTopicName`,
  `ValidateTopicFilter`, `MatchTopic` (with the `$`-topic exclusion),
  `HasWildcard`, `IsSharedFilter`, `SplitSharedFilter` and `SharePrefix`.

## 0.3.0

Additions only; nothing is removed or changed.

- `LivenessProvider`, which the broker's `Services` implement: `Live` reports
  whether the node can still recover without a restart and, when it cannot,
  why. A metrics module serving a liveness endpoint fails it while `Live`
  reports false.
- `RecoverabilityProvider`, optional on a store provider: `Unrecoverable`
  returns the error every rewrite of its journal fails on, such as a damaged
  record, and nil while the journal can recover. The broker's liveness reads
  it.

## 0.4.0

Additions only; nothing is removed or changed.

- `Context.DecisionTTL`, the broker's `auth.decisionTtlSeconds`: the longest an
  authentication decision is held before it is made again, and the longest a
  module may hold an authorization answer it caches.

## 0.5.0

The inbound family's behaviour changes; no identifier is added, removed or
changed.

- `InboundInterceptor` sees a PUBLISH with its topic alias resolved and the
  Topic Alias property cleared, where 0.4.0 handed it an alias-only PUBLISH with
  an empty topic. The broker checks the PUBLISH (retain, QoS, Subscription
  Identifier, the alias) before the family runs, so a malformed one is closed
  before any interceptor sees it, and an alias a dropped PUBLISH sets is kept.
- A Drop or Disconnect of a QoS 2 PUBLISH whose packet identifier the session
  holds open (a retransmission) is not honoured: the broker answers it with the
  first attempt's verdict and counts it in
  `hoardmq_inbound_refusals_ignored_total`. Refusing it answered a delivered
  message as refused and left the identifier held, so the client's next message
  under it was never delivered.
- The interface's documentation now states that the family runs before
  authorization and sees retransmissions, and that a module deciding on a
  publish's topic or payload uses `PublishAdmission`.

**What a module author changes:** nothing, unless the module relied on seeing
an empty topic for an alias-only PUBLISH, or on refusing a QoS 2
retransmission.

## 0.6.0

Additions only; nothing is removed or changed.

- `Auditor` and `Context.Audit`: a module records events in the node's audit
  log. `Context.Audit` is nil when the node keeps no audit log, and during
  `hoardmq check`. A module's events never wait: when the log's queue is full
  the event is dropped, counted in the log's own drops, and `Audit` reports
  false.

## 0.7.0

Additions only; nothing is removed or changed.

- `UnsubscribeInterceptor` and `CapUnsubscribe`: a module sees each filter of
  an UNSUBSCRIBE after validation and before authorization. Drop refuses that
  filter: at MQTT 5 the UNSUBACK carries the outcome's reason for it, or
  Unspecified Error (0x80) when the reason is not an error. Below 5 the
  UNSUBACK has no codes, so a Drop closes the connection. Disconnect closes with
  the reason.
- `ClusterListener`, `ClusterEvent`, `ClusterEventKind` and `CapCluster`: a
  module is told when the consensus membership gains or loses a member
  (`member.joined`, `member.left`, from every node's own view), when a client's
  lease moves to this node from another (`lease.moved`, from the new owner), and
  when this node begins its drain (`drain.started`). An event carries its kind,
  time, the member, and for a lease move the namespace, client identifier and
  the two nodes. The broker never waits for a listener: each has a queue of
  1,024 events, a full queue drops the event, counted in
  `hoardmq_cluster_events_dropped_total{module}`, and delivery is at most once.

## 0.8.0

Additions only; nothing is removed or changed.

- `auth.Identity.AdmittedBy`: the identifier of the module whose allow admitted
  the connection: the authentication chain's decider, the certificate chain's,
  or the enhanced-authentication mechanism's. The broker sets it, and nothing a
  module returns does. It is empty for an anonymous client, a verified
  certificate alone, and a session recovered from disk.
- The broker hands each authorizer its own copy of the identity, so a change
  one makes to its fields reaches no other authorizer and not the connection.
  The attribute map is shared and must not be changed.

## 0.9.0

A break in the experimental `ext/store` package; the stable packages are unchanged.

- **Retained records carry a version.** `store.Version` (an epoch and a number, compared epoch first; JSON `[E, N]`,
  decoded only into typed `uint64`) orders retained writes to a topic, and `store.MergeRetained` decides which copy
  wins.
- **`Journal` changes:**
  - `Retain(m)` becomes `Retain(m, v Version)`;
  - `RetainDelete(ns, topic)` becomes two methods: `RetainClear(ns, topic, v Version)` (a client's clear, kept as a
    tombstone) and `RetainDrop(ns, topic)` (remove whatever is held).
- **Encoders:** `EncodeRetain` takes the version. `EncodeRetainDelete` becomes `EncodeRetainClear` and
  `EncodeRetainDrop`; `EncodeRetainClear` at the zero Version writes the drop. `RetainRecordVersioned` and
  `RecordVersion` are new.
- **Records:** a retain record's payload carries `"rv"` when stamped, and a delete with `"rv"` is a clear. Records
  without it read as before: the zero Version, and a delete that removes. `"rv"` is reserved on `mqtt.Message`.
- **`State`:**
  - it gains `RetainedVersions` and `RetainedTombstones`;
  - `Apply` merges retained records by version;
  - `MergeRetainedMessage`, `MergeRetainedClear`, `DropRetained`, `DropTombstoneIf` and `RetainedHeld` are new;
  - `SetRetained` merges at the zero Version: stored into an empty topic or over an unversioned message, never over
    a stamped one; an empty payload drops.

**What a provider changes:** implement `Retain(m, v)`, `RetainClear` and `RetainDrop`. A provider that stores what
the encoders return calls `EncodeRetain(m, v)`, `EncodeRetainClear` and `EncodeRetainDrop`. Recovery through
`State.Apply` merges with no change on the provider's side.
