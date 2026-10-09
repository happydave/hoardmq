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
