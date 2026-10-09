# Dependency and licence policy

Restated from the project design's Non-Functional Requirements so the gate and
the reasoning live together.

## Policy

- **Standard library first.** A dependency is added only when the standard
  library cannot reasonably do the job. A FIPS build must remain a build flag,
  which holds only while all cryptography is standard-library.
- **Permitted families:** MIT, Apache-2.0, BSD (2- and 3-clause), ISC,
  Unlicense/0BSD, MPL-2.0 (unmodified, with a NOTICE line), EPL-2.0 (test-only
  clients, pinned).
- **Forbidden families:** GPL, LGPL, AGPL, SSPL, Business Source License,
  Commons Clause.
- **Named allowlist:** `go.etcd.io/raft/v3` (Apache-2.0) as the adopted Raft
  core; `coder/websocket` (ISC) only as a fallback if the in-house WebSocket
  server proves insufficient; `paho.golang` and `paho.mqtt.golang` (EPL-2.0)
  as test-only clients. Extensions may carry their own dependencies under the
  same families (for example `franz-go`, BSD-3, for Kafka).
- **No third-party cryptography.**
- **In use (2026-09-03):** `go.etcd.io/raft/v3` v3.7.0 (Apache-2.0) and its runtime dependency `google.golang.org/protobuf` v1.36.11 (BSD-3) are compiled into the binary and listed in NOTICE; `paho.golang`, `paho.mqtt.golang` and their transitive modules are test-only.

## Gates

Run from the module root. All must pass before a work item closes.

| Gate | Command | What it proves |
|---|---|---|
| Format | `gofmt -l .` prints nothing | Source is formatted |
| Vet | `go vet ./...` | No vet findings |
| Lint | `golangci-lint run ./...` | Standard linters plus misspell, per `.golangci.yml` |
| Test | `go test ./...` | All tests pass (never `-short`) |
| Licence | `go run ./tools/licensecheck` | Every module the build or the tests import has a licence file in a permitted family; EPL-2.0 is accepted only for the pinned test clients |
| NOTICE | `go generate ./... && git diff --exit-code NOTICE` | The committed NOTICE matches the module list |
| Fuzz | `go test -run '^$' -fuzz=FuzzDecodeV5 -fuzztime=60s ./internal/packet` and likewise for `FuzzDecodeV4`, `FuzzDecodeV3`, `FuzzConnect` | No panic or hang from arbitrary bytes; accepted packets survive encode and decode unchanged. Any corpus files written under `testdata/fuzz` are committed |
| Conformance | `go test ./conformance -conformance.results=$PWD/conformance/results.json && go run ./conformance/cmd/coverage && git diff --exit-code conformance/COVERAGE.md` | Every statement-annotated scenario passes, every annotation names a real Appendix B identifier, and the committed coverage report matches the code |
| Simulation | `go test ./sim` | The fixed seed corpus, the fault-free baseline, every negative control and the 1,000-client failover pass; `HOARDMQ_SIM_RANDOM=k` is the nightly form. A failing seed is reproduced with `HOARDMQ_SIM_SEED=n` before a fix is claimed |
| Interoperability | `HOARDMQ_EXTERNAL=1 go test ./conformance/external` | The Paho interoperability suites give the outcome recorded in `conformance/external/paho/expected.json` (an unexpected failure, or an expected failure that starts passing, fails the gate). Minutes long and needs python3; required before a checkpoint closes, not for every work item |

The race detector is in no gate. `-race` runs in two places only:
- a work item opened for a specific race (a race report, or an intermittent
  failure the code points to a race), over the package that showed it, at
  `-count=1` first;
- a negative control whose subject is a race (a lock, an atomic or a channel
  hand-off removed), over the test it controls, broken and unbroken.

Every such run sets `GORACE=log_path=<absolute path>`, so a report lands in
its own file, and a rate is measured as separate processes, not
`-count=N`. When such an item needs the whole module,
`HOARDMQ_SIM_REDUCED=1 go test -race -timeout 40m ./...` keeps the
simulation corpus to its first two seeds.

## How detection works

`internal/modlicense` finds the first `LICENSE*`, `LICENCE*` or `COPYING*`
file in each module's cache directory and classifies the text by well-known
phrases (for example "Permission is hereby granted, free of charge" is MIT;
"Redistribution and use in source and binary forms" is BSD). Forbidden
families are tested first so a permissive file with a Commons Clause rider is
still refused. A module with no licence file, or with text that matches
nothing known, fails the gate; the fix is to read the licence by hand and, if
it is permitted, teach the detector the phrase.
