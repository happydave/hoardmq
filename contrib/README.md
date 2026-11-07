# Contributed modules

Modules kept in this repository but built outside the broker's Go module, each with dependencies of its own that the
broker and its default binary never carry. Each one is a Go module whose module file is committed as `go.mod.txt`
and whose checksums are committed as `go.sum.txt`. Its directory name starts with `_`, which the Go tool skips, so
`go build ./...` at the root never compiles it.

To use one, copy its directory out, rename the two files, and build a binary with it:

```sh
cp -r contrib/_authsql ../hoardmq-authsql
cd ../hoardmq-authsql && mv go.mod.txt go.mod && mv go.sum.txt go.sum && cd -
go run ./cmd/hoardmq-build --hoardmq-dir . --with github.com/happydave/hoardmq-authsql=../hoardmq-authsql --output hoardmq
```

`--with` may be given once per module, so one binary can carry several.

To run its own tests against a checkout, add a `replace` in the copy first:
`go mod edit -replace github.com/happydave/hoardmq=<checkout>`, then `go test ./...`.

| Directory | Module | Identifier | What it adds |
|---|---|---|---|
| `_authsql` | `github.com/happydave/hoardmq-authsql` | `auth.sql` | Authentication against a PHC hash a PostgreSQL query reads, with pgx (MIT). docs/security-and-access.md, ["SQL (`auth.sql`, out of tree)"](../docs/security-and-access.md#sql-authsql-out-of-tree). |
| `_sinkpostgres` | `github.com/happydave/hoardmq-sinkpostgres` | `sink.postgres` | Accepted publishes written to PostgreSQL tables in batches, with `sink.kafka`'s buffer and policy, with pgx (MIT). docs/modules.md, ["Postgres sink (`sink.postgres`, out of tree)"](../docs/modules.md#postgres-sink-sinkpostgres-out-of-tree). |

The broker's tests (`cmd/hoardmq-build/contrib_test.go`) copy each module out the same way, run its tests, build a
binary with it, and show with the licence tooling that its dependencies stay out of the broker's module.
