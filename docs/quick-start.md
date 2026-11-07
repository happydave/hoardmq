# Quick start

A first broker: build it, give it a one-node configuration, run it and connect a client. The configuration here is a
development broker's: anonymous clients are admitted, and every client may publish and subscribe to every topic.
Before running HoardMQ for anyone else, read [security-and-access.md](security-and-access.md); the configuration
document's reference is [configuration.md](configuration.md), and running a cluster is
[operations.md](operations.md).

Go 1.26.4 or later builds the broker from a checkout ([development.md](development.md#building-and-testing)):

```sh
go build ./cmd/hoardmq
```

A one-node configuration, `hoardmq.json` in the working directory. Only `version` and `node.dataDir` are required; a
node with no listener accepts no client, and the last two members open it to anonymous clients and hand authorization
to `acl.json`:

```json
{
  "version": 1,
  "node": { "dataDir": "data" },
  "listeners": [ { "name": "plain", "type": "tcp", "address": "127.0.0.1:1883" } ],
  "auth": { "anonymous": true },
  "modules": { "authz.file": { "path": "acl.json" } }
}
```

and `acl.json`, allowing every topic to every client:

```json
{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}
```

Check the configuration, then run the broker; with no `-config` and no `HOARDMQ_CONFIG`, both read `hoardmq.json` in
the working directory:

```sh
./hoardmq check
./hoardmq serve
```

`serve` logs one line per listener. An MQTT 5.0, 3.1.1 or 3.1 client then connects to `127.0.0.1:1883` and may
publish and subscribe to any topic. SIGINT or SIGTERM stops the broker. The command's reference follows.

## Command

```
usage: hoardmq <command> [flags]

commands:
  serve          validate the configuration and run the broker
  drain          ask a running node to leave gracefully (redirect clients, hand leases off, exit)
  check          validate the configuration and report every problem
  clients        list, search, inspect, disconnect and invalidate clients (admin API)
  retained       list and delete retained messages (admin API)
  cluster        cluster status, the shard map and a client's lease (admin API)
  traces         start, stop and fetch a client's packet trace (admin API)
  modules        list, enable and disable modules (admin API)
  audit          the most recent audit events (admin API)
  config         show the running configuration, validate a document, or reload this node's file (admin API)
  apikeys        list, create and revoke the cluster's API keys (admin API)
  credentials    list, show, add, update and remove client credentials in the in-broker database (admin API)
  bans           list, add and remove bans by client identifier, username, address, prefix or pattern (admin API)
  limits         show and set the per-client PUBLISH rates and the node's connect rate, cluster-wide (admin API)
  whoami         the identity a credential authenticates as (admin API)
  login          log in through an OpenID Connect provider and print the ID token the admin API accepts
  hash-password  read a password from stdin and print its hash for a users file (-scram: the SCRAM form; -argon2id: Argon2id)
  version        print the build version, the extension API's version and the modules compiled in
```

`serve` and `check` take `-config PATH`; when the flag is absent the path comes
from `HOARDMQ_CONFIG`, else `hoardmq.json` in the working directory. `serve`
binds every listener before it creates the data directory (a listener that
cannot bind stops the start with nothing created), logs one line per
listener, and runs until SIGINT or SIGTERM, when it tells MQTT 5.0 clients
the server is shutting down and waits up to five seconds for connections to
end — or until a drain completes (see ["Drain and readiness"](operations.md#drain-and-readiness)), when it exits
0, at once or, with `cluster.drain.holdExitMs`, on the stop signal or once
that hold has passed. SIGHUP reloads the configuration file (see ["Reloading the
configuration"](configuration.md#reloading-the-configuration)). `drain -address host:port [-wait] [-timeout d]` posts to a node's
observability listener, which answers only a loopback caller; with `-wait` it polls until the drain is complete or
the node has exited, which is what the Helm chart's preStop hook runs. Only a
refused, reset or closed connection reads as the exit: a status request that
times out or fails otherwise is printed, and the wait goes on until `-timeout`,
when it exits 1.
`drain -admin host:port -token-file f` does the same through the admin API,
which is the only way once the admin API is configured (see ["Admin API"](operations.md#admin-api)).

The admin verbs (`clients`, `retained`, `cluster`, `traces`, `modules`,
`audit`, `config`, `apikeys`, `credentials`, `bans`, `limits`, `whoami`) are thin clients of the admin API: `hoardmq clients list
-search gw`, `hoardmq clients inspect CLIENT-ID`, `hoardmq clients invalidate
CLIENT-ID`, `hoardmq cluster status`, `hoardmq modules disable ID`, `hoardmq
bans add -reason abuse -expires 1h cidr 198.51.100.0/24`, `hoardmq limits set
-client-message-rate 100`, and so on
(`hoardmq clients -h` lists a group's verbs). Flags go before the arguments.
`-admin` takes `host:port` or an `https://` URL (default
`$HOARDMQ_ADMIN_ADDRESS`, else `127.0.0.1:9091`), `-token-file` a file holding
the bearer token (default: the value of `$HOARDMQ_ADMIN_TOKEN`), `-cacert`,
`-cert` and `-key` the TLS trust and client certificate (either makes the
scheme https), `-json` prints the API's JSON instead of text. Those two
environment variables are the client's and are not configuration overrides.
The bearer token may be a configured token, an API key, or the ID token
`hoardmq login` writes (see ["Single sign-on"](security-and-access.md#single-sign-on-oidc)).

Exit codes: 0 ok, 1 configuration, start-up or API problem (an API refusal
prints its status and detail), 2 usage error.

A path that performs no work performs no writes: usage, `version`, `check`, a
bad flag and an unavailable feature leave the filesystem exactly as they found
it. This is a tested invariant, not a habit.
