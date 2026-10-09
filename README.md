# HoardMQ

HoardMQ is an MQTT broker written in Go: full MQTT 5.0 and 3.1.1 (with
protocol level 3 accepted as an input dialect), clustered from the start, free
for commercial use, and built to stand against the enterprise tiers of the
incumbents on scale, cluster resilience and security. It is early: this
repository currently holds a live single node: listeners over TCP, TLS and
WebSocket, the wire codec for protocol levels 3, 4 and 5, the per-connection
state machine with authentication and authorization, sessions with QoS 0 to
2 and Wills, wildcard subscriptions, retained messages and shared
subscriptions, a write-ahead log that makes persistent sessions and
retained messages survive a restart, and the project gates. The design
lives in the project's ticket repository.

The public repository on GitHub is published from the project's private
repository as a fresh history: each commit there is a snapshot of the private
`main` without `sim/results/`, made by `tools/publish` after a check for
site-specific names, addresses and personal email. `sim/results/` holds the
measurement runs' reports and artifacts, which name the hosts and network they
ran on and run to several hundred megabytes. They and the full history stay in
the private repository, so the paths into `sim/results/` that this README and
the code's comments cite do not resolve on GitHub, and the test that replays
published results skips there.

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
end — or until a drain completes (see "Drain and readiness"), when it exits
0, at once or, with `cluster.drain.holdExitMs`, on the stop signal or once
that hold has passed. SIGHUP reloads the configuration file (see "Reloading the
configuration"). `drain -address host:port [-wait] [-timeout d]` posts to a node's
observability listener; with `-wait` it polls until the drain is complete or
the node has exited, which is what the Helm chart's preStop hook runs. Only a
refused, reset or closed connection reads as the exit: a status request that
times out or fails otherwise is printed, and the wait goes on until `-timeout`,
when it exits 1.
`drain -admin host:port -token-file f` does the same through the admin API,
which is the only way once the admin API is configured (see "Admin API").

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
`hoardmq login` writes (see "Single sign-on").

Exit codes: 0 ok, 1 configuration, start-up or API problem (an API refusal
prints its status and detail), 2 usage error.

A path that performs no work performs no writes: usage, `version`, `check`, a
bad flag and an unavailable feature leave the filesystem exactly as they found
it. This is a tested invariant, not a habit.

## Configuration

One JSON document, validated against the published schema
[schema/hoardmq.schema.json](schema/hoardmq.schema.json). `hoardmq check`
reports every problem in one run, as `PATH: /json/pointer: message`, sorted.

```json
{
  "version": 1,
  "node": { "name": "n1", "dataDir": "/var/lib/hoardmq", "walSync": "commit", "walGroupMaxDelayMs": 0, "walGroupMaxSize": 256 },
  "listeners": [
    { "name": "plain", "type": "tcp", "address": ":1883" },
    { "name": "secure", "type": "tls", "address": ":8883",
      "tls": { "certFile": "cert.pem", "keyFile": "key.pem", "clientCAFile": "ca.pem", "minVersion": "1.3" } },
    { "name": "web", "type": "ws", "address": ":8080", "path": "/mqtt", "maxConnections": 0, "maxHandshakes": 512 },
    { "name": "webs", "type": "wss", "address": ":8443", "tls": { "certFile": "cert.pem", "keyFile": "key.pem" } }
  ],
  "cluster": { "id": "hoardmq", "replicas": 2, "shards": 64, "listen": ":7000",
               "auth": { "mode": "mtls", "tls": { "certFile": "node.pem", "keyFile": "node-key.pem", "caFile": "cluster-ca.pem" } },
               "peers": [ { "name": "n2", "address": "n2:7000" }, { "name": "n3", "address": "n3:7000" } ],
               "heartbeatMs": 1000, "heartbeatMisses": 3, "maxFrameBytes": 16777216,
               "raft": { "tickMs": 100, "electionTicks": 10, "heartbeatTicks": 1, "snapshotEntries": 10000,
                         "snapshotTrailing": 1000, "promoteLag": 100, "joinTimeoutMs": 30000 },
               "discovery": { "mode": "static" },
               "leases": { "ttlMs": 30000, "takeoverTimeoutMs": 5000 }, "routing": { "syncMs": 30000 },
               "replication": { "ackTimeoutMs": 2000 },
               "drain": { "ratePerSecond": 100, "timeoutMs": 30000, "confirmTimeoutMs": 30000, "holdExitMs": 0 },
               "readiness": { "stableMs": 5000 },
               "advertise": "broker1.example:1883" },
  "limits": { "maxPacketSize": 1048576, "receiveMaximum": 1024, "maxQueuedMessages": 1000,
              "maxKeepAlive": 0, "connectTimeout": 10, "connectRate": 0, "connectBurst": 0,
              "clientMessageRate": 0, "clientMessageBurst": 0, "clientByteRate": 0, "clientByteBurst": 0 },
  "overload": { "queuedMessages": 1000000, "replicationLag": 400000, "memoryBytes": 0 },
  "flapping": { "enabled": true, "windowSeconds": 60, "maxCount": 15, "banSeconds": 300 },
  "auth": { "anonymous": false, "decisionTtlSeconds": 3600, "argon2MemoryBytes": 268435456,
            "cache": { "ttlSeconds": 60, "maxEntries": 100000 } },
  "modules": { "auth.file": { "path": "users.json" }, "authz.file": { "path": "acl.json" } },
  "log": { "level": "info", "format": "text" },
  "mqtt": {
    "maximumQoS": 2, "retainAvailable": true, "wildcardSubscriptionAvailable": true,
    "subscriptionIdentifiersAvailable": true, "sharedSubscriptionAvailable": true,
    "topicAliasMaximum": 65535
  }
}
```

Only `version` and `node.dataDir` are required; the values shown for
`limits`, `auth`, `overload`, `flapping`, `log` and `mqtt` are the defaults, as are `cluster.id`,
`cluster.replicas`, `cluster.shards`, the heartbeat and frame limits,
`cluster.raft`, `cluster.leases`, `cluster.routing`, `cluster.replication`, `cluster.drain` and `cluster.readiness` (`cluster.listen`, `cluster.auth`,
`cluster.peers` and `cluster.advertise` are empty by default, which is a
one-member cluster). The `mqtt` object is
the operator's switch for the MQTT 5.0 capabilities a server may decline;
HoardMQ declines none unless told to. Unknown members are rejected
everywhere except inside `modules.*`, whose contents belong to the named
module; the schema also describes `modules.sink.kafka`, the Kafka
forwarder's configuration.

### Reloading the configuration

A running node re-reads its configuration file on SIGHUP, on `POST
/config/reload` or `hoardmq config reload` (see "Admin API"). It reads the
file and the environment overrides it started with, validates them as
`hoardmq check` does, and compares the result with the document in force:

- a document with any problem changes nothing, and every problem is reported
  together, as `check` reports them;
- a document that changes any member outside the table below changes
  nothing, and each such member is named by its JSON pointer: only a restart
  applies it;
- otherwise the document is put into force whole, and no connection is
  closed.

| Member | Applies from |
|---|---|
| `log.level` | at once |
| `limits.clientMessageRate`, `clientMessageBurst`, `clientByteRate`, `clientByteBurst` | each connection's next PUBLISH |
| `limits.connectRate`, `connectBurst`, and a listener's `connectRate` and `connectBurst` | the next CONNECT |
| a listener's `maxConnections` | the next accepted connection; connections over a lowered bound stay |
| `limits.maxQueuedMessages`, `limits.queueDrop` | the next message queued; a longer queue is not trimmed |
| `limits.maxSessionExpiry` | the next expiry a client sets; sessions keep theirs |
| `overload.*` | the next stress sample |
| `flapping.*` | the next abnormal end; bans already written stand |

A listener's members are reloadable while the listener keeps its name, type
and address; adding or removing a listener, and every other member (the
cluster, the node, per-connection limits such as `maxPacketSize` and
`maxKeepAlive`, `mqtt`, `auth` and `modules`), needs a restart. The
administered limits (`PUT /limits`) keep their precedence over the reloaded
ones. An applied reload also has `auth.file`, `authz.file` and `auth.scram`
re-read their files; a file that cannot be read fails validation and changes
nothing, and a module whose re-read fails afterwards keeps what it had and is
reported. A reload applies to the node it runs on only. Each one is logged,
audited as `config.reload` and counted in
`hoardmq_config_reloads_total{outcome}` (`applied`, `refused`, `invalid`). The
Helm chart restarts its pods when their configuration changes (its
`checksum/config` annotation).

### Listeners

`tcp`, `tls`, `ws` (MQTT over WebSocket, path `/mqtt` by default) and `wss`
(the same over TLS). TLS listeners require `tls.certFile` and `tls.keyFile`,
accept `tls.clientCAFile` (a client certificate is then required and
verified) and `tls.minVersion` (`1.2` default, `1.3`). Certificate files are
re-read when they change, checked at most once per second at handshake time;
a pair that fails to load keeps the previous one in service. Every listener
speaks every protocol level; the level is chosen per connection from CONNECT.

`connectRate` (CONNECTs a second; 0, the default, is unlimited) and
`connectBurst` (how many it admits at once; 0 is twice the rate) limit how fast
a listener admits connections, and `limits.connectRate` and
`limits.connectBurst` do the same for the node across every listener. A CONNECT
over either limit is refused as soon as it is read, before its credentials are
examined or any session work starts: CONNACK 0x9F (Connection rate exceeded) at
MQTT 5.0; at 3.1.1 and 3.1, which have no code for it, the connection is closed
without a CONNACK. A CONNECT one limit refuses takes nothing from the other.
Refusals are counted by `hoardmq_connect_rate_refused_total{listener,limit}`
(`limit` is `listener` or `node`). Why the limits are off by default, and how to
size one, is under "The connect path's cost" below.

### Authentication and authorization

**A fresh server refuses everyone.** Anonymous connections are refused
unless `auth.anonymous` is `true`, and every publish, subscribe and
unsubscribe is refused unless an authorizer allows it. The two lines that
open a development broker:

```json
"auth": { "anonymous": true },
"modules": { "authz.file": { "path": "acl.json" } }
```

with an `acl.json` of `{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}`.

`auth.file` checks username and password against a users file:

```json
{ "users": [ { "username": "alice", "password": "$pbkdf2-sha256$i=600000$...", "attributes": { "role": "admin" } } ] }
```

Passwords are PHC strings produced by `hoardmq hash-password` (the password
is read from stdin so it never lands in shell history). PBKDF2-HMAC-SHA256
with 600,000 iterations is the first supported algorithm; it is in the Go
standard library and FIPS-approved. A plain-text password in the file is
refused at start with a message naming the user. `hoardmq hash-password
-scram [-iterations N]` writes the SCRAM form instead,
`$scram-sha-256$i=N$salt$StoredKey$ServerKey` (at least 4,096 iterations,
600,000 by default): the same PBKDF2 derivation, stored as the two keys RFC
5802 says a server keeps, which serves both password login and SCRAM (below).
A user unknown to the users file passes to the next authenticator; a known
user with a wrong password is refused.

`hoardmq hash-password -argon2id [-memory KiB] [-time N] [-parallelism N]`
writes the Argon2id form, `$argon2id$v=19$m=KiB,t=N,p=N$salt$hash`: 64 MiB,
three passes and four lanes by default (RFC 9106's second recommended option). Argon2id is memory-hard, so it resists
guessing on GPUs better than PBKDF2 when passwords are chosen by people; PBKDF2
stays the default. It is accepted wherever a password hash is, in a users
file and as an `auth.db` credential's `passwordHash`, within bounds read before
anything is derived: at most 1 GiB, 16 passes and 16 lanes, a salt of at least
8 bytes and a hash of 16 to 64. The implementation is HoardMQ's own, on the
standard library, and matches RFC 9106's and the reference implementation's
test vectors. A build with `-tags fips` leaves it out: it accepts and writes no
Argon2id hash, and refuses a users file holding one, naming the user.

A client that presents a certificate verified against `tls.clientCAFile`
is authenticated by that; its subject common name is its identity and, when
no username was sent, its username for authorization.

#### The authenticator chain

Each authenticator answers allow, deny or pass; the first allow or deny
decides, and a chain that passes admits only a verified certificate on its
own, or an anonymous client when `auth.anonymous` is on. Certificate
authenticators (`auth.x509`) run first, before every other authenticator and
before an enhanced-authentication method, so a certificate they refuse is
refused whatever else the client sends. The others run in module identifier
order. A refusal by the chain is 0x86 (return code 4) when the client sent a
username or password, else 0x87 (return code 5); a certificate authenticator's
refusal is always 0x87. The audit record of every refusal carries the
authenticator's reason (`jwt: token expired`, `x509: revoked (ocsp)`,
`scram: invalid proof`, `ldap: invalid credentials`, `db: wrong password`),
never a credential. Among the built-in authenticators the order is `auth.db`,
`auth.file`, `auth.jwt`, `auth.ldap`: a user known to the in-broker database or
a users file is decided there before a directory is asked.

An allow is held for the session: at the earlier of the credential's own
expiry (a token's `exp` plus the skew) and `auth.decisionTtlSeconds` after
the decision (default 3,600, 1 to 86,400), the node makes it again with what
the client presented, without the client. Allowed again, it is held again;
otherwise the session ends with DISCONNECT 0x87 (a plain close at 3.1.1). A
connection admitted without an allow — anonymous, or a certificate with no
certificate authenticator — holds nothing to make again. A SCRAM session is
not re-decided by the node (nothing can be checked without the client); the
client re-authenticates (§ 4.12.1), and its certificate, if any, is checked
again.

#### The connect path's cost

Verifying a password derives a key: 600,000 iterations of PBKDF2, about 62 ms
of one core, so a 32-thread host derives at most 320–370 a second with every
thread busy. The node therefore keeps each password verification that matched,
for `auth.cache.ttlSeconds` (default 60, 0 to 3,600; 0 turns the cache off) and
at most `auth.cache.maxEntries` of them (default 100,000; when full, a new one
is not kept until old ones lapse). The cache is shared by every connection and
every password authenticator (`auth.file`, `auth.db`, and a password checked
against the SCRAM form), so a client that connects again within the TTL is
answered without deriving. An entry is a keyed MAC — under a secret made when
the node starts and kept nowhere else — of the stored credential and the
password, and the time it lapses: never the password, the stored hash or a
derived key. The stored credential is part of the key, so a changed or removed
credential is never answered from the cache: a CONNECT is verified against the
credential the node holds, from the moment a users file is reloaded or an
`auth.db` change is applied. Only a match is kept, so a client that knows no
password can neither add an entry nor crowd one out. What it costs: someone who
can read the broker's memory can test guesses against the passwords verified
within the last TTL at one HMAC a guess — the passwords themselves pass through
that memory as their clients connect. The cache holds no decision of the chain:
a directory, a token and a certificate are decided on every CONNECT.
`hoardmq_auth_password_derivations_total` and
`hoardmq_auth_password_cache_hits_total` count what was derived and what the
cache answered; `hoardmq_auth_password_cache_entries` is its size.

An Argon2id verification at the default cost holds 64 MiB and took 45–47 ms
here (its four lanes run at once; a 32-thread host, against about 30 ms for the
reference Go implementation). The node holds at most `auth.argon2MemoryBytes`
of them at once (default 268,435,456, 256 MiB, at least 1 MiB): a verification
waits for its share, and one whose hash needs more than all of it is refused,
with the reason `…memory cost exceeds auth.argon2MemoryBytes…` rather than a
wrong password. At the defaults four run at once, so a reconnect storm of
Argon2id users is verified at roughly 85 a second until the cache answers the
repeats. The budget changes only with a restart.

The connect-rate limits (see "Listeners") are off by default because no single
rate serves both jobs a limit could have. When a node of a three-node cluster
serving 100,000 clients is lost, the survivors take its clients back within
about three seconds of the loss being declared, 12,000 to 16,500 in the busiest
second, and every client is back 31–33 s after the crash. At 500 a second a
listener, the cap HiveMQ ships, the two survivors admit about 1,000 a second and
recovery took 56–65 s; a host of 32 threads derives about 370 keys a second
(`sim/results/connectrate-2026-09-26/`). A limit low enough to hold key
derivation inside the host stretches recovery to about a minute, and one high
enough not to slow it holds no derivation back. So the derivation cost is held
by the cache (a client's repeats) and by each listener's `maxHandshakes` (how
many handshakes, their derivations included, run at once). For a listener whose
clients log in by password, size `connectRate` to the derivations the host can
spare: at 62 ms a derivation, 100 a second takes about six cores.

#### JWT access tokens (`auth.jwt`)

```json
"auth.jwt": { "issuer": "https://idp.example", "audiences": ["hoardmq"], "jwksUri": "https://idp.example/keys" }
```

The token is read from the password field (`"from": "username"` reads the
username field instead). The module claims only a compact JWS whose
unverified `iss` is `issuer`; anything else passes, so a password meant for
`auth.file` is never taken for a token. Verification: RS256/384/512 (RSA keys
of at least 2,048 bits), ES256/384/512 and EdDSA against the JWKS; `none` and
HMAC are refused; `iss` equal to `issuer`; `aud` naming one of `audiences`;
`exp` and `iat` present, with `clockSkewSeconds` of skew (default 60, 0 to
300) and `exp - iat` at most `maxTokenLifetimeSeconds` (default 86,400, 60 to
31,536,000); access tokens (`typ: at+jwt`) are accepted and `azp` is not
checked. The identity is `usernameClaim` (default `sub`): a client that sent a
username beside the token must have sent exactly that value, and a client that
sent none takes it as its username for authorization. With `clientIdClaim`
set, that claim must equal the client identifier.

The key set is fetched in the background when the node starts, refetched for
a token naming a key it does not hold (at most once a minute) and when it is
an hour old — the held set keeps serving meanwhile. A CONNECT waits for a
fetch at most `fetchWaitMs` (default 2,000, 0 to 10,000); a node that holds no
key set refuses the token (`jwt: provider unavailable`). `caFile` names the
roots for an https endpoint; `jwksUri` must be https unless its host is
loopback.

Introspection (RFC 7662) replaces local verification:

```json
"auth.jwt": { "issuer": "https://idp.example", "audiences": ["hoardmq"],
              "introspection": { "endpoint": "https://idp.example/introspect", "clientId": "broker",
                                 "clientSecretFile": "/etc/hoardmq/introspection.secret", "timeoutMs": 2000 } }
```

Each CONNECT with a claimed token posts it with HTTP Basic client
authentication, bounded by `timeoutMs` (100 to 10,000) over at most 64
connections to the endpoint. The answer must say `active: true`; when it
carries `exp`, `aud` or `iss`, each must hold as above. An unreachable or
failing endpoint refuses the token.

By default only a JWS naming `issuer` is introspected. For a provider that
issues opaque reference tokens, `"opaqueTokens": true` (under `introspection`)
claims any other non-empty value too, and posts it to the endpoint. That means
a password meant for another authenticator is sent to the identity provider
too, when it reaches `auth.jwt`. `auth.db` and `auth.file` decide the users
they know before `auth.jwt` runs, so their users' passwords are not sent.
`"opaqueTokenPrefix": "…"` narrows the claim to values that start with it (a
provider's token prefix), and is the way to keep other passwords out. A JWS
naming another issuer is never claimed.

#### Certificate revocation (`auth.x509`)

```json
"auth.x509": { "ocsp": true, "crl": true, "failOpen": false }
```

For a client whose certificate `tls.clientCAFile` verified, every certificate
of the verified chain below the root is checked, at once: by OCSP and, when OCSP
has no definite answer, by the CRL. The leaf uses `ocspUrl` and the first of
`crlUrls` when they are set, and otherwise its own responder and its first
`http` or `https` distribution point. An intermediate always uses its own. A
certificate revoked anywhere in the chain is refused (`x509: revoked (…)` for
the leaf, `x509: intermediate revoked (…)` above it), and the chain's status is
unknown when any certificate's is. When every
enabled source is unreachable, slow, unsigned by the issuer, stale or says
`unknown`, the status is unknown: refused by default (fail-closed), admitted
with `failOpen: true` — and every unknown status is counted either way
(`hoardmq_auth_revocation_unknown_total{policy}`). A good certificate is
admitted on its own; beside a username or password it passes, and that
credential is decided by its own authenticator. An OCSP answer and a CRL are
cached until their `nextUpdate` (at most 100,000 answers and 64 lists); a
CONNECT waits for a fetch at most `fetchWaitMs` (default 2,000) and the fetch
runs to `timeoutMs` (default 5,000) whoever stops waiting. OCSP is written
in-house (RFC 6960 with SHA-1 CertIDs and no nonce, RFC 5019); a response may
be signed by the issuer or by a responder it delegated to. A CRL is cached
for the issuer it was accepted for.

A CRL with an issuing distribution point (a partitioned list, as public CAs
publish) speaks only for the certificates in its scope. A certificate must
name one of the list's distribution points among its own, and must be of the
kind the list covers (user or CA certificates). A list limited to some
revocation reasons can say revoked, never good. Outside its scope the list says
nothing, and the status is unknown rather than good, including for a leaf
whose own points differ from a `crlUrls` list's. An intermediate that names no
OCSP responder and no distribution point therefore has an unknown status:
refused under the default policy, admitted with `failOpen`. Not supported, and
refused as unusable: indirect CRLs, lists scoped to attribute certificates or
naming their point relative to the issuer, delta CRLs, and any other critical
extension; https endpoints under a private CA are not supported either.

#### SCRAM enhanced authentication (`auth.scram`)

```json
"auth.scram": { "path": "users.json" }
```

MQTT 5.0 clients that name the method `SCRAM-SHA-256` in CONNECT authenticate
by RFC 5802/7677 SCRAM over AUTH packets: the server's challenge is an AUTH
0x18, the client answers with AUTH 0x18, and the CONNACK carries the server's
final message, which proves the server holds the credential. Users are read
from a users file in the `auth.file` format; only users stored in the SCRAM
form can use it — the `$pbkdf2-sha256$` hash is itself the value SCRAM derives
its client key from, so a stolen file would log in without cracking anything.
A user the file does not hold is answered with a salt of its own and refused
at the last step, as a wrong password is. A CONNECT naming the method must
not carry a password, and a username it carries must be the authenticated
user. Re-authentication (AUTH 0x19, § 4.12.1) runs the same exchange during
the session; a failure, or another user, ends it with DISCONNECT 0x87.
Channel binding (`-PLUS`) and SASLprep are not supported: usernames and
passwords are compared as the bytes sent. An unknown method is answered
CONNACK 0x8C.

#### LDAP (`auth.ldap`)

```json
"auth.ldap": { "url": "ldaps://dir.example.com",
               "search": { "baseDn": "ou=people,dc=example,dc=com",
                           "filter": "(&(objectClass=person)(uid=${username}))",
                           "bindDn": "cn=hoardmq,ou=services,dc=example,dc=com",
                           "bindPasswordFile": "/etc/hoardmq/ldap.secret" } }
```

The node binds to the directory as the client: after a search (above: bind as
the service account — or anonymously when `bindDn` and `bindPasswordFile` are
both left out — find the one entry the filter matches under `baseDn`, `scope`
`sub` by default or `one`, then bind as that entry's DN with the client's
password), or directly with `"bindDn": "uid=${username},ou=people,dc=example,dc=com"`
in place of `search`. `${username}` stands for the client's username: in the
filter it can only be a whole assertion value, and it goes into the request as
that value's bytes, so no username changes the search; in a DN it is escaped
(RFC 4514). Filters support `&`, `|`, `!`, equality and presence.

The module claims a CONNECT with a non-empty username and a non-empty
password; anything else passes without contacting the directory (an empty
password would be an unauthenticated bind, which a directory may answer with
success). A search that finds no entry passes (a user the directory does not
know); more than one is refused (`ldap: ambiguous user`); a wrong password is
refused (`ldap: invalid credentials`). A directory that cannot be reached,
answers busy or unavailable, refuses the service account, or does not answer
within `timeoutMs` (default 2,000, 100 to 30,000) is unavailable, and
`onUnavailable` decides: `deny` (the default: refused, `ldap: directory
unavailable`) or `pass` (the next authenticator in the chain decides; with
none that claims the client, it is refused as any unclaimed credential is).
Every unavailable decision is counted by policy
(`hoardmq_auth_ldap_unavailable_total{policy}`). An allow is held for the
session and made again at `auth.decisionTtlSeconds`, which binds again; under
`deny`, a directory outage at that moment ends the session.

`ldaps://` is TLS from the first byte; `ldap://` with `"startTLS": true`
upgrades before anything is sent. A password never crosses a network in clear:
`check` refuses a plain `ldap://` URL unless its host is loopback. TLS 1.2 is
the minimum, the server certificate is verified against `caFile` (or the
system roots) and the URL's host. At most `maxConnections` (default 16, 1 to
256) connections are held open and reused; a connection the directory closed
while it was idle is replaced without failing the client. The client is
written in-house over the standard library (RFC 4511); SASL binds, referrals,
substring filters and group or attribute mapping are not supported.

#### The in-broker database (`auth.db`)

```json
"auth.db": {}
```

Client credentials kept in the cluster itself and administered through the
admin API (`/credentials`, below) or `hoardmq credentials`. They are records
in the consensus log, so a credential added through any node authenticates on
every node once that node has applied it, and survives restarts. A password is
stored only as a PHC string: the node hashes a password it is given with
PBKDF2 (600,000 iterations) or, with `"scram": true`, into the SCRAM form, or
stores a `passwordHash` that `hoardmq hash-password` wrote; neither the
password nor the hash is ever returned, logged or audited. A known user is
allowed only with the right password (`db: wrong password`); an unknown user
passes. The database holds at most 100,000 credentials; SCRAM exchanges read
users files only (`auth.scram`), not this database.

```sh
hoardmq credentials add -password-file alice.pw -attribute team=blue alice
hoardmq credentials update -password-file alice.pw -scram alice
hoardmq credentials list
hoardmq credentials remove alice
```

The password is read from a file (`-password-file`), or a hash from
`-hash-file`, never from the command line.

SQL authentication is not built in: it is planned as an out-of-tree module on
the public extension seam.

`authz.file` applies ACL rules in order; the first applicable rule that
mentions the action decides:

```json
{ "rules": [
  { "principal": { "username": "admin" }, "topic": "#", "allow": ["publish", "subscribe", "unsubscribe"] },
  { "topic": "devices/${clientid}/#", "allow": ["publish", "subscribe", "unsubscribe"] },
  { "principal": { "cn": "gateway" }, "topic": "gw/${cn}/+", "allow": ["publish"] },
  { "principal": { "username": "guest" }, "topic": "secret/#", "deny": ["subscribe"] }
] }
```

`principal` members (`username`, `clientId`, `cn`) must all match when
present; `topic` is a filter with `${clientid}`, `${username}` and `${cn}`
substituted; a subscribe or unsubscribe of a wildcard filter is allowed only
by a rule that covers every topic it could match.

### Connection behaviour

Keep-alive expiry closes at one and a half times the client's value, or the
server's `limits.maxKeepAlive` when that is lower (advertised as Server Keep
Alive to MQTT 5.0 clients). `limits.receiveMaximum` bounds the client's
unacknowledged QoS 1 and 2 publishes; `limits.maxPacketSize` bounds inbound
packets and is advertised. The client's Maximum Packet Size is honoured
outbound: a publish that does not fit is dropped silently, any other packet
is retried without its Reason String and User Properties. Topic aliases work
in both directions. Every refusal carries a reason code at MQTT 5.0; at 3.1
and 3.1.1, which have none, the connection is closed. When the server ends a
connection it flushes the DISCONNECT, half-closes the socket and keeps
reading for half a second, so a client that is still sending receives the
reason rather than a connection reset.

`limits.clientMessageRate` (PUBLISH packets a second) and
`limits.clientByteRate` (their topic and payload bytes a second), each with a
burst (`clientMessageBurst`, `clientByteBurst`; 0 is twice the rate), limit
each connection's publishing; 0, the default, is unlimited. A client over
either rate is throttled, never refused or disconnected: every PUBLISH it
sends is read and handled, and after one that leaves it in debt the broker
stops reading its connection until the debt is repaid, so TCP carries the
back-pressure to the client. The keep-alive runs from the end of the pause,
so a paused client is not closed for its silence. Throttling is counted by
`hoardmq_client_throttled_total{reason}` (`message` or `byte`) and
`hoardmq_client_throttle_seconds_total`. The rates can be changed while the
cluster runs (see "Overload and bans"); a connection reads the new rates at
its next PUBLISH.

A routed message never waits for its subscriber's socket. When a
connection's outbound queue is full, the message stays in the session's
queue — within `limits.maxQueuedMessages` and its drop policy, QoS 0
included — and is sent, in order, once the connection has written half its
queue out; the publisher's own connection is not held up. Deferrals are
counted by `hoardmq_deliveries_deferred_total{listener}`.

### Overload and bans

**The stress level.** Once a second each node measures three indicators
against a limit: messages queued in its sessions (`overload.queuedMessages`,
default 1,000,000), frames sent to its replicas and not yet acknowledged
(`overload.replicationLag`, default 400,000), and the live heap
(`overload.memoryBytes`; 0, the default, takes `GOMEMLIMIT` when the process
has one and otherwise leaves the heap out). The node's level is set by its
highest fraction: under 0.6 it is 0, under 0.8 1, under 0.95 2, else 3. Each
node advertises its level to its peers in its heartbeat, and each acts on the
**cluster level**, the highest of its own and its connected peers'. In a
failover storm at 100,000 clients the replication backlog peaked at 84,314
frames, about a fifth of its limit, and the stress level stayed 0
(`sim/results/overload-2026-09-26/`).

**What each level sheds**, in this order and never by closing a connection:

| Level | Connections paused | Pause before each read | New CONNECTs |
|---|---|---|---|
| 0 | none | — | admitted |
| 1 | the noisiest 10% of those that published in the last second | 100 ms | admitted |
| 2 | the noisiest 50% | 250 ms | admitted |
| 3 | every one that published in the last second | 500 ms | refused |

Noisiest is by PUBLISH bytes read in the last second; a connection already
paused keeps its place while the level holds, so a pause that works does not
move to a quieter client. The pause holds the keep-alive, as a client rate's
does. At level 3 a new CONNECT is refused as soon as it is read: CONNACK 0x89
(Server busy) at MQTT 5.0, return code 3 (Server unavailable) at 3.1.1 and
3.1. At level 0 the node walks no connections. `GET /api/v1/overload`
shows the node's level, indicators and limits, and each peer's level.

**Bans.** The ban list is one list for the cluster, stored in the consensus
log, so a ban written through any node refuses on every node once applied
there. An entry has a kind and a value: `clientid`, `username`, `address` (an
IP address, written in its canonical form, `192.0.2.1`), `cidr` (a prefix,
canonical: `10.0.0.0/8`), `clientid-regex` and `username-regex` (RE2 at most
256 bytes, matched against the whole value). It may carry a reason and an
expiry. A ban is checked as soon as the CONNECT is read, before the
connect-rate limits and before any credential is examined, so a banned
client costs no rate token and no key derivation: CONNACK 0x8A (Banned) at
MQTT 5.0, return code 5 (Not authorized) at 3.1.1 and 3.1. It is counted by
`hoardmq_connects_banned_total{listener,kind}` and audited as an
authentication failure with the reason `ban: <kind>`. An entry at or past its
expiry refuses nothing; the node that leads the consensus core deletes lapsed
entries every ten seconds (audited as `ban.delete` by `sweeper`). A ban
refuses new connections only: a client already connected when it is written
stays connected until it next connects. The list holds at most 100,000
entries, the limit of a collection in the consensus log.

**Flapping.** A client whose established connections end abnormally on its
side — the network closed or reset, the keep-alive expired, or it broke the
protocol — `flapping.maxCount` times (default 15) within
`flapping.windowSeconds` (default 60) is banned by client identifier for
`flapping.banSeconds` (default 300), as an ordinary entry with the source
`flapping`, audited as `ban.create` by `flapping`. A client's DISCONNECT is
not a flap, and neither is any end the broker chose (a takeover, a redirect,
an administrative close, shutdown) nor any CONNECT refused before it was
established, so a storm of the broker's own refusals cannot become a storm of
bans. An existing ban, an operator's included, is left as it is. Each node
tracks at most 100,000 identifiers, and writes at most one ban per
identifier per ban time and 100 a second. `flapping.enabled: false` turns
the detector off. `hoardmq_connections_ended_total{how}` (`graceful`,
`server`, `abnormal`) counts what the detector saw;
`hoardmq_flapping_trips_total` and `hoardmq_flapping_bans_total{outcome}` what
it did.

**What cluster-wide costs.** Every ban write, an operator's or the
detector's, is one proposal to the consensus core, and each node rebuilds its
ban index when the list changes, off the connect path; a CONNECT reads the
index with one atomic load. In a failover storm the survivors' connections
end at the broker's choice (takeovers) and the lost node's end on a node that
is gone, so the detector writes nothing: in seven failover runs at 100,000
clients the survivors saw two or three abnormal ends a run and tripped none
(`sim/results/overload-2026-09-26/`).

**Administered limits.** `PUT /api/v1/limits` (or `hoardmq limits set`)
stores the per-client message and byte rates and bursts and the node's
connect rate and burst in the consensus log, for every node: a member it
sets overrides the configuration, a member it leaves out falls back to it.
A new connect rate applies to each node's gate at once, keeping each
listener's own limit; a new client rate applies to each connection from its
next PUBLISH. Limits apply to the default namespace, the only one until
namespaces are activated.

### Sessions

Session State is what MQTT 5.0 section 4.1 lists: the session's existence,
its subscriptions, the inbound QoS 2 identifiers awaiting PUBREL, the
outbound QoS 1 and 2 messages in flight, the queued messages, the Will and
its delay, and the expiry time. A client that reconnects with Clean Start 0
before its Session Expiry Interval elapses gets its in-flight messages
resent (PUBLISH with DUP, or PUBREL) in their original order and then its
queue, subject to its Receive Maximum; nothing is resent on a timer. QoS 2
inbound uses Method A: the message is routed on receipt and the packet
identifier is remembered until PUBREL, so a duplicate PUBLISH is not routed
twice; after a failure, only a publish the broker had accepted is absorbed
this way (see "Exactly-once across an owner loss"). Level 3 and 4 clients
with Clean Session 0 never expire.

Server-defined behaviour: because routing comes first, a publish whose
acknowledgement carries an error was not accepted, but may already have
reached some subscribers. An error PUBREC ends the QoS 2 exchange and releases
its identifier (MQTT 5.0 section 4.3.3), so the publisher's retry is a new
message, whether or not it reuses the identifier, and a subscriber that
received the refused one receives the retry too, at QoS 2 as at QoS 1. A
subscriber whose queued copy could not be journaled does not: that copy was
withdrawn (see "Durability and group commit"). QoS 2's exactly-once delivery
covers the publishes the broker accepted; a refused publish is outside it.
The error PUBREC waits until the identifier's release is durable (see
"Exactly-once across an owner loss"), so a refusal can arrive late. At 3.1.1
and 3.1, whose PUBACK and PUBREC carry no reason code, a refused QoS 1 or 2
publish is not acknowledged at all: the connection is closed, and the publish
stays unacknowledged for the client to send again.

Queues are bounded by `limits.maxQueuedMessages`; `limits.queueDrop` picks
whether a full queue drops the `oldest` message (default) or refuses the
`newest`. `limits.maxSessionExpiry` caps the Session Expiry Interval a
client may ask for (0 means no cap); when it applies, MQTT 5.0 clients are
told in CONNACK. A Will is published after its Will Delay Interval when a
connection ends abnormally, at once when the delay is 0, at session end if
that comes first, never after a normal DISCONNECT, and not at all when a new
connection to the same client identifier arrives before the delay elapses.
A second connection with the same client identifier takes the session over
and the first receives DISCONNECT 0x8E.

### Subscriptions, retained messages and shared subscriptions

Topic filters follow section 4.7: `+` matches one level, `#` the rest, and
a filter that starts with a wildcard never matches a topic that starts with
`$`. When several of a session's subscriptions match one message the
session receives it once, at the highest granted QoS, with every
Subscription Identifier attached. No Local, Retain As Published and Retain
Handling 0, 1 and 2 behave as specified; level 3 and 4 subscribers get no
properties, see RETAIN on the stored message sent at subscribe time, and
never on a forwarded message.

Retained messages are last-writer-wins per topic; an empty retained payload
removes the entry; a retained message expires with its Message Expiry
Interval (checked on access and swept every minute). Retained messages are
sent after the SUBACK and never for a shared subscription. In a cluster the
retained store is sharded by topic (see "Data-plane replication").

Shared subscriptions (`$share/{name}/{filter}`) deliver each message to one
member of the group. `mqtt.sharedSubscriptionStrategy` selects
`round-robin` (default), `random`, `sticky`, `hash` (by publisher client
identifier) or `local-first` (round-robin on one node). Offline members are
skipped while any member is online; when all are offline the message queues
on one member. When a member's session ends, its QoS 1 messages still in
flight or queued are re-offered to the group; QoS 2 messages in flight are
never redistributed. Level 3 and 4 clients may use `$share/` filters.

### Modules

Everything that authenticates, authorizes, persists, measures or traces is a module
on one registry, built-in or not: `auth.file`, `auth.jwt`, `auth.x509`,
`auth.scram`, `auth.ldap`, `auth.db`, `authz.file`, `store.wal`, `metrics.internal`,
`metrics.prometheus`, `traces.otlp` and the Kafka forwarder `sink.kafka` register the same way a third-party module would, receive
their configuration from `modules.<id>`, declare the hook families they
provide, and run provision, validate, start and stop. `store.wal` and `metrics.internal`
are instantiated by default when no module of their family is configured.
Hook families: connection lifecycle, authentication, authorization, inbound
and outbound packet interceptors (continue, modify, drop, disconnect with a
reason), subscribe interceptors, delivery events, retained and Will events,
publish admission and publish accepted (below), store providers, metrics
providers and trace providers. `hooks.timeoutMs` bounds every hook
call (0, the default, calls inline) and `hooks.fallback` says what a
timed-out or panicking hook counts as: `continue` (default) or
`disconnect`. `hooks.order` lists the modules called first, in that order, in
every hook family, both authentication chains (certificate authenticators
still run before the rest) and the lifecycle (stop in reverse); every other
module follows in identifier order, and a change applies at the next restart.
`hoardmq check` provisions and validates the configured modules read-only,
so a bad users-file path or an unknown module identifier is reported beside
any schema problem.

**The extension API** is the package tree `github.com/happydave/hoardmq/ext`. It
imports only the standard library:
- `ext` holds the module contract, registration (`ext.Register` from a package's
  `init`), the module context and services, and the hook families;
- `ext/mqtt` holds the packets and the message envelope;
- `ext/auth` holds authentication and authorization;
- `ext/records` holds the replicated records store's values;
- `ext/store` (experimental) holds the store provider's journal, record format and state. A store provider must
  feed the replication tap (`SetTap`), and one without it is refused;
- `ext/trace` (experimental) holds the tracer and the exporter a trace module supplies.

The API has its own version, `ext.APIVersion` (0.2.0), which `hoardmq version` prints
beside the modules compiled in. [ext/CHANGELOG.md](ext/CHANGELOG.md) states what a
version may change. Built-in modules reach the broker only through this API, and tests
hold them to it (`internal/module/seam_test.go`).

**Building with other modules:** `hoardmq-build` compiles a binary with the built-in
modules plus each module named, from a version or a local directory:

```
go run ./cmd/hoardmq-build --hoardmq-dir . --with example.com/mymodule=../mymodule --output hoardmq
go run ./cmd/hoardmq-build --hoardmq-version v0.1.0 --with example.com/mymodule@v1.2.0
```

It writes a throwaway module whose `main` imports each named module, and runs
`go build -mod=mod` in it. `cmd/hoardmq/main.go` is the same generated `main` with
no modules (`go generate ./cmd/hoardmq`). The worked example of a module built
outside the broker's tree is
[cmd/hoardmq-build/testdata/hello](cmd/hoardmq-build/testdata/hello/hello.go). Its
module file is kept as `go.mod.txt`: copy the directory and rename it to `go.mod`.
Its test builds it with the tool, offline, and runs its lifecycle in the binary,
including a hot disable and enable through the admin API.

**Publish admission and publish accepted** are the two hooks a sink is built on
(API 0.2.0):
- **The admission check** (`ext.PublishAdmission`) is called once for each
  client PUBLISH. It runs after authorization, topic alias resolution and the
  QoS 2 retransmission check, and before any delivery.

  It may refuse with a reason code, which takes the path of any refused
  publish:
  - an error PUBACK or PUBREC at MQTT 5;
  - the connection closed at 3.1.1 and 3.1, and for QoS 0.

  It is counted as `hoardmq_drops_total{reason="admission"}`.
- **The accepted listener** (`ext.PublishAcceptedListener`) is told of each
  publish the node accepts, once, on that node. It is called after the step
  that decides acceptance and before the acknowledgement:
  - QoS 0 after dispatch;
  - QoS 1 after the journal sync and the replication barrier;
  - QoS 2 after those and the packet identifier's acceptance.

  It also hears Wills and module publishes. A refused publish, a copy from
  another node, a shared-group give or a retained hand-over is never reported.

An inbound interceptor runs earlier, on the raw packet, before authorization and
alias resolution, so a sink refuses from the admission check, not from an
interceptor.

### Kafka forwarder (`sink.kafka`)

Every binary carries `sink.kafka`: the default one, the container image and
`hoardmq-build`'s. It runs only when configured. It produces each publish the
node accepts on a topic a route matches to that route's Kafka topic, MQTT to
Kafka only, with the franz-go client. It is built on the public extension API
alone, and `internal/module/seam_test.go` holds it to that.

```json
"modules": { "sink.kafka": {
  "brokers": ["kafka-0.kafka:9092"],
  "routes": [
    { "filter": "sensors/+/temperature", "topic": "sensor-readings", "key": "topic" },
    { "filter": "alarms/#", "topic": "alarms", "key": "userProperty", "keyProperty": "site" }
  ],
  "clientId": "hoardmq-<node>",
  "tls": { "caFile": "/etc/kafka/ca.pem", "certFile": "/etc/kafka/client.pem", "keyFile": "/etc/kafka/client.key" },
  "sasl": { "mechanism": "scram-sha-512", "username": "hoardmq", "passwordFile": "/run/secrets/kafka" },
  "headers": { "metadata": true, "userProperties": true },
  "acks": "all", "compression": "none", "lingerMs": 5,
  "buffer": { "maxRecords": 100000, "maxBytes": 67108864, "whenFull": "refuse", "waitMs": 1000 },
  "flushTimeoutMs": 5000
} }
```

**Configuration.** Only `brokers` and `routes` are required; the other values
shown are the defaults.
- `acks`: `all` (the idempotent producer) or `leader`.
- `compression`: `none`, `gzip`, `snappy`, `lz4` or `zstd`.
- SASL: `plain`, `scram-sha-256` or `scram-sha-512`. The password comes from a
  file and is never logged.
- TLS always verifies the server.
- `hoardmq check` reports every problem at `/modules/sink.kafka`, naming the
  field, and connects to nothing.

**Mapping.** A route matches the node's default namespace unless it names a
`namespace`, and a message is produced once to each distinct Kafka topic its
routes name (the first matching route naming a topic decides). A Kafka topic is
1 to 249 of `A-Z a-z 0-9 . _ -`, and a shared filter is refused.
- **Key:** the MQTT topic (`topic`, the default), the publisher's client
  identifier (`clientId`), the value of a named user property (`userProperty`
  with `keyProperty`), or `none`. A missing identifier or property gives no key.
- **Value:** the payload, empty rather than null.
- **Timestamp:** when the node accepted the publish.
- **Headers, in order:**
  - first `mqtt.topic`, `mqtt.qos` and `mqtt.source` (`client`, `will` or
    `module`);
  - then, when set, `mqtt.clientId`, `mqtt.retain`, `mqtt.contentType`, `mqtt.payloadFormat`,
    `mqtt.responseTopic`, `mqtt.correlationData`, and `mqtt.namespace` outside
    the default namespace;
  - then every MQTT 5 user property, in order, repeats kept.

  `headers.metadata` and `headers.userProperties` turn each group off. The
  username is never sent, and message expiry is not mapped.

**Delivery.** At least once into Kafka while the accepting node runs. The node
that accepted a publish forwards it, once; a copy another node forwards to its
subscribers is not forwarded again, and a Will goes from the node that
publishes it.
- Every node needs the same `modules.sink.kafka`: a node without it forwards
  nothing.
- Messages one connection publishes on one topic reach one partition in order
  when keyed by topic or client identifier.
- A QoS 1 resend is a new message and can reach Kafka twice, as it can reach a
  subscriber twice; a QoS 2 publish is forwarded once.
- The PUBACK or PUBREC means what it always means, that the message is durable
  on this node and its replicas. It does not wait for Kafka.

**When Kafka is down or slow.** The forwarder buffers in memory, up to
`buffer.maxRecords` records or `buffer.maxBytes` bytes. While the buffer is full:
- **`whenFull: "refuse"`** (the default) refuses a client's matching QoS 1 or 2
  publish with Quota Exceeded (0x97). That publish then reaches no MQTT
  subscriber either, and a 3.1.1 publisher is disconnected, since 3.1.1 has no
  way to refuse a publish. A publish admitted while there was room, which then
  meets a full buffer, waits up to `buffer.waitMs` for room before it is
  counted `overflow`.
- **`whenFull: "drop"`** refuses nothing and counts each forward it cannot
  buffer.
- **QoS 0** is never refused: its forward is dropped and counted.

**Counted, never silent:**
- `hoardmq_sink_kafka_produced_total{topic}`;
- `hoardmq_sink_kafka_dropped_total{topic,reason}`, where reason is one of:
  - `buffer_full`: the buffer was full;
  - `overflow`: refuse's wait ran out;
  - `rejected`: Kafka refused the record for good, for example too large, an
    unknown topic or not authorized;
  - `shutdown`: abandoned at stop;
- `hoardmq_sink_kafka_refused_total{topic}`;
- the gauges `hoardmq_sink_kafka_buffered_records` and
  `hoardmq_sink_kafka_buffered_bytes`.

Drops are also logged at Warn, at most once a second per reason, with their
count.

**What is lost, plainly.** A node that crashes loses what its buffer held. Those
messages were acknowledged to their publishers and delivered to MQTT
subscribers, and no counter can count them afterwards. The buffered gauges,
scraped before the crash, are that exposure. At a stop, a drain's included, the
forwarder flushes for up to `flushTimeoutMs`, then abandons the rest, counted
`shutdown` and logged. A record already sent and still awaiting Kafka's answer
then may have been written, and is counted `shutdown` all the same. A drained
node's buffer is not handed to another node.

**A live test** runs the default binary against a real Kafka. It skips unless
both variables are set:

```
SINKKAFKA_LIVE_BROKERS=127.0.0.1:7068 SINKKAFKA_LIVE_CONTAINER=<podman container> go test -count=1 -v -run '^TestLiveKafka$' ./modules/sinkkafka
```

It pauses, stops and starts the container to show each policy. The test's
variables do not start with `HOARDMQ_`, which the broker reads as configuration
overrides.

### Observability

`observability.address` (empty by default) starts an HTTP listener serving
`/metrics` in the Prometheus text format, `/healthz` (liveness, 200 while
the process serves), `/readyz` (readiness: joined, caught up, unfenced, not
draining, topology stable — see "Drain and readiness"), `/servingz` (ready
and able to acknowledge a QoS 1 or 2 publish; not a probe) and `/drain` (POST
starts the graceful leave, GET reports it; closed with 403 once the admin API
is configured, which then owns the drain). The host must be a loopback
address unless `observability.allowRemote` is true. Metrics are exported by the
`metrics.prometheus` module, which replaces `metrics.internal` and reads the
same counters: connections (total by listener, active, and closed by cause —
`take` when the session was taken away with a connection still attached,
`detach` when the connection was closed with the state left behind, which is
what a takeover does, `evict` when the state was discarded here because the
lease moved, and `evict-import` for the part of `evict` an adopted session's
import caused), cluster peers
connected with cluster frames, bytes, reconnects, heartbeat misses and
refusals, the consensus core (term, commit and applied index, snapshot
index, leader flag, members by role, proposals by outcome, snapshots,
dropped frames, and `hoardmq_raft_proposal_seconds_total{outcome}`, the time from a
proposal to its outcome), leases (active, operations by outcome, takeovers by kind,
adoptions, fences, connect refusals), routes (active by scope, updates and
syncs by direction, forwards by kind, forward failures), replication
(records and acknowledgements, barrier time and failures, degraded
barriers, replicas held, shard map version, retained forwards and
queries, and `hoardmq_replication_backlog_frames{peer}`, the frames pushed to a
peer and not yet acknowledged: replication runs one stream a peer, so this is
the followers' lag), the drain (clients redirected, leases handed off, sessions confirmed
and unconfirmed, seconds) and
readiness (`hoardmq_topology_changes_total`), sessions and
subscriptions by namespace, publishes by namespace and QoS, deliveries by
event, drops by reason (`queue_full`, `expired`, `refused`, `hook`),
authentication failures by listener and reason, key-set fetches
(`hoardmq_auth_jwt_key_fetches_total{outcome}`), introspections by outcome,
revocation checks by method and status and unknown statuses by policy,
enhanced-authentication exchanges by method and outcome, held decisions made
again by outcome (`hoardmq_auth_rechecks_total`), password verifications derived
and answered from the cache and the cache's size, CONNECTs refused by a
connect-rate limit by listener and limit, LDAP decisions by outcome
(`hoardmq_auth_ldap_decisions_total`), unavailable directories by policy and
directory connections by outcome, the cluster stress level by scope
(`hoardmq_overload_level{scope}`, `node` or `cluster`) and its indicators
(`hoardmq_overload_indicator{name}`), connections paused for stress,
CONNECTs refused for overload and for a ban, bans held and invalid, lapsed
bans deleted by outcome, connections ended by how, flapping trips and bans,
clients throttled and for how long, deliveries deferred, Wills, retained messages
by namespace, the log index, hook failures by module, and the journals (below).

The node's own journal and the replica log each report, labelled `store="node"` or
`store="replica"` and read without the store's lock: the last index
(`hoardmq_journal_index`), the last checkpoint's index and the seconds since this
process wrote one or, if it has written none, since it opened the log
(`hoardmq_journal_checkpoint_index`, `hoardmq_journal_checkpoint_age_seconds`),
checkpoints written (`hoardmq_journal_checkpoints_total`), every fsync of the log
and the time they took (`hoardmq_journal_fsyncs_total`,
`hoardmq_journal_fsync_seconds_total`; the second over the first is the mean
latency), failed fsyncs, rewrites after one and failed rewrites
(`hoardmq_journal_fsync_failures_total`, `hoardmq_journal_repairs_total`,
`hoardmq_journal_repair_failures_total`), and the callers that waited for a sync
under the commit policy and how long (`hoardmq_journal_sync_callers_total`,
`hoardmq_journal_sync_wait_seconds_total`). A failed fsync with no repair after it
is a disk to look at; a failed repair refuses publishes until one succeeds (see
"Durability and group commit"). `hoardmq_journal_failures_total{op}` counts what
the session layer saw fail: `append` when a record could not be written, which
refuses the QoS 1 or 2 publish that wrote it (see "Durability and group commit"),
and `sync` when a wait for durability failed, which refuses the QoS 1 or 2 publish
or state change that waited.

An append-only audit log under `node.dataDir/audit/` records authentication
failures, module enable and disable, trace start and stop, server start and
stop, and the cluster's lease, membership and replication events as one JSON
object per line, rotated at `observability.audit.maxBytes` keeping
`observability.audit.maxFiles` files; `observability.audit.enabled` (default
true) turns it off. Events are handed to one writer over a queue of
`observability.audit.queue` entries (default 8192) and committed in batches,
one write and one sync per batch, so no connection or administrative call
waits on the audit file. A refused CONNECT is the one event a reconnect
storm produces by the hundred thousand: when the queue is full it is dropped
and counted rather than queued. Every other event waits for space and is
never dropped; an event accepted before shutdown is on disk before the
server exits. Drops are visible three ways — an `audit.dropped` record with
the count and the time of the previous batch, written into the log itself
when the writer next runs; `hoardmq_audit_dropped_total`; and
`hoardmq_audit_queue_depth` — alongside `hoardmq_audit_records_total`,
`hoardmq_audit_batches_total` (records over batches is the group-commit
ratio) and `hoardmq_audit_write_failures_total` (batches lost to a write or
sync error, logged once per distinct error). A trace
recording captures one client's inbound and outbound packets in a ring of
`observability.traceEvents` entries without touching other clients; the
admin API starts, fetches and stops it.

#### OpenTelemetry traces

The `traces.otlp` module records spans for the publish path and exports them as
OTLP over HTTP in the JSON encoding (OTLP 1.11.0), batched by the broker itself;
it is off unless configured:

```json
"modules": {
  "traces.otlp": { "endpoint": "http://otel-collector:4318/v1/traces", "sampleRatio": 0.01 }
}
```

`endpoint` is used as given (`/v1/traces` is not appended). Sampling is decided
once, where the trace starts: a PUBLISH whose MQTT 5 user property `traceparent`
(W3C Trace Context; `tracestate` is carried with it) is valid continues that
trace, recorded if its sampled flag is set; any other publish starts a trace with
probability `sampleRatio` (default 0.01). A client that sets the sampled flag is
therefore traced whatever the ratio. A traced publish is a `publish` span (server)
on the node that took it in, a `forward` span (producer) for each node it is
forwarded to, and a `deliver` span (consumer) on each receiving node, one trace
across the cluster. The broker never adds, removes or reorders a user property —
MQTT 5 requires them forwarded unaltered — so its own span context crosses between
nodes in the forward's envelope, and subscribers receive exactly the properties
the publisher sent. Spans carry the topic, the client identifier, the namespace,
the node, the QoS, counts and the MQTT reason code of a refusal; never a payload or
a property's value.

A finished span is queued (`queueSize`, default 8192) and never waits: a full queue
drops it. One goroutine sends a batch every `flushIntervalMs` (default 1000) or at
`batchSize` spans (default 512), with a `timeoutMs` deadline (default 5000);
429, 502, 503, 504 and a failed connection are retried twice with backoff and
`Retry-After`, anything else drops the batch. Shutdown sends what is queued for up
to two seconds. The counters: `hoardmq_otel_spans_started_total`,
`hoardmq_otel_spans_exported_total`, `hoardmq_otel_spans_dropped_total{reason}`
(`queue_full`, `export_failed`, `rejected` by the collector, `shutdown`),
`hoardmq_otel_exports_total{outcome}` (`ok`, `retried`, `failed`) and
`hoardmq_otel_queue_depth`. Disabling the module through the admin API
(`modules:write`) stops sampling at once. With the module unconfigured the publish
path does no tracing work and allocates nothing for it. These spans are separate
from trace recordings (above), which keep one client's packets for the admin API.

### Admin API

`admin.address` (empty by default: nothing is bound) starts the admin REST
API on its own listener. Every request, whatever its path, is authenticated
before it is routed; one that no identity matches is answered 401 with
nothing about the resource, and audited as `admin.auth.failure` in the
droppable class the MQTT `auth.failure` uses.

```json
"admin": {
  "address": "127.0.0.1:9091",
  "roles": [
    { "name": "viewer", "scopes": ["read"] },
    { "name": "operator", "scopes": ["read", "clients:write", "retained:write", "cluster:drain"] }
  ],
  "tokens": [
    { "name": "ops", "tokenFile": "/etc/hoardmq/admin/ops.token", "scopes": ["admin"] },
    { "name": "dashboard", "tokenFile": "/etc/hoardmq/admin/dash.token", "roles": ["viewer"] }
  ],
  "tls": { "certFile": "admin.pem", "keyFile": "admin-key.pem", "clientCAFile": "ops-ca.pem" },
  "certificates": [ { "commonName": "alice", "scopes": ["clients:read", "clients:write"] } ],
  "oidc": {
    "issuer": "https://example.okta.com",
    "clientIds": ["0oa1b2c3d4"],
    "roleMappings": [ { "group": "hoardmq-operators", "roles": ["operator"] } ]
  },
  "maxBodyBytes": 1048576
}
```

Identities are bearer tokens, client certificates, API keys (below) and ID
tokens from an OpenID Connect provider (below). A token is the trimmed
content of its `tokenFile`, at least 16 bytes, sent as `Authorization: Bearer
<token>`; the document names the file, never the token, and the broker keeps
only its SHA-256 digest. A client certificate counts only when
`tls.clientCAFile` verified it at the handshake (a certificate another CA
signed is refused there); its common name, looked up in `certificates`, is
the identity, and a verified certificate with no entry is refused rather than
passed over for a token. A non-loopback address requires `tls`, so a token
never crosses a network in clear text; without a client CA no certificate is
asked for. `check` refuses an admin listener with no token, certificate or
`oidc` identity, certificate identities without a client CA, a token file
that cannot be read or is shorter than 16 bytes, two identities with one
token, and an address that collides with a client listener,
`observability.address` or `cluster.listen`.

Scopes: `clients:read`, `clients:write`, `retained:read`, `retained:write`,
`cluster:read`, `cluster:drain`, `config:read`, `config:write`,
`traces:read`, `traces:write`, `modules:read`, `modules:write`,
`audit:read`, `apikeys:read`, `apikeys:write`, `credentials:read`,
`credentials:write`, `bans:read`, `bans:write`, `limits:read`,
`limits:write`, and two shorthands, `read`
(every `:read` scope) and `admin` (every scope). Each route needs one; a
principal without it gets 403 naming the scope. Every route that is not a GET
is audited with the identity (`token:<name>`, `cert:<common name>`,
`apikey:<id>` or `oidc:<user>`), the action, the target, the outcome (`ok`,
`denied`, `not-found`, `conflict`, `bad-request`, `too-large`, `error`) and
the request identifier, which the response also carries as `X-Request-Id`; a
refused request of any method is audited too, and a 401's record carries the
refusal's reason when an identity decided it (`apikey: revoked`, `oidc: token
expired`, `oidc: no role mapped`, `certificate: no identity`), never the
credential. Allowed reads are not.

Roles name scope sets once: a token or certificate may hold `roles` beside or
instead of `scopes`, and holds the union; an OIDC identity holds the roles its
groups map to. Every identity holds at least one scope: `check` refuses a
token or certificate with neither, a token, certificate or mapping that names
an undefined role, a role without scopes and two roles with one name. Roles
act on the default namespace, the only one until namespaces are activated.

The routes, under `/api/v1` (the served OpenAPI 3.1 document,
`GET /api/v1/openapi.json`, is generated from the same table that
dispatches, and describes every schema):

| Method and path | Scope | What it does |
|---|---|---|
| `GET /clients?search=&node=&after=&limit=` | clients:read | every client the cluster holds, by client identifier, paged, each naming the node that holds it; `node` keeps one node's |
| `GET /clients/{id}` | clients:read | the session, its connection and its lease |
| `GET /clients/{id}/subscriptions` | clients:read | the client's subscriptions |
| `POST /clients/{id}/disconnect` | clients:write | close the connection with 0x98; the session stays |
| `DELETE /clients/{id}/session` | clients:write | end the session (below) |
| `GET /subscriptions?topic=` | clients:read | the subscriptions here a topic reaches |
| `GET /retained?filter=&after=&limit=` | retained:read | retained messages across the cluster |
| `DELETE /retained?topic=` | retained:write | remove one retained message |
| `GET /cluster` | cluster:read | readiness and reason, consensus view, members, feature level, shard map, drain, sealed copies |
| `GET /cluster/shards` | cluster:read | every shard's leader and followers |
| `GET /cluster/leases/{id}` | cluster:read | a client's lease in this node's table |
| `GET`, `POST /cluster/drain` | cluster:read, cluster:drain | the drain's status; start it |
| `GET /config` | config:read | the running document, secrets redacted |
| `POST /config/validate` | config:write | validate a document as `check` does |
| `POST /config/reload` | config:write | re-read this node's file and apply it where every change is reloadable (see "Reloading the configuration") |
| `GET`, `POST`, `DELETE /traces/{id}` | traces:read, traces:write | fetch, start, stop a client's packet recording (not the OpenTelemetry spans) |
| `GET /modules`, `POST /modules/{id}/enable`, `/disable` | modules:read, modules:write | list and toggle modules |
| `GET /audit?limit=` | audit:read | the newest audit events |
| `GET /whoami` | any identity | the identity, its roles, scopes, groups and expiry |
| `GET /apikeys` | apikeys:read | the cluster's API keys, never a key |
| `POST /apikeys` | apikeys:write | create a key (shown once) |
| `DELETE /apikeys/{id}` | apikeys:write | revoke a key; 200 once every live node has applied it (below) |
| `GET /credentials?after=&limit=` | credentials:read | the in-broker database's client credentials: form, attributes, version; never a password or hash |
| `GET /credentials/{username}` | credentials:read | one credential |
| `POST /credentials` | credentials:write | add one: `username`, and `password` (with `scram`) or `passwordHash`, and `attributes`; 409 when it exists |
| `PUT /credentials/{username}` | credentials:write | change its password, its attributes (replaced whole) or both; `ifVersion` makes it conditional |
| `DELETE /credentials/{username}?ifVersion=` | credentials:write | remove it |
| `GET /bans?after=&limit=` | bans:read | the ban list, paged by key |
| `POST /bans` | bans:write | add one: `kind`, `value`, `reason`, and `expiresInSeconds` or `expiresAt`; 409 when it exists |
| `DELETE /bans/{key}?ifVersion=` | bans:write | remove one, by its key `<kind>:<value>`, URL-escaped |
| `GET /limits` | limits:read | the configured limits, the administered record and the limits in force |
| `PUT /limits` | limits:write | replace the administered record (see "Overload and bans") |
| `GET /overload` | cluster:read | this node's stress level, indicators and limits, and each peer's level |

A client action called on any node acts where the client is held. A node
that holds no session for the client, and whose lease table names another
node, sends the call there over the cluster transport as the caller's
identity, and answers with that node's answer and its name in the
`X-Hoardmq-Node` header. That node checks the caller's scope again and runs
the call as it would its own; both nodes audit it as the caller
(`forwardedTo`, `forwardedFrom`, one request identifier). A call is forwarded
once: one that arrives forwarded is never sent on. When it cannot be
forwarded (that node is unreachable, runs a build that takes no forwarded
calls, or has no admin API of its own), it is answered 409 with that node as
`owner` and the reason in the detail, and nothing runs there; 502 when that
node ran it but its answer is larger than a cluster frame; 504 when it gave
no answer within `cluster.replication.ackTimeoutMs` plus 5 s, so the call may
have run; 503 when that node is already running 16 forwarded calls. Each
outcome is counted (`hoardmq_admin_forwards_total{outcome}`: answered,
unreachable, unsupported, admin-off, too-large, timeout, busy). Only the
client routes are forwarded: inspect, subscriptions, disconnect, invalidate
and traces. The client list reads the lease table every node holds, so a
search on any node finds a client wherever it is; a client held on the
answering node carries its session summary (`session`). One search at a time
walks the table, holding its read lock only while it keeps one page, about
2 ms at 100,000 clients. A client identifier goes in its path segment
URL-escaped (`/` as `%2F`). Server-defined behaviour: a disconnect closes
the connection with `0x98 Administrative action` and the Will is published
as for any server-initiated close; an invalidation closes the connection,
discards the session and its journal records (replicated as a delete),
releases its lease, and publishes its Will, since the session has ended. A
session held here whose lease has moved to another node is not discarded:
that copy is on its way to its new owner and may be the one it fetches, and
the call goes to the lease's owner. One for which this node has seen a later
grant is refused with 409. An
invalidation and a retained delete answer once their records are durable
here and on the live replicas (`"durable": true`), or after
`cluster.replication.ackTimeoutMs` with `"durable": false`. A retained delete
whose record this node's journal failed to take answers `"durable": false` at
once, and a retained delete delivers nothing to subscribers. The configuration
view replaces every member whose name contains `password`, `secret`, `token` or
`key` with `[redacted]`, module configuration included. `POST /config/validate` needs
`config:write` because it reads the files the posted document names, and it
changes nothing. `POST /config/reload` answers 200 whether or not it applied
the file, with `applied`, `restartRequired`, the changed pointers and the
problems; `hoardmq config reload` exits 1 when nothing was applied. No API call walks every session under one lock: a client
list takes one session shard's lock at a time.

The credential routes act on the cluster's database, not this node's: a write
answers once it has committed in the consensus log and applied on the node
that served it (503 when the cluster has no reachable leader; a write that
timed out may still have landed, so a retried create can answer 409). An
update or a delete with `ifVersion` is refused with 409 unless the credential
is at that version, and an update without one is made at the version it read.
Every write is audited as `credential.create`, `credential.update` or
`credential.delete` with the username, the stored form and what changed.

The ban and limits routes act on the cluster's list and record the same way
(503 without a reachable leader). A ban's key is `<kind>:<value>`
(`cidr:10.0.0.0/8` is deleted as `/bans/cidr:10.0.0.0%2F8`); an invalid ban
or limit is refused with 400 naming the problem, and a `namespace` other
than `default` with 400. Writes are audited as `ban.create`, `ban.delete` and
`limits.update`.

### Single sign-on (OIDC)

`admin.oidc` accepts ID tokens an OpenID Connect provider issued as admin
identities: any provider that publishes a discovery document, with nothing
specific to one (Okta is the first one it is aimed at). `hoardmq login` obtains one; the CLI and any other
client present it as `Authorization: Bearer <ID token>`. The admin API keeps no
session and serves no login page: every request carries the token and every
request is verified. Single sign-on is OIDC only; SAML is not supported.

| Member | Meaning |
|---|---|
| `issuer` | the provider's issuer, exactly as its tokens carry it (`https://example.okta.com`, `https://example.okta.com/oauth2/default`); https, or http on a loopback host |
| `clientIds` | the client identifiers whose tokens are accepted: a token's `aud` must name one (and its `azp`, when present) |
| `usernameClaim` | the claim naming the identity, audited as `oidc:<value>`; default `sub` (Okta's is the immutable user id; `email` is not guaranteed unique) |
| `groupsClaim` | the claim listing the identity's groups; default `groups` |
| `roleMappings` | `{group, roles}`: the roles a group's members hold. An identity no mapping names is refused |
| `caFile` | PEM certificates for the provider's TLS instead of the system roots |
| `clockSkewSeconds` | allowed disagreement on `exp`, `iat` and `nbf`, 0 to 300; default 60 |
| `maxTokenLifetimeSeconds` | the longest `exp` − `iat` accepted whatever the provider issues, 60 to 604800; default 86400 |

A token is claimed by OIDC when it is a signed JWT whose `iss` is the
configured issuer; anything else is left to the other identities. It is then
verified against the issuer's published keys: the header's algorithm is one of
RS256, RS384, RS512 (RSA keys of at least 2048 bits), ES256, ES384, ES512 or
EdDSA and fits the key its `kid` names; `none`, HMAC algorithms, critical
header extensions and access tokens (`typ: at+jwt`) are refused; the
signature, the issuer, the audience, `exp` (required), `iat` (required), `nbf`
and the lifetime cap are checked with the skew. A token is accepted until its
`exp` plus the skew; the provider sets the lifetime (Okta: 60 minutes, not
configurable) and there is no refresh — log in again.

The provider is first contacted by the first request carrying one of its
tokens, never by `check` or at start. Its discovery document must name the
configured issuer exactly; its `jwks_uri` is read from there. The key set is
fetched again when a token names a key it lacks (the provider has rotated) and
when it is an hour old, at most once a minute either way, so tokens naming
unknown keys cannot drive requests at the provider; concurrent requests wait
for one fetch (at most ten seconds). A fetch that fails keeps the keys already
held. While the provider has never answered, its tokens are refused and every
other identity works.

`hoardmq login -issuer URL -client-id ID [-listen 127.0.0.1:8250] [-scope
"openid profile email groups"] [-token-file PATH] [-no-browser] [-timeout 5m]`
runs the authorization-code flow with PKCE (S256) and a loopback redirect: it
prints the provider's login URL (and opens it in a browser unless
`-no-browser`), receives the redirect on `http://<listen>/callback`, checks its
`state`, exchanges the code, checks the ID token's issuer, audience and
`nonce`, and writes the token to `-token-file` (mode 0600) or stdout; a login
that fails writes nothing. Register the redirect URI with the provider as a
native (public, PKCE, no secret) application: Okta matches it exactly, port
included, so register `http://127.0.0.1:8250/callback` and keep `-listen`
there. On Okta's org authorization server the groups claim comes from the
application's Groups claim filter and the `groups` scope; on a custom
authorization server that scope does not exist unless created (Okta answers
`invalid_scope`), so drop it with `-scope "openid profile email"` when the claim
is configured to be included always. On a host without a browser, forward the
port (`ssh -L 8250:127.0.0.1:8250`). Then:

```sh
hoardmq login -issuer https://example.okta.com -client-id 0oa1b2c3 -token-file ~/.hoardmq/id-token
hoardmq whoami -token-file ~/.hoardmq/id-token
```

`hoardmq whoami` prints the identity a credential authenticates as: its
name, credential kind, roles, scopes, groups and expiry.

### API keys

An API key is a scoped, revocable credential made through the API, for
automation: `hmq_`, a 12-character key identifier, `_` and a 43-character
secret, sent as `Authorization: Bearer <key>`.

```sh
hoardmq apikeys create -name ci-deploys -scopes clients:read,cluster:read -expires 720h
hoardmq apikeys list
hoardmq apikeys revoke 3f9c0a1b2d4e
```

`POST /api/v1/apikeys` (`apikeys:write`) takes a `name`, `scopes` and/or
`roles` (roles are expanded to scopes at creation: a later change to a role
does not change the key) and `expiresInSeconds` (absent: no expiry; else at
least 60). The answer carries the key once; the cluster stores only its SHA-256
digest, and no log, audit record, list or document holds it. A key holds no
scope its creator does not (403, audited), and an API key cannot create keys,
since one it made would outlive its own revocation. A key does not depend on
its creator afterwards: it outlives an OIDC creator's token. `GET
/api/v1/apikeys` (`apikeys:read`) lists every key with its creator, expiry and
revocation, never the key or its digest. An expired or revoked key is refused
and stays listed.

Keys are the cluster's: a key is a record in the replicated `admin.apikeys`
collection, so one created on any node authenticates on every node once that
node has applied it. `DELETE /api/v1/apikeys/{id}` writes the revocation and
answers 200 only once every node the log calls alive has applied it: from
then on no node takes the key. A node that has not confirmed in time is named
in a 503; the revocation is written, the named node refuses the key once it
applies it or the log declares it dead, and revoking again confirms it. A
node the log has declared dead is not waited for.

A node authenticates API keys only while it is caught up: it has renewed its
lease record with the leader within half the lease ttl (15 s by default), and
it has applied the log through that renewal. So a node cut off from the
leader, or started and not yet renewed (the first renewal is tried at start
and retried until a leader answers), refuses every API key (401,
reason `apikey: this node is not caught up with the cluster`) while tokens,
certificates and OIDC still work. A build that kept keys per node wrote them
to `<node.dataDir>/admin/apikeys.json`; this build reads that file at start
(one that does not parse refuses the start), puts each key into the
collection where no key holds its identifier, revocations included, and
writes the file no more. Until every node runs this build, a node of the
older build keeps its own keys and takes no key of the cluster's, and a
newer node takes none while the leader runs the older build.

### Cluster transport

Nodes talk over two authenticated, framed connections per peer pair, both
opened by the node whose name sorts lower and accepted by the other. The
**control** connection carries the liveness and coordination classes —
heartbeats, lease renewals and notifications, takeover requests, drain and
redirect signals, and raft messages (appends bounded to a fixed size) — and
the **data** connection the bulk classes: replication, forwarded publishes,
route updates, takeover acknowledgements, raft snapshots, and admin calls
forwarded to the node that holds their client, with their answers. The data
connection is the ordered stream the publish-order guarantee rests on:
forwarded publishes from one node to a peer arrive in the order they were
admitted, whichever goroutines sent them. On every connection the reader
only decodes and hands each frame to its class's own queue and drainer, so a
handler that takes long delays its own class and nothing else; a node is
never declared silent because its peer's handlers were busy. `cluster.listen` binds the peer
listener; `cluster.peers` names the other nodes; `node.name` must be set and
`cluster.id` must agree, or the peer is refused. `cluster.auth.mode` is
`mtls` (each node presents a certificate issued by the cluster CA in
`auth.tls.caFile`, and the certificate's common name must be the node's
name) or `secret` (both sides prove a shared secret from `auth.secretFile`
with an HMAC over fresh nonces; the connection is authenticated but not
encrypted, so use mutual TLS anywhere the network is not trusted). The
framing is versioned; peers negotiate the highest version both support, so
a rolling upgrade to a newer framing keeps the cluster talking. Framing
version 2 added an urgent lane beside the ordered stream (heartbeats and
lease renewals written and flushed ahead of queued data); version 3 added
the second connection: a version-2 peer is served on one connection carrying
every class, with the same per-class dispatch on the receiver, until it is
upgraded. Version 4 added the forwarded admin calls; a node on a link below
it answers an admin call for that peer's client with 409 naming the peer. Heartbeats every `cluster.heartbeatMs` on each connection carry a
stress level; `cluster.heartbeatMisses` silent intervals on the control
connection mark a peer down (the data connection allows ten times that,
since a slow data reader is back-pressure, not silence), and the dialing
side reconnects with backoff for as long as the node runs. A data connection
that resets is redialed without the peer going down; the forwards and
replication frames it carried are failed to their senders and the peer's
replicas re-synced. A node with no peers configured is a valid one-member
cluster.

### Consensus core

Every node runs a consensus core, a one-member group when it has no peers
(there is no single-node mode). The core is `go.etcd.io/raft/v3` behind
the project's own interface: propose a command and get its result once
applied, an apply callback in log order, snapshots in a format the project
owns, membership changes and a leader hint. The replicated state machine
holds the membership (names, ids, addresses, roles), a cluster-wide
configuration set and a monotone epoch; the lease table and the shard map
join it in later work. Its log is `node.dataDir/logs/raft`, on the same
write-ahead log engine as the session state; hard state and entries are
written and synced before any message leaves the node, which the tests
show with an instrumented store.

A node with a log resumes from it. A node with an empty log and configured
peers asks them to admit it; the leader adds it as a learner, it catches up,
and the leader promotes it to voter once its log is within
`cluster.raft.promoteLag` entries of the commit index. If no peer answers
within `cluster.raft.joinTimeoutMs`, only the designated node bootstraps a
new group: the lowest-named node among itself and its peers, or the one
with `cluster.raft.bootstrap: true`. A member that comes back with an empty
log (a wiped volume) is removed and re-admitted as a learner rather than
starting a second group. A node whose window passes after an answer has named
a leader keeps asking to be admitted rather than bootstrapping, since a group
exists: while it waits it is not ready and admits no client,
`hoardmq_raft_join_wait_seconds` is the time it has waited (0 once it has
joined or bootstrapped), and a warning naming the wait repeats every 30
seconds. A snapshot is taken every
`cluster.raft.snapshotEntries` applied entries and the log is compacted
behind a `snapshotTrailing` window. A node that restarts, or installs a
snapshot, is **recovering** until its applied index reaches the commit index
the leader announces (or, for a node the leader has nothing to send, a few
heartbeats confirm it is current): the state-machine components apply the
replayed history silently and are told once, when recovery ends, to
reconcile against the final state — the replicator reconciles its copies
against the current shard map on one worker, the lease service starts the
timers for the offline leases it owns and seals local sessions whose lease
moved while the node was down (see "Leases and session takeover"). **A
recovering node admits no client**: a CONNECT is refused with Use Another
Server and the leader's address when it knows one, otherwise Server Unavailable
(`hoardmq_lease_ops_total{op="acquire",outcome="recovering"}`), because its
lease table is the one it had when it stopped and may name it owner of a
session the cluster has since given to another node. A single node is
its own only voter and elects itself as soon as it restarts, so there the
refusal lasts milliseconds. Acting on each replayed change would act on
states the node had already acted on before it stopped (WI 1291 records the
durable session that was lost this way). `hoardmq_raft_recoveries_total{kind}`
counts them; a snapshot arriving at or below the applied index is installed
and counted as `hoardmq_raft_snapshots_total{direction="in-stale"}`, never
refused. `cluster.discovery.mode` is `static`
(the peer list) or `dns`: with `discovery.dns` (`service`, `name`,
`replicas`, `port`) the peers are `<name>-<i>.<service>:<port>` for every
ordinal below `replicas`, this node's ordinal coming from `node.name`, as a
StatefulSet behind a headless Service provides; a name that does not
resolve yet is retried, never treated as absence. `/readyz` reports ready
only when the core is a member that has caught up with the leader. Raft
messages arriving from the transport wait a bounded time for the core's
inbox when the consensus loop is behind — five raft ticks, one sixth of the
control connection's silence limit — and are then dropped and counted
(`hoardmq_raft_dropped_frames_total{type}`), so a stalled apply loop holds
the consensus class's drainer for at most that bound per message and never
leaves the control reader blocked behind a full class queue; a snapshot message is
never dropped while the core runs, because the leader holds the follower
until it hears that the snapshot arrived (WI 1345).

### Leases and session takeover

Every client identifier is owned by exactly one node, recorded as a lease
with an epoch in the consensus core. A client that connects to the node
that already owns it renews locally, without a round trip — the renewal
follows in the background, names the epoch the node saw, and applies only
if the lease is still that node's at that epoch, so one that arrives after
the lease has moved changes nothing
(`hoardmq_lease_ops_total{op="renew",outcome="stale"}`); a client that
connects elsewhere takes its session over: the lease's epoch rises, the
previous owner closes that client's connection with Session Taken Over and
hands the session state to the new owner, which resumes it before the
CONNACK. The new owner writes the session whole — a delete, then its state
— so a replica that still held the old copy replaces it rather than merging;
and a peer's earlier refusal of a record for that session (a stale-epoch
write from the old owner) holds barriers only until that whole write is
sent. A client that comes back with Clean Start to a node holding no session
for it, while its lease exists, is handed nothing, so that node writes the
session's delete at its new token before the new session, persistent or not:
no replica keeps the old subscriptions or queue under the new session. Lease
operations from one node are batched into one proposal, so a reconnect storm
costs log entries rather than round trips.

**The epoch is a fencing token.** It is the log index of the command that
last changed the lease's owner, so it is the same on every node and never
goes backwards — a lease released and acquired again is granted above
anything it had before, where a counter would restart at one. (Operations
batched into one command share its index; a command carries one node's
operations, and one that would grant a client to two nodes is refused at
apply.) What matters is who compares it with what. Each node keeps the
highest token it has **seen** for each client — from its lease table, from
the result of its own acquire, from a replication frame or a takeover
request — because the table's view lags the grant: a node is told its
acquire succeeded before its own table has applied it. A follower refuses a
session record whose epoch is below the highest it has seen, whatever its
table says (`hoardmq_lease_stale_token_total{where="replication"}`). A
previous owner asked for a session by several nodes hands it to the one
holding the highest token and to no other; a requester below the highest
token seen is answered *superseded*, the session is not given up on its
account, and its CONNECT is refused (`where="takeover"`, `where="acquire"`).

A node below the cluster's feature level neither adopts a dead owner's
leases nor raises the level: it serves nobody, so what it adopted nobody
could reach. A node that replayed a reassignment during recovery — where no
adoption is queued, because replay acts on nothing — queues one for every
offline lease it owns and holds no session for when recovery ends
(`hoardmq_lease_adoptions_recovered_total`).

**A fetch nobody answered is not a session held nowhere.** A new owner that
cannot get a session from its previous owner fetches a replica, and the
fetch says whether every node it asked replied. When one did not and no copy
came back, the client's CONNECT is refused with `0x89 Server Busy`
(`hoardmq_lease_ops_total{op="acquire",outcome="fetch-unanswered"}`) rather
than served a fresh session: serving it would also drop this node's own
replica copy, which may be the only one. The adoption stays for the client's
retry, and the adoption worker fetches again on its own — after 500 ms,
doubling to 8 s, six times — so a client that never returns does not leave
the lost node's routes hidden. When those retries are spent the adoption is
given up and logged with the lease's evidence
(`hoardmq_lease_adoptions_abandoned_total`); the next CONNECT is then served
fresh. If the give-up's own fetch found a copy that a former holder kept, and
held back from it only because a replica source did not answer, the worker
installs that copy instead, as an adoption's import does, and the client is
served it (`hoardmq_lease_adoptions_kept_copy_total`, `adopt_gave_up_kept_copy`,
logged at Warn with the copy's holder). It loses at most what was written after
it, where a fresh session loses everything. If the silent source held a newer
copy, the client can be given again a QoS 2 message it already completed; that
needs the source up and silent through the whole retry window. Only a fetch that waited on nothing alive by the log, such as a replica
that did not answer, counts towards those six; a wait on a previous owner or a
down replica holder the log says is alive has no such end (below). A fetch that every node answered with "not found" serves a fresh
session at once — with the exceptions below for nodes that could not be asked.

**A previous owner or replica holder that could not be asked is not an answer
until it is declared dead.** The previous owner is asked whenever the new owner
has a control connection to it, the one a takeover request travels on — so a
restarted owner is asked before its data connection is back, and so is one the
leader has declared dead, whose log may still hold the session (counted as
`pulls_control_only` when the data connection is not up). With no connection
to it, the new owner cannot ask it, and until the leader declares that owner
dead it may still hold the session: a partition that heals before the
declaration gives the session back. So a takeover or adoption that finds no
copy anywhere, with such an owner, is refused with `0x89 Server Busy` and the
adoption kept, as for a fetch nobody answered. Once the owner is declared
dead, the next fetch serves a fresh session, counted as below. A previous
owner that can be asked but does not answer is judged the same way while the
log says it is alive (`adopt_owner_silent`). The adoption worker never gives
such an adoption up on time: a previous owner that gave the session away keeps
it, sealed, for as long as nothing shows the session held elsewhere, so the
wait ends only when the owner answers, is declared dead, or the lease moves. A
copy found anywhere is served at once, whatever the owner's state. Once the
adoption is older than twice `cluster.leases.ttlMs` with the owner still alive
by the log, the leader, which declares any node it has not heard from for a
ttl, can reach that owner; so a CONNECT refused for the wait from then on is
answered `0x9C Use Another Server` naming the leader, whose CONNECT takes the
lease over and asks the owner itself (`0x89 Server Busy` on the leader, or
when no leader's address is known). The wait is logged once at Warn when it
passes that point, naming the node waited on, and counted
(`hoardmq_lease_adoption_waits_total{on="owner"|"silent-owner"|"holder"}`,
`adopt_waits_past_bound`, `adopt_waiting_now`), and so are the redirects
(`hoardmq_lease_ops_total{op="acquire",outcome="adoption-redirect"}`,
`adopt_redirected`). A client that cannot follow the redirect (MQTT 3.1.1),
or reaches only this node, is refused until the owner can be asked or is
declared dead, as a CONNECT for a live owner this node cannot reach always is.
Counted in the failover artifact as `takeover_owner_undeclared` (and
`…_served`) and `adopt_owner_undeclared`. A replica holder
of the session — one of the previous owner's replica targets under the current
shard map — that the fetch skipped because it was down is judged the same way:
while the leader has not declared it dead it may come back with its copy, so a
fetch that found nothing is refused and the adoption kept, with no time end:
the wait ends when the holder answers, a copy is found, the holder is declared
dead or the lease moves. It is judged ahead of a replica that did not answer,
whose retries would otherwise end the wait while the holder still has the copy,
and past twice `cluster.leases.ttlMs` a refused CONNECT is sent to the leader as
for an owner (`takeover_holder_undeclared`, `adopt_holder_undeclared`,
`{on="holder"}` above). With no leader, as when the holder's failure leaves the
survivors without a quorum, the client is refused until the holder returns or is
declared, and meanwhile the forget of any other node lost waits for this node's
adoptions to finish. A holder declared dead is not waited for. Once the shard map
no longer holds the previous owner, as after its declaration, the owner's targets
under that map are not the nodes it replicated to, and the copy it fed may be on
any member; so a fetch under such a map judges every member it could not reach
because it was down, the previous owner aside, as such a holder (counted in the
failover artifact's `replication_fetch` as `none_peer_down`).
A holder, or another node that may keep a copy, that is back on its control
connection while its data connection — the one a fetch travels on — is not up
yet is not down: a restarted node in its first moments, or one back from a
partition, can be declared dead by a new leader and still hold the session.
The fetch cannot ask it yet and reads unanswered, so the CONNECT is refused and
the adoption kept as for a fetch nobody answered, until its data connection is
up and the next fetch asks it (`hoardmq_replication_fetches_total{outcome="none-returning"}`).

A client refused that way may retry on another node, which takes the lease from
the node that refused it. That node never held the session, so it answers the
pull with no state — and names the owner it was still waiting on. It names it
from its unfinished adoption, or, when its adoption worker has already found the
lease gone, from a note kept for twice `cluster.leases.ttlMs`. The retry can
also arrive before the first node has refused, while its own takeover is still
waiting on the previous owner, or after its import was refused for the later
grant: a CONNECT that is not a clean start notes the owner it takes the lease
from as it proposes the acquire and again on the grant, so that node names it
too. The new owner,
finding no copy anywhere, judges the named owner as it judges its own previous
owner: not declared dead in its own table, so it refuses with `0x89 Server
Busy` and its adoption waits on that owner from then on — pulling it when it has
a link, otherwise until the declaration. The same holds when a retry's adoption
fetch pulls a node that names an owner. The name is an optional member of the
takeover answer: an older build ignores it, and a newer node given an older
answer judges as before, so with an older node on either side the takeover
behaves as the older one does. Counted as `takeover_answered_pending` (of which
`…_noted` from a note, `…_taking` from a takeover's note, and
`pending_owner_notes` held, both kinds) on the node that answers, and
`takeover_pending_owner` (and `…_served`) and `adopt_pending_owner` on the node
that takes the lease.

**A session found nowhere is served empty, counted and logged.** When every
node asked has answered that it holds no copy of a session the client's lease
says existed, the client is served a fresh session: Session Present false, its
subscriptions and queued messages gone. Refusing could not bring back state
that no longer exists anywhere, and would only repeat. Each such CONNECT is
counted in `hoardmq_sessions_rebuilt_empty_total{reason}` and logged at Warn
("session served empty although its lease says one existed") with the client,
the reason, the lease's last-known owner and the token it is now served at; a
takeover among them also counts `hoardmq_lease_takeovers_total{kind="empty"}`.
On a cluster that lost fewer nodes than it keeps copies the count should stay
at zero, so any count is worth reading. The reasons: `takeover` (taken over
from an owner that had nothing and named no owner it was waiting on, or that has
been declared dead, and no replica had a copy), `adoption` (a dead owner's
lease adopted here, and no copy anywhere),
`adoption-ended` (the lease is offline here and its adoption ended earlier
without a copy — or the session expired and the lease is not yet released,
the one way a count can include a session that ended by design),
`grant-unfetched` (the lease was granted here by an acquire whose CONNECT
never got a session here, and nothing recorded that it gave up), `raced`
(the lease became this node's while the CONNECT's own acquire was in flight),
and `no-expiry` (any of these for a lease that, when the CONNECT looked it up,
carried no session expiry: such a session ends with its connection, so this
one is no loss and is counted apart). A clean start and a client's first
connect are never counted.

**A lease granted after its CONNECT gave up is fetched before it is served.**
A CONNECT that runs out of `cluster.leases.takeoverTimeoutMs` while its
acquire is queued or proposed is refused, but the acquire can still be
granted — after a leader crash, when the proposal waits for an election —
moving the lease here with nobody fetching the session. The refused CONNECT
leaves a note, and the client's next CONNECT here treats the lease as
adopted: it asks the previous owner, then the replicas, before it is served,
with the same rules as any adoption (an unanswered fetch refuses with
`0x89 Server Busy` and is fetched again on the retry; "not found" everywhere
serves a fresh session, counted as `adoption`). A session this node already
holds is never fetched over. A refused first CONNECT, which found no lease,
leaves a mark instead: if its acquire then creates the lease here and the
client's next CONNECT here finds it still at that grant's token, the client
never had a session, so nothing is fetched and nothing is counted served
empty. If the acquire instead took the lease from another node — the client
may have connected there meanwhile and been given a session — the client's
next CONNECT here fetches from that node, then its replicas, as above, and a
takeover pull that reaches this node first is told that node's name, so the
taker asks it too. A lease that moved since is counted `grant-unfetched` as
before. Counted in the failover artifact as `acquires_abandoned` (first
CONNECTs included), `grants_unfetched_adopted`, `first_grants_unfetched` and
`first_grants_taken_adopted`.

**A session handed to a takeover is sealed, not deleted, until the new owner
confirms it.** The previous owner's pass takes the session out of reach — its
connection closed with Session Taken Over, its routes removed, so a publish
forwarded to it is reported undelivered and a CONNECT here is refused with
`0x89 Server Busy` — but writes nothing to its log, so its log and every replica
copy of the session stay as they were. The new owner confirms once it holds the
session at its grant and its records are acknowledged by its replicas; only then
does the previous owner drop its sealed copy and log the session's delete. So an
answer that is lost or late, or a previous owner that dies after answering,
leaves the replicas the new owner falls back to; a delete written at the
hand-over could reach a replica ahead of the grant and remove the only copy. The
node's own checkpoints carry its sealed copies, so a restart finds them in its log
until their deletes are written; a resync or a rebalance never sends them. A requester
asking again is answered from the sealed copy. If the lease comes back to the
previous owner, the sealed copy is what it resumes, used only when no other node
has the session. A seal nothing confirms is never dropped because time has passed:
the new owner may be alive and still unable to fetch it, and the sealed copy can be
the only one. Every twice `cluster.leases.ttlMs` the previous owner asks the lease
owner's replica targets, with the replica fetch, whether one holds the session at
the owner's grant (its own replica copy answers when it is one of them); it drops
the seal, with its delete, when one does or when the lease no longer exists, and
keeps it while the lease is back on that node. With one configured copy the owner
has no targets, so an unconfirmed seal is kept until it is confirmed, used, or its
lease ends. At most eight such checks, together with the replica copies' checks
below, are in flight on a node. The
confirmation is a message on the takeover-request class, marked by its first
byte, which an older build ignores; with an older node on either side the
hand-over behaves as the older one does, and nothing is lost. The new owner also
keeps an answer that arrives after it stopped waiting, while it has seen no
higher grant for the client — for the client's retry, for its adoption worker,
or for the node that takes the lease next — never putting it over a session it
already holds, and confirms it once imported. The deferred reclaim of a session
whose lease has left, when no pull came for it, seals it the same way; a session
whose lease no longer exists is evicted. A node's recovery seals the same way every
session its log holds whose lease another node took while it was down: that node may
not have fetched it yet, and the restarted node's log can hold the only copy (a
drained node whose receiver's pull came after it stopped, while its replica target
was down), so the copy waits for the new owner's pull and confirmation, or the
check above. Counted in the failover artifact: `sealed` (of which `sealed_by_reclaim` and `sealed_by_recovery`), `sealed_now`,
`seals_confirmed`, `seals_expired` (ended at a check: `seals_held` on a target holding the session,
`seals_lease_gone` on the lease's end; `hoardmq_lease_seal_ends_total{end}`), `seal_checks_held` and
`seal_checks_not_held` (`hoardmq_lease_seal_checks_total{outcome}`),
`reclaimed_on_confirm`, and the confirmations sent (of which
`confirms_sent_holders` went to the other live members the new owner does not
replicate to, besides the previous owner and a moved session's old targets; see
`owner-unwritten` below), received, and dropped as
never held, never durable or not delivered, and those acted on (`confirms_acted`) with their time from receipt to
the drop or eviction (`confirm_act_ms_total`, `confirm_act_ms_max`). A node acts on up to 64 confirmations at
once: each waits for the journal fsync covering its delete, and a node loss sends the adopter them in a burst,
so with fewer the moves they end would keep refusing behind the queue;
`hoardmq_lease_takeovers_total{kind="given"}` counts takeovers served from the
node's own sealed copy.

**A fetched session is installed only while no later grant for its client has
been seen, and a pass never answers across an install.** An adopted session's
state, or the state a CONNECT's takeover brought back, is installed only if
this node has seen no grant for the client above the one the state was fetched
for. The check and the start of the install are one step under the session's
lock, and a takeover pass that finds an install in progress waits for it and
seals what it installed. Without that, a pass could answer "no state" while the
adopter was still installing the session, whose write then dropped the replica
copy the new owner's fallback needed. A refused install writes nothing, so that
copy stays. A CONNECT whose state is refused this way is answered `0x89 Server
Busy`. Counted in the failover artifact as `adopt_import_refused` and
`connect_import_refused`, and, per node, `seal_waits_import` (Seals that waited
for an install) and `import_waits_import`. A trace stays beside them:
`adopt_import_after_empty_answer` (an install that followed an empty answer for
the same client, which should read zero) with a capped `import_race_sample`,
`reclaimed_sample` (offline sessions a confirmation evicted), and the worker's
widest gap from its lease check to its install and longest install.

**A session installed from a hand-over is not given again a message it
arrived holding.** A publish on a session's owner is routed into the session
there and forwarded to every other node with a subscriber for its topic. If
the session is then handed to one of those nodes, the answer carrying it is
handled on arrival while the forward waits in the routing plane's queue for
that peer, so the forward can be applied after the session is installed. The
installed session remembers the messages its state held — by the node each was
published on, that node's sequence for it and its arrival time — and a routed
copy of one of them is skipped: the session keeps the one it has, and the
forward still counts as delivered, since it was. Counted in
`hoardmq_deliveries_duplicate_total{namespace}`. A session recovered from the
node's own log remembers nothing, and a retained message sent again on a
SUBSCRIBE is not checked. A message the session was sent on the old owner and
completed there is no longer in its state, so the old owner, when it forwards,
remembers every message it sends each session for 10 s (at most the last 64)
and hands those over with the session; the new owner skips a routed copy of
them the same way. A forward later than that, or a session sent more than 64
messages in the window, can still be given one again; so can a session taken
over from a replica after its owner crashed, since the list is not replicated.

`hoardmq_lease_proposals_total{op}` counts what this node asks the log to
carry, by kind; `renew` is an acquire that names an epoch — a reconnect to
the same owner — and with `disconnected` is the traffic that changes no
ownership.

Leases are time-fenced. Each node renews all of its leases at once by
recording that it is alive every quarter of `cluster.leases.ttlMs`; a node
whose record has not been renewed for the ttl has lapsed.

Whether a node has lapsed is decided once, by the consensus leader, and
published in the log. The leader declares a node dead when it has itself
applied no alive record from that node for a ttl, measured on its own
monotonic time — not by comparing any clock with the time stamped in the
record, which another leader may have written on another clock and which a
step of the wall clock would misread. A follower watches the same records, so
a new leader judges from what it saw itself; a node that has been watching
for less than a ttl (after a restart, a recovery or a snapshot restore)
declares nobody. The declaration moves the node's leases to the leader, is
made for a node that owns none as well, and is cleared by that node's next
alive record. Every other reader — the connect path, the replica rebalance,
the shard map, a drain choosing its target, readiness's topology — reads
that fact and no clock: a node is alive when it has an alive record and is
not declared dead. `hoardmq_lease_nodes_declared_dead` is how many are
declared now and `hoardmq_lease_declarations_total` how many declarations
this node has proposed. A node's own fence is measured on its monotonic time
too, so a wall-clock step does not disconnect its clients. The times in the
table (alive stamps, disconnect and expiry times) remain the leader's wall
clock and are not compared with another node's for liveness; after a
backward step they stay ahead until real time passes them.

A renewal is its
own proposal on the transport's urgent lane, never part of the batched
lease traffic, bounded by the smaller of the takeover timeout and the ttl,
with two allowed in flight, so several attempts fit in every ttl and a
storm on the data path cannot starve the signal; three consecutive failures
log a warning before the fence. A newly elected leader waits one ttl before
judging the record of any owner it can still reach, so every live node gets
a full ttl to renew with it after an election; an owner whose link is down
is adopted as soon as its record lapses. The fence has one exception, the
mirror of that grace: a node whose record has lapsed does not fence while
it can reach a majority of its peers and the group has no leader, or has
had one for less than a ttl — with no leader nothing can be adopted or
taken over, so no second owner can appear, and fencing would only
disconnect every client for the length of an election; the deferral is
counted (`hoardmq_lease_fence_deferred_total`). A node that
cannot renew (it has lost quorum) disconnects every client with Use
Another Server, or Server Busy when it knows no other node, and refuses new
connections with Server Unavailable, or Use Another Server and a Server
Reference when the leader's `cluster.advertise` address is known, until it
renews again. The majority side does not take a client over from an
unreachable owner that has not been declared dead, so that a client
identifier is not given a second live session across a partition: an acquire
lists the
members its node has no live link to, and the lease table refuses it where it
applies — against whichever node owns the lease at that point in the log — if
the owner is listed and alive
(`hoardmq_lease_ops_total{op="acquire",outcome="owner-unreachable"}`). A node
whose own lease table already shows the owner alive, and which has no live
link to it, gives that refusal itself and proposes nothing
(`outcome="owner-unreachable-local"`): after a node crash every one of its
clients retries until the declaration, and each retry would otherwise be a
consensus proposal made to be refused. Only the log grants a lease; a node
whose table is behind the declaration refuses a client once more than the log
would have. When
the leader declares an owner dead, it adopts its leases: an offline session's Will is published
after its Will Delay and its Session Expiry fires as if the owner were still
there. A member that comes back with a wiped volume is re-admitted (see
Consensus core); a client whose owner died reconnects anywhere and resumes
what the lease carries. The queued messages of a dead owner's sessions are
recovered from the followers (see "Data-plane replication").

A takeover asks the previous owner for the session **whenever it can be
reached**, whatever the verdict on it: rebuilding the session from a replica
while an owner that was wrongly judged goes on queueing into a session it no
longer owns loses what it queued. The verdict — whether the previous owner was
alive by the log when the acquire applied — decides whether to wait when a
reachable owner does not answer (until it is declared dead, for at most a
ttl), and whether an owner that could not be asked lets "not found" everywhere
serve a fresh session (only once it is declared dead; see above). An owner that
answers with no session is holding nothing back, and the replica is asked at
once.

The pull and that wait share the CONNECT's budget,
`cluster.leases.takeoverTimeoutMs`, and a pull the owner does not answer
spends all of it. The replica is then asked on a budget of its own,
`cluster.replication.ackTimeoutMs` — the bound every replica fetch already
has — so a CONNECT whose previous owner is silent completes within the two
together (7 s at the defaults), served from the replica when a copy answers
(`hoardmq_lease_takeovers_total{kind="replica"}`). An owner that has not
answered may still hand the session over late, so when no copy answers the
CONNECT is refused with Server Busy and the adoption kept, as for a fetch
nobody answered, rather than served a fresh session.

### Routing across nodes

Each node keeps a cluster route table: which nodes hold subscribers for
which topic filters. A node publishes an entry when a filter gains its
first local subscriber and a tombstone when it loses its last, each with a
version from the node's own counter (seeded from its start time, so a
restart never loses to its own old tombstones); entries go to every peer at
once, a peer that comes up receives the node's full set, and every
`cluster.routing.syncMs` the full set goes to everyone as anti-entropy.
Tables merge by higher version, tombstone winning a tie, so they converge
after a partition heals with unsubscribes honoured.

A full set is **authoritative about absence**: it carries a snapshot version
taken when it was composed, and the receiver withdraws any entry it holds for
that node which the set does not carry and whose version is below the
snapshot. Anything the sender publishes after composing the set has a higher
version and survives it. This is how a node that has restarted retires the
entries its previous incarnation published — it no longer knows they exist,
so it can never tombstone them one at a time, but it can say what it holds
now. For the same reason the full set is sent even when it is empty: "I hold
nothing" is a message, and a node that sent nothing at all would leave its
peers routing to it forever. Which is why a restarting node sends nothing at all,
no entry and no full set, until it has loaded the sessions its store holds:
its table is empty until then, and a set sent from it would withdraw at every
peer the routes of sessions its journal still holds, so that a publish to one of
them would be answered No Matching Subscribers and dropped (WI 2259). A down peer's entries are hidden, not deleted,
and a QoS 1 or 2 publish matching one is refused: that peer's subscriber
cannot receive it. The entries go when the membership removes the node, when
its own next full set retires them, or — for a node that does not come back —
when its sessions are **served** elsewhere. Owned elsewhere is not enough: a
reassignment moves every lease the moment it applies, and the adopter
imports each session afterwards, from a queue; until the import advertises
the session's routes nobody else does, and a publish would be acknowledged
with no subscriber. So a node puts a **served fact** in the lease log once its
change hooks have run up to the index a lost owner was emptied at (the lease
table records it, from the log, so it is the same number on every node) and it
has no adoption unfinished: its hooked index, and how far its own route updates
had gone. A node forgets a lost owner's entries once the table shows that owner
owning nothing, it is itself in that state, and every other member that owns
leases has a fact at or past the index **and** this node holds that member's
route updates up to the fact's mark. The last condition is what keeps the
replacing routes ahead of the forget: the fact travels through consensus and
the routes on the data connection, in either order. For it, every delta batch
a node sends carries the next of a per-node sequence (seeded from its start
time, like the entry versions), and every full set the last number it holds;
a skipped number is a gap (`hoardmq_route_update_gaps_total`), and nothing is
proved from that peer again until its next full set. A member that owns
nothing is not waited for. One that is down no longer holds the refusal by
being down: once its fact and its routes are here, its own hidden entries
refuse for it. A client of the lost node can take its lease from the adopter
before the adopter imported its session; the adopter's adoption then ends
without an import, so each side counts the session as unfinished: the taker
until the session is held there, after which it puts a newer fact in the log,
and the adopter until the taker's fact reaches the grant it took the lease at,
so the adopter's own fact follows it and every node waits for the taker's
routes as of the import. Two survivors can each hand sessions to the other, so
the taker's claim does not wait for its own hand-ons: once every session it
took that way is held or its wait has ended, it puts a **cover** in the log, the grant through which
they are held with its route mark, and a hand-on ends on the taker's cover or
its fact. The forget reads a member's routes up to the higher of the two marks.
Each wait ends at twice the lease ttl at the latest, counted in the lease
statistics (`transit`, with `handed_covered` for hand-ons a cover ended); a
cover never claims a grant at or above that of a take that ended that way.
Each node proposes its fact once per lost owner, whether it holds the owner's
entries hidden or never received them (a node restarted beside the lost owner:
see "An owner whose routes are unknown"), and again whenever sessions it
took that way are held there (`hoardmq_lease_proposals_total{op="served"}`,
covers included;
`hoardmq_route_served_proposals_total{outcome}` counts how each ended, and a
failed one is proposed again; `hoardmq_route_cover_proposals_total{outcome}`
does the same for covers). A node checks every 500 ms, and sooner when a
fact applies, its own last adoption or a take in transit ends, or a member's
routes arrive.
`hoardmq_route_owners_hidden{reason}` says why a hidden owner is still held
(`leases`; `adopting`, this node's own adoptions and sessions in transit; `unsynced`, a fact or routes
not yet here from a member it can reach, or its own hooks behind the index;
`unreachable`, the same from a member that is down; `unknown`) — a refusal that
will end from one that will not — and `hoardmq_route_owners_forgotten_total`
counts the owners forgotten this way. Across a rolling upgrade from a build
without the fact, neither build forgets on the other's word: the older build
reads adoption state from full sets that no longer carry it, and the newer
reads facts the older never proposes, so a node lost mid-roll keeps its
entries hidden until the roll ends. The
tombstones of a session that is *moving* to another node — a lease takeover
or a hand-off, not an unsubscribe — are held back for
`cluster.leases.takeoverTimeoutMs`, so the other nodes keep forwarding to
the previous owner while the new one's routes are still in flight; the
message finds no session there and the publish is refused rather than
accepted with nowhere to deliver it.
The same answer is given when another session on the node still
subscribes to the filter: the forward reaches the subscriber that stayed and
is still reported to the sender as failed (`reason="session-moving"`),
because the session in transit was exported without it. The mark lasts the
grace whatever happens to the filter's count in between — the moving
session's own subscription being removed is a fall in the count, not the end
of the move — and ends early only when the count rises again, the client
coming back. A shared-subscription delivery to a member that stayed is a
complete delivery and is not refused.

The move ends early, for the publishes that can show they reached the moved
session, once the new owner has confirmed it holds the session durably (the
sealed hand-over's confirmation) and this node holds the new owner's route
for the filter. A publish made here is accepted when it was forwarded to the
new owner after the confirmation arrived. A forward from the new owner is
accepted when its publish sequence is at or above the one the confirmation
reported, the first the new owner routed with the session in place.
A forward from any other node carries no such proof — that node routes by its
own table, which may not yet hold the new owner's route — so it is refused for
the whole grace, and the route stays advertised until the grace ends so such
a node keeps forwarding here rather than matching nothing.
`hoardmq_moves_resolved_total` counts the confirmations applied to a move, and
`hoardmq_move_refusals_lifted_total{path}` the publishes accepted this way
(`local`, `forward`). A move of a shared filter, and a move whose confirmation
never comes, last the grace.

A publish is routed locally as before and then forwarded once to each remote
node with a matching subscription, over the ordered stream to that peer in
the order the publishing connection produced it; the receiving node delivers
in arrival order, so the specification's per-publisher ordering holds across
nodes. Forwards are never forwarded again. A shared subscription group that
spans nodes is dispatched by the node holding the most of its members
(lowest name on ties): a publish that matches the group on any other node is
sent to that owner, which applies the configured strategy over the member
nodes and hands the message to the chosen node for one local member, so
the group receives each message once. Forwards between nodes are
acknowledged: each frame carries a per-peer sequence, the receiving node
applies the frames in order and answers once they are durable there (its own
sync and its own followers), and a QoS 1 or 2 publish is not acknowledged
until those answers arrive — a lost frame, a reset link or a receiver that
could not make the message durable fails the publish instead. A late answer
fails it too, and the message may by then have been delivered (see Sessions).
When a group's owner hands a QoS 1 or 2 message on to a third node, its answer
to the publishing node names that frame as given on, and the publish waits
until the owner relays whether the third node held it: the third node's own
answer to the owner, after its durability wait. A give the third node refused,
a third node lost before answering, no answer within
`cluster.replication.ackTimeoutMs`, or a reset of the link the relay would come
over refuses the publish. The owner counts relays as
`hoardmq_group_gives_relayed_total{outcome}` (`held`, `refused`). A node of an
older build on either side of the pair answers and waits as before.
An MQTT 5 acknowledgement of No Matching Subscribers means no node matched
the publish: a forward sent to a subscriber's node counts as a match. Retained publishes are stored by the topic's
shard leader (see "Data-plane replication"), not by the receiving node.

### Data-plane replication

Sessions and retained messages are assigned to `cluster.shards` virtual
shards by a hash of the namespaced client identifier or topic. A shard map
in the consensus core names each shard's leader and followers; the
consensus leader computes it from the members the lease table says are alive
(see "Leases and session takeover"), with `cluster.replicas` copies (default 2, bounded by the
live member count), and proposes a new map only when that basis changes.
A member a drain has marked leaving (below, "Drain and readiness") is left
out of the basis while the members left still give the copy count; when they
would not, every alive member stays in, so a drain never lowers the copies.
The map version is exported as `hoardmq_shardmap_version`, the basis's size
as `hoardmq_shardmap_basis_size` and the members marked leaving as
`hoardmq_shardmap_leaving`; a map change's audit record names them.

A node that stops being a session's replica target at a map change, while the
session's owner is alive, keeps its copy until the owner confirms the session
durable at its new targets: the owner sends the takeover confirmation it
already sends a previous owner, once the whole write the map change sent its
new targets is acknowledged. Until then the copy is counted
`hoardmq_replicas_kept_total{reason="moved-target"}`; on the confirmation it
goes, `hoardmq_replicas_dropped_total{reason="not-a-target-confirmed"}`. A
confirmation can be lost (its durability wait is given up after a ttl, and a
failed send is not retried), so a copy kept this way for twice
`cluster.leases.ttlMs` asks the owner's targets, as an `owner-unwritten` copy
does, and goes once one holds the session at the owner's grant
(`not-a-target-held`). No time alone ends the keep, because an owner that died
before it was declared would otherwise leave no copy at all. The rule holds
for a copy written under the owner's current grant on a node the owner does
not replicate to, whatever map it is judged against: a later map change that
moves the shard again, or the node's restart, keeps it the same way. So does a
copy on a shard the node no longer holds whose hand-over the shard's holders
refused, as a holder that already has the session does: it is judged by the
rule of its grant, this one or `owner-unwritten`'s, and checked until it goes. A
publish whose barrier starts before this node's rebalance has sent those whole
writes also waits on the old targets that are up, where its earlier records
went. Confirmations sent so count `hoardmq_lease_confirmations_moved_total`.

Bounded by the live member count, the copies can number fewer than
`cluster.replicas` says: with fewer live members than copies, each shard has
fewer holders, and an acknowledged publish rests on fewer copies than
configured. Two gauges, set at scrape, count the work held so on a node:
`hoardmq_replication_sessions_short`, the persistent sessions it owns with
fewer than `cluster.replicas - 1` targets, and
`hoardmq_replication_retained_shards_short`, the shards it leads with fewer
than `cluster.replicas - 1` followers. Both are counted against the current
map, so they read zero while the map's members number at least the copies,
and a node down but still named by the map is counted as a copy until the map
drops it.

A session lives where its client connects; its owner replicates every
record it journals (metadata, subscriptions, queued and in-flight entries)
to the shard's holders other than itself, up to the copy count, on the
ordered cluster stream. Each record carries the owner's lease epoch —
stamped when the record is written, from the epoch the lease service granted
this node at connect, not looked up when the record is fanned out: a node
journals for a session only while it owns that session's lease, so the
record's own epoch is the generation it was written under, where a lookup at
fan-out time can read a newer epoch belonging to a different owner. A
follower refuses a record whose epoch is behind its own lease table (or names
a different owner at the same epoch), answering rejected — a record carrying
no generation at all is judged by where it came from instead, applied from the
node the table names as owner and refused from any other — and a rejected
record fails the barrier covering it, as a lost frame does, because the
follower does not hold it. It refuses a record it cannot apply at all for the
same reason: one naming a session, or an entry, its copy does not hold says
the copy has diverged from the owner's, so the owner sends that session
whole — once per key per acknowledgement timeout — and the barrier passes once
the follower has acknowledged the repaired copy. The exception is the window between a gap the follower
reported and the resync that answers it, where a record that changes nothing
is expected and is acknowledged, the resync being what repairs the copy.
Followers persist replicas in a separate log under `node.dataDir/replica`,
keep them materialised in memory, and acknowledge after syncing; a refusal is
answered at once and acknowledges nothing, so the frames applied ahead of it
count as held only once the follower's acknowledgement after its sync
arrives. A failed replica fsync may have dropped every write not yet on the
disk, whichever peer's stream it came from, and a later fsync that succeeds
does not write them again: a follower whose batch did not sync, or saw any
fsync of its replica log fail before its own returned, acknowledges nothing
more on that stream and reports it to the owner as a gap, and the owner breaks
and resyncs the stream. The follower acknowledges again once the resync's
marker shows every frame it lost has failed at the owner, and what came since
has synced. The node keeps serving throughout. A QoS 1 or 2 PUBLISH is acknowledged only after the publisher's node
synced its own log **and** every follower its records went to acknowledged
everything sent to it so far. The barrier names the records it covers —
the publisher's session, every session the message was delivered into, and
a retained topic — and waits on their targets under the current shard map: a session's followers
with this node as owner, a retained topic's followers on its shard leader or
the leader a non-leader forwarded it to. A target that is down, one whose
stream was reset with frames in flight, and one that is up but silent for
`cluster.replication.ackTimeoutMs` all fail the publish with Implementation
Specific Error rather than acknowledge without the copies. A node that is
down but still named by the map therefore refuses the publishes whose
records it holds and no others, until the map drops it. The map follows the
declaration: the leader's map driver proposes a map without the node as soon
as the declaration applies, and otherwise checks the members every third of
`cluster.leases.ttlMs`. A record a peer refused fails every barrier naming its key while
the map names that peer, whether or not it is still the key's target, until
the key is rewritten whole or the stream resynced, and a barrier already
waiting then passes only once the peer has acknowledged the rewrite. A node the map has
dropped is waited on by nothing, and one it names again is waited on again
with its stream as it is.
A node that has seen a later grant for a session than the copy it holds
refuses, at once, every publish that wrote into that copy: another node owns
the session now, and a write here reaches no copy the new owner reads, whatever
a replica's lease table says yet. The refusal is counted as
`hoardmq_replication_barrier_failures_total{peer="-",reason="superseded"}`.

For QoS 2 the PUBREC state (the client has the message) is replicated to the
followers before the PUBREL goes out, so a takeover rebuilt from a replica
re-sends the PUBREL, not the PUBLISH — exactly once survives an owner loss.
A QoS 1 message the client had acknowledged can still be delivered again
after such a rebuild, because a client's PUBACK is replicated after the fact
and there is nothing to hold while it travels: QoS 1 permits it, and the
simulation harness's properties state the exception.

Retained writes go through the topic's shard leader: a node that does not
lead the shard forwards the message to the leader and its PUBACK waits for
the leader (which in turn waits for its followers); the leader stamps the
message, stores it and replicates it. A write the leader's journal fails to
append or sync is refused, and so is the publish that waits on it (0x83); the
topic's next retained write is judged on its own. Retained reads on subscribe scatter
to every shard leader (or to the one leader for an exact topic) and gather
within the acknowledgement timeout, so a wildcard subscriber sees retained
messages from every shard — but only to leaders that might hold a match. Each
node tells its peers which shards it holds retained messages for and leads,
and a subscribe whose filter falls outside a peer's current statement is
answered without asking that peer; a wildcard filter skips a peer only when
its statement is empty. Absence of a statement is never read as absence of
messages: a peer that has said nothing, or whose link has changed state since
it last spoke, is always asked. A write that makes a shard non-empty
announces it on the same ordered stream the write's barrier waits for, so the
statement reaches every live peer before the publisher's acknowledgement
does; the statement shrinks again only on a periodic recompute, which costs a
query and never a message. If the leader is down at write time the message is
stored locally and counted.

A shard-map change that moves a retained shard's holders is a move, which the
shard's leader under the new map runs to completion before the shard takes a
retained write again. It sends each new follower a release (a replication
frame naming the shard) and then the shard whole; once they acknowledge, it
sends a release to every node that held the shard and no longer does, and the
move completes when each of them alive by the log has acknowledged it. Until
then a node that stopped holding the shard keeps its copies (counted
`hoardmq_replicas_kept_total{reason="retained-moved"}`, persisted beside the
reconciled map version so a restart keeps them), and the leader refuses a
retained publish on the shard with 0x83 (`hoardmq_replication_barrier_failures_total{reason="moving"}`):
a leader that died before its new followers held the shard would otherwise
take messages it had acknowledged with it. If the leader is lost first, a
node that kept copies hands them to the shard's next leader, which installs a
handed-over message only for a topic it holds nothing for. A draining node's
leaving phase waits for its releases too.

Only a shard's leader under the map a node last acted on is taken to hold
the shard whole: a follower's push may never have landed, and a node that
held nothing has only what was handed to it. So whenever a shard's leader
changes, every node that held it hands its copies to the new leader (marked,
installed only where the leader holds nothing), and makes the hand-over
again until it is acknowledged, through later map changes too. Until then a
release drops nothing on that node, from any sender
(`hoardmq_replicas_kept_total{reason="retained-handover-owed"}`); a release
that arrived meanwhile is acted on once the hand-over is in. Only the shard's
leader under the map it has applied answers a hand-over; a node that does not
lead the shard and holds the topic, or keeps its own copies of the shard,
refuses it and the sender makes it again
(`hoardmq_retained_handovers_total{outcome="refused-not-leader"}`). An old
leader that still follows the shard keeps its own copies as a follower's until
the new leader's release and push reach it. A node never deletes its own
retained copies of a shard it leads under the map it has applied
(`{reason="retained-own-leads"}`).

When an owner dies, the lease service adopts its offline leases and, with
replication attached, fetches each session's replica from the shard's
holders — and when they have nothing, from the copy a former holder kept
because its hand-over went unacknowledged, the adopter's own included — and
imports it, so queued and in-flight messages and subscriptions survive the
node; a client reconnecting elsewhere pulls the state from the adopter as in
a live takeover, and a takeover whose previous owner cannot answer, or
answers that it holds nothing, falls back to the replica the same way. A
holder answers a fetch from a small pool of its own, apart from the worker
that applies the asking peer's replication stream: a client's CONNECT is
holding a handshake slot for the answer, and after a node loss that worker
is seconds deep in the storm's appends. A fetch reads a copy and orders
against nothing in the stream, and when the pool is full it goes to the
worker rather than being dropped. A
shard-map change moves state without closing any connection: owners send
whole snapshots to targets they gained, followers drop replicas of shards
they no longer hold — but only while the session's owner is alive to make
new copies, since otherwise the copy being dropped may be the last one and
the node taking the lease over would have nowhere to read it from. Holding a
shard is not on its own a reason to hold a copy: an owner replicates to at
most `replicas-1` of a shard's holders, so a node keeps a copy only while it
is one of that session's targets, and drops it — and refuses one handed to
it — when it is not, again only while the owner is alive, and only once the
owner has written the session where another node holds it (see
`owner-unwritten` and `moved-target` below). A copy of a session
the dropping node owns itself is from that session's previous owner, so it is
dropped rather than handed on, and a hand-over carries the copy's own epoch,
which a receiver whose lease has moved past refuses — a new
shard leader promotes its replica retained messages (or receives them from
the old holders), and new followers receive the shard's retained messages
whole. A peer that reconnects is resynced whole. A resync refused because
one ran less than an acknowledgement timeout ago is retried when that
interval ends, and every resync ends with a marker frame the peer
acknowledges, so barriers recover once the peer is caught up, even when
there was nothing to resend.

Metrics: `hoardmq_replication_records_total{direction}`,
`hoardmq_replication_acks_total{outcome}`,
`hoardmq_replication_barrier_seconds_total`,
`hoardmq_replication_barrier_failures_total{peer,reason}` (`reason`:
`refused` — a send to the peer failed, which is how a down target usually
reads — `resync`, `down`, `timeout`, `stopped`, `not-held` for a key the peer
refused, `unscoped`, or `unnamed`, with `peer` `-`, for a wait that named no
record),
`hoardmq_replication_refusal_fallback_total{peer,kind}` (a follower's refusal
the owner could not tie to a key — `no-key` when neither the follower's
rejection nor the owner's record of recent frames names one, `mismatch` when
they disagree, `at-cap` when the set of refused keys is full: it blocks every
wait on that peer, as `unscoped`, until the stream is resynced; a follower
names the key it refused, so between current builds this stays at zero),
`hoardmq_replication_gaps_total{peer}` (a follower saw the owner's sequence
jump — frames were lost with the link up, or the owner's first push to it
never arrived; it rejects the frame that revealed it and the owner resyncs
the stream),
`hoardmq_replication_unsynced_batches_total{peer,cause}` (a follower's batch
whose writes may not be on its disk: `failed` when its own replica fsync
failed, `covered` when another fsync of the replica log failed before it
returned; the stream acknowledges nothing until the owner has resynced it),
`hoardmq_replication_unapplied_total{reason,type}` (a record that changed
nothing on the follower, `type` the record's: `reason` `no-session` or
`no-entry` is a copy that has diverged from its owner — the record is
refused and the owner repairs the key; `holed` is a record inside a gap
window, between a gap the follower reported and the resync mark that
closes it, acknowledged as the resync is what repairs it),
`hoardmq_replication_repairs_total{outcome}` (a key the owner sent whole to
a follower that refused a record for it: `snapshot`, `delete` when the
owner no longer serves the session, `repeat` for a refusal inside the
acknowledgement timeout of the last repair, which that repair answers),
`hoardmq_replicas_dropped_total{reason}` (a replica copy this node must not
hold, dropped: `owned-upsert`, `owned-delete` and `owned-rebalance` are copies
of a session this node owns — from the session's previous owner — dropped at
its own write of the session, at its own delete of it and at a rebalance;
`not-a-target-confirmed` and `not-a-target-held` are copies of a session this
node is not replicated to, from an earlier grant, or copies kept
after this node stopped being the session's target (`moved-target`), dropped
on the owner's confirmation or once a check finds one of the owner's targets
holding the session at the owner's grant — see `owner-unwritten` below),
`hoardmq_replication_stream_blocked{peer,reason}` (1 while no barrier to that
peer may pass and 0 otherwise; `reason` is `holed` — frames are missing and
the resync has not landed — or `refused` — a key the peer refused has not been
sent whole since — or `none`, which is also what a peer the shard map does
not name reads, since no barrier waits on it; with
`hoardmq_replication_stream_blocked_seconds{peer}` the age of the state. A
QoS 1 or 2 publish that needs a blocked peer is refused, so this pair is what
says whether that is happening *now* and for how long; the harness's wedged
node is WI 1519's measurement of the same state from outside),
`hoardmq_replication_unstamped_total{type}` (a session record written with no
lease generation while this node's table holds one for the client — the write
is named in the log beside it; a follower applies such a record when it comes
from the node the table names as owner, and refuses it from any other),
`hoardmq_replication_resync_deferred_total`,
`hoardmq_replication_send_failures_total`,
`hoardmq_replicas_kept_total{reason}` (a session or retained copy this node
no longer holds under the shard map but kept: `owner-not-alive`;
`owned-unserved` — this node owns the session's lease and does not serve the
session yet, because the adoption that imports it has not run, so the copy
may be the only one there is; `owner-unwritten` — a copy this node still holds
the shard for but is not a target of, written under an earlier grant than the
owner's: the owner may not have fetched the session yet, so the copy stays
until the owner confirms it holds the session durably at its grant (the
confirmation a new owner sends its previous owner goes also to every other live
member it does not replicate to; one that names a grant this node's lease table
has not applied yet is judged again as the table catches up), or until the node, asking the owner's
targets every twice `cluster.leases.ttlMs`, finds one holding the session at
the owner's grant or later. No time alone ends the keep: an owner can die
before it is declared with the session written nowhere else. The checks are
counted in `hoardmq_replicas_unwritten_checks_total{outcome}` (`started`,
`held`, `not-held`), at most eight in flight per node; `moved-target` — a copy
kept after this node stopped being the session's target, or one written under
the owner's current grant on a node the owner does not replicate to, until the
owner confirms the session at its targets or a check finds one holding it; or
`map-behind` — the map is behind what the node last acted on, or the file
recording that version could not be read; `handover-unacknowledged` — a node
that stops holding a copy hands it to the shard's new holders and lets go
only once they acknowledge it, sessions and retained messages alike; a
hand-over carries the copy's own epoch, so a copy older than the lease is
refused and stays kept, and a copy of a session the node itself owns is
dropped, never handed over. A refused session copy whose owner is alive is
also counted `moved-target` or `owner-unwritten` and goes as those do),
`hoardmq_replication_fetches_total{outcome}` (a takeover's fetch of a replica:
`local`, `remote`, `kept-copy` — a former holder's kept copy, taken only once
every source has answered that it holds none — `none`, and
`none-kept-unconfirmed`, a fetch that found a kept copy while a source did not
answer, and so read as unanswered: the source may hold the newer copy the owner
fed (an adoption given up after such fetches serves that copy: "A fetch nobody
answered" above), and `none-returning`, a fetch that found nothing while a node that may hold
the copy was back on its control connection and could not be asked yet, which
also reads as unanswered), `hoardmq_replicas_held`,
`hoardmq_shardmap_version`, `hoardmq_retained_forwards_total{outcome}`,
`hoardmq_retained_queries_total{direction,outcome}` (`outcome="skipped"` is a
query a peer's statement made unnecessary),
`hoardmq_retained_presence_total{direction}`,
`hoardmq_retained_promotions_total`. The audit log records shard-map changes
and replica fetches.

### Drain and readiness

A drain is the node's graceful leave, started by the admin API's `POST
/api/v1/cluster/drain`, the observability listener's `POST /drain` when the
admin API is not configured, the `hoardmq drain` command or a Kubernetes
preStop hook. It closes the client
listeners (readiness turns false at once), disconnects connected clients —
MQTT 5 clients with `0x9C Use Another
Server` and a Server Reference naming a live node's advertised address,
round-robin over the members alive by lease record, older clients with a
plain close — at `cluster.drain.ratePerSecond`, or faster where that is
what disconnects them all by `cluster.drain.timeoutMs` less
`cluster.leases.takeoverTimeoutMs` (less half the timeout, if that is
smaller) from the drain's start, so the drain's length does not grow with
its clients (25 s of redirecting at the defaults), waits up to `cluster.drain.timeoutMs` for those clients'
leases to be taken over elsewhere, then hands every lease it still owns to
one live node (the consensus leader when that is another live node, else
the lowest name) with one proposal: the receiver adopts them, pulls each
session from this node (or its replicas), and runs their Will and expiry
timers, so offline sessions and clients that did not come back survive the
node. The hand-off is made even when nothing is left to hand: it is the
node's word that it is leaving, and the lease table marks it so until the
node's next incarnation is heard alive. A hand-off that fails, as one sent
to a leader that loses an election does, is made again each second until
`cluster.drain.timeoutMs` has passed, counted
`hoardmq_drain_handoff_retries_total`; one the lease table refuses is not.
From the hand-off the node proposes no alive record and does not fence. Next the drain leaves the shard map (phase
`leaving-placement`): it waits until a map without it has applied here, and
then until every replica copy it kept as a moved target has been confirmed
held at its owner's new targets, each wait at most twice
`cluster.replication.ackTimeoutMs`. A node the copy count keeps in the map
does not wait. The outcome is counted as
`hoardmq_drain_placement_total{outcome}`: `left`, `placed` (the copy count
kept it), `map-expired` or `copies-expired`; a drain that made no hand-off
counts `not-handed-off`, and stays in the map through its down window. A redirect or a hand-off never
targets a node marked leaving. Its down window then needs no session's
copy, which is what keeps a rolling restart's publishes from waiting on the
restarting pod. The drain then waits, up to `cluster.drain.confirmTimeoutMs` (default
30 s, the lease ttl's default), until every session it held and handed off
is confirmed held by its receiver — the confirmation the sealed hand-over
already sends, once the receiver holds the session and its replicas have it. A
session's owner sends its confirmation to every other live member it does not
replicate the session to, and to every member marked leaving. So a session that
the receiver loses at once to a further owner, as when a redirected client
returns, is confirmed by that owner, and so is a copy this node kept as a moved
target for a session that changed owner across the map change.
Meanwhile the node answers pulls and admits no client; it is a replica
target only if the copy count kept it in the map. Sessions still unconfirmed when the wait ends
are counted (`hoardmq_drain_sessions_unconfirmed_total`), audited and logged
at Warn with their clients: those still held here stop with the node, a
restart keeps them, and a node that never returns loses them. A session whose
copy left this node without a confirmation is waited for and counted the same
way. Confirmed ones count `hoardmq_drain_sessions_confirmed_total`, including
those whose confirmation came before the wait began. The drain then reports
complete and `serve` exits 0. With `cluster.drain.holdExitMs` above 0 (default
0), a drain that no signal started is followed by that long a wait for
SIGINT or SIGTERM, which ends it at once, before the exit; the status still
reads complete meanwhile. That is for a Kubernetes preStop hook running
`hoardmq drain -wait`: the hook runs in the broker's container, whose
processes the kernel kills when the broker exits, and the kubelet sends
SIGTERM only once the hook has returned, so the hold lets the hook read the
completion and exit 0 first. A drain started by the signal itself
(`cluster.drain.onSignal`) is not held. With no other live node the clients get Server Busy,
the leases lapse for a future leader to adopt, and nothing is awaited.
`GET /drain` (and the admin API's `GET /api/v1/cluster/drain`) returns `{"draining","phase","clientsRemaining","leasesRemaining",
"sessionsAwaiting","sessionsConfirmed","sessionsUnconfirmed","copiesKept","complete"}` (`copiesKept` while it is not zero);
each phase is audited as `server.drain`.

Readiness (`/readyz`) is true only when the node serves, is not draining,
is not recovering,
its consensus core is a caught-up member, its leases are not fenced, its
build understands the cluster's feature level (below), and
the topology — the membership with roles and the set of nodes alive by lease
record — has been unchanged for
`cluster.readiness.stableMs` (default 5 s; 0 disables the condition). A node
joining, or one lost and declared dead, therefore makes every node briefly not
ready, which is what keeps traffic off a cluster still settling. The shard
map is not part of it: a drain or a return moves only the map, and the other
nodes stay ready. Liveness (`/healthz`) is
the process only.

A ready node can still refuse QoS 1 and 2 publishes: while it lacks the
routes of an owner that holds sessions — after restarting beside a peer it
cannot hear — any subscriber could be on that owner, so it refuses every
QoS 1 and 2 publish (see "Routing across nodes"). That state is
**degraded**, and readiness does not report it, so a load balancer keeps the
node in service, and its connections and QoS 0 traffic go on. `/servingz`
answers 200 `serving` when the node is ready and knows every owner's routes,
503 `degraded: routes-unknown` when it is ready without them, and 503
`not ready: <reason>` when it is not ready. The gauges
`hoardmq_node_degraded{reason="routes-unknown"}` (1 while degraded) and
`hoardmq_node_degraded_seconds` (how long the routes have been unknown, 0
when not degraded) carry the same state, and `GET /api/v1/cluster` carries
`serving` and `servingReason`. The chart's readiness probe stays on
`/readyz`.

### Upgrades and the cluster feature level

Two builds in one cluster must never apply the same replicated operation
differently, so a change to what a replicated operation *does* — a
lease-table operation every voter applies, or how a follower applies a
replication frame — is keyed to a **cluster feature level**: one integer in
the lease table's replicated state. Every node advertises the highest level
its build understands in the alive record it already sends. The consensus
leader raises the cluster's level to the lowest level any member advertises,
once every member (voters and learners) has a record; a member with no
record holds the raise back. The raise is itself a replicated operation, so
every node switches at the same position in the log, not at the moment its
own binary was replaced. The level only rises.

During a rolling upgrade the cluster therefore keeps the old semantics until
the last member is on the new build, then switches once. A node whose build
is **below** the cluster's level — rolled back, or added from an older image
— stays a consensus member so it keeps receiving the log, but serves nobody:
`/readyz` answers `level`, a CONNECT is refused with `0x9C Use Another
Server` naming the leader (`0x88` when none is known), and it logs the two
levels at Error. It also refuses replication frames stamped with a level it
does not understand, so the sessions it is a replica target for fail their
QoS 1 and 2 barriers until it is upgraded or leaves: adding a node from an
older image is a partial outage, not a no-op. A cluster's first node is alone
in the membership when it starts and raises the level to its own at once.
Such a node still applies the log, with the semantics its build has; what
that must become is an open design question, and no gated change ships
before it is answered (`docs/release-checklist.md`).

Replication frames carry the level their sender read (`lvl`, absent at
level 1), because a frame is applied off the log and the receiver's reading
may differ. A frame of an unknown *kind* is still acknowledged unapplied, as
before: a kind added later must be safe for a peer that ignores it.

Level 1 is everything that shipped up to the first release tag. Metrics:
`hoardmq_cluster_feature_level`, `hoardmq_cluster_feature_level_build`
(this node's build), `hoardmq_cluster_feature_level_min_advertised` (what a
raise waits for; 0 while a member has no record),
`hoardmq_cluster_feature_level_raises_total{outcome}` and
`hoardmq_cluster_feature_level_refusals_total{where}` (`connect`, `frame`).
Raises and a node falling below the level are audited as `cluster.level`.

**Making a gated change:** raise `lease.BuildLevel` in the same commit, read
`Table.Level()` where a log-applied operation is applied and the frame's
level where a frame is, keep the old behaviour below the new level, and add
the semantic to the inventory in the project's upgrade-gate design.

### Kubernetes deployment

`Dockerfile` builds the image (a static binary on a distroless base, running
as non-root, `hoardmq serve` with the configuration at
`/etc/hoardmq/config.json`). `deploy/helm/hoardmq` is the supported
deployment: one StatefulSet with parallel pod management (the broker's
join-not-bootstrap forms the cluster), a headless Service publishing
not-ready addresses for discovery, a client Service, a PodDisruptionBudget
of one, volume claim templates, mutual TLS between nodes from a
chart-generated CA reused across upgrades (or cert-manager), a preStop
`hoardmq drain -wait` with the broker holding its exit for the SIGTERM that
follows the hook (`cluster.drain.holdExitMs`), a termination grace period of
the drain's two timeouts plus a margin, readiness on `/readyz` and liveness on `/healthz`. With `admin.enabled` the
chart configures the admin API on the pod's loopback with a generated drain
token and operator token, and the preStop drains through it. No operator and
no CRD are required. The chart's README covers the values, the N-1
capacity trade-off and the rolling-restart runbook; `go test ./deploy/...`
keeps the templates consistent with `values.yaml` without Helm installed.

### Persistence

`node.dataDir/logs/state/` holds the write-ahead log: segmented append-only
files with a length, type and CRC-32C per record and a versioned header per
segment, plus a checksummed checkpoint of the whole state. Sessions with a
non-zero Session Expiry Interval, their subscriptions, in-flight and queued
messages, Wills and expiry times, and every retained message are journaled;
transient sessions are not, because nothing of theirs could be recovered.
A queued message leaves the log, and every replica, when it leaves the queue,
sent or expired, at every QoS, so neither a restart nor a takeover from a
replica sends a QoS 0 message again once it has gone out; a crash between the
send and the record reaching the disk, or a takeover before a replica has
applied it, still can. An in-flight message that expires before its session
resumes leaves the log and every replica too, when the resumption drops it.

Under the default `node.walSync: "commit"`, a QoS 1 or 2 PUBLISH is
acknowledged only after its records are fsynced. `"relaxed"` syncs every
`node.walSyncIntervalMs` (default 100) instead and is opt-in. Segments roll
at `node.walSegmentBytes` (default 64 MiB); a checkpoint is written at
start, at shutdown and after every 10,000 records, and the segments before
it are dropped. Sessions keep being journaled while a checkpoint reads them,
so it takes the log's position first and, for each session, the position it
read that session at; recovery replays the records after the first and skips
a session's records up to its own, so each is applied once. The replica log's
checkpoint is read under the lock its records are applied under, and is
exact at one position. On start, a record cut short at the end of the last segment
(a torn write) is discarded; a checksum failure anywhere stops the node with
the segment and offset named, so a damaged log is never silently read past.

### Environment overrides

Environment variables override scalar members before validation, so a bad
value is reported at the member it targets and an unrecognised variable is
reported as a problem rather than ignored:

```
HOARDMQ_NODE_NAME            HOARDMQ_NODE_DATA_DIR
HOARDMQ_NODE_WAL_SYNC        HOARDMQ_NODE_WAL_SYNC_INTERVAL_MS   HOARDMQ_NODE_WAL_SEGMENT_BYTES
HOARDMQ_NODE_WAL_GROUP_MAX_DELAY_MS     HOARDMQ_NODE_WAL_GROUP_MAX_SIZE
HOARDMQ_NODE_SESSION_SHARDS             HOARDMQ_NODE_STORE_ADMISSION
HOARDMQ_CLUSTER_ID           HOARDMQ_CLUSTER_LISTEN
HOARDMQ_CLUSTER_REPLICAS     HOARDMQ_CLUSTER_SHARDS
HOARDMQ_CLUSTER_AUTH_MODE    HOARDMQ_CLUSTER_AUTH_SECRET_FILE
HOARDMQ_CLUSTER_AUTH_TLS_CERT_FILE   HOARDMQ_CLUSTER_AUTH_TLS_KEY_FILE   HOARDMQ_CLUSTER_AUTH_TLS_CA_FILE
HOARDMQ_CLUSTER_HEARTBEAT_MS HOARDMQ_CLUSTER_HEARTBEAT_MISSES    HOARDMQ_CLUSTER_MAX_FRAME_BYTES
HOARDMQ_CLUSTER_RAFT_TICK_MS HOARDMQ_CLUSTER_RAFT_ELECTION_TICKS HOARDMQ_CLUSTER_RAFT_HEARTBEAT_TICKS
HOARDMQ_CLUSTER_RAFT_SNAPSHOT_ENTRIES   HOARDMQ_CLUSTER_RAFT_SNAPSHOT_TRAILING
HOARDMQ_CLUSTER_RAFT_PROMOTE_LAG        HOARDMQ_CLUSTER_RAFT_JOIN_TIMEOUT_MS
HOARDMQ_CLUSTER_DISCOVERY_MODE          HOARDMQ_CLUSTER_DISCOVERY_DNS_SERVICE
HOARDMQ_CLUSTER_DISCOVERY_DNS_NAME      HOARDMQ_CLUSTER_DISCOVERY_DNS_REPLICAS   HOARDMQ_CLUSTER_DISCOVERY_DNS_PORT
HOARDMQ_CLUSTER_LEASES_TTL_MS           HOARDMQ_CLUSTER_LEASES_TAKEOVER_TIMEOUT_MS   HOARDMQ_CLUSTER_ADVERTISE
HOARDMQ_CLUSTER_ROUTING_SYNC_MS         HOARDMQ_CLUSTER_REPLICATION_ACK_TIMEOUT_MS
HOARDMQ_CLUSTER_DRAIN_RATE_PER_SECOND   HOARDMQ_CLUSTER_DRAIN_TIMEOUT_MS   HOARDMQ_CLUSTER_DRAIN_CONFIRM_TIMEOUT_MS
HOARDMQ_CLUSTER_DRAIN_HOLD_EXIT_MS
HOARDMQ_CLUSTER_READINESS_STABLE_MS
HOARDMQ_LIMITS_MAX_PACKET_SIZE   HOARDMQ_LIMITS_RECEIVE_MAXIMUM
HOARDMQ_LIMITS_MAX_QUEUED_MESSAGES   HOARDMQ_LIMITS_MAX_KEEP_ALIVE
HOARDMQ_LIMITS_CONNECT_TIMEOUT       HOARDMQ_LIMITS_QUEUE_DROP
HOARDMQ_LIMITS_MAX_SESSION_EXPIRY
HOARDMQ_LOG_LEVEL            HOARDMQ_LOG_FORMAT
HOARDMQ_OBSERVABILITY_ADDRESS
```

Arrays (`listeners`, `cluster.peers`), booleans such as
`cluster.raft.bootstrap`, and `modules` cannot be overridden from the
environment.

### Durability and group commit

Under `walSync: "commit"` (the default) a QoS 1 or 2 acknowledgement, a
session change or a takeover is answered only after the record is fsynced.
Concurrent callers share fsyncs: whoever finds no fsync in flight leads one,
everyone who flushed before it started is released by it, and callers that
arrive while it runs join the next, which starts the moment the current one
finishes — so the disk sees one fsync per group, not one per operation, and
no caller waits more than two. A caller returns only after an fsync issued
after its own write reached the file.

A failed fsync may have dropped every write not yet on the disk, and a later
fsync that succeeds does not write them again, so after one the journal is not
whole and no later fsync releases a caller. The next group rewrites it instead
(so do the broker's 30 s checkpoint check and its shutdown checkpoint): the
state at the log's last index, rebuilt from the latest checkpoint, the records
after it that fsyncs covered, and the records kept in memory until one did, is
written as a checkpoint with its own fsync; appends move to a new segment and
the segments before it are deleted. The callers whose own fsync failed get the
error; the others are released once the rewrite lands. A rewrite that fails
fails its callers, and the next is tried no sooner than a second later. Every
fsync of a segment file is serialized with every other, so a failure the
kernel reports to a roll's or a checkpoint's fsync is seen by the group whose
fsync it overlapped. The replica log is rewritten the same way. The group
commit's counters in the harness's storm statistics (`sync`) include `failures`,
`repairs` and `repair_failures`, and the checkpoints written, their time and the
part of it under the store's lock (`checkpoints`, `checkpoint_sec_total`,
`checkpoint_hold_sec_total`). The same failures, repairs and checkpoints, for both
logs, are on `/metrics` as the `hoardmq_journal_*` series (see "Observability").

A record that fails to append refuses the QoS 1 or 2 publish that wrote it,
whichever session it was written into: the publisher's own, a subscriber's on
this node or on a node the publish reached (directly, or through a shared
group's dispatch owner), or the retained store when this node holds the topic.
The failure is the publish's, so it does not mark the subscriber's session. A
copy queued for the subscriber whose record failed is withdrawn, so the
publisher's retry is its one copy; a copy already sent to the subscriber stays
in flight and completes. A delivery nothing acknowledges (a QoS 0 publish, a
Will, a redistribution, a retained message sent on subscribe) keeps its copy.
Copies whose records were written stay where they are, so a subscriber that
received one is sent the retry too (see "Sessions").

A SUBACK or UNSUBACK for a session the journal records grants a filter only
once the filter's record is appended and fsynced, and a CONNACK accepts a
connection only once the fsync after the connect's records succeeded; none of
them waits for the replicas. These waits share the group commit above, so a
burst of subscribes shares its fsyncs. A filter whose record failed to append,
or whose fsync failed, is refused with 0x83 (0x80 in a 3.1.1 or 3.1 SUBACK) and
left as the session held it before the request: in memory, in routing and, by
a record that undoes the request, in the journal. Because MQTT 5.0 section
3.10.4 stops new messages for a filter as soon as its UNSUBSCRIBE arrives, a
message published while a refused UNSUBSCRIBE waited is not delivered for that
filter. At 3.1.1 and 3.1, whose UNSUBACK carries no reason code, a refused
UNSUBSCRIBE closes the connection without one. A refused CONNECT (0x83; return
code 3 at 3.1.1 and 3.1) leaves the session as an abnormal end without a Will
would: offline with its expiry, or ended when its expiry is 0. A session with
a Session Expiry Interval of 0 is not journaled, so its SUBSCRIBE and
UNSUBSCRIBE wait for nothing.

`walGroupMaxDelayMs` lets a leader hold before its fsync to collect more
callers; `0`, the default, never holds — under load arrivals overlap every
fsync anyway, and a lone writer pays nothing. `walGroupMaxSize` fires a held
group early once that many callers joined, and only matters when the delay is
non-zero. The default pair was chosen by measurement on a 100,000-client
failover (WI 1292 round 4). `walSync: "relaxed"` syncs on an interval instead
and acknowledges before the disk has the record.

## Codec

`internal/packet` is the wire codec: one decoder and one encoder for MQTT
V3.1 (`MQIsdp`, level 3), 3.1.1 (level 4) and 5.0 (level 5). The level is
fixed per connection from the CONNECT packet. Every 5.0 property and reason
code is typed; unknown property identifiers, bad flags and overruns are
reported as Malformed Packet, value and sequencing violations as Protocol
Error, and an unsupported protocol version as a CONNACK in the client's own
framing. Properties are stripped at encode time for level 3 and 4 peers. The
decoder holds no buffer while a connection is idle: it blocks on a one-byte
read and takes a pooled buffer only once a packet has begun. Fuzz targets
cover every level.

## Conformance

`conformance/` is a black-box suite that drives a real node over TCP with
the Eclipse Paho clients (`paho.golang` for 5.0, `paho.mqtt.golang` for
3.1.1) and the project codec for hand-crafted frames. Every test names the
normative statements it observes, by the identifiers of the specifications'
Appendix B (`conformance/statements/mqtt5.json`, 251 statements;
`mqtt311.json`, 139). The test binary writes a results file, and
`go run ./conformance/cmd/coverage` renders
[conformance/COVERAGE.md](conformance/COVERAGE.md): every statement with
`pass`, `fail`, `not-covered` or `n/a-client` (client-only statements, each
with its reason in `statements/scope.json`). A test that names an unknown
identifier fails the run.

```
go test ./conformance -conformance.results=$PWD/conformance/results.json
go run ./conformance/cmd/coverage && git diff --exit-code conformance/COVERAGE.md
HOARDMQ_EXTERNAL=1 go test ./conformance/external     # Paho interoperability suites, minutes
```

The external test fetches the upstream `paho.mqtt.testing` suites into
`tmp/` at a pinned commit, runs them against an in-process node on
`localhost:1883` and compares with `conformance/external/paho/expected.json`,
where each expected failure carries its reason; the last run is recorded in
`last-run.json` and rendered at the end of `COVERAGE.md`.

## Simulation harness

`sim/` is the cluster's acceptance gate: real nodes in one process, a
seeded workload of publishers and durable subscribers, a seeded schedule of
faults (partitions with a minority probe, dropped, delayed and reordered
frames, crashes with torn or corrupted logs, drains, clock skew), and the
design's invariants checked as properties afterwards — one live owner per
client identifier, no double delivery, per-publisher ordering, no
acknowledged QoS 1 or 2 loss, minority refusal, checksum fails closed. A
seed names a schedule and a workload; re-running it re-injects the same
faults (execution rides the real scheduler, so interleavings differ — see
`sim/README.md` for what "deterministic" means here). Every property is
paired with an injected defect in `broker.Hooks` that disables its mechanism
so the negative control shows the property catches the failure. `sim/results/`
holds the failover measurements with their methodology.

Building the harness changed the broker: forwards between nodes are now
acknowledged by the receiving node after the message is durable there, and
a QoS 1 or 2 publish fails (Implementation Specific Error) instead of being
acknowledged when a forward was lost to a link reset, skipped in the
receiver's sequence (the acknowledgement is cumulative, so a gap is reported
before the next frame's acknowledgement could cover it), or a matching
subscriber's node is unreachable; the replication barrier fails, rather than
skips, when a follower is down; a live takeover closes the old connection
before exporting the session so nothing is sent twice without DUP; a session
leaving a node holds its route removal for the takeover timeout, so a
publish sent while its new owner's route is still in flight is refused
rather than accepted with nowhere to deliver it; and a QoS 2 publish refused
in the PUBREC releases its packet identifier with the exchange.

Running it to completion found six more, all reachable in production: the
broker panicked (`send on closed channel`) when a client connected while it
was closing other connections; a durable session could be lost because the
shard map could move backwards through a snapshot restore while the
rebalance dropped replicas against it; a takeover decided whether to ask the
previous owner for its state from this node's reading of that node's clock,
so a skewed clock left two nodes holding one session with the queue on the
wrong one; the rebalance could drop the last copy of a session whose owner
had died; a replication stream that had dropped frames kept acknowledging,
because frames pushed after the break took fresh sequence numbers and their
barriers no longer saw it, so a barrier passed over a peer whose copy had a
gap; and a takeover could adopt a copy from a shard holder the previous
owner was not replicating to, since a shard's holders are its leader and
every follower while an owner replicates to at most `replicas-1` of them.

**Exactly-once across an owner loss.** A client completes a QoS 2 exchange
when it receives the PUBREL, and after that it is entitled to treat a
re-delivery as a new message. So the broker does not send the PUBREL until
the PUBREC state is in its own journal and replicated: while an owner cannot
reach its followers, or its journal failed to take the state, it holds the
PUBREL and retries (rewriting the session whole in the second case), and the
exchange stalls rather than completing somewhere the replicas cannot see. A
session rebuilt from a replica therefore never finds a completed exchange it
does not know about, and its
re-send arrives under the identifier the client still has open — a
retransmission the client absorbs, not a second message. The identifier is
reserved when the message is queued and travels in the same replicated
record, so a rebuild re-sends under it rather than allocating a fresh one.

The cost is latency, not duplication: a QoS 2 delivery to a subscriber whose
owner is cut off does not complete until the owner can replicate again or
the session is re-homed. `TestDefectPubrelUnreplicated` is the negative
control — with the hold disabled the harness sees the subscriber's
application take the same message twice.

The inbound side has the mirror rule. After a client receives PUBCOMP it may
reuse the packet identifier for a new message, and a node that recovered, or
a replica that took over, still holding the identifier open would answer
that message as a duplicate and drop it. So PUBCOMP goes out only once the
identifier's release is in the node's journal and on the replicas; otherwise
the session holds it and retries every 250 ms, and a PUBREL the client
resends on a new connection waits the same way. It costs one more fsync per
inbound QoS 2 exchange, shared by the group commit, and one replica round
trip: about 12 ms between PUBREL and PUBCOMP on a three-node cluster on one
host.

A refused QoS 2 publish releases its identifier too: after a PUBREC with an
error the client may send a new message under it at once. So the error PUBREC
waits for the release in the same way and is held and retried otherwise. While
the session's followers are unreachable, the refusal reaches the client late:
when they return, or once the shard map replaces them. A successful publish
pays nothing for this.

An identifier's opening can reach the journal or a replica before the
publish's own copies do: a queued copy in another session, or a forward to
another node. So once a publish's acknowledgement is decided, the broker also
records that it was accepted, and a PUBLISH under an open identifier is
absorbed as a duplicate only when that record is there. An identifier that a
restart or a takeover brings back open without it is refused when the client
sends the PUBLISH again (Implementation Specific Error; at 3.1.1 and 3.1, a
close), because that publish may never have reached its subscribers. The
client's retry is then a new message. Each such refusal is counted by
`hoardmq_qos2_unconfirmed_refused_total{level}`. The record is not waited
for, but a client that sends PUBREL as soon as it reads PUBREC meets it on
the replica: the whole exchange went from about 16 ms to about 20 ms at the
median on a three-node cluster on one host.

The cost of that honesty is refusals: while a node is unreachable and until
the shard map drops it or its sessions are re-homed, QoS 1 and 2 publishes
that cannot be placed are answered Implementation Specific Error and the
client retries. The design prefers that to a silent loss.

**An owner whose routes are unknown.** Hiding an unreachable owner's routes
only works over routes this node has. A node that has just restarted holds
none: route knowledge reaches a node from its owner and nowhere else, so a
node restarting beside a peer it cannot reach has an empty table for that
peer and nothing to hide. It cannot say a publish matches no subscriber,
because it cannot say anything at all about that peer's subscribers. So it
does not: while any owner's route set is unknown, QoS 1 and 2 publishes are
refused with Implementation Specific Error, the same answer as a hidden
match, and the publishes still go to the owners this node does know about.

An owner is unknown when no full route sync has arrived from it since this
node started **and** the replicated lease state cannot prove its routes do
not matter. An owner nothing was ever taken from, holding no lease, has no
subscribers to miss. An owner that stays unreachable has its leases adopted
after the lease ttl, but the reassignment moves the leases before the adopting
node has imported the sessions or published their routes, so holding nothing
is not enough: the refusal ends when the owner's sessions are served
elsewhere, by the same proof that ends a hidden owner's refusal (each member
that owns leases has said in the log that its adoptions are finished, and this
node holds that member's routes as of then; this node's own adoptions are
finished too). A node that restarts beside a lost owner therefore refuses
until the adoptions finish, and says in the log that its own are finished, as
a node that had the owner's routes does. `hoardmq_route_owners_unserved{reason}`
counts the owners held this way (`unsynced`, `adopting`, `unreachable`, with
the meanings of `hoardmq_route_owners_hidden`). A node that cannot read the
lease state yet — one still replaying its log — has proved nothing and
refuses.

**A node that has just restarted, as a receiver.** A restarted node loads the
sessions its store holds before its consensus core, lease service and
replication start, and its lease table names it owner of what it owned when it
stopped until its core has caught up with the leader. Until both have
happened, a forwarded QoS 1 or 2 publish into one of its sessions is refused to
its sender (`not-serving` below), and the publish with it. Taken in that
window, it would be acknowledged on this node's journal alone, and a client
that meanwhile took the session over elsewhere from a replica, because this
node could not yet answer its takeover, would never receive it. A QoS 0
forward is delivered as before.

**A session that has left this node.** When a session moves to another node,
its old owner keeps advertising its routes for a grace period, so the other
nodes keep forwarding here, find no session, and refuse — rather than
matching nothing anywhere while the new owner's route is still in flight.
That protects every node except the one doing it: a node cannot see its own
routes through the two rules above, because the match skips itself and the
hidden set holds only unreachable peers. So the node still advertising the
route was the one node blind to it, and answered its own publishes as though
the route did not exist — including with No Matching Subscribers, which is a
positive claim it was not entitled to make. It now refuses a QoS 1 or 2
publish whose topic matches a filter it is handing over, for the same window
in which it is telling everyone else to forward there.

In practice the window is the length of the ignorance and no longer.
Measured on the simulation's seed 5: 0.11–0.20 s at a cold cluster start,
which is the transport connecting and is over long before the readiness
gate's five seconds of required stability; 0.10 s for a restart whose peers
are reachable; and 1.92–1.99 s for a restart beside a partitioned peer,
ending when the partition does. `hoardmq_route_owners_unknown` reports how
many owners are unknown right now, and
`hoardmq_forward_failures_total` separates the three causes a publish is
refused for *at dispatch* by its `reason` label: `subscriber-node-down` (a
hidden owner holds a matching route), `routes-unknown` (an owner's route set
has never arrived) and `session-moving` (a session is leaving this node and
its route is still advertised). The same family carries the causes a forward
fails for once it has been dispatched, among them `sequence-gap` — the
receiver saw a sequence above the one it expected and reported the frames
between as missing — and `receiver-refused`: the receiver refused that one
forward on its own account (it reached no session there, `no-local-subscriber`
on the receiver; a session on its filter was moving away, `session-moving`; a
record the delivery wrote there failed to append, `not-journaled`; or the
receiver has just restarted and does not serve its sessions yet,
`not-serving`), which
fails that publish and no other; and `give-refused`: a group's owner handed the
message on to a third node that did not hold it, which also fails that publish
alone. A failure the
receiver cannot place on one frame (a gap, a durability wait that failed)
fails every forward to it up to that sequence. `hoardmq_forward_late_frames_total` counts the other
half of that: a frame arriving *below* the expected sequence, which is a
frame a gap already reported missing turning up late. A gap with no late
frame behind it is a loss; a gap the late frame follows was a reordering,
and the publish it refused had in fact been delivered. A sender admits its
forwards to a peer in sequence order, and two fault-free 10,000-client
failover runs reported neither a gap nor a late frame
(`sim/results/sequence-order-2026-09-22/`). A late frame does not lower
the sequence expected next, so one reordering reports one gap. A frame
below the expected sequence that fills no reported gap is the sender's
count starting again after a restart, counted by
`hoardmq_forward_stream_restarts_total`.

## Building and testing

Go 1.26 or later; no Makefile, the Go tool is the build system.

```
go test -timeout 30m ./...                                              # the sim package alone runs for about ten minutes, which is Go's default: without this the gate panics or passes by luck
go build ./...                                                          # the test gates all compile the test-augmented package, so only this catches a non-test file referencing a _test.go declaration
go vet ./...
golangci-lint run ./...
go run ./tools/licensecheck
go generate ./... && git diff --exit-code NOTICE
go test -run '^$' -fuzz=FuzzDecodeV5 -fuzztime=60s ./internal/packet    # and V4, V3, FuzzConnect
go test ./deploy/...                                                    # Helm chart self-check
go test ./cmd/hoardmq-build                                             # builds the example module into a binary with hoardmq-build, offline, and runs it
SINKKAFKA_LIVE_BROKERS=… SINKKAFKA_LIVE_CONTAINER=… go test -run '^TestLiveKafka$' ./modules/sinkkafka   # sink.kafka against a real Kafka (see "Kafka forwarder")
go test -timeout 30m ./sim                                              # simulation harness: seed corpus, negative controls, 1k-client failover
HOARDMQ_SIM_RANDOM=20 go test -timeout 60m ./sim -run TestCorpus        # nightly: random seeds, each printed
docker build -t hoardmq:dev .                                           # the container image
```

The gates and the dependency policy behind them are in
[docs/dependency-policy.md](docs/dependency-policy.md). The race detector
is in no gate: `-race` runs only in a work item that tracks a race, over the
package that showed it, and in a negative control whose subject is a race
(same document). The broker compiles in the adopted Raft core
`go.etcd.io/raft/v3` (Apache-2.0) and its `google.golang.org/protobuf`
(BSD-3), and the Kafka forwarder's client `github.com/twmb/franz-go` with its
`kmsg`, `klauspost/compress` and `pierrec/lz4` (BSD-3), all listed in NOTICE;
the conformance
suite imports the two Paho Go clients (EPL-2.0, test only, pinned) and what
they bring in, none of which reaches the binary.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

The HoardMQ name is covered by [TRADEMARK.md](TRADEMARK.md), not by the
software licences. Third-party attributions are generated into
[NOTICE](NOTICE).
