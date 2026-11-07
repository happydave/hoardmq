# Security and access

Who may connect, publish, subscribe and administer: the authenticators and authorizers, the cost of the connect path,
single sign-on for the admin API, and API keys. Listener TLS is in [configuration.md](configuration.md#listeners),
cluster authentication between nodes in [clustering.md](clustering.md#cluster-transport), and the admin API's routes
in [operations.md](operations.md#admin-api).

## Authentication and authorization

**A fresh server refuses everyone.** Anonymous connections are refused
unless `auth.anonymous` is `true`, and every publish, subscribe and
unsubscribe is refused unless an authorizer allows it. The two lines that
open a development broker:

```json
"auth": { "anonymous": true },
"modules": { "authz.file": { "path": "acl.json" } }
```

with an `acl.json` of `{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}`.

**A session belongs to the principal that made it.** The principal is the
username authentication settled on: the authenticator's (a JWT's username
claim, an enhanced authentication's user), else the CONNECT's username the
authenticator verified, else, with no username, the verified certificate's
common name; anonymous clients are one principal. While a client
identifier's session exists, a CONNECT for it by another principal is
refused with Not authorized (0x87; return code 5 at 3.1.1 and 3.1), whether
it resumes the session or starts clean, and the session, its connection and
its queue are left as they were (counted in
`hoardmq_connect_refusals_total{reason="principal"}`). To give a client
identifier to another principal, end its session through the admin API
(`DELETE /clients/{id}/session`, `hoardmq clients invalidate`).
`auth.sessionBinding: "off"` lets any principal resume or replace any
session, as before. Across nodes, the node a CONNECT arrives at refuses it
from its own lease table, before any takeover: each lease records a 64-bit
digest of its session's principal (a truncated SHA-256 of the namespace and
username), so the rightful client's connection elsewhere is never closed by
the refused CONNECT (counted also in
`hoardmq_lease_ops_total{op="acquire",outcome="principal"}`). A lease from
before the digest, or one whose newest acquire the node has not yet applied,
falls through to the comparison above, made after the takeover. A takeover
by the session's own principal costs one digest a CONNECT and 8 bytes a lease.

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

### The authenticator chain

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
the client presented, without the client.
- **Allowed again with the same identity** (principal, attributes, the module
  that allowed): it is held again.
- **Allowed with another identity:** the session ends with DISCONNECT 0x98
  (Administrative action; a plain close at 3.1.1). The client reconnects as it
  now is, rather than keep rights the decision no longer gives.
- **Otherwise:** it ends with 0x87.

In both closing cases the session keeps its state. Its Will is authorized
with the new identity when the principal is the same, and otherwise as the
principal alone, without attributes. A connection admitted without an allow
(anonymous, or a certificate with no certificate authenticator) holds nothing
to make again. A SCRAM session is not re-decided by the node, since nothing can
be checked without the client. The client re-authenticates (§ 4.12.1), and its
certificate, if any, is checked again. A re-authentication that gives another
identity also ends the session with 0x98.

### The connect path's cost

Verifying a password derives a key: 600,000 iterations of PBKDF2, about 62 ms
of one core, so a 32-thread host derives at most 320–373 a second with every
thread busy ([docs/evidence/auth-cost.md](evidence/auth-cost.md)). The node
therefore keeps each password verification that matched,
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
reference Go implementation; [docs/evidence/auth-cost.md](evidence/auth-cost.md)).
The node holds at most `auth.argon2MemoryBytes`
of them at once (default 268,435,456, 256 MiB, at least 1 MiB): a verification
waits for its share, and one whose hash needs more than all of it is refused,
with the reason `…memory cost exceeds auth.argon2MemoryBytes…` rather than a
wrong password. At the defaults four run at once, so a reconnect storm of
Argon2id users is verified at roughly 85 a second (4 at once at about 47 ms
each: 4 ÷ 0.047 s) until the cache answers the repeats. The budget changes only
with a restart.

The connect-rate limits (see ["Listeners"](configuration.md#listeners)) are off by default because no single
rate serves both jobs a limit could have. When a node of a three-node cluster
serving 100,000 clients is lost, the survivors take its clients back within
about three seconds of the loss being declared, 12,017 to 16,438 in the busiest
second, and every client is back 31–33 s after the crash. At 500 a second a
listener, the cap HiveMQ ships, the two survivors admitted at most 1,033–1,148
in any one second and recovery took 56.0–65.2 s; a host of 32 threads derives
about 373 keys a second
([docs/evidence/connect-path.md](evidence/connect-path.md)). A limit
low enough to hold key
derivation inside the host stretches recovery to about a minute, and one high
enough not to slow it holds no derivation back. So the derivation cost is held
by the cache (a client's repeats) and by each listener's `maxHandshakes` (how
many handshakes, their derivations included, run at once). For a listener whose
clients log in by password, size `connectRate` to the derivations the host can
spare: at 62 ms a derivation, 100 a second takes about six cores.

### JWT access tokens (`auth.jwt`)

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

### Certificate revocation (`auth.x509`)

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
credential is decided by its own authenticator. An OCSP answer is cached until
its `nextUpdate` or the end of its lifetime below, whichever comes first, and a
CRL until its `nextUpdate` (at most 100,000 answers and 64 lists); a
CONNECT waits for a fetch at most `fetchWaitMs` (default 2,000) and the fetch
runs to `timeoutMs` (default 5,000) whoever stops waiting. OCSP is written
in-house (RFC 6960 with SHA-1 CertIDs and no nonce, RFC 5019); a response may
be signed by the issuer or by a responder it delegated to. Since a request
carries no nonce, an answer without a `nextUpdate` is accepted only while its
`thisUpdate` is at most `ocspMaxAgeSeconds` old (default 86,400, a day; 60 to
2,592,000), so an old answer replayed after a revocation is refused as stale.
An answer with a `nextUpdate` is accepted until then, but for the leaf
certificate only while its `thisUpdate` is at most `ocspMaxLifetimeSeconds` old
(default 864,000, ten days, the longest a public CA's answer for a subscriber
certificate may run; 3,600 to 31,622,400), and for an intermediate at most 366
days old, so a far-future `nextUpdate` does not keep an old answer usable. A
responder that pre-signs answers without `nextUpdate`, or with a `nextUpdate`
further off than ten days, needs the setting above the age it serves them at.
A CRL is cached for the issuer it was accepted for.

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

### SCRAM enhanced authentication (`auth.scram`)

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
A user the file does not hold is answered with a salt of its own and the
default iteration count, and refused at the last step, as a wrong password is;
a user hashed with another count (`-iterations`) is told apart from unknown
users by it. A CONNECT naming the method must not carry a password, and a
username it carries must be the authenticated user. Re-authentication (AUTH
0x19, § 4.12.1) runs the same exchange during the session; a failure, or
another user, ends it with DISCONNECT 0x87.
Channel binding (`-PLUS`) and SASLprep are not supported: usernames and
passwords are compared as the bytes sent. An unknown method is answered
CONNACK 0x8C.

### LDAP (`auth.ldap`)

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

### The in-broker database (`auth.db`)

```json
"auth.db": {}
```

Client credentials kept in the cluster itself and administered through the
admin API (`/credentials`, ["Admin API"](operations.md#admin-api)) or `hoardmq credentials`. They are records
in the consensus log, so a credential added through any node authenticates on
every node once that node has applied it, and survives restarts. A password is
stored only as a PHC string: the node hashes a password it is given with
PBKDF2 (600,000 iterations) or, with `"scram": true`, into the SCRAM form, or
stores a `passwordHash` that `hoardmq hash-password` wrote; neither the
password nor the hash is ever returned, logged or audited. A known user is
allowed only with the right password (`db: wrong password`); an unknown user
passes. SCRAM exchanges read users files only (`auth.scram`), not this
database.

How many credentials it holds is `cluster.records.maxPerCollection`, the most
records a write from this node may leave one collection of the cluster's
records store holding (credentials, API keys and bans are each a collection):
100,000 by default, 1 to 1,000,000. Every node holds every record, so the cost
is on every node. Measured with PBKDF2 credentials under 36-byte usernames
([docs/evidence/auth-cost.md](evidence/auth-cost.md)):

| | 100,000 | 1,000,000 |
|---|---|---|
| Heap held on each node | 33.3 MB | 405 MB |
| Raft snapshot | 25 MB | 252 MB |
| More heap on a follower staging a snapshot | 30 MB | 625 MB |
| The consensus loop held for a snapshot's capture | 9 ms | 174 ms |

The cap travels with each write, so every node decides a write alike whatever
its own setting; set the same value on every node so that a write's acceptance
does not depend on where it was made. Lowering it removes nothing: a new
username past it is refused (the API answers 409, "the credential database is
full", counted as `hoardmq_records_refused_total{collection,reason="full"}`),
and an existing one still updates. `hoardmq_records_limit` is the node's cap,
and `hoardmq_records{collection}` the count held. The store is meant for up to
a million credentials. Above that, keep them where they already live and
authenticate against it: `auth.http` (a webhook), `auth.sql` (built out of tree
from `contrib/`), or `auth.ldap`.

```sh
hoardmq credentials add -password-file alice.pw -attribute team=blue alice
hoardmq credentials update -password-file alice.pw -scram alice
hoardmq credentials list
hoardmq credentials remove alice
```

The password is read from a file (`-password-file`), or a hash from
`-hash-file`, never from the command line.

### SQL (`auth.sql`, out of tree)

```json
"auth.sql": { "connString": "postgres://hoardmq@db.example.com/mqtt?sslmode=verify-full&sslrootcert=/etc/hoardmq/db-ca.pem",
              "passwordFile": "/run/secrets/db",
              "query": "SELECT password_hash, tenant FROM mqtt_users WHERE username = ${username}" }
```

An authenticator that checks a client's password against a PHC hash a PostgreSQL query reads. It is not built in:
it lives in [contrib/_authsql](../contrib/README.md) as a module of its own, built with `hoardmq-build`, so its driver
(pgx, MIT) is never in the broker's module or the default binary. The broker's own verifier checks the password, so
every form `hoardmq hash-password` writes is accepted (PBKDF2, SCRAM, and Argon2id where the build allows it), and
the password is never sent to the database.

- **The query** is one statement whose first column is the PHC string. Every other column, when not NULL, becomes
  an attribute by its name, as text, at most 32. `${username}`, `${clientid}` and `${cn}` stand where a value would,
  unquoted; each is sent as a parameter, never written into the SQL. A query with no placeholder, an unknown one, or
  a `$1` of its own is refused at `check`.
- **Decisions:**
  - it claims a CONNECT with a non-empty username and password; anything else passes;
  - no row passes (a user the database does not know);
  - more than one row is refused (`sql: ambiguous user`);
  - a NULL or empty hash (`sql: no password hash`), a hash the verifier cannot read
    (`sql: stored credential unreadable`) and a wrong password (`sql: wrong password`) are refused;
  - a match allows with the row's attributes.
- **An allow is held** for the session and made again at `auth.decisionTtlSeconds`, which queries again: a changed
  hash or a removed row ends the session with 0x87 then.
- **A database that does not answer** within `timeoutMs` (default 2,000, 100 to 30,000; a pool connection included),
  or answers with an error, is unavailable. `onUnavailable` decides, as `auth.ldap`'s does: `deny` (the default) or
  `pass`. Each is counted (`hoardmq_auth_sql_unavailable_total{policy}`) and logged at Warn with the database's error;
  decisions are counted by outcome (`hoardmq_auth_sql_decisions_total{outcome}`: `allowed`, `refused`, `unknown`,
  `unavailable`).
- **Its own credential:** `connString` is a libpq URL or keyword string, read by pgx with libpq's defaults (the `PG*`
  environment variables, `~/.pgpass`). A password in it is refused; put it in `passwordFile`, which is never logged.
  A host that is not this machine (loopback or a Unix socket) must be reached with `sslmode=verify-full` or
  `verify-ca` (or `require` with `sslrootcert`), so the password never crosses a network in clear or to an
  unverified server. At most `maxConnections` (default 8, 1 to 64) are held open.

`check` connects to nothing, so a query that is wrong for the schema shows only at the first CONNECT, as an
unavailable answer with Postgres's message. Run the query by hand once. How to copy the module out and build it is in
[contrib/README.md](../contrib/README.md). A live test runs it against a real Postgres:

```
AUTHSQL_LIVE_URL='postgres://postgres@127.0.0.1:7066/postgres?sslmode=disable' AUTHSQL_LIVE_PASSWORD=<password> \
  AUTHSQL_LIVE_CONTAINER=<podman container> go test -count=1 -v -run '^TestAuthSQLLive$' ./cmd/hoardmq-build
```

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
present; `topic` is a filter with `${clientid}`, `${username}`, `${cn}` and
`${attr.<name>}` substituted; a subscribe or unsubscribe of a wildcard filter is
allowed only by a rule that covers every topic it could match. A template value
that is empty or contains a wildcard (`+`, `#`) or a level separator (`/`) makes
the rule unusable for that identity: it neither allows nor denies, and a later
rule decides. The client identifiers the broker assigns never contain them.

`${attr.<name>}` is an attribute the authenticators gave the identity at
CONNECT. Attributes come from:
- `auth.file`'s `attributes`;
- the in-broker database's credentials (`hoardmq credentials add -attribute`);
- `auth.http`'s `client_attrs`;
- `auth.sql`'s columns.

So `{ "topic": "${attr.tenant}/v1/${attr.system}/#", "allow": ["publish",
"subscribe"] }` gives each device its own tenant and system subtree. A name is
letters, digits, `.`, `_` and `-`; a malformed `${attr.` in a topic is refused when
the file loads. An attribute the identity was not given makes the rule unusable,
like an empty value. Substitution is one pass, so a value is never read as a
template. A template only fills in a topic and grants nothing by itself.

### HTTP authentication and authorization (`auth.http`)

```json
"auth.http": { "url": "https://auth.example.com/mqtt",
               "caFile": "/etc/hoardmq/auth-ca.pem",
               "bearerTokenFile": "/run/secrets/auth-http" }
```

An HTTP endpoint decides each CONNECT and each publish, subscribe and
unsubscribe, in EMQX's request and response shapes. Each question is a POST of
one JSON object with the header `hoardmq-hook: <action>`:
- `action`: `connect`, `publish`, `subscribe` or `unsubscribe`;
- `clientid`, `username`, `cert_common_name`, `namespace`, `node`;
- on a CONNECT, `peerhost`, `peername`, `listener`, and `password` when the
  client sent one and `sendPassword` is true (the default);
- on an authorization, `topic`: the topic or filter.

The endpoint answers 204 (allow) or 200 with `{"result": "allow" | "deny" |
"ignore"}`; `ignore` passes to the next module in the chain. A CONNECT's allow
may carry `is_superuser` (allowed every action without a request, held as the
attribute `auth.http.superuser`, `true` or `false` on every allow),
`client_attrs` (attached to the identity) and `expire_at` (Unix seconds). The
authorizer allows every action without a request only to a connection
`auth.http` itself admitted with `is_superuser`. An `auth.http.superuser`
attribute from anywhere else (a users-file user, an `auth.db` credential, a
mechanism, another module, or the endpoint's own `client_attrs`) grants
nothing, and the endpoint is asked as for anyone. An endpoint that stops
answering `is_superuser` ends the bypass at the connection's next re-decision,
which closes it with 0x98. A `cache-control: max-age=N` header holds the answer
N seconds:
- **a CONNECT's allow** until the earlier of `expire_at` and `max-age`, and
  never past `auth.decisionTtlSeconds`; then the broker asks again. An allow
  whose `expire_at` has already passed is a deny;
- **an authorization** for `max-age` within `auth.decisionTtlSeconds`, keyed
  by namespace, client identifier, username, action and topic, at most
  `cache.maxEntries` answers (default 100,000, 0 to 1,000,000; the oldest go
  first, `hoardmq_auth_http_cache_entries`). Without `max-age` nothing is held.

**It fails closed.** An endpoint that cannot be reached, does not answer within
`timeoutMs` (default 2,000, 100 to 30,000), answers anything but 200 or 204, a
redirect (unless `followRedirects`), a body over 64 KiB or one that is not an
answer, is unavailable, and `onUnavailable` decides: `deny` (the default) or
`pass`. EMQX reads a 4xx or 5xx as ignore; here it is no answer. An unavailable
answer is never held, and each is counted by policy
(`hoardmq_auth_http_unavailable_total{policy}`) and logged at Warn. Requests
are counted in `hoardmq_auth_http_requests_total{action,outcome}`, and held
answers in `hoardmq_auth_http_cache_hits_total`.

**Its credentials.** `caFile` verifies the endpoint (default the system roots;
TLS 1.2 at least), `certFile` and `keyFile` present a client certificate, and
`bearerTokenFile` sends `Authorization: Bearer <token>`; `headers` adds fixed
ones. No log line or metric label the module writes carries the client's
password, the token, a header value or the URL. `authenticate: false` or `authorize: false` leaves
that half to other modules.

> **Warning.** With `sendPassword` on, the endpoint receives every client's
> password in clear inside the request. `check` refuses an `http://` URL
> unless its host is loopback; use `https://` with `caFile`, and an endpoint
> you trust with the passwords. Turn `sendPassword` off when the endpoint
> decides on the username, client identifier or certificate alone.

## Single sign-on (OIDC)

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

## API keys

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
