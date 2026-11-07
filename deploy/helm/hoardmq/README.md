# HoardMQ Helm chart

Deploys a HoardMQ cluster as one plain StatefulSet: no operator, no CRDs
by default, mutual TLS between nodes from a chart-generated CA, a preStop
drain that redirects clients before a pod stops, and readiness that requires
a joined, caught-up node on a stable topology.

This chart is a reference: copy it and adapt it to your deployment. HoardMQ
releases are source releases, with no chart repository and no image, so the
chart names no image and refuses to render without `image.repository` and
`image.tag`. Build an image from the reference `Dockerfile` at the release's
tag (docs/operations.md, ["Kubernetes deployment"](../../../docs/operations.md#kubernetes-deployment)), push it to a registry your cluster
pulls from, and name it:

```sh
helm install hoardmq deploy/helm/hoardmq --namespace hoardmq --create-namespace \
  --set image.repository=<registry>/hoardmq --set image.tag=<tag>
```

`Chart.yaml`'s `version` and `appVersion` are the release tag without its
`v`.

## What the chart creates

| Object | Purpose |
|---|---|
| StatefulSet `<release>` | `replicaCount` pods, `podManagementPolicy: Parallel` (the broker's join-not-bootstrap ordering forms the cluster), a volume claim per pod, `terminationGracePeriodSeconds` = drain timeout + confirmation timeout + margin, preStop `hoardmq drain -wait`, readiness on `/readyz`, liveness on `/healthz` |
| Service `<release>-headless` | `publishNotReadyAddresses: true`; peers resolve `<release>-<i>.<release>-headless` before they are ready |
| Service `<release>` | the client-facing MQTT port(s) |
| ConfigMap `<release>` | the broker's JSON configuration; the pod's name becomes `node.name` and its headless DNS name the advertised client address, and `metrics.prometheus` serves the probes, `/metrics` and `/drain` (to loopback callers only, the preStop among them; closed with `admin.enabled`, which adds the admin API on the pod's loopback) |
| Secrets `<release>-cluster-tls`, `<release>-cluster-ca` | one certificate per ordinal, each with the node name as its common name, and the CA with its key, which no pod mounts (`tls.mode: generated`, both kept across upgrades), or one cert-manager `Certificate` per ordinal from a private CA or a named issuer (`tls.mode: certManager`); with `tls.mode: certManagerCSI` no node key is in any Secret, only the private CA's; see "TLS between nodes" |
| Secret `<release>-admin` | with `admin.enabled` only: the admin API's `drain.token` (the preStop's) and `operator.token`, generated once and kept across upgrades and uninstall |
| PodDisruptionBudget | `maxUnavailable: 1` |
| NetworkPolicy `<release>` | with `networkPolicy.enabled` only; see "Security" |
| ServiceMonitor `<release>` | with `serviceMonitor.enabled` only (needs the prometheus-operator CRD); see "Security" |

## Sizing: the N-1 trade-off

A rolling restart, a node failure and a voluntary disruption all run the
cluster one node short. Size for that:

- capacity (connections, throughput, memory) must fit on `replicaCount - 1`
  nodes;
- `cluster.replicas` (copies of each session and retained message) must be
  at most `replicaCount - 1`, so a node loss leaves a copy standing; the
  default 2 copies on 3 pods is the smallest safe deployment;
- a quorum of the consensus core is `floor(replicaCount / 2) + 1` voters;
  with 3 pods, one may be down;
- `persistence.size` holds the journals and two raft snapshots while one is
  built: leave twice `hoardmq_raft_snapshot_bytes` free beside the logs (about
  50 B a client without a Will, more with one).

The drain moves clients off a pod at `cluster.drain.ratePerSecond` (100 a
second by default) or faster: it redirects every client within
`cluster.drain.timeoutSeconds` less the broker's takeover timeout
(`cluster.leases.takeoverTimeoutMs`, 5 s by default, or half the timeout if
that is less), so 25 s at the defaults whatever the pod's client count. A pod
with 20,000 clients is redirected at 800 a second; one with 2,000 at the
rate, in 20 s. The 5 s left serve the last clients' takeovers and a retry of
a failed lease hand-off. What the faster pace costs is under "What a rolling
restart refuses". After handing its leases off, a drain waits up to
`cluster.drain.confirmTimeoutSeconds` (default 30) for every session it
handed off to be confirmed held by the pod that received it; the
termination grace period is both timeouts plus the margin (75 s by
default), so the kubelet does not kill a pod whose drain is still waiting.

## Scaling

`cluster.maxReplicas` (default 7) is the most pods the cluster can grow to
without a rolling restart. Every node dials every ordinal below it, retrying
the names of pods that do not run, so changing `replicaCount` up to it
changes neither the configuration nor the pod template: `helm upgrade --set
replicaCount=4` starts `<release>-3`, which the running pods are already
dialing, and restarts nothing. Lowering it stops the highest pods the same
way; a stopped pod stays a member of the consensus core, counted in its
quorum, until it is removed (see "Scaling down").

An ordinal below the maximum that no pod runs costs its redial on each pod
(every 5 s once the backoff has grown), a `dial failed` line at Debug, and an
entry among the peers of the admin API's overload view. It is not a member:
readiness and the shard map count only the consensus members, and the
self-fence's quorum only its voters. A one-pod release waits the join window
(`cluster.raft.joinTimeoutMs`, 30 s by default) before it starts its cluster,
as several pods already do on a first start.

Growing past the maximum is two upgrades: first raise `cluster.maxReplicas`
alone, which changes the configuration and rolls the pods one at a time, as
any configuration change does; then raise `replicaCount`. The chart refuses
a `replicaCount` above `cluster.maxReplicas`. Both in one upgrade would start
the new pods before the running ones dial them, and the rollout would wait
on a pod that cannot join.

With `tls.mode: certManager` the chart makes a Certificate for every ordinal
below the maximum, and each pod waits for all of them to be issued; see "TLS
between nodes".

## Scaling down

Lowering `replicaCount` stops the highest pods, each after its preStop drain,
and nothing more: a stopped pod stays a consensus member, counted in the
quorum, and its volume claim stays. Removing it is a step of its own, which
needs `admin.enabled`:

1. Lower `replicaCount` (`helm upgrade --set replicaCount=2`).
2. The preStop's drain hands each stopped pod's leases off and marks it
   leaving, so it can usually be removed at once. A pod that stopped without
   its drain is refused (`owns-leases`, or `not-drained` while it is still
   alive by the log) until its leases are adopted after it is declared dead,
   which `hoardmq cluster status` shows as `alive=false`.
3. Remove each, highest ordinal first:

   ```sh
   kubectl exec <release>-0 -- /usr/local/bin/hoardmq cluster remove \
     -admin 127.0.0.1:9091 -token-file /etc/hoardmq/admin/operator.token <release>-2
   ```

   The call goes to the leader, which refuses a removal that would leave the
   voters short of a majority, and answers with the members left.
4. Delete the pod's claim (`kubectl delete pvc data-<release>-2`). A pod
   scaled back up later then starts empty and joins as a new member. On its
   old volume it would restart from a log that ends before its removal, which
   no member answers: it would stay not ready (`recovering`) for good, admitting
   no client, until its claim is deleted and the pod restarted.

Two pods are two voters, both needed for a quorum: a cluster scaled down to two
has no fault tolerance, and to one has a single voter.

## Limits

The broker's own connect-rate, connection, subscription and keep-alive limits
are off by default, and its outbound in-flight cap is 65,535; the chart sets
them, per pod:

| Value | Default | Past it |
|---|---|---|
| `limits.connectRate`, `limits.connectBurst` | 2,000 CONNECTs a second across the pod's listeners; burst 0, twice the rate | the CONNECT is refused before its credentials are read: CONNACK 0x9F at MQTT 5.0, a close at 3.1.1 and 3.1; `hoardmq_connect_rate_refused_total{listener,limit="node"}` |
| `listeners.mqtt.maxConnections`, `listeners.mqtts.maxConnections` | 20,000 connections on each listener | the connection is closed as it is accepted; `hoardmq_max_connections_refused_total{listener}` |
| `limits.maxSubscriptions` | 100 topic filters a client's session holds | the filter is refused in the SUBACK, 0x97 at 5.0 and 0x80 below, and the client stays connected; a filter the session holds is replaced as usual; `hoardmq_subscription_limit_refusals_total{namespace}` |
| `limits.maxKeepAlive` | a keep-alive of at most 3,600 s, a client's 0 included | a 5.0 client asking for more, or for 0, is told 3,600 s as its Server Keep Alive; a 3.1.1 client cannot be told; either is closed, Keep Alive timeout, after 5,400 s of silence |
| `limits.maxInflightMessages` | 1,024 QoS 1 and 2 messages in flight to one client | the rest wait in the session's queue (`limits.maxQueuedMessages`); a client whose Receive Maximum is lower keeps its own |

Each but `limits.maxKeepAlive` and `limits.maxInflightMessages`, which are read
at start, is reloadable without a restart, and 0 turns any but the in-flight cap
off; the chart's pods restart on any
configuration change anyway. Values in `extraConfig.limits`
and `extraConfig.listeners` replace these.

Why these values:

- **The connection bound fits the default memory limit.** At 250,000 clients
  the host's available memory fell by about 11 GiB, from 78 GiB to 67 GiB at
  its lowest sample, with the three nodes on it and the clients on another host
  ([docs/evidence/memory-250k.md](../../../docs/evidence/memory-250k.md), an earlier
  build), about 46 KiB a client with its replicas, so a pod with the default
  1 GiB holds about 22,700. A rolling restart of three pods leaves the last about three quarters
  of all clients (see "What a rolling restart refuses"), so the defaults hold
  about 26,000 clients through a roll and 40,000 through a node loss. For more,
  raise `resources.limits.memory` and both `maxConnections` together: the
  100,000-client figures below need about 75,000 a pod for a roll. With both
  listeners enabled a pod may hold their sum; split the bound between them.
- **The connect rate does not hold back a failover at the measured scale.**
  When a node of three serving 100,000 clients is lost, the two survivors take
  about 16,700 clients each, at most 12,017–16,438 a second across the cluster
  ([docs/evidence/connect-path.md](../../../docs/evidence/connect-path.md)).
  A bucket of 4,000 refilled at
  2,000 a second admits a survivor's share within about 6.4 s of the first,
  against about 3 s unlimited, while every client was back 31–33 s after the
  crash; at the chart's own capacity a survivor's share fits the burst. This is
  arithmetic, not a measurement, and a refused client's own reconnect backoff
  adds to it. In a rolling restart at 100,000 clients the last pod redirects
  3,067 clients a second, about 1,534 a second to each of the other two
  (3,067 ÷ 2), under the rate
  ([docs/evidence/rolling-restart-in-process.md](../../../docs/evidence/rolling-restart-in-process.md)).
  What the limit holds back is a reconnect
  flood or a misbehaving fleet: whatever arrives, a pod starts session work for
  at most 2,000 a second. A limit low enough to bound password derivation
  costs more than it saves (docs/security-and-access.md, ["The connect path's cost"](../../../docs/security-and-access.md#the-connect-paths-cost)); the
  cache and `maxHandshakes` bound that instead.
- **The subscription limit bounds one client's share** of the routing table,
  its session's journal and its replicas. A client that needs many topics
  reaches them through wildcards; 100 distinct filters is a judgement, not a
  measurement. Raise it for clients that hold more by design.
- **The keep-alive bound closes a client that sends nothing.** MQTT lets a client
  send a keep-alive of 0, and the broker's own default honours it with no read
  deadline; TCP keep-alive (15 s idle, then 9 probes 15 s apart) closes only a
  connection whose peer has gone, after about 150 s. A live client that never
  sends holds its connection and session until 5,400 s pass without a packet.
  An hour is a judgement, not a measurement; lower it to reclaim idle
  connections sooner, at the cost of more PINGREQs from clients that were
  idle by design.
- **The in-flight cap bounds what a client that stops acknowledging holds.**
  Without it a 3.1.1 client, which sends no Receive Maximum, may have 65,535
  messages in flight: measured on one broker over loopback with 256-byte
  payloads, a client that stops acknowledging holds about 52.5 MB with its
  1,000 queued, against about 1.52 MB at 1,024 (the broker's
  `TestOutboundInflightScale`, three runs each;
  [docs/evidence/inflight.md](../../../docs/evidence/inflight.md)). At 1,024 a
  client acknowledging at once received 178–186 thousand messages a second,
  against 141–196 thousand without the bound, and one acknowledging 1 ms after
  each message 161–172 thousand, against 128–198 thousand; without the bound
  the test's sender was the limit. A bound of B over a round trip of R
  allows at most B/R messages a second to one client, about 100,000 at 1,024
  and 10 ms; raise it for clients that need more over a long round trip.

The chart sets no per-client message or byte rate.

## Readiness and the rolling restart

`/readyz` is true only when the node is a caught-up member (not one whose
applied index has stopped short of its leader's commit for 20 s, which reads
`behind`), its leases are not fenced, it is not draining, and the membership and alive set have been
unchanged for `cluster.readiness.stableSeconds`. The shard map is not part of
it, so the other pods stay ready while a restarting pod leaves the map and
returns to it; the restarted pod becomes ready once its own view has been
stable that long. The
readiness probe is deliberately fast (`periodSeconds: 2`,
`failureThreshold: 1`) so a draining pod leaves the client Service at once.

The liveness probe on `/healthz` (`periodSeconds: 10`, `timeoutSeconds: 2`,
`failureThreshold: 3`) restarts a pod about 30 s after the broker says it
cannot recover without a restart: its consensus loop stopped on an error, or
its own or its replica journal cannot be rewritten because what the rewrite
reads is damaged. A full or failing disk does not fail it; the pod stays up,
refusing QoS 1 and 2 publishes, until the disk works. A pod restarted over a
damaged journal refuses to start and goes into `CrashLoopBackOff`, its sessions
served from their replicas on the other pods; its volume needs the damaged files
restored or cleared. The startup probe reads `/healthz` too.

Kubernetes runs the preStop hook before SIGTERM: `hoardmq drain -wait`
(through the admin API when `admin.enabled`)
closes the listeners, redirects each MQTT 5 client with `0x9C Use Another
Server` and a Server Reference naming a live pod, waits up to
`cluster.drain.timeoutSeconds` for those clients to be taken over elsewhere,
hands every remaining lease to a live pod, leaves the shard map (below), and waits up to
`cluster.drain.confirmTimeoutSeconds` for that pod to confirm it holds each
session. Sessions left unconfirmed are logged at Warn and counted in
`hoardmq_drain_sessions_unconfirmed_total`. The hook then reads the drain's
completion and exits 0, and the broker exits on the SIGTERM the kubelet sends
next. A status request that times out does not end the hook: it keeps asking
until the drain completes or the grace period ends. The hook runs in the broker's container, and the kernel kills it if the
broker exits first, so after its drain the broker waits up to
`cluster.drain.holdExitSeconds` (15 s by default) for that SIGTERM; without one it exits
when the hold ends.

## What a rolling restart refuses

**The drain's pace.** Each pod redirects its clients by its deadline (see
Sizing), and each takes the clients of the pods drained before it, so in a
rolling restart of three pods the last redirects about three quarters of all
clients in its 25 s. In the in-process harness's rolling restart, with every
drain cut at 75 s as the kubelet cuts it, no drain was cut and nothing was
lost: at 10,000 clients the drains took 25.2–25.3 s and a roll 92 s, refusing
68–137 probe publishes against 287–360 for the build before, which cut its
last drain each time; at 100,000 clients the drains took 25.8–59.7 s (the last
pod 3,067 clients a second) and a roll 94.7–128.2 s, against 1,676 s at a
fixed 100 a second
([docs/evidence/rolling-restart-in-process.md](../../../docs/evidence/rolling-restart-in-process.md)).
At 100,000 clients the last pod's step once refused or failed 18.9–45.1% of the
probe's publishes for about 20 s (in 6 of 6 rolls with node logs on; 41% in 1
of 3 without), while the replication
stream to the pod that rejoined during its drain fell seconds behind on its
acknowledgements. In a later hoardmq build that stream wakes only the waits
an acknowledgement or a refusal can decide. In 3 interleaved node-logged rolls
of each build, the last step refused or failed 1.6–3.9% of the probe against
35.6–41.3%, a roll 2.30–2.83% against 11.66–13.35%, and rolls took 95.1–95.2 s
against 97.2–98.0 s. The second pod's step refused or failed 568–1,016 of 32,000
around the
first pod's restart, against 498 before. Nothing was lost in any roll
([docs/evidence/rolling-restart-in-process.md](../../../docs/evidence/rolling-restart-in-process.md)).
The build before the redirect deadline, its drains cut at the grace, refused
4,280–5,827, failed 4,693–5,743 and dropped 95,723–115,049 connections a roll
at 100,000 clients (the rolls of 2026-10-09 on the same page).

A draining pod leaves the shard map before it stops. Its hand-off marks it
leaving, the map moves the copies it held to the other pods, and the drain
waits until the owners have confirmed those copies held there (each pod's log
shows `drain: placement` with `outcome` `left`). For its down window no
session keeps a replica on it, so a publish does not wait on it. The pod
rejoins the map when it is back, at the next map change or within a third of
the lease ttl.

What is still refused is the routing plane's, around a client's move: a
publish dispatched while its subscriber's pod is going away
(`subscriber-node-down`), or a forward reaching the subscriber's old pod just
after it moved. Each is answered `0x83 Implementation specific error`. On the
runbook below (one publisher at up to 10 messages a second, one durable
subscriber), a restart of all three pods refused 3–23 of 600 publishes in 5
runs, against 103–209 in 5 interleaved runs of the build before it, where the
replication barrier waited on the restarting pod. Every message was delivered
in all 10 ([docs/evidence/rolling-restart-kind.md](../../../docs/evidence/rolling-restart-kind.md)).

Retained publishes are refused while their shard moves. A map change that
moves a retained shard's holders makes its leader refuse retained publishes on
it (0x83) until the new followers hold the shard and the pods that held it
have let their copies go, which keeps a message the leader acknowledged from
dying with a leader lost in between. In the in-process harness's rolling
restart at 10,000 clients, with one retained publisher a pod writing a
retained message a second, 28.4–30.8% of retained publishes were refused over a
roll in 5 runs, an eighth to about a quarter of them for a moving shard, against
37.0–39.8% on the build before, mostly as barrier timeouts there; no retained
message was lost in either
([docs/evidence/rolling-restart-in-process.md](../../../docs/evidence/rolling-restart-in-process.md)).
Since every pod that held a shard also hands its copies to the shard's new
leader at a leader change, 22.5–28.7% were refused in 3 runs, against 27.3–30.5% on
the build before it, and none was lost
([docs/evidence/rolling-restart-in-process.md](../../../docs/evidence/rolling-restart-in-process.md)).
A client that publishes retained messages republishes on 0x83, as for any
refusal.

This needs more pods than copies. A drain never lowers the copy count, so
with `cluster.replicas` equal to `replicaCount` (or two pods and two copies)
the draining pod stays in the map (`placed`). Its down window then refuses
the publishes whose sessions keep a replica on it, as all rolling restarts
did before (99–281 of 600 in the 26 recorded runs from 2026-09-23 to 2026-10-05,
[docs/evidence/rolling-restart-kind.md](../../../docs/evidence/rolling-restart-kind.md)).

This is the deliberate trade: refuse rather than acknowledge with one copy.
A client must read the PUBACK reason code and republish what was refused.
Paho's `wait_for_publish()` reports only the client library's own result and
treats a `0x83` PUBACK as success, so a client that relies on it loses the
guarantee without seeing any error. The routing refusals show as
`hoardmq_forward_failures_total{reason}`; a pod the copy count kept in the map
shows its down window as `hoardmq_replication_barrier_failures_total{peer,reason}`,
with `peer` the restarting pod, and a `replication barrier failed` warning per
publish. Either stops when the rollout does. Refusals that continue after the rollout has
settled are not this trade: see the runbook's step 4.

## Security

**The container** runs with `containerSecurityContext`, on by default:

| Field | What it does |
|---|---|
| `readOnlyRootFilesystem: true` | the image's filesystem cannot be written. The broker writes only its data directory, the volume claim mounted at `/var/lib/hoardmq`; every other mount is read-only |
| `allowPrivilegeEscalation: false` | no process in the container gains privileges its parent lacks (setuid binaries, file capabilities) |
| `capabilities.drop: [ALL]` | no Linux capability; the broker binds no port below 1024 by default and needs none |
| `runAsNonRoot: true` | refuses to start as root; the pod also sets user and group 65532, the image's `nonroot` |
| `seccompProfile: RuntimeDefault` | the container runtime's default system-call filter; the pod sets it too, so it stays if the container's value is replaced |

A value of your own replaces the block whole (`containerSecurityContext: {}`
sets none). An image of your own that writes elsewhere needs that path
mounted writable, with an `emptyDir` in `extraVolumes` and
`extraVolumeMounts`.

**A NetworkPolicy** (`networkPolicy.enabled: false` by default; a policy that
does not fit a deployment cuts its traffic) admits, on ingress only:
- the cluster port (`cluster.port`) from the release's own pods;
- the client ports (`mqtt`, `mqtts` when enabled, every `listeners.extra`) from
  `networkPolicy.clientFrom`;
- the observability port (probes, `/metrics`) from
  `networkPolicy.observabilityFrom`.

Each `from` list takes NetworkPolicy peers (`podSelector`,
`namespaceSelector`, `ipBlock`) verbatim, and an empty one admits any source.
Egress is not restricted: authentication endpoints, Kafka and databases are the
deployment's own. A policy is enforced only by a network plugin that supports
it.

**A ServiceMonitor** (`serviceMonitor.enabled: false` by default) scrapes
`/metrics` on every pod's observability port, through the headless Service,
every `serviceMonitor.interval`. `serviceMonitor.labels` are added to it for a
Prometheus that selects by label. It needs the prometheus-operator CRD: without
it the install fails with "no matches for kind ServiceMonitor".

## Upgrading

```sh
helm upgrade hoardmq deploy/helm/hoardmq -n hoardmq --reuse-values --set image.tag=<new>
kubectl -n hoardmq rollout status statefulset/hoardmq --timeout=30m
```

The StatefulSet replaces one pod at a time, highest ordinal first, each
drained by its preStop and waited for until Ready, so an upgrade refuses what
a rolling restart refuses (above). `helm rollback hoardmq <revision> -n
hoardmq` goes back the same way, before the cluster has raised its feature
level. What a cluster running two builds refuses, how the level is raised and
what cannot be rolled back are in [docs/upgrade.md](../../../docs/upgrade.md).

## TLS between nodes

- `generated` (default): the chart makes a CA and one certificate per
  ordinal, valid for `tls.generatedValidityDays`. The certificates are kept in
  the Secret `<release>-cluster-tls` and the CA, with its key, in
  `<release>-cluster-ca`; both are annotated `helm.sh/resource-policy: keep`
  and reused on every upgrade (the templates look them up). An upgrade that
  raises `replicaCount` signs each new ordinal's certificate with the stored
  CA and keeps the others'.
- `certManager`: one cert-manager Certificate per ordinal below
  `cluster.maxReplicas`, whether or not a pod runs it: every pod's volume
  names all of their Secrets, and a pod waits to start until they are issued.
  With `tls.certManager.issuerRef` empty the chart makes a private CA (a
  self-signed Issuer, a CA Certificate in `<release>-cluster-ca`, and a CA
  Issuer that signs the nodes'); otherwise the named issuer signs them and
  must put its CA in each Secret's `ca.crt`. cert-manager renews them; a pod
  reads its certificate at start, so a renewal is in force from its next
  restart.
- `certManagerCSI`: no pod starts, a restart included, while cert-manager or
  its issuer is down. In exchange no Secret holds a node key: cert-manager's
  CSI driver makes each pod's key on its node, writes it with the pod's
  certificate and the CA into that pod's volume alone, and destroys it with
  the pod. The common name is the pod's name, with both auth usages, valid
  for `tls.certManagerCSI.duration` (2160h) and renewed in the volume
  `tls.certManagerCSI.renewBefore` (360h) before it ends; the broker reads it
  at start, so a renewal is in force from the container's next start. The
  issuer is the private CA above, or `tls.certManager.issuerRef`, which must
  not be a self-signed issuer (the driver refuses those). A deleted pod comes
  back with a new key. Prerequisites, neither set up by this chart:
  cert-manager, and the CSI driver on every node:

  ```sh
  helm install cert-manager-csi-driver oci://quay.io/jetstack/charts/cert-manager-csi-driver \
    --version v0.16.1 -n cert-manager --wait
  kubectl get csidrivers csi.cert-manager.io
  ```

  cert-manager's default approver approves the driver's requests to a
  cert-manager.io issuer; a cluster that turns it off (`disableAutoApproval`)
  needs an approver of its own for them.

**Switching to `certManagerCSI`.** From `certManager` with the same issuer
(the private CA or a named one), `helm upgrade` rolls the pods one at a time
and the cluster keeps serving: both modes sign from the same CA. From
`generated` the CA changes, and while the upgrade rolls, a pod on the new CA
and one on the old refuse each other. Switch from `generated` with a full
restart instead: `kubectl scale statefulset/<release> --replicas=0`, then
`helm upgrade` with the new mode, which starts every pod on the new CA
together; no client is served between the two. Either way, delete what the
old mode leaves holding node keys: `<release>-cluster-tls` (`generated`,
kept by its resource policy), or each `<release>-cluster-tls-<i>`
(`certManager`; cert-manager keeps a deleted Certificate's Secret unless it
runs with `--enable-certificate-owner-ref`).

A peer accepts a connection only when the certificate's common name is the
node name the peer claims and the certificate carries both the client auth
and server auth usages, which the chart's certificates do (cert-manager adds
them only when asked, and the chart asks). An issuer that drops requested
usages yields certificates the peers refuse.

**Each container sees only its own key.** The broker container mounts its
own certificate, key and the CA out of the volume by `subPathExpr` on the
pod's name; no pod mounts `<release>-cluster-ca`. The pod's volume itself, on
the node's kubelet, still holds every ordinal's certificate and key: a pod
template is shared by every ordinal and cannot name one ordinal's Secret.
Keys that never leave their own pod need cert-manager's CSI driver
(`csi-driver`): `tls.mode: certManagerCSI`.

**An old Secret.** A `<release>-cluster-tls` Secret made by an earlier chart
has no stored CA key. It is kept as it is, but an upgrade that needs a
certificate for an ordinal it lacks fails, naming the Secret: delete
`<release>-cluster-tls`, upgrade, then restart every pod (`kubectl rollout
restart statefulset/<release>`), since the running pods hold the old CA. The
same applies when the two Secrets hold different CAs.

**Rotation** (`generated`): delete both Secrets, upgrade, and restart every
pod.

The nodes verify each other's chain against the CA; the cluster port is
never exposed outside the headless Service.

## Configuration

`extraConfig` is merged over the generated document, so modules, limits and
MQTT capabilities are set there:

```yaml
extraConfig:
  modules:
    auth.file: { path: /etc/hoardmq/users.json }
    authz.file: { path: /etc/hoardmq/acl.json }
  limits:
    maxQueuedMessages: 5000
```

`extraConfig.listeners` replaces the whole list, the built-in `mqtt` included.
To add a listener beside them, use `listeners.extra`. Each entry has a `name`
(a port name: lower case, at most 15 characters), a `type` and a `port`, and
every other member goes to the broker's listener as written. Each gets a
container port and a port of its name on the client Service:

```yaml
listeners:
  extra:
    - { name: ws, type: ws, port: 8080, path: /mqtt }
```

An entry without `name`, `type` or `port`, or with a name already used
(`mqtt`, `mqtts` or another entry's), fails the render.

Mount the referenced files with `extraVolumes` and `extraVolumeMounts`
(standard Kubernetes volume and volumeMount entries, verbatim), for example
a Secret holding `users.json` mounted read-only under `/etc/hoardmq-auth`.
Mount them beside `/etc/hoardmq`, not inside it: that path is the
configuration ConfigMap's own read-only mount.

**Authorization is deny-by-default.** With no authorization module loaded
every publish, subscribe and unsubscribe is refused, so `auth.anonymous:
true` on its own produces a cluster that connects clients and delivers
nothing. A test cluster needs an ACL as well — see the runbook below.
Anonymous access is the quickest way to a test cluster and the wrong way to
production.

Outside Kubernetes (bare containers, systemd), where no preStop hook runs
the drain, set `cluster.drain.onSignal: true` in the configuration document
so SIGTERM drains before exiting; a second signal exits at once.

The observability listener binds all interfaces inside the pod so the
kubelet's probes and a Prometheus scrape reach it. Its `/drain` answers only
loopback callers, which the preStop is, and refuses any other with 403.
Anything sharing the pod's network namespace is a loopback caller: another
container in the pod, `kubectl port-forward`, and a sidecar proxy that
forwards inbound connections to the broker from a loopback address, which
makes every caller look local. Where that matters, enable the admin API: its
drain needs the preStop's token, and it closes the listener's `/drain`.

## The admin API

`admin.enabled: true` (default false) configures each broker's admin REST API
on the pod's loopback, `127.0.0.1:<admin.port>` (default 9091): it is reached
with `kubectl port-forward`, never over the network, so it needs no TLS. The
chart generates the Secret `<release>-admin` holding two tokens, mounted at
`/etc/hoardmq/admin`:

- `drain.token`, scoped `cluster:drain` and `cluster:read`: the preStop runs
  `hoardmq drain -admin 127.0.0.1:<admin.port> -token-file
  /etc/hoardmq/admin/drain.token -wait`, because the admin API closes the
  observability listener's `/drain`;
- `operator.token`, every scope, for people:

```sh
kubectl -n hoardmq port-forward pod/hoardmq-0 9091:9091 &
export HOARDMQ_ADMIN_TOKEN=$(kubectl -n hoardmq get secret hoardmq-admin -o jsonpath='{.data.operator\.token}' | base64 -d)
hoardmq cluster status
hoardmq clients list -search sensor
```

Both tokens are kept across upgrades (`lookup`) and survive `helm uninstall`
(`helm.sh/resource-policy: keep`); delete the Secret to rotate them, then
restart the pods. More identities, roles, single sign-on (`extraConfig.admin.roles`,
`extraConfig.admin.oidc`), TLS and certificate identities go in
`extraConfig.admin` as docs/operations.md, ["Admin API"](../../../docs/operations.md#admin-api) and
docs/security-and-access.md, ["Single sign-on (OIDC)"](../../../docs/security-and-access.md#single-sign-on-oidc) describe; an ID token from
`hoardmq login` works through the port-forward like any bearer token. An API
key is the cluster's: created through any pod's API, it authenticates on
every pod that has applied it, and its revocation answers 200 once every
live pod has applied that (docs/security-and-access.md, ["API keys"](../../../docs/security-and-access.md#api-keys)). `mergeOverwrite`
replaces the `admin.tokens` list as a whole, so repeat the two above if the
preStop is to keep working. The chart refuses to render `extraConfig.admin`
without `admin.enabled`: the admin API closes `/drain`, and the preStop would
still post to it.

## Runbook: rolling restart without loss (acceptance test)

Run on a single-node kind cluster with the podman provider (`export
KIND_EXPERIMENTAL_PROVIDER=podman`); `docker` in place of `podman` works the
same way.

The clients run **inside** the cluster. `kubectl port-forward` binds one pod
and dies when that pod restarts, and the Server Reference a drain sends names
`<pod>.<headless>.<namespace>.svc`, which resolves only in the cluster. A
client outside would be measuring the port-forward, not the broker.

### 1. A local cluster and the images

```sh
kind create cluster --name hoardmq --wait 120s

# The broker image, and a client image with Paho.
podman build --build-arg VERSION="$(git describe --tags --always --dirty)" \
  -t docker.io/library/hoardmq:dev .
mkdir -p /tmp/mqttclient && cat > /tmp/mqttclient/Dockerfile <<'EOF'
FROM docker.io/library/python:3.12-slim
RUN pip install --no-cache-dir paho-mqtt==2.1.0
EOF
podman build -t docker.io/library/mqttclient:dev /tmp/mqttclient

# An image archive is the route that works with the podman provider.
# Tag the images under docker.io/library so the kubelet's short-name
# resolution finds them from `image: hoardmq:dev`.
for i in hoardmq mqttclient; do
  podman save docker.io/library/$i:dev -o /tmp/$i.tar
  kind load image-archive /tmp/$i.tar --name hoardmq
done
```

### 2. Install with anonymous access and an allow-all ACL

Authorization is deny-by-default: without an authorization module the broker
accepts connections and refuses every publish and subscribe. Mount an ACL.

```sh
helm lint deploy/helm/hoardmq

kubectl create namespace hoardmq
kubectl -n hoardmq create configmap hoardmq-acl \
  --from-literal=acl.json='{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}'

helm install hoardmq deploy/helm/hoardmq -n hoardmq \
  --set image.repository=hoardmq --set image.tag=dev \
  --set auth.anonymous=true \
  --set 'extraConfig.modules.authz\.file.path=/etc/hoardmq-acl/acl.json' \
  --set-json 'extraVolumes=[{"name":"acl","configMap":{"name":"hoardmq-acl"}}]' \
  --set-json 'extraVolumeMounts=[{"name":"acl","mountPath":"/etc/hoardmq-acl","readOnly":true}]'

kubectl -n hoardmq rollout status statefulset/hoardmq --timeout=300s
```

### 3. A durable QoS 1 subscriber and a QoS 1 publisher

Paho 2.x needs the version 2 callback API, and MQTT 5 durability is
`clean_start=False` plus a session expiry — `clean_session=` is a 3.1.1
argument and is rejected alongside `protocol=mqtt.MQTTv5`.

```python
# sub.py — resubscribes only when the broker reports no session, so a
# reconnect that keeps the session is measured, not papered over.
import time
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

TOTAL, DEADLINE, seen = 600, 300, set()

def on_connect(c, u, flags, rc, props=None):
    print(f"connect rc={rc} session_present={flags.session_present}", flush=True)
    if not flags.session_present:
        c.subscribe("t/x", qos=1)

def on_disconnect(c, u, flags, rc, props=None):
    ref = getattr(props, "ServerReference", None) if props is not None else None
    code = getattr(rc, "value", rc)
    print(f"disconnect rc={rc} code={code} server_reference={ref}", flush=True)

props = Properties(PacketTypes.CONNECT)
props.SessionExpiryInterval = 3600
c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="sub", protocol=mqtt.MQTTv5)
c.on_connect, c.on_disconnect = on_connect, on_disconnect
c.on_message = lambda cl, u, m: seen.add(int(m.payload))
c.connect("hoardmq", 1883, keepalive=30, clean_start=False, properties=props)
c.loop_start()
start = time.time()
while len(seen) < TOTAL and time.time() - start < DEADLINE:
    time.sleep(1)
print("received", len(seen), "missing", sorted(set(range(1, TOTAL + 1)) - seen), flush=True)
```

```python
# pub.py — publishes only while connected AND reads the PUBACK reason code.
# wait_for_publish() reports only Paho's own local result: a PUBACK carrying
# 0x83 satisfies it. The broker's answer arrives through on_publish, and a
# publisher that does not read it will score refusals as deliveries.
import threading, time
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

TOTAL, connected, cv, codes = 600, threading.Event(), threading.Condition(), {}
ok, refused = set(), {}

def on_publish(c, u, mid, rc=None, props=None):
    with cv:
        codes[mid] = getattr(rc, "value", rc)
        cv.notify_all()

props = Properties(PacketTypes.CONNECT)
props.SessionExpiryInterval = 3600
c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="pub", protocol=mqtt.MQTTv5)
c.on_connect = lambda cl, u, f, rc, p=None: connected.set()
c.on_disconnect = lambda cl, u, f, rc, p=None: connected.clear()
c.on_publish = on_publish
c.connect("hoardmq", 1883, keepalive=30, clean_start=False, properties=props)
c.loop_start()
time.sleep(5)
for i in range(1, TOTAL + 1):
    while not connected.wait(timeout=180):
        pass
    info = c.publish("t/x", str(i), qos=1)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        continue
    info.wait_for_publish(timeout=180)
    with cv:
        deadline = time.time() + 10
        while info.mid not in codes and time.time() < deadline:
            cv.wait(timeout=1)
        code = codes.pop(info.mid, None)
    (ok.add(i) if code in (0, 16) else refused.setdefault(i, code))
    time.sleep(0.1)
print("acked", len(ok), "refused", len(refused), sorted(refused.items())[:20], flush=True)
```

Run each in a pod on the `mqttclient:dev` image with the scripts in a
ConfigMap (`kubectl -n hoardmq create configmap mqtt-clients --from-file=sub.py
--from-file=pub.py`), the subscriber first so its session exists before the
publisher starts.

### 4. Meanwhile, the rolling restart

```sh
kubectl -n hoardmq rollout restart statefulset/hoardmq
kubectl -n hoardmq rollout status statefulset/hoardmq --timeout=600s
kubectl -n hoardmq logs mqtt-sub -f
```

Expected: every message arrives at the subscriber (`received 600 missing []`).
Expect a few refusals **during** the rollout: 3–23 of 600 in the 5 runs of
2026-10-05 with each draining pod leaving the shard map, all of them the routing
plane's around a client's move (see "What a rolling restart refuses", and
[docs/evidence/rolling-restart-kind.md](../../../docs/evidence/rolling-restart-kind.md)). A
client must honour them and republish. Each pod's log shows `drain:
placement` with `outcome` `left`; `placed` means the copy count kept the pod
in the map, and its down window then refuses as before. What
must not happen is refusals that continue after the rollout has settled: that
means a node is still routing to a peer that no longer holds the subscriber.
Check with `hoardmq_routes_active{scope="remote"}` on the node holding it,
which should be 0. The clients log `Use another server`
disconnects with code 156 (0x9C) and a Server Reference naming a live pod,
and reconnect with `session_present=True`; a one-second poll of the Ready
condition shows one pod not ready at a time for the length of its drain plus
the stability window, and all three not ready together only while the
topology settles after the cluster forms (a shard-map change alone does not
reset readiness); no pod is killed before its drain
completes (the grace period is 75 s by default, and a drain redirects its
clients within its first 25 s at any client count).

No `Warning FailedPreStopHook` event is expected: each pod's log shows `drain
complete`, then `drain complete; waiting for the stop signal`, then `stop
signal after the drain; exiting` about a second later, when the hook has
returned. A `FailedPreStopHook` means a drain that did not finish within the
grace period, or a hook that could not reach its node. The kubelet's journal
on the node gives the hook's exit code, which the event does not carry: `exited
with 1` is a hook that gave up waiting or was refused, `137` a hook killed with
its broker. A hook that wrote to stderr is logged over several lines (`err=<`
… `>`), the exit code on the second. With `cluster.drain.holdExitSeconds` set to 0 a broker exits as
its drain completes, and its hook is killed with it, recording the event,
unless the hook happened to read the completion first.

### Recorded runs

`deploy/helm/rolling-restart/` holds the runbook as a recorded run:
`run.sh <out-dir> <image-tag>` performs steps 2–4 in a fresh namespace
(`CTX` names the kube context, `REPOSITORY` the image's repository,
`hoardmq` by default) with step 3's scripts extended to write one
JSON line per event, and keeps every pod incarnation's log, every pod's
refusal counters once a second, the pods' readiness and the namespace's
events. It keeps the pods' logs through the namespace's deletion, which
stops every pod through its preStop hook, and on a kind cluster the node's
kubelet lines about preStop hooks (`kubelet-prestop.txt`; set
`KIND_EXPERIMENTAL_PROVIDER=podman` for the podman provider). `HELM_ARGS`
adds install arguments: with `HELM_ARGS="--set admin.enabled=true"` the run
also keeps each pod's audit events
(`audit-<pod>.json`, read with `hoardmq audit tail` through the pod's own
loopback admin API and the operator token; `ADMIN_PORT` if `admin.port` is
changed), where each restart shows a `cluster.drain` by
`token:prestop-drain` followed by the drain's `server.drain` `complete`.
`attribute.py <out-dir>...` classifies every refused publish by the path that
refused it — the replication barrier (and on which peer, and whether that
peer is a replica target of the publisher's or the subscriber's session), the
forward path, or dispatch — and totals the counters over pod incarnations.

Last executed: 2026-11-03, five recorded runs on kind (node v1.37.0, kind v0.33.0,
podman provider): two of that day's `main` interleaved with two of the hardened
container's chart (the security context, read-only root, on by default), and one more of the
latter: 1 and 33 refused against 39, 22 and 5, all refusals around the
subscriber's move (`attribute.py`: dispatch to a node going down, the routing
barrier), but one replication-barrier refusal in a run of `main`. All 600 delivered
in each, no preStop hook failure.
Before that, 2026-10-05 (evening), eleven recorded runs on kind (node v1.37.0,
podman provider), five of that day's `main` interleaved with five of the build
that takes a draining pod out of the shard map, and one more of that build with
`admin.enabled`: 103–209 refused against 3–23, all 600 delivered
in each, no preStop hook failure. Before that,
2026-10-05, three recorded runs of the build that holds the broker's exit for
the stop signal: with `admin.enabled`, without, and with
`cluster.drain.holdExitSeconds=0`; all 600 messages delivered in each, no
`FailedPreStopHook` in the first two and every hook killed in the third. Before
that, 2026-10-04, two runs of that day's build, with `admin.enabled` and
without, and 2026-09-23, sixteen runs of that day's build and of the same with
the replication barrier narrowed to the targets of its keys, all messages
delivered in each. Each set is summarised in
[docs/evidence/rolling-restart-kind.md](../../../docs/evidence/rolling-restart-kind.md).

### Cleanup

`helm uninstall hoardmq -n hoardmq` keeps the TLS Secrets and the volumes;
delete them by hand if the data is not wanted. Because the Secrets are kept,
reinstalling after a chart change reuses the old certificates — delete the
namespace for a clean run. On an ephemeral cluster, `kind delete cluster
--name hoardmq`.
