# HoardMQ Helm chart

Deploys a HoardMQ cluster as one plain StatefulSet: no operator, no CRDs
by default, mutual TLS between nodes from a chart-generated CA, a preStop
drain that redirects clients before a pod stops, and readiness that requires
a joined, caught-up node on a stable topology.

```sh
helm install hoardmq deploy/helm/hoardmq --namespace hoardmq --create-namespace \
  --set image.repository=hoardmq --set image.tag=dev
```

## What the chart creates

| Object | Purpose |
|---|---|
| StatefulSet `<release>` | `replicaCount` pods, `podManagementPolicy: Parallel` (the broker's join-not-bootstrap ordering forms the cluster), a volume claim per pod, `terminationGracePeriodSeconds` = drain timeout + confirmation timeout + margin, preStop `hoardmq drain -wait`, readiness on `/readyz`, liveness on `/healthz` |
| Service `<release>-headless` | `publishNotReadyAddresses: true`; peers resolve `<release>-<i>.<release>-headless` before they are ready |
| Service `<release>` | the client-facing MQTT port(s) |
| ConfigMap `<release>` | the broker's JSON configuration; the pod's name becomes `node.name` and its headless DNS name the advertised client address, and `metrics.prometheus` serves the probes, `/metrics` and `/drain` (closed with `admin.enabled`, which adds the admin API on the pod's loopback) |
| Secret `<release>-cluster-tls` | the cluster CA and one certificate per ordinal, each with the node name as its common name (`tls.mode: generated`, kept across upgrades), or one cert-manager `Certificate` per ordinal (`tls.mode: certManager`) |
| Secret `<release>-admin` | with `admin.enabled` only: the admin API's `drain.token` (the preStop's) and `operator.token`, generated once and kept across upgrades and uninstall |
| PodDisruptionBudget | `maxUnavailable: 1` |

## Sizing: the N-1 trade-off

A rolling restart, a node failure and a voluntary disruption all run the
cluster one node short. Size for that:

- capacity (connections, throughput, memory) must fit on `replicaCount - 1`
  nodes;
- `cluster.replicas` (copies of each session and retained message) must be
  at most `replicaCount - 1`, so a node loss leaves a copy standing; the
  default 2 copies on 3 pods is the smallest safe deployment;
- a quorum of the consensus core is `floor(replicaCount / 2) + 1` voters;
  with 3 pods, one may be down.

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

## Readiness and the rolling restart

`/readyz` is true only when the node is a caught-up member, its leases are
not fenced, it is not draining, and the membership and alive set have been
unchanged for `cluster.readiness.stableSeconds`. The shard map is not part of
it, so the other pods stay ready while a restarting pod leaves the map and
returns to it; the restarted pod becomes ready once its own view has been
stable that long. The
readiness probe is deliberately fast (`periodSeconds: 2`,
`failureThreshold: 1`) so a draining pod leaves the client Service at once.

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
pod about 3,070 clients a second) and a roll 95–128 s, against 1,676 s at a
fixed 100 a second (hoardmq `sim/results/roll2251b-10k-2026-10-09/`,
`roll2251b-100k-2026-10-09/`). **A known cost at that size:** in 1 of 3 rolls
the last pod's step refused about 41% of the probe's publishes (11,542
refused and 718 failed without an acknowledgement over the roll, against at
most 2,965 and 269 at a fixed 1,050 a second); the other rolls refused
3,370–4,163. Nothing was lost in any roll. The spike comes with replication
barrier timeouts on one receiver, and it is investigated as hoardmq work
item 2254. The build before B, its drains cut at the grace, refused
4,280–5,827, failed 4,693–5,743 and dropped about 100,000 connections a
roll.

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
in all 10 (hoardmq `sim/results/leaving-2026-10-05/`).

Retained publishes are refused while their shard moves. A map change that
moves a retained shard's holders makes its leader refuse retained publishes on
it (0x83) until the new followers hold the shard and the pods that held it
have let their copies go, which keeps a message the leader acknowledged from
dying with a leader lost in between. In the in-process harness's rolling
restart at 10,000 clients, with one retained publisher a pod writing a
retained message a second, 28–31% of retained publishes were refused over a
roll in 5 runs, an eighth to a quarter of them for a moving shard, against
37–40% on the build before, mostly as barrier timeouts there; no retained
message was lost in either (hoardmq `sim/results/roll2207-10k-2026-10-08/`).
Since every pod that held a shard also hands its copies to the shard's new
leader at a leader change, 23–29% were refused in 3 runs, against 27–31% on
the build before it, and none was lost
(`sim/results/roll2269-10k-2026-10-09/`).
A client that publishes retained messages republishes on 0x83, as for any
refusal.

This needs more pods than copies. A drain never lowers the copy count, so
with `cluster.replicas` equal to `replicaCount` (or two pods and two copies)
the draining pod stays in the map (`placed`). Its down window then refuses
the publishes whose sessions keep a replica on it, as all rolling restarts
did before (99–277 of 600, `sim/results/rollingrestart-2026-09-23/`).

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

## TLS between nodes

- `generated` (default): the chart makes a CA and one certificate per
  ordinal, valid for `tls.generatedValidityDays`, stores them in a Secret
  annotated `helm.sh/resource-policy: keep`, and reuses that Secret on every
  upgrade (the template looks it up). Rotate by deleting the Secret and
  upgrading.
- `certManager`: an Issuer (self-signed unless `tls.certManager.issuerRef`
  names one) and one Certificate per ordinal; cert-manager rotates.

A peer accepts a connection only when the certificate's common name is the
node name the peer claims, so the certificates are per-node, not per-release:
each pod is pointed at its own through `HOARDMQ_CLUSTER_AUTH_TLS_CERT_FILE`,
built from the pod's name. A Secret kept from an earlier release therefore
has certificates only for the ordinals that existed when it was created;
raising `replicaCount` means deleting the Secret and upgrading.

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
kubelet's probes reach it; `/metrics` and `/drain` are on the same port.
Restrict it with a NetworkPolicy if pods in other namespaces must not reach
it, or enable the admin API, which closes that `/drain`.

## The admin API

`admin.enabled: true` (default false) configures each broker's admin REST API
on the pod's loopback, `127.0.0.1:<admin.port>` (default 9091): it is reached
with `kubectl port-forward`, never over the network, so it needs no TLS. The
chart generates the Secret `<release>-admin` holding two tokens, mounted at
`/etc/hoardmq/admin`:

- `drain.token`, scoped `cluster:drain` and `cluster:read`: the preStop runs
  `hoardmq drain -admin 127.0.0.1:<admin.port> -token-file
  /etc/hoardmq/admin/drain.token -wait`, because the admin API closes the
  observability listener's unauthenticated `/drain`;
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
`extraConfig.admin` as the broker's README describes; an ID token from
`hoardmq login` works through the port-forward like any bearer token. An API
key is the cluster's: created through any pod's API, it authenticates on
every pod that has applied it, and its revocation answers 200 once every
live pod has applied that (the broker's README, "API keys"). `mergeOverwrite`
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
podman build -t docker.io/library/hoardmq:dev .
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
plane's around a client's move (see "What a rolling restart refuses"). A
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
(`CTX` names the kube context) with step 3's scripts extended to write one
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

Last executed: 2026-10-05 (evening), eleven recorded runs on kind (node v1.37.0,
podman provider), five of `main` at `27e8aa1` interleaved with five of WI 1788's
`1b6881b`, which takes a draining pod out of the shard map, and one more of
`1b6881b` with `admin.enabled`: 103–209 refused against 3–23, all 600 delivered
in each, no preStop hook failure (`sim/results/leaving-2026-10-05/`). Before that,
2026-10-05, three recorded runs of hoardmq `d524845`: with `admin.enabled`, without, and with
`cluster.drain.holdExitSeconds=0`; all 600 messages delivered in each, no
`FailedPreStopHook` in the first two and every hook killed in the third
(`sim/results/rollingrestart-2026-10-05/`). Before that, 2026-10-04, two runs
of `7bfa424` (`sim/results/rollingrestart-2026-10-04/`), and 2026-09-23,
sixteen runs of `c2da075` and `3897466`, all messages delivered in each
(`sim/results/rollingrestart-2026-09-23/`).

### Cleanup

`helm uninstall hoardmq -n hoardmq` keeps the TLS Secret and the volumes;
delete them by hand if the data is not wanted. Because the Secret is kept,
reinstalling after a chart change reuses the old certificates — delete the
namespace for a clean run. On an ephemeral cluster, `kind delete cluster
--name hoardmq`.
