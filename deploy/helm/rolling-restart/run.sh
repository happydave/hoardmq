#!/usr/bin/env bash
# One execution of the chart README's rolling-restart runbook, recorded so
# every refused publish can be attributed: the publisher's and subscriber's
# events, every broker pod incarnation's log, the refusal counters of every
# pod once a second, and the pods' readiness once a second.
#
#   run.sh <out-dir> <image-tag>
#
# Env: CTX (kube context, default kind-hoardmq), NS (default hoardmq),
# CHART (default ../hoardmq beside this file), CLIENT_IMAGE (default
# mqttclient:dev), HELM_ARGS (more arguments for helm install, split on
# whitespace, e.g. "--set admin.enabled=true"), ADMIN_PORT (default 9091).
# The images must already be loaded into the cluster.
#
# The run also keeps the namespace's events and, with the admin API enabled,
# each broker pod's audit events, read through the pod's own loopback admin
# API with the operator token. The pods' logs are kept through the
# namespace's deletion, which stops every pod through its preStop hook, and on
# a kind context the node's kubelet lines about preStop hooks, which carry the
# hook's exit code (KIND_EXPERIMENTAL_PROVIDER names the container engine,
# docker by default).
set -u
OUT=$1
TAG=$2
CTX=${CTX:-kind-hoardmq}
NS=${NS:-hoardmq}
HERE=$(cd "$(dirname "$0")" && pwd)
CHART=${CHART:-$HERE/../hoardmq}
CLIENT_IMAGE=${CLIENT_IMAGE:-mqttclient:dev}
ADMIN_PORT=${ADMIN_PORT:-9091}
read -r -a EXTRA <<<"${HELM_ARGS:-}"
PODS="hoardmq-0 hoardmq-1 hoardmq-2"

k() { kubectl --context "$CTX" -n "$NS" "$@"; }
note() { echo "$(date -u +%FT%T.%3NZ) $*" | tee -a "$OUT/run.log"; }
epoch() { date +%s.%N; }

mkdir -p "$OUT/logs"
if [ -e "$OUT/run.log" ]; then
  echo "refusing: $OUT already holds a run" >&2
  exit 2
fi
START=$(date +%s)
note "run start tag=$TAG ctx=$CTX ns=$NS helm_args=${HELM_ARGS:-}"

# A namespace left from an earlier run would hand this one its TLS Secret and
# volumes; start from none.
kubectl --context "$CTX" delete namespace "$NS" --ignore-not-found --wait=true >/dev/null
kubectl --context "$CTX" create namespace "$NS" >/dev/null
k create configmap hoardmq-acl \
  --from-literal=acl.json='{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}' >/dev/null
helm --kube-context "$CTX" install hoardmq "$CHART" -n "$NS" \
  --set image.tag="$TAG" \
  --set auth.anonymous=true \
  --set 'extraConfig.modules.authz\.file.path=/etc/hoardmq-acl/acl.json' \
  --set-json 'extraVolumes=[{"name":"acl","configMap":{"name":"hoardmq-acl"}}]' \
  --set-json 'extraVolumeMounts=[{"name":"acl","mountPath":"/etc/hoardmq-acl","readOnly":true}]' \
  "${EXTRA[@]}" >"$OUT/helm-install.txt"
if ! k rollout status statefulset/hoardmq --timeout=300s >>"$OUT/run.log"; then
  note "precondition failed: the StatefulSet never became ready"
  exit 3
fi
k get pods -o wide >"$OUT/pods-installed.txt"
k get pods -o jsonpath='{range .items[*]}{.metadata.name} {.spec.containers[0].image}{"\n"}{end}' >"$OUT/images.txt"
note "installed"

# Every incarnation of every broker pod: a new pod of the same name is
# streamed from its first line, so a re-attach repeats lines, which the
# analysis removes.
for p in $PODS; do
  ( while [ ! -e "$OUT/stop" ]; do
      kubectl --context "$CTX" -n "$NS" logs -f --timestamps "$p" >>"$OUT/logs/$p.log" 2>>"$OUT/logs/$p.err"
      sleep 0.3
    done ) &
done
( while [ ! -e "$OUT/stop" ]; do
    echo "$(epoch) $(k get pods -o jsonpath='{range .items[*]}{.metadata.name}={.metadata.uid}/{.status.podIP}/{range .status.conditions[?(@.type=="Ready")]}{.status}{end} {end}' 2>/dev/null)"
    sleep 1
  done >>"$OUT/readiness.txt" ) &

k create configmap mqtt-clients --from-file="$HERE/pub.py" --from-file="$HERE/sub.py" --from-file="$HERE/sampler.py" >/dev/null
client() {
  cat <<EOF | k apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: $1
spec:
  restartPolicy: Never
  containers:
    - name: c
      image: $CLIENT_IMAGE
      imagePullPolicy: IfNotPresent
      command: ["python", "-u", "/c/$2"]
      env:
        - { name: NS, value: "$NS" }
        - { name: DURATION, value: "900" }
      volumeMounts:
        - { name: c, mountPath: /c }
  volumes:
    - name: c
      configMap: { name: mqtt-clients }
EOF
}
waitlog() { # pod pattern timeout
  local end=$(( $(date +%s) + $3 ))
  until k logs "$1" 2>/dev/null | grep -q "$2"; do
    [ "$(date +%s)" -ge "$end" ] && return 1
    sleep 0.5
  done
}

client mqtt-sampler sampler.py
client mqtt-sub sub.py
if ! waitlog mqtt-sub '"ev": "connect"' 120; then
  note "precondition failed: the subscriber never connected"
  touch "$OUT/stop"; exit 3
fi
note "subscriber connected"
client mqtt-pub pub.py
if ! waitlog mqtt-pub '"ev": "pub"' 120; then
  note "precondition failed: the publisher never published"
  touch "$OUT/stop"; exit 3
fi
note "traffic started"
sleep 15
note "rollout restart"
echo "rollout_start $(epoch)" >>"$OUT/times.txt"
k rollout restart statefulset/hoardmq >>"$OUT/run.log"
k rollout status statefulset/hoardmq --timeout=900s >>"$OUT/run.log" 2>&1
echo "rollout_done $(epoch)" >>"$OUT/times.txt"
note "rollout complete"

end=$(( $(date +%s) + 900 ))
for p in mqtt-pub mqtt-sub; do
  until [ "$(k get pod "$p" -o jsonpath='{.status.phase}')" = Succeeded ]; do
    if [ "$(date +%s)" -ge "$end" ]; then
      note "timed out waiting for $p"
      break
    fi
    sleep 2
  done
done
echo "clients_done $(epoch)" >>"$OUT/times.txt"
k logs mqtt-pub >"$OUT/pub.jsonl"
k logs mqtt-sub >"$OUT/sub.jsonl"
k logs mqtt-sampler >"$OUT/sampler.jsonl"
k get pods -o wide >"$OUT/pods-end.txt"
k get events --sort-by=.lastTimestamp >"$OUT/events.txt" 2>&1
if k get secret hoardmq-admin >/dev/null 2>&1; then
  for p in $PODS; do
    k exec "$p" -- /usr/local/bin/hoardmq audit tail -admin "127.0.0.1:$ADMIN_PORT" \
      -token-file /etc/hoardmq/admin/operator.token -limit 1000 -json >"$OUT/audit-$p.json" 2>"$OUT/audit-$p.err"
  done
  note "admin audit kept for $PODS"
fi
note "pub: $(grep -h '^acked' "$OUT/pub.jsonl")"
note "sub: $(grep -h '^received' "$OUT/sub.jsonl" | cut -c1-120)"
kubectl --context "$CTX" delete namespace "$NS" --wait=true >/dev/null
note "namespace deleted"
touch "$OUT/stop"
sleep 2
for j in $(jobs -p); do pkill -P "$j"; kill "$j"; done 2>/dev/null
wait 2>/dev/null
if [ "${CTX#kind-}" != "$CTX" ]; then
  engine=${KIND_EXPERIMENTAL_PROVIDER:-docker}
  : >"$OUT/kubelet-prestop.txt"
  for node in $(kubectl --context "$CTX" get nodes -o jsonpath='{.items[*].metadata.name}'); do
    if ! "$engine" exec "$node" journalctl -u kubelet --no-pager --since "@$START" >"$OUT/kubelet.tmp" 2>>"$OUT/run.log"; then
      note "kubelet journal not read on $node"
    fi
    # A hook that wrote to stderr is logged as a multi-line err=< … > value,
    # whose later lines hold the exit code.
    awk 'tolower($0) ~ /prestop/ { print; cont = /err=<$/; next } cont { print; if ($0 ~ /: +> /) cont = 0 }' \
      "$OUT/kubelet.tmp" >>"$OUT/kubelet-prestop.txt"
    rm -f "$OUT/kubelet.tmp"
  done
  note "kubelet: $(grep -c 'PreStop hook failed' "$OUT/kubelet-prestop.txt") preStop hook failures since the run started"
else
  note "not a kind context: no kubelet lines kept"
fi
note "run end"
