# sampler.py — reads every broker pod's /metrics once a second and writes the
# refusal counters as JSON lines. A pod's counters restart with it, so each
# line carries the address the pod's headless name resolved to: a new address
# is a new incarnation, and a run's total is the sum of each incarnation's
# last reading.
import json, os, socket, time, urllib.request

NS = os.environ.get("NS", "hoardmq")
PODS = os.environ.get("PODS", "hoardmq-0 hoardmq-1 hoardmq-2").split()
PORT = int(os.environ.get("METRICS_PORT", "9090"))
DURATION = float(os.environ.get("DURATION", "600"))
FAMILIES = (
    "hoardmq_forward_failures_total",
    "hoardmq_replication_barrier_failures_total",
    "hoardmq_drain_sessions_unconfirmed_total",
    "hoardmq_routes_active",
    "hoardmq_connections_active",
)


def read(pod):
    try:
        ip = socket.gethostbyname(f"{pod}.hoardmq-headless.{NS}.svc")
    except OSError as e:
        return {"pod": pod, "ok": False, "err": f"resolve: {e}"}
    try:
        with urllib.request.urlopen(f"http://{ip}:{PORT}/metrics", timeout=0.8) as r:
            body = r.read().decode()
    except Exception as e:
        return {"pod": pod, "ip": ip, "ok": False, "err": str(e)}
    series = {}
    for line in body.splitlines():
        if line.startswith(FAMILIES):
            name, _, value = line.rpartition(" ")
            series[name] = float(value)
    return {"pod": pod, "ip": ip, "ok": True, "series": series}


end = time.time() + DURATION
while time.time() < end:
    tick = time.time()
    for pod in PODS:
        rec = read(pod)
        rec["t"] = time.time()
        print(json.dumps(rec), flush=True)
    time.sleep(max(0.0, 1.0 - (time.time() - tick)))
