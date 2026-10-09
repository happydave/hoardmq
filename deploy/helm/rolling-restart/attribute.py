#!/usr/bin/env python3
"""Attribute every publish refused during a rolling-restart run to its cause.

    attribute.py [--json] <run-dir>...

A run directory is what run.sh writes. For each refused publish (any PUBACK
code other than 0 or 16) the node that answered it is found, and the refusal
is classified from what that node recorded:

- replication barrier: the node's own "replication barrier failed" warning for
  client "pub", whose error names the peer the barrier failed on and why;
- routing barrier: the same warning, from the forward half of the barrier; the
  receiver's side is read from its "forwarded frames not durable" warning or,
  failing that, from its refusal counters in the same seconds;
- at dispatch: no warning (the session manager refuses before the barrier),
  read from the answering node's refusal counters in the same seconds.

The counters are also totalled per run, summed over pod incarnations (a pod's
counters restart with it), as a second, independent view of the same run.
"""
import collections
import gzip
import json
import os
import re
import sys

OK_CODES = (0, 16)
WINDOW = 1.5  # seconds either side of a publish in which a counter step is attributed to it


def load_jsonl(path):
    out = []
    if not os.path.exists(path):
        return out
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("{"):
                try:
                    out.append(json.loads(line))
                except ValueError:
                    pass
    return out


def ts(s):
    # RFC 3339 with nanoseconds -> epoch seconds, UTC.
    import datetime
    m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?Z", s)
    base = datetime.datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc)
    return base.timestamp() + (float(m.group(2)) if m.group(2) else 0.0)


def node_logs(run):
    """Every distinct broker log line, per pod, parsed; re-attached streams repeat lines."""
    out = {}
    logdir = os.path.join(run, "logs")
    for name in sorted(os.listdir(logdir)):
        if name.endswith(".log"):
            pod, opener = name[:-4], open
        elif name.endswith(".log.gz"):
            pod, opener = name[:-7], gzip.open
        else:
            continue
        seen, recs = set(), []
        with opener(os.path.join(logdir, name), "rt") as f:
            for line in f:
                _, _, body = line.partition(" ")
                body = body.strip()
                if body in seen or not body.startswith("{"):
                    continue
                seen.add(body)
                try:
                    j = json.loads(body)
                except ValueError:
                    continue
                j["_t"] = ts(j["time"])
                j["_pod"] = pod
                recs.append(j)
        recs.sort(key=lambda r: r["_t"])
        out[pod] = recs
    return out


def replication_reason(err):
    m = re.search(r"the copy is not held: (\S+?)(?: \((.*)\))?$", err)
    if not m:
        if "deadline exceeded" in err:
            return "?", "timeout"
        return "?", "other"
    peer, why = m.group(1), m.group(2) or ""
    if why == "":
        return peer, "down"
    if why.startswith("a frame was not delivered"):
        return peer, "refused (a frame was not delivered)"
    if why.startswith("frames were dropped"):
        return peer, "resync"
    if "refused a record" in why:
        return peer, "not-held"
    if why.startswith("replication stopped"):
        return peer, "stopped"
    if why.startswith("a refusal could not be scoped"):
        return peer, "unscoped"
    return peer, why


def counter_steps(sampler):
    """Per pod: a list of (t, series, delta) steps between consecutive readings of one incarnation."""
    steps = collections.defaultdict(list)
    last = {}
    for r in sampler:
        if not r.get("ok"):
            continue
        pod, ip, t, series = r["pod"], r["ip"], r["t"], r["series"]
        prev = last.get(pod)
        if prev and prev[0] == ip:
            for k, v in series.items():
                d = v - prev[2].get(k, 0.0)
                if d > 0:
                    steps[pod].append((t, k, d))
        elif prev is None or prev[0] != ip:
            # A new incarnation: its first reading counts from zero.
            for k, v in series.items():
                if v > 0:
                    steps[pod].append((t, k, v))
        last[pod] = (ip, t, series)
    return steps


def counter_totals(sampler):
    """Sum over incarnations of each series' last reading, per pod and in all."""
    last = {}
    for r in sampler:
        if r.get("ok"):
            last[(r["pod"], r["ip"])] = r["series"]
    total = collections.Counter()
    for series in last.values():
        for k, v in series.items():
            if k.startswith(("hoardmq_forward_failures_total", "hoardmq_replication_barrier_failures_total")):
                total[k] += v
    return total, len(last)


def label(series, key):
    m = re.search(key + r'="([^"]*)"', series)
    return m.group(1) if m else "?"


def steps_near(steps, pod, t0, t1, prefix, reasons):
    got = collections.Counter()
    for t, k, d in steps.get(pod, []):
        if t0 - WINDOW <= t <= t1 + WINDOW and k.startswith(prefix):
            r = label(k, "reason")
            if r in reasons:
                got[r] += d
    return got


NODES = ["hoardmq-0", "hoardmq-1", "hoardmq-2"]
SHARDS, REPLICAS, NAMESPACE = 64, 2, "default"  # the chart's defaults


def shard_of(name):
    """shardmap.ShardOf: FNV-32a of namespace, a zero byte and the name."""
    h = 0x811C9DC5
    for b in NAMESPACE.encode() + b"\0" + name.encode():
        h = ((h ^ b) * 0x01000193) & 0xFFFFFFFF
    return h % SHARDS


def targets(client, owner):
    """shardmap.Compute(basis=NODES).Targets(shard, owner): the shard's holders
    other than the owner, cut to REPLICAS-1."""
    s, n = shard_of(client), len(NODES)
    holders = [NODES[(s + j) % n] for j in range(min(REPLICAS, n))]
    return [h for h in holders if h != owner][: REPLICAS - 1]


def placement(sampler):
    """(t, publisher's pod, subscriber's pod) at each sampled second: the
    subscriber's pod holds its local route, and the publisher's is the other
    pod with a connection, or the same pod when it has two."""
    by_t = collections.defaultdict(dict)
    for r in sampler:
        if r.get("ok"):
            by_t[round(r["t"])][r["pod"]] = r["series"]
    out = []
    for t in sorted(by_t):
        pods = by_t[t]
        conn = {p: s.get("hoardmq_connections_active", 0) for p, s in pods.items()}
        sub = [p for p, s in pods.items() if s.get('hoardmq_routes_active{scope="local"}', 0) >= 1]
        sub = sub[0] if len(sub) == 1 else None
        pub = None
        if sub and conn.get(sub, 0) >= 2:
            pub = sub
        else:
            others = [p for p, c in conn.items() if c >= 1 and p != sub]
            pub = others[0] if len(others) == 1 else None
        out.append((t, pub, sub))
    return out


def clients_at(timeline, t):
    cur = (None, None)
    for ts, pub, sub in timeline:
        if ts <= t:
            cur = (pub, sub)
    return cur


def attribute(run):
    pub = load_jsonl(os.path.join(run, "pub.jsonl"))
    sub = load_jsonl(os.path.join(run, "sub.jsonl"))
    sampler = load_jsonl(os.path.join(run, "sampler.jsonl"))
    logs = node_logs(run)
    steps = counter_steps(sampler)
    timeline = placement(sampler)
    times = {}
    if os.path.exists(os.path.join(run, "times.txt")):
        for line in open(os.path.join(run, "times.txt")):
            k, v = line.split()
            times[k] = float(v)

    warns = []
    for pod, recs in logs.items():
        for r in recs:
            if r.get("msg") == "replication barrier failed" and r.get("client") == "pub":
                warns.append(r)
    warns.sort(key=lambda r: r["_t"])
    used = set()

    pubs = [e for e in pub if e.get("ev") == "pub"]
    refused = [e for e in pubs if e.get("code") not in OK_CODES]
    causes = collections.Counter()
    predicted = collections.Counter()  # replication-barrier refusals by whether the peer is a target
    detail = []
    for e in refused:
        t0, t1 = e["t"], e.get("answered", e["t"])
        match = None
        for i, w in enumerate(warns):
            if i in used:
                continue
            if t0 - 0.2 <= w["_t"] <= t1 + 0.2:
                match = i
                break
        if e.get("code") is None:
            cause = ("no answer", "")
        elif match is not None:
            used.add(match)
            w = warns[match]
            err = w.get("err", "")
            node = w["_pod"]
            if err.startswith("replication:"):
                peer, why = replication_reason(err)
                cause = ("replication barrier", f"{why}; waiting on {peer}; at {node}")
                pubn, subn = clients_at(timeline, t0)
                if pubn and subn:
                    need = set(targets("pub", pubn)) | set(targets("sub", subn))
                    verdict = "a target of pub or sub" if peer in need else "no named key's target"
                else:
                    verdict = "placement unknown"
                predicted[verdict] += 1
            elif err.startswith("routing:"):
                m = re.search(r"forwards? to (\S+) ", err)
                peer = m.group(1) if m else "?"
                if "not delivered" in err:
                    rec = None
                    for r in logs.get(peer, []):
                        if r.get("msg") == "forwarded frames not durable; reported to the sender" and r.get("peer") == node and t0 - 0.5 <= r["_t"] <= t1 + 0.5:
                            rec = r
                            break
                    if rec is not None:
                        rpeer, rwhy = replication_reason(rec.get("err", ""))
                        cause = ("routing barrier: receiver not durable (the receiver's replication barrier)", f"{rwhy}; waiting on {rpeer}; receiver {peer}")
                    else:
                        near = steps_near(steps, peer, t0, t1, "hoardmq_forward_failures_total", {"session-moving", "no-local-subscriber", "sequence-gap"})
                        if near:
                            cause = (f"routing barrier: receiver {near.most_common(1)[0][0]}", f"receiver {peer}")
                        else:
                            cause = ("routing barrier: send failed, receiver cause not seen", f"receiver {peer}")
                elif "not acknowledged" in err:
                    cause = ("routing barrier: not acknowledged", f"{'timeout' if 'deadline' in err else 'peer down before ack'}; receiver {peer}")
                else:
                    cause = ("routing barrier: other", err[:80])
            else:
                cause = ("barrier: other", err[:80])
        else:
            # The client reaches the brokers through the Service's address, so
            # the answering pod is read from whichever pod counted a
            # dispatch-time refusal in these seconds.
            near, at = collections.Counter(), []
            for pod in sorted(steps):
                got = steps_near(steps, pod, t0, t1, "hoardmq_forward_failures_total", {"session-moving", "subscriber-node-down", "routes-unknown"})
                if got:
                    near += got
                    at.append(pod)
            if near:
                cause = (f"at dispatch: {near.most_common(1)[0][0]}", f"at {'/'.join(at)}")
            else:
                cause = ("at dispatch: cause not seen", f"code {e.get('code')}")
        causes[cause[0]] += 1
        detail.append({"i": e["i"], "t": round(t0 - times.get("rollout_start", 0), 3), "code": e.get("code"), "cause": cause[0], "detail": cause[1]})

    fine = collections.Counter((d["cause"], d["detail"]) for d in detail)
    totals, incarnations = counter_totals(sampler)
    summ = [e for e in sub if e.get("ev") == "summary"]
    first = min((d["t"] for d in detail), default=None)
    last = max((d["t"] for d in detail), default=None)
    return {
        "run": run,
        "published": len(pubs),
        "acked": len(pubs) - len(refused),
        "refused": len(refused),
        "delivered": summ[-1]["received"] if summ else None,
        "missing": summ[-1]["missing"] if summ else None,
        "rollout_sec": round(times["rollout_done"] - times["rollout_start"], 1) if "rollout_done" in times else None,
        "refusals_from_rollout_start": [first, last],
        "causes": dict(causes.most_common()),
        "causes_fine": {f"{c} | {d}": n for (c, d), n in fine.most_common()},
        "replication_refusals_by_peer_role": dict(predicted),
        "placement_changes": [(round(t - times.get("rollout_start", 0), 1), p, q) for i, (t, p, q) in enumerate(timeline) if i == 0 or (p, q) != timeline[i - 1][1:]],
        "counters": dict(sorted(totals.items())),
        "incarnations_sampled": incarnations,
        "unmatched_warnings": len(warns) - len(used),
        "detail": detail,
    }


def main(argv):
    as_json = "--json" in argv
    runs = [a for a in argv if a != "--json"]
    results = [attribute(r) for r in runs]
    if as_json:
        print(json.dumps(results, indent=1))
        return
    for r in results:
        print(f"== {r['run']}")
        print(f"published {r['published']}  acked {r['acked']}  refused {r['refused']}  delivered {r['delivered']}  missing {r['missing']}  rollout {r['rollout_sec']} s  refusals from rollout start {r['refusals_from_rollout_start']}")
        for c, n in r["causes"].items():
            print(f"  {n:4d}  {c}")
        print("  -- fine")
        for c, n in r["causes_fine"].items():
            print(f"  {n:4d}  {c}")
        print("  -- replication-barrier refusals: the peer waited on was", r["replication_refusals_by_peer_role"])
        print("  -- placement (seconds from rollout start, pub, sub):", r["placement_changes"])
        print("  -- counters, summed over", r["incarnations_sampled"], "pod incarnations")
        for k, v in r["counters"].items():
            print(f"  {v:6.0f}  {k}")
        print(f"  warnings for client pub not matched to a refusal: {r['unmatched_warnings']}")


if __name__ == "__main__":
    main(sys.argv[1:])
