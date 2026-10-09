package sim

// timelineHTML is the per-run viewer: one self-contained page with the
// run's JSON embedded at write time, because a file:// page cannot fetch a
// sibling file. No server, no build step, no external reference — the
// design priority is reading a violation against the faults and connection
// events around it, which is the shape every WI 1273 diagnosis took.
//
// The product never compiles this: sim is a test-only package.
const timelineHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>sim run timeline</title>
<style>
body { background:#17140f; color:#e8e2d5; font:14px/1.45 system-ui, sans-serif; margin:1.5rem; }
h1 { font-size:1.2rem; } h2 { font-size:1rem; margin-top:1.4rem; }
code, td.mono { font-family:ui-monospace, monospace; }
table { border-collapse:collapse; margin:.4rem 0; }
td, th { border:1px solid #3a3428; padding:.15rem .5rem; text-align:left; font-size:.85rem; }
.violation { color:#ff8c7a; white-space:pre-wrap; font-family:ui-monospace,monospace; font-size:.8rem; margin:.3rem 0; }
.ok { color:#9fd08c; }
.muted { color:#8d8471; }
#lanes text { fill:#e8e2d5; font-size:10px; font-family:ui-monospace,monospace; }
#lanes .grid { stroke:#3a3428; stroke-width:1; }
.fault { fill:#b58c3a; opacity:.55; }
.ev-ok { fill:#9fd08c; } .ev-refused { fill:#e0b34a; } .ev-dropped { fill:#ff8c7a; } .ev-disconnect { fill:#b48ce0; }
.dl { stroke:#7aa7c7; stroke-width:1; }
</style>
</head>
<body>
<h1>sim run <span id="title" class="mono"></span></h1>
<div id="meta" class="muted"></div>
<div id="verdict"></div>
<h2>Coverage</h2><div id="coverage"></div>
<h2>Stats</h2><div id="stats"></div>
<h2>Schedule</h2><div id="schedule" class="mono"></div>
<h2>Timeline</h2>
<div id="legend" class="muted">
<svg width="14" height="12" style="vertical-align:middle"><rect class="fault" x="0" y="0" width="14" height="12"/></svg> fault span (hover for the step) ·
<svg width="10" height="10" style="vertical-align:middle"><circle class="ev-ok" cx="5" cy="5" r="3"/></svg> CONNACK accepted ·
<svg width="10" height="10" style="vertical-align:middle"><circle class="ev-refused" cx="5" cy="5" r="3"/></svg> CONNACK refused ·
<svg width="10" height="10" style="vertical-align:middle"><circle class="ev-dropped" cx="5" cy="5" r="3"/></svg> connection dropped ·
<svg width="10" height="10" style="vertical-align:middle"><circle class="ev-disconnect" cx="5" cy="5" r="3"/></svg> server DISCONNECT ·
<svg width="6" height="12" style="vertical-align:middle"><line class="dl" x1="3" y1="0" x2="3" y2="12"/></svg> delivery (hover for publisher/topic/seq) — one lane per client, busiest first
</div>
<div id="cap" class="muted"></div>
<svg id="lanes" width="1200"></svg>
<h2>Violations</h2><div id="violations"></div>
<script>
const run = __RUN_JSON__;
(function () {
  const $ = (id) => document.getElementById(id);
  const esc = (s) => String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
  $("title").textContent = run.test + " seed " + run.seed;
  $("meta").textContent = run.go_version + (run.revision ? " @ " + run.revision.slice(0, 12) : "") +
    " · " + run.duration_ms + " ms · written " + run.written +
    " · mutex wait Δ " + run.runtime.mutex_wait_delta_sec.toFixed(3) + " s (" +
    run.runtime.mutex_wait_per_op_us.toFixed(2) + " µs/op over " + run.runtime.ops + " ops)";
  const vio = run.violations || [];
  $("verdict").innerHTML = vio.length ? '<b class="violation">' + vio.length + ' violation(s)</b>'
                                      : '<b class="ok">no violations</b>';
  const cov = run.coverage || {};
  let ch = "<table><tr><th>evidence</th><th>count</th></tr>";
  ch += "<tr><td>publications</td><td>" + cov.publications + "</td></tr>";
  ch += "<tr><td>deliveries</td><td>" + cov.deliveries + "</td></tr>";
  ch += "<tr><td>connection events</td><td>" + cov.events + "</td></tr>";
  ch += "<tr><td>probes</td><td>" + cov.probes + "</td></tr></table>";
  ch += "<table><tr><th>exercised</th><th></th></tr>";
  for (const k of Object.keys(cov.exercised || {}).sort())
    ch += "<tr><td>" + esc(k) + "</td><td class=" + (cov.exercised[k] ? '"ok">yes' : '"violation">NO') + "</td></tr>";
  $("coverage").innerHTML = ch + "</table>";
  let sh = "<table>";
  for (const k of Object.keys(run.stats || {}).sort())
    sh += "<tr><td>" + esc(k) + "</td><td>" + run.stats[k] + "</td></tr>";
  $("stats").innerHTML = sh + "</table>";
  $("schedule").innerHTML = (run.schedule || []).map(s => esc(s.text)).join("<br>") || "no faults";
  $("violations").innerHTML = vio.length
    ? vio.map(v => '<div class="violation">' + esc(v.Property + ": " + v.Detail) + "</div>").join("")
    : '<div class="ok">none</div>';

  // Timeline: faults lane + the busiest client lanes. Time axis spans the
  // run's events; a run with no events draws nothing and says so.
  const ledger = run.ledger || {};
  const events = ledger.Events || [], deliveries = ledger.Deliveries || [];
  const byClient = {};
  for (const e of events) (byClient[e.Client] = byClient[e.Client] || { ev: [], dl: [] }).ev.push(e);
  for (const d of deliveries) (byClient[d.Client] = byClient[d.Client] || { ev: [], dl: [] }).dl.push(d);
  const clients = Object.keys(byClient).sort((a, b) =>
    (byClient[b].ev.length + byClient[b].dl.length) - (byClient[a].ev.length + byClient[a].dl.length));
  const CAP = 40;
  const shown = clients.slice(0, CAP);
  $("cap").textContent = clients.length > CAP
    ? "showing the " + CAP + " busiest of " + clients.length + " clients"
    : (clients.length ? "" : "no connection events in this run");
  const times = [];
  for (const e of events) times.push(Date.parse(e.At));
  for (const d of deliveries) times.push(Date.parse(d.At));
  const faultsAt = Date.parse(run.faults_at);
  for (const s of (run.schedule || [])) { times.push(faultsAt + s.at_sec * 1000); times.push(faultsAt + (s.at_sec + s.dur_sec) * 1000); }
  if (!times.length) return;
  const t0 = Math.min(...times), t1 = Math.max(...times, t0 + 1);
  const W = 1160, L = 120, ROW = 16;
  const x = (t) => L + (t - t0) / (t1 - t0) * (W - L);
  const svg = $("lanes");
  const rows = 1 + shown.length;
  svg.setAttribute("height", (rows + 2) * ROW + 20);
  let g = "";
  const lane = (i) => 10 + i * ROW;
  g += '<text x="0" y="' + (lane(0) + 11) + '">faults</text>';
  for (const s of (run.schedule || [])) {
    const sx = x(faultsAt + s.at_sec * 1000), w = Math.max(2, x(faultsAt + (s.at_sec + s.dur_sec) * 1000) - sx);
    g += '<rect class="fault" x="' + sx + '" y="' + lane(0) + '" width="' + w + '" height="12"><title>' + esc(s.text) + "</title></rect>";
  }
  shown.forEach((c, i) => {
    const y = lane(i + 1);
    g += '<line class="grid" x1="' + L + '" y1="' + (y + 6) + '" x2="' + W + '" y2="' + (y + 6) + '"/>';
    g += '<text x="0" y="' + (y + 10) + '">' + esc(c) + "</text>";
    for (const d of byClient[c].dl)
      g += '<line class="dl" x1="' + x(Date.parse(d.At)) + '" y1="' + y + '" x2="' + x(Date.parse(d.At)) + '" y2="' + (y + 12) +
           '"><title>' + esc(d.Publisher + "/" + d.Topic + " seq " + d.Seq + (d.Dup ? " DUP" : "")) + "</title></line>";
    for (const e of byClient[c].ev) {
      let cls = "ev-disconnect";
      if (e.Kind === "dropped") cls = "ev-dropped";
      else if (e.Kind === "connack") cls = e.Code === 0 ? "ev-ok" : "ev-refused";
      g += '<circle class="' + cls + '" cx="' + x(Date.parse(e.At)) + '" cy="' + (y + 6) +
           '" r="3"><title>' + esc(e.Kind + " on " + e.Node + (e.Code ? " code " + e.Code : "") +
           (e.Kind === "connack" ? " present=" + e.Present : "")) + "</title></circle>";
    }
  });
  svg.innerHTML = g;
})();
</script>
</body>
</html>
`
