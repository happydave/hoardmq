package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/testcert"
)

// TestScale is the measurement across hosts. It runs only when
// HOARDMQ_SIM_SCALE_LAYOUT names a layout (sim/README.md, "Across hosts").
func TestScale(t *testing.T) {
	cfg, ok, err := readScaleConfig(os.Getenv)
	if !ok {
		t.Skipf("set %s to run the measurement across hosts", scaleLayoutEnv)
	}
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	prov := readProvenance(os.Args, os.Environ(), dir)
	rec, err := runScale(ctx, cfg, prov, os.Getenv("HOARDMQ_SIM_ARTIFACT"), t.Logf)
	if rec != nil {
		t.Log(scaleReport(rec))
	}
	if err != nil {
		t.Fatal(err)
	}
}

// The layouts' paths in these tests are built at run time under the
// temporary directory and never written: the checks read them as strings.
// scaleTestTop, the temporary directory's first component, is a run root too
// shallow to hold a run.
var (
	scaleTestDir    = filepath.Join(os.TempDir(), "hoardmq-scale", "test")
	scaleTestBroker = filepath.Join(scaleTestDir, "bin", "hoardmq")
	scaleTestRuns   = filepath.Join(scaleTestDir, "runs")
	scaleTestTop    = "/" + strings.Split(strings.Trim(os.TempDir(), "/"), "/")[0]
)

func goodLayout() scaleLayout {
	n := func(name, host, cores, addr string, base int) scaleNode {
		return scaleNode{Name: name, Host: host, Cores: cores, Address: addr, ObsPort: base, ClientPort: base + 1,
			ClusterPort: base + 2, Binary: scaleTestBroker, RunRoot: scaleTestRuns}
	}
	n1 := n("n1", "local", "0-3,16-19", "203.0.113.40", 7060)
	n1.ObsAddress = "127.0.0.1"
	return scaleLayout{
		Nodes: []scaleNode{n1, n("n2", "host-b", "0-3,8-11", "203.0.113.135", 7063), n("n3", "host-b", "4-7,12-15", "203.0.113.135", 7066)},
		Driver: scaleDriver{Binary: filepath.Join(scaleTestDir, "bin", "simclients"), Cores: "4-15,20-31",
			Sources: []string{"203.0.113.41", "203.0.113.80"}},
	}
}

func TestScaleLayoutProblems(t *testing.T) {
	if p := layoutProblems(goodLayout()); len(p) != 0 {
		t.Fatalf("the planned layout: %v, want none", p)
	}
	cases := []struct {
		name string
		edit func(*scaleLayout)
		want string
	}{
		{"shared client address", func(l *scaleLayout) { l.Nodes[2].ClientPort = l.Nodes[1].ClientPort }, "n2 clients and n3 clients both bind 203.0.113.135:7064"},
		{"wildcard", func(l *scaleLayout) { l.Nodes[1].Address = "0.0.0.0" }, `address "0.0.0.0" is not a specific IP address`},
		{"empty address", func(l *scaleLayout) { l.Nodes[1].Address = "" }, `address "" is not a specific IP address`},
		{"shallow run root", func(l *scaleLayout) { l.Nodes[0].RunRoot = scaleTestTop }, fmt.Sprintf("run root %q must be absolute", scaleTestTop)},
		{"relative binary", func(l *scaleLayout) { l.Nodes[0].Binary = "bin/hoardmq" }, `binary "bin/hoardmq" is not an absolute path`},
		{"no cores", func(l *scaleLayout) { l.Nodes[0].Cores = "" }, `cores "" is not a CPU list`},
		{"source not an address", func(l *scaleLayout) { l.Driver.Sources = []string{"host-a"} }, `source "host-a" is not an IP address`},
	}
	for _, tc := range cases {
		l := goodLayout()
		l.Nodes = append([]scaleNode(nil), l.Nodes...)
		tc.edit(&l)
		got := strings.Join(layoutProblems(l), "; ")
		if !strings.Contains(got, tc.want) {
			t.Errorf("%s: %q, want it to name %q", tc.name, got, tc.want)
		}
	}
}

// passingFacts is a broker host that meets every condition for goodLayout's
// two nodes on host-b and a million clients.
func passingFacts() hostFacts {
	return hostFacts{MemAvailableKB: 90 << 20, NofileHard: 1 << 20, NTP: "yes", Load1: 0.1,
		Addrs:        []string{"127.0.0.1", "203.0.113.135"},
		Listening:    []string{"0.0.0.0:22", "127.0.0.1:631"},
		BinarySHA256: map[string]string{scaleTestBroker: "abc"}}
}

func TestScaleHostProblems(t *testing.T) {
	l := goodLayout()
	remote := l.Nodes[1:]
	ref := map[string]string{scaleTestBroker: "abc"}
	million := scaleNeeds{Clients: 1_000_000, Nodes: 3, NodeBytes: 48 << 10, DriverBytes: 32 << 10}
	if p := hostProblems(passingFacts(), remote, false, million, ref); len(p) != 0 {
		t.Fatalf("passing facts: %v", p)
	}
	need := hostMemoryNeed(million, 2, false)
	cases := []struct {
		name string
		edit func(*hostFacts)
		w    scaleNeeds
		want string
	}{
		{"address missing", func(f *hostFacts) { f.Addrs = []string{"127.0.0.1"} }, million, "address 203.0.113.135 is not on the host"},
		{"port in use", func(f *hostFacts) { f.Listening = append(f.Listening, "203.0.113.135:7064") }, million, "port 7064 is in use"},
		{"memory a KiB short", func(f *hostFacts) { f.MemAvailableKB = need/1024 - 1 }, million, "MemAvailable"},
		{"open files at sshd's default", func(f *hostFacts) { f.NofileHard = 524_288 }, million, "open-file hard limit 524288"},
		{"conntrack under four times", func(f *hostFacts) { f.ConntrackLoaded, f.ConntrackMax = true, 4*1_000_000-1 }, million, "nf_conntrack_max 3999999"},
		{"clock", func(f *hostFacts) { f.NTP = "no" }, million, "not NTP-synchronized"},
		{"load", func(f *hostFacts) { f.Load1 = 4.5 }, million, "1-minute load 4.50"},
		{"stray broker", func(f *hostFacts) { f.Strays = []int{4242} }, million, "already running: pid [4242]"},
		{"run directory", func(f *hostFacts) { f.RunDirExists = true }, million, "the run's directory already exists"},
		{"another binary", func(f *hostFacts) {
			f.BinarySHA256 = map[string]string{scaleTestBroker: "def"}
		}, million, `sha256 "def" here, "abc"`},
	}
	for _, tc := range cases {
		f := passingFacts()
		tc.edit(&f)
		got := strings.Join(hostProblems(f, remote, false, tc.w, ref), "; ")
		if !strings.Contains(got, tc.want) {
			t.Errorf("%s: %q, want it to name %q", tc.name, got, tc.want)
		}
	}
	// The boundaries pass: memory exactly the need, conntrack exactly four
	// times, and sshd's default limit at ten thousand clients. MemAvailable
	// is read in KiB, so the need is a whole number of KiB here, and the case
	// above is one KiB under it.
	if need%1024 != 0 {
		t.Fatalf("need %d bytes is not a whole number of KiB", need)
	}
	f := passingFacts()
	f.MemAvailableKB, f.ConntrackLoaded, f.ConntrackMax = need/1024, true, 4*1_000_000
	if p := hostProblems(f, remote, false, million, ref); len(p) != 0 {
		t.Errorf("at the boundaries: %v", p)
	}
	f = passingFacts()
	f.NofileHard = 524_288
	if p := hostProblems(f, remote, false, scaleNeeds{Clients: 10_000, Nodes: 3, NodeBytes: 48 << 10}, ref); len(p) != 0 {
		t.Errorf("524,288 files at 10,000 clients: %v", p)
	}
	// The driver's host needs four times every client in its table:
	// 4,000,000 passes at a million clients and one fewer is refused.
	drv := hostFacts{MemAvailableKB: 200 << 20, NTP: "yes", ConntrackLoaded: true, ConntrackMax: 4_000_000}
	if p := hostProblems(drv, nil, true, million, nil); len(p) != 0 {
		t.Errorf("driver host at 4,000,000: %v", p)
	}
	drv.ConntrackMax = 3_999_999
	if got := strings.Join(hostProblems(drv, nil, true, million, nil), "; "); !strings.Contains(got, "nf_conntrack_max 3999999 is under 4000000") {
		t.Errorf("driver host at 3,999,999: %q, want a refusal", got)
	}
}

func TestScaleParseFactsCaptures(t *testing.T) {
	for _, tc := range []struct {
		file      string
		conntrack bool
		hard      int64
		addr      string
	}{{"facts-host-a.txt", true, 1048576, "192.0.2.10"}, {"facts-host-b.txt", false, 524288, "192.0.2.20"}} {
		data, err := os.ReadFile(filepath.Join("testdata", "scale", tc.file))
		if err != nil {
			t.Fatal(err)
		}
		f := parseFacts(string(data))
		if f.ConntrackLoaded != tc.conntrack || f.NofileHard != tc.hard || f.NTP != "yes" || f.MemAvailableKB == 0 ||
			!strings.Contains(strings.Join(f.Addrs, " "), tc.addr) || len(f.Listening) == 0 || len(f.BinarySHA256) != 1 {
			t.Errorf("%s: %+v", tc.file, f)
		}
	}
}

func TestScaleBuildProblems(t *testing.T) {
	clean, dirty := false, true
	good := binaryInfo{Path: "/b", Revision: "c1", Modified: &clean}
	if p := buildProblems("c1", []binaryInfo{good}); len(p) != 0 {
		t.Fatalf("matching build: %v", p)
	}
	for _, tc := range []struct {
		name string
		b    binaryInfo
		want string
	}{
		{"another commit", binaryInfo{Path: "/b", Revision: "c0", Modified: &clean}, "built from c0, the test from c1"},
		{"modified tree", binaryInfo{Path: "/b", Revision: "c1", Modified: &dirty}, "built from a modified tree"},
		{"no record of modification", binaryInfo{Path: "/b", Revision: "c1"}, "built from a modified tree"},
		{"no commit", binaryInfo{Path: "/b"}, "records no commit"},
	} {
		if got := strings.Join(buildProblems("c1", []binaryInfo{tc.b}), "; "); !strings.Contains(got, tc.want) {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestKillAllowed(t *testing.T) {
	rec := procRecord{PID: 4242, StartTicks: 1000, Exe: "/bin/hoardmq"}
	if !killAllowed(rec, rec) {
		t.Fatal("the recorded process itself is refused")
	}
	for name, now := range map[string]procRecord{
		"another pid":              {PID: 4243, StartTicks: 1000, Exe: "/bin/hoardmq"},
		"a reused pid":             {PID: 4242, StartTicks: 1001, Exe: "/bin/hoardmq"},
		"another executable":       {PID: 4242, StartTicks: 1000, Exe: "/bin/bash"},
		"a process with no record": {PID: 4242},
	} {
		if killAllowed(rec, now) {
			t.Errorf("%s: permitted", name)
		}
	}
	if killAllowed(procRecord{PID: 1, StartTicks: 5, Exe: "/sbin/init"}, procRecord{PID: 1, StartTicks: 5, Exe: "/sbin/init"}) {
		t.Error("pid 1: permitted")
	}
}

// TestNodeSignalOnlyTheRecordedProcess runs the signal path against a sleep
// the test starts: a record with another start time leaves it running, the
// right record ends it.
func TestNodeSignalOnlyTheRecordedProcess(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })
	ctx := context.Background()
	rec, alive, err := observeProc(ctx, localShell{}, cmd.Process.Pid)
	if err != nil || !alive || rec.Exe == "" {
		t.Fatalf("observing the sleep: %+v, alive %v, %v", rec, alive, err)
	}
	wrong := rec
	wrong.StartTicks++
	p := &nodeProc{node: scaleNode{Name: "sleeper"}, sh: localShell{}, rec: wrong}
	if sent, err := p.signal(ctx, "KILL"); err == nil || sent {
		t.Fatalf("a record with another start time: sent %v, %v", sent, err)
	}
	select {
	case <-done:
		t.Fatal("the sleep ended on a refused signal")
	case <-time.After(300 * time.Millisecond):
	}
	p.rec = rec
	if sent, err := p.signal(ctx, "KILL"); err != nil || !sent {
		t.Fatalf("the recorded process: sent %v, %v", sent, err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the sleep still runs after the permitted signal")
	}
}

func TestRemoveAllowed(t *testing.T) {
	root := scaleTestRuns
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	shallow := scaleTestTop
	id := "scale-20261110-201500-a1b2c3"
	ok := []string{root + "/" + id, root + "/" + id + "/n2/data"}
	for _, p := range ok {
		if !removeAllowed(p, p, root, id) {
			t.Errorf("%s: refused", p)
		}
	}
	for name, c := range map[string][4]string{
		"root":              {"/", "/", root, id},
		"home":              {home, home, root, id},
		"the run root":      {root, root, root, id},
		"climbing out":      {root + "/" + id + "/../x", root + "/x", root, id},
		"another run":       {root + "/scale-20261110-201500-ffffff", root + "/scale-20261110-201500-ffffff", root, id},
		"an id of no form":  {root + "/x", root + "/x", root, "x"},
		"through a symlink": {root + "/" + id + "/n2", "/srv/elsewhere", root, id},
		"a shallow root":    {shallow + "/" + id, shallow + "/" + id, shallow, id},
	} {
		if removeAllowed(c[0], c[1], c[2], c[3]) {
			t.Errorf("%s: permitted", name)
		}
	}
}

// TestRemoveDirRefusesASymlink makes a run directory whose node directory is
// a symlink to a directory outside it: the removal is refused and the target
// survives; the real run directory is removed.
func TestRemoveDirRefusesASymlink(t *testing.T) {
	root := filepath.Join(t.TempDir(), "runs")
	outside := filepath.Join(t.TempDir(), "keep")
	id := "scale-20261110-201500-a1b2c3"
	if err := os.MkdirAll(filepath.Join(root, id), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "f"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, id, "n2")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := removeDir(ctx, localShell{}, link, root, id); err == nil {
		t.Fatal("removal through a symlink was not refused")
	}
	if _, err := os.Stat(filepath.Join(outside, "f")); err != nil {
		t.Fatalf("the symlink's target lost its file: %v", err)
	}
	if err := removeDir(ctx, localShell{}, filepath.Join(root, id), root, id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, id)); !os.IsNotExist(err) {
		t.Fatalf("the run directory survived: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "f")); err != nil {
		t.Fatalf("removing the run directory reached the symlink's target: %v", err)
	}
}

func TestScaleParseSampleCaptures(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "scale", "sampler-host-a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 2 {
		t.Fatalf("%d lines captured", len(lines))
	}
	for _, line := range lines {
		s, ok := parseSample(line)
		if !ok {
			t.Fatalf("unparsed: %q", line)
		}
		if s.MemAvailKB <= 0 || s.Bytes[0] <= 0 || s.Conntrack < 0 || s.RSSKB["n1"] <= 0 {
			t.Errorf("%q read as %+v", line, s)
		}
		// n2's pid named no process: gone, never a zero.
		if _, has := s.RSSKB["n2"]; has || len(s.Gone) != 1 || s.Gone[0] != "n2" {
			t.Errorf("%q: n2 read as %v, gone %v", line, s.RSSKB, s.Gone)
		}
	}
	if _, ok := parseSample("listening on 7060"); ok {
		t.Error("a line of another kind parsed")
	}
}

func TestScaleParseMetrics(t *testing.T) {
	body := `# HELP hoardmq_sessions_active x
hoardmq_sessions_active{namespace="default"} 300
hoardmq_sessions_active{namespace="other"} 33
hoardmq_replicas_held 310
hoardmq_overload_indicator{name="queued_messages"} 1000
hoardmq_overload_indicator{name="heap_bytes"} 2.5e+09
hoardmq_raft_leader 1
hoardmq_connections_total 99
`
	m := parseMetrics(body)
	want := map[string]float64{mSessions: 333, mReplicas: 310, mQueued: 1000, mHeap: 2.5e9, mLeader: 1}
	if !reflect.DeepEqual(m, want) {
		t.Fatalf("%v, want %v", m, want)
	}
}

// TestScaleNodeConfig parses a generated configuration as the broker does
// and compares it with the defaults: only the members the measurement sets
// may differ.
func TestScaleNodeConfig(t *testing.T) {
	l := goodLayout()
	dir := t.TempDir()
	ca, err := testcert.NewCA("scale test")
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue("n2", 2)
	if err != nil {
		t.Fatal(err)
	}
	for name, b := range map[string][]byte{"ca.pem": ca.PEM, "node.pem": leaf.CertPEM, "node-key.pem": leaf.KeyPEM, "acl.json": scaleACL} {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	data, err := nodeConfig(l.Nodes[1], l.Nodes, dir)
	if err != nil {
		t.Fatal(err)
	}
	doc, problems := config.Parse(data, nil)
	if len(problems) > 0 {
		t.Fatalf("problems: %v\n%s", problems, data)
	}
	if got := doc.Listeners; len(got) != 1 || got[0].Address != "203.0.113.135:7064" {
		t.Fatalf("listeners %+v", got)
	}
	if doc.Cluster.Listen != "203.0.113.135:7065" || doc.Cluster.Advertise != "203.0.113.135:7064" || len(doc.Cluster.Peers) != 2 {
		t.Fatalf("cluster %+v", doc.Cluster)
	}
	if doc.Observability.Address != "203.0.113.135:7063" || !doc.Observability.AllowRemote {
		t.Fatalf("observability %+v", doc.Observability)
	}
	def := config.Default()
	def.Node.Name, def.Node.DataDir = doc.Node.Name, doc.Node.DataDir
	def.Listeners = doc.Listeners
	def.Cluster.Listen, def.Cluster.Advertise, def.Cluster.Peers = doc.Cluster.Listen, doc.Cluster.Advertise, doc.Cluster.Peers
	def.Cluster.Discovery, def.Cluster.Auth = doc.Cluster.Discovery, doc.Cluster.Auth
	def.Observability.Address, def.Observability.AllowRemote = doc.Observability.Address, doc.Observability.AllowRemote
	def.Auth.Anonymous = doc.Auth.Anonymous
	def.Log.Format = doc.Log.Format
	def.Modules = doc.Modules
	a, _ := json.Marshal(def)
	b, _ := json.Marshal(doc)
	if string(a) != string(b) {
		t.Fatalf("the configuration differs from the defaults beyond the members the measurement sets:\ndefaults %s\nnode     %s", a, b)
	}
	if doc.Cluster.Auth.Mode != "mtls" || doc.Cluster.Auth.TLS == nil || doc.Cluster.Auth.TLS.CAFile != dir+"/ca.pem" {
		t.Fatalf("cluster auth %+v", doc.Cluster.Auth)
	}
	var authz struct{ Path string }
	_ = json.Unmarshal(doc.Modules["authz.file"], &authz)
	if _, ok := doc.Modules["metrics.prometheus"]; !ok || len(doc.Modules) != 2 || authz.Path != dir+"/acl.json" {
		t.Fatalf("modules %v, want metrics.prometheus and authz.file on the run's rule file", doc.Modules)
	}
	if !doc.Auth.Anonymous || doc.Log.Format != "json" {
		t.Fatalf("auth %+v, log %+v", doc.Auth, doc.Log)
	}
	n1, err := nodeConfig(l.Nodes[0], l.Nodes, dir)
	if err != nil {
		t.Fatal(err)
	}
	doc1, problems := config.Parse(n1, nil)
	if len(problems) > 0 {
		t.Fatalf("n1's problems: %v", problems)
	}
	if doc1.Observability.Address != "127.0.0.1:7060" || doc1.Observability.AllowRemote {
		t.Fatalf("n1's loopback observability: %+v", doc1.Observability)
	}
}

func TestScaleObserveSelf(t *testing.T) {
	rec, alive, err := observeProc(context.Background(), localShell{}, os.Getpid())
	if err != nil || !alive {
		t.Fatalf("%+v %v %v", rec, alive, err)
	}
	exe, _ := os.Executable()
	if rec.Exe != exe || rec.StartTicks == 0 {
		t.Fatalf("%+v, want exe %s", rec, exe)
	}
	if _, alive, _ := observeProc(context.Background(), localShell{}, 4_194_000); alive {
		t.Fatal("a pid with no process read as alive")
	}
}

func TestSettledAt(t *testing.T) {
	t0 := time.Unix(0, 0)
	at := func(s int, v float64) timedValue { return timedValue{At: t0.Add(time.Duration(s) * time.Second), V: v} }
	if _, ok := settledAt([]timedValue{at(0, 1), at(10, 2), at(20, 3), at(30, 4), at(40, 5)}, 30*time.Second); ok {
		t.Error("a rising series settled")
	}
	got, ok := settledAt([]timedValue{at(0, 1), at(10, 2), at(20, 2), at(30, 2), at(40, 2), at(50, 2)}, 30*time.Second)
	if !ok || !got.Equal(t0.Add(10*time.Second)) {
		t.Errorf("settled %v at %v, want at 10 s", ok, got.Sub(t0))
	}
	if _, ok := settledAt([]timedValue{at(0, 2), at(10, 2), at(20, 2)}, 30*time.Second); ok {
		t.Error("settled before the hold had passed")
	}
}

func TestRejoinedAt(t *testing.T) {
	t0 := time.Unix(0, 0)
	series := func(f func(s int) rejoinReading) []rejoinReading {
		var out []rejoinReading
		for s := 0; s <= 120; s += 5 {
			r := f(s)
			r.At = t0.Add(time.Duration(s) * time.Second)
			out = append(out, r)
		}
		return out
	}
	// Short copies at 0 throughout and the node holding nothing: not
	// rejoined, whatever readiness says.
	if _, ok := rejoinedAt(series(func(int) rejoinReading { return rejoinReading{Ready: true, Serving: true} }), 30*time.Second); ok {
		t.Error("a node holding no copies rejoined")
	}
	// Copies rise until 40 s and then hold: rejoined at 40 s.
	got, ok := rejoinedAt(series(func(s int) rejoinReading {
		return rejoinReading{Ready: s >= 10, Serving: s >= 10, Replicas: float64(min(s, 40) * 1000)}
	}), 30*time.Second)
	if !ok || !got.Equal(t0.Add(40*time.Second)) {
		t.Errorf("rejoined %v at %v, want at 40 s", ok, got.Sub(t0))
	}
	// Copies keep changing to the end: not rejoined.
	if _, ok := rejoinedAt(series(func(s int) rejoinReading {
		return rejoinReading{Ready: true, Serving: true, Replicas: float64(s)}
	}), 30*time.Second); ok {
		t.Error("a changing copy count rejoined")
	}
	// A node short of copies at the start of the interval: not that one.
	got, ok = rejoinedAt(series(func(s int) rejoinReading {
		short := 0.0
		if s < 60 {
			short = 5
		}
		return rejoinReading{Ready: true, Serving: true, Replicas: 7, Short: short}
	}), 30*time.Second)
	if !ok || !got.Equal(t0.Add(60*time.Second)) {
		t.Errorf("with short copies until 60 s: rejoined %v at %v, want at 60 s", ok, got.Sub(t0))
	}
}

func TestReadScaleConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "layout.json")
	data, _ := json.Marshal(goodLayout())
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{scaleLayoutEnv: path, scaleClientsEnv: "1000000"}
	get := func(k string) string { return env[k] }
	c, ok, err := readScaleConfig(get)
	if !ok || err != nil {
		t.Fatalf("%v %v", ok, err)
	}
	if c.Queue != (queueRequest{Sessions: 100_000, Depth: 10, PayloadBytes: 64}) || c.Victim != "n2" || c.Hold != holdDefault {
		t.Fatalf("defaults: %+v", c)
	}
	env[scaleQueueEnv] = "1000x5"
	if c, _, err = readScaleConfig(get); err != nil || c.Queue.Sessions != 1000 || c.Queue.Depth != 5 {
		t.Fatalf("queue 1000x5: %+v %v", c.Queue, err)
	}
	env[scaleQueueEnv] = "2000000x5"
	if _, _, err = readScaleConfig(get); err == nil {
		t.Fatal("more offline sessions than clients accepted")
	}
	if _, ok, _ := readScaleConfig(func(string) string { return "" }); ok {
		t.Fatal("selected with no layout")
	}
}
