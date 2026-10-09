package sim

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestSettingsRecordOnlyNamedVariables: what a measurement records is
// committed, so a credential in the environment must never reach it, and a
// setting that changes the measurement must.
func TestSettingsRecordOnlyNamedVariables(t *testing.T) {
	environ := []string{
		"HOARDMQ_SIM_RAFT_TICK_MS=100",
		"HOARDMQ_SIM_NODELOG=",
		"TMPDIR=/dev/shm",
		"AWS_SECRET_ACCESS_KEY=do-not-publish",
		"PATH=/usr/bin",
		"HOARDMQ_TOKEN=do-not-publish",
	}
	got := readSettings(environ)
	want := map[string]string{"HOARDMQ_SIM_RAFT_TICK_MS": "100", "HOARDMQ_SIM_NODELOG": "", "TMPDIR": "/dev/shm"}
	if len(got) != len(want) {
		t.Fatalf("recorded %v, want exactly %v", got, want)
	}
	for k, v := range want {
		if gv, ok := got[k]; !ok || gv != v {
			t.Errorf("%s: recorded %q (present %v), want %q", k, gv, ok, v)
		}
	}
}

// gitRepo makes a repository with one committed file, insulated from the
// host's git configuration.
func gitRepo(t *testing.T) string {
	t.Helper()
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-c", "user.name=sim", "-c", "user.email=sim@example.invalid", "-c", "commit.gpgsign=false"}, args...)...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	run("init", "-q")
	if err := os.WriteFile(filepath.Join(dir, "tracked.go"), []byte("package x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run("add", "tracked.go")
	run("commit", "-q", "-m", "one")
	return dir
}

func headOf(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(out))
}

// TestTreeRecordsCommitAndDirtyState is plan B1's scenario: a modified tracked
// file is named, the tree is marked dirty, and a second, different
// modification gives a different digest.
func TestTreeRecordsCommitAndDirtyState(t *testing.T) {
	dir := gitRepo(t)

	clean := readTree(dir)
	if clean.Unavailable != "" || clean.Commit != headOf(t, dir) || clean.Dirty == nil || *clean.Dirty {
		t.Fatalf("a clean checkout: %+v", clean)
	}

	if err := os.WriteFile(filepath.Join(dir, "tracked.go"), []byte("package x\n\nvar A = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "untracked.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	one := readTree(dir)
	if one.Dirty == nil || !*one.Dirty || one.DiffSHA256 == "" {
		t.Fatalf("a modified tracked file must mark the tree dirty with a digest: %+v", one)
	}
	if one.ChangedTotal != 2 || !contains(one.Changed, " M tracked.go") || !contains(one.Changed, "?? untracked.txt") {
		t.Errorf("status must name the modified and the untracked file: %+v", one)
	}

	if err := os.WriteFile(filepath.Join(dir, "tracked.go"), []byte("package x\n\nvar A = 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if two := readTree(dir); two.DiffSHA256 == one.DiffSHA256 {
		t.Errorf("two different modifications gave the same digest %s", one.DiffSHA256)
	}
}

// TestTreeUnavailableOutsideAGitCheckout: a tree copied without its .git is
// the common way this would go wrong, and it must say so rather than record a
// clean tree.
func TestTreeUnavailableOutsideAGitCheckout(t *testing.T) {
	t.Setenv("GIT_CEILING_DIRECTORIES", os.TempDir())
	tree := readTree(t.TempDir())
	if tree.Unavailable == "" || tree.Commit != "" || tree.Dirty != nil {
		t.Fatalf("outside a checkout the tree must be unavailable, with no commit and no dirty flag: %+v", tree)
	}
	raw, err := json.Marshal(tree)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"dirty"`) {
		t.Errorf("an unavailable tree must not publish a dirty flag: %s", raw)
	}
}

func TestTreeUnavailableWithoutGit(t *testing.T) {
	t.Setenv("PATH", "")
	if tree := readTree(t.TempDir()); tree.Unavailable != "git not found" {
		t.Errorf("with no git on PATH: %+v", tree)
	}
}

// TestTreeChangeDuringRunIsDetected is plan B4: a tracked change is caught,
// an untracked file — which the measurement itself writes — is not.
func TestTreeChangeDuringRunIsDetected(t *testing.T) {
	dir := gitRepo(t)
	start := readTree(dir)

	if err := os.WriteFile(filepath.Join(dir, "report.md"), []byte("written by the run"), 0o600); err != nil {
		t.Fatal(err)
	}
	if c := checkTree(dir, start); c.Changed == nil || *c.Changed {
		t.Errorf("an untracked file written during the run is not a change to the tree: %+v", c)
	}

	if err := os.WriteFile(filepath.Join(dir, "tracked.go"), []byte("package x\n\nvar B = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if c := checkTree(dir, start); c.Changed == nil || !*c.Changed {
		t.Errorf("a tracked file edited during the run must be reported: %+v", c)
	}

	if c := checkTree(dir, Tree{Unavailable: "git not found"}); c.Changed != nil || c.Unavailable == "" {
		t.Errorf("with no start tree the check cannot be made and must say so: %+v", c)
	}
}

// TestEffectiveTimingsAreTheClustersOwn is plan B2: the record reads the
// timings through the resolution the nodes are configured with, so the two
// cannot disagree.
func TestEffectiveTimingsAreTheClustersOwn(t *testing.T) {
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret")
	if err := os.WriteFile(secret, []byte("simulation-cluster-shared-secret-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := &Cluster{t: t, o: Options{Nodes: 1, TTL: 30 * time.Second, Replicas: 1, Shards: 16}, secret: secret, AckTimeout: time.Second, TakeoverTimeout: time.Second}
	node := &Node{Name: "n1", Dir: filepath.Join(dir, "n1"), TCP: "127.0.0.1:1883", Cluster: "127.0.0.1:7946", Obs: "127.0.0.1:9090", c: c}
	c.Nodes = []*Node{node}

	e := c.effective(1000)
	if e.HeartbeatMs != 100 || e.HeartbeatMisses != 3 || e.RaftTickMs != 10 || e.LeaseTTLMs != 30000 || e.Replicas != 1 {
		t.Errorf("harness defaults: %+v", e)
	}

	t.Setenv("HOARDMQ_SIM_RAFT_TICK_MS", "100")
	t.Setenv("HOARDMQ_SIM_HEARTBEAT_MS", "1000")
	e = c.effective(1000)
	doc := c.config(node)
	if e.RaftTickMs != 100 || e.HeartbeatMs != 1000 {
		t.Errorf("product timings requested: %+v", e)
	}
	if e.RaftTickMs != doc.Cluster.Raft.TickMs || e.HeartbeatMs != doc.Cluster.HeartbeatMs ||
		e.HeartbeatMisses != doc.Cluster.HeartbeatMisses || e.WALGroupDelayMs != doc.Node.WALGroupMaxDelayMs {
		t.Errorf("the record %+v disagrees with the node's configuration %+v / %+v", e, doc.Cluster, doc.Node)
	}
}

// TestProvenanceSectionTracesOnItsOwn is plan B5's content: a published report
// names the commit, the settings, the effective timings and the command.
func TestProvenanceSectionTracesOnItsOwn(t *testing.T) {
	dirty, changed := true, false
	p := Provenance{
		Command:   []string{"/tmp/go-build123/b001/sim.test", "-test.run=^TestFailover$", "-test.timeout=1h0m0s"},
		Settings:  map[string]string{"HOARDMQ_SIM_FAILOVER": "100000", "TMPDIR": "/dev/shm"},
		Effective: &Effective{Clients: 100000, LeaseTTLMs: 30000, HeartbeatMs: 100, HeartbeatMisses: 3, RaftTickMs: 10, Replicas: 2, TempRoot: "/dev/shm", TimingSet: "harness (stress), overridden: ttl 30000 ms", ClientSources: 8, ClientDial: dialAtConnect},
		Tree:      Tree{Commit: "abc123", Dirty: &dirty, DiffSHA256: "d1ff", Changed: []string{" M sim/x.go"}, ChangedTotal: 1},
		TreeAtEnd: &TreeCheck{Changed: &changed},
		Host:      "ai2", GoVersion: "go1.27.1",
	}
	s := provenanceSection(p)
	for _, want := range []string{
		"commit `abc123` with tracked files modified (diff sha256 `d1ff`)",
		"` M sim/x.go`",
		"Unchanged during the run.",
		"`HOARDMQ_SIM_FAILOVER=100000`, `TMPDIR=/dev/shm`",
		"heartbeat 100 ms × 3 misses, raft tick 10 ms",
		"Effective (timings: harness (stress), overridden: ttl 30000 ms):",
		"Clients dial from 8 source addresses: source address, port chosen at connect.",
		"`sim.test -test.run=^TestFailover$ -test.timeout=1h0m0s`",
		"Host `ai2`, go1.27.1",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("section lacks %q:\n%s", want, s)
		}
	}

	p.Tree = Tree{Unavailable: "git rev-parse: fatal: not a git repository"}
	p.TreeAtEnd = &TreeCheck{Unavailable: "the tree was not readable when the run started"}
	s = provenanceSection(p)
	if !strings.Contains(s, "unavailable (git rev-parse: fatal: not a git repository)") || strings.Contains(s, "commit `") {
		t.Errorf("an unavailable tree must be stated as such and name no commit:\n%s", s)
	}
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
