package sim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/config"
)

// A measurement records how it was produced, so a published result can be
// traced to its parameters from the repository alone. Before this, those
// parameters lived in driver scripts outside version control, and a round
// published as "product timings" ran at the harness's 10 ms raft tick
// (WI 1379).

// Provenance is what a measurement records about its own production. A value
// that could not be read is stated as unavailable with the reason; nothing
// here is ever a default filled in for something unobserved.
type Provenance struct {
	// Command is the test binary's command line.
	Command []string `json:"command"`
	// Settings are the recorded environment variables that were set, empty
	// values included: see recordedSetting for which names.
	Settings  map[string]string `json:"settings"`
	Effective *Effective        `json:"effective,omitempty"`
	Tree      Tree              `json:"tree"`
	// TreeAtEnd is the same tree read again when the artifact is written.
	TreeAtEnd *TreeCheck `json:"tree_at_end,omitempty"`
	Host      string     `json:"host"`
	GoVersion string     `json:"go_version"`
}

// Effective is what the cluster was actually given. An unset variable means
// a harness default, so the settings alone do not say this.
type Effective struct {
	Clients         int `json:"clients"`
	LeaseTTLMs      int `json:"lease_ttl_ms"`
	HeartbeatMs     int `json:"heartbeat_ms"`
	HeartbeatMisses int `json:"heartbeat_misses"`
	RaftTickMs      int `json:"raft_tick_ms"`
	WALGroupDelayMs int `json:"wal_group_delay_ms"`
	// The other timings the nodes read (WI 1805). A record without them was
	// written when the harness ran each at its own value whatever the set.
	TakeoverTimeoutMs     int    `json:"takeover_timeout_ms"`
	AckTimeoutMs          int    `json:"ack_timeout_ms"`
	JoinTimeoutMs         int    `json:"join_timeout_ms"`
	ConnectTimeoutSec     int    `json:"connect_timeout_sec"`
	ReadinessStableMs     int    `json:"readiness_stable_ms"`
	DrainRatePerSecond    int    `json:"drain_rate_per_sec"`
	DrainTimeoutMs        int    `json:"drain_timeout_ms"`
	DrainConfirmTimeoutMs int    `json:"drain_confirm_timeout_ms"`
	Replicas              int    `json:"replicas"`
	Shards                int    `json:"shards"`
	TempRoot              string `json:"temp_root"`
	// TimingSet names the set the values above came from, and any knob
	// moved away from it (WI 1475).
	TimingSet string `json:"timing_set"`
	// ClientSources is how many source addresses the clients dialled from,
	// and ClientDial how one real socket dialled (WI 1477).
	ClientSources int    `json:"client_sources"`
	ClientDial    string `json:"client_dial,omitempty"`
	// The remote-client mode (WI 1480): where the clients ran and the
	// addresses the nodes listened on. Absent for an in-process run.
	ClientMode string `json:"client_mode,omitempty"`
	ClientHost string `json:"client_host,omitempty"`
	// Auth is how the clients authenticated: anonymous, jwt, ldap or file.
	Auth          string   `json:"auth"`
	NodeAddresses []string `json:"node_addresses,omitempty"`
	// The client listener's connect-rate limit and the password cache's TTL
	// the nodes ran with (WI 1283).
	ConnectRate     int `json:"connect_rate"`
	ConnectBurst    int `json:"connect_burst"`
	AuthCacheTTLSec int `json:"auth_cache_ttl_sec"`
	// TraceRatio is traces.otlp's sampleRatio when the run traces (WI 1313).
	TraceRatio *float64 `json:"trace_ratio,omitempty"`
}

// Tree is the source the measurement was built from. Dirty is nil when the
// tree could not be read, so an unavailable tree never reads as a clean one.
type Tree struct {
	Commit string `json:"commit,omitempty"`
	// Dirty says whether tracked files differ from Commit; DiffSHA256 is the
	// digest of that difference, so two dirty trees can be told apart.
	Dirty      *bool  `json:"dirty,omitempty"`
	DiffSHA256 string `json:"diff_sha256,omitempty"`
	// Changed is git's short status, untracked files included, bounded at
	// changedBound entries; ChangedTotal is the full count.
	Changed      []string `json:"changed,omitempty"`
	ChangedTotal int      `json:"changed_total,omitempty"`
	Unavailable  string   `json:"unavailable,omitempty"`
}

// TreeCheck says whether the tree changed while the measurement ran. Changed
// is nil when the check could not be made.
type TreeCheck struct {
	Changed     *bool  `json:"changed,omitempty"`
	Unavailable string `json:"unavailable,omitempty"`
}

const (
	gitBound     = 10 * time.Second
	changedBound = 50
)

// runtimeSettings are the non-harness variables that change what a
// measurement measures: TMPDIR puts the node stores on disk or tmpfs.
var runtimeSettings = map[string]bool{
	"TMPDIR": true, "GOTMPDIR": true, "GOMAXPROCS": true, "GOGC": true, "GOMEMLIMIT": true, "GODEBUG": true,
}

// recordedSetting decides whether a variable is recorded. Results are
// committed, and an environment can carry credentials, so the rule names what
// is recorded rather than what is left out.
func recordedSetting(name string) bool {
	return strings.HasPrefix(name, "HOARDMQ_SIM_") || runtimeSettings[name]
}

// readSettings returns the recorded variables from an environment list in
// os.Environ's form.
func readSettings(environ []string) map[string]string {
	out := make(map[string]string)
	for _, kv := range environ {
		name, value, ok := strings.Cut(kv, "=")
		if ok && recordedSetting(name) {
			out[name] = value
		}
	}
	return out
}

// readProvenance records the command, settings, source tree, host and Go
// version. dir is any directory inside the source tree.
func readProvenance(command, environ []string, dir string) Provenance {
	p := Provenance{
		Command:   append([]string(nil), command...),
		Settings:  readSettings(environ),
		Tree:      readTree(dir),
		GoVersion: runtime.Version(),
	}
	if h, err := os.Hostname(); err != nil {
		p.Host = "unavailable: " + err.Error()
	} else {
		p.Host = h
	}
	return p
}

// effective reads the values the cluster's nodes were built with, through
// the same resolution Cluster.config uses.
func (c *Cluster) effective(clients int) Effective {
	doc := config.Default()
	c.applyTimings(&doc)
	e := Effective{
		Clients:               clients,
		LeaseTTLMs:            doc.Cluster.Leases.TTLMs,
		HeartbeatMs:           doc.Cluster.HeartbeatMs,
		HeartbeatMisses:       doc.Cluster.HeartbeatMisses,
		RaftTickMs:            doc.Cluster.Raft.TickMs,
		WALGroupDelayMs:       doc.Node.WALGroupMaxDelayMs,
		TakeoverTimeoutMs:     doc.Cluster.Leases.TakeoverTimeoutMs,
		AckTimeoutMs:          doc.Cluster.Replication.AckTimeoutMs,
		JoinTimeoutMs:         doc.Cluster.Raft.JoinTimeoutMs,
		ConnectTimeoutSec:     doc.Limits.ConnectTimeout,
		ReadinessStableMs:     doc.Cluster.Readiness.StableMs,
		DrainRatePerSecond:    doc.Cluster.Drain.RatePerSecond,
		DrainTimeoutMs:        doc.Cluster.Drain.TimeoutMs,
		DrainConfirmTimeoutMs: doc.Cluster.Drain.ConfirmTimeoutMs,
		Replicas:              c.o.Replicas,
		Shards:                c.o.Shards,
		TempRoot:              os.TempDir(),
	}
	e.TimingSet = timingLabel(c.o.Timings, e)
	e.Auth = "anonymous"
	if c.jwt != nil {
		e.Auth = "jwt"
	}
	if c.directory != nil {
		e.Auth = "ldap"
	}
	if c.users != "" {
		e.Auth = "file"
	}
	doc.Listeners = []config.Listener{{}}
	c.knobs.apply(&doc)
	e.ConnectRate, e.ConnectBurst, e.AuthCacheTTLSec = doc.Listeners[0].ConnectRate, doc.Listeners[0].ConnectBurst, doc.Auth.Cache.TTLSeconds
	if c.knobs.traceSet {
		r := c.knobs.traceRatio
		e.TraceRatio = &r
	}
	return e
}

// readTree reads the commit, the tracked-file difference and the short
// status of the git checkout containing dir, within gitBound.
func readTree(dir string) Tree {
	ctx, cancel := context.WithTimeout(context.Background(), gitBound)
	defer cancel()
	commit, err := git(ctx, dir, nil, "rev-parse", "HEAD")
	if err != nil {
		return Tree{Unavailable: err.Error()}
	}
	diff := &countingHash{h: sha256.New()}
	if _, err := git(ctx, dir, diff, "diff", "--no-ext-diff", "--binary", "HEAD"); err != nil {
		return Tree{Unavailable: err.Error()}
	}
	status, err := git(ctx, dir, nil, "status", "--porcelain=v1", "--untracked-files=normal")
	if err != nil {
		return Tree{Unavailable: err.Error()}
	}
	dirty := diff.n > 0
	t := Tree{Commit: commit, Dirty: &dirty}
	if dirty {
		t.DiffSHA256 = hex.EncodeToString(diff.h.Sum(nil))
	}
	if status != "" {
		lines := strings.Split(status, "\n")
		t.ChangedTotal = len(lines)
		t.Changed = lines[:min(len(lines), changedBound)]
	}
	return t
}

// checkTree reads the tree again and reports whether its commit or its
// tracked-file difference moved since start. Untracked files are not compared:
// the measurement writes an untracked report into the tree it measures.
func checkTree(dir string, start Tree) *TreeCheck {
	if start.Unavailable != "" {
		return &TreeCheck{Unavailable: "the tree was not readable when the run started"}
	}
	end := readTree(dir)
	if end.Unavailable != "" {
		return &TreeCheck{Unavailable: end.Unavailable}
	}
	changed := end.Commit != start.Commit || end.DiffSHA256 != start.DiffSHA256
	return &TreeCheck{Changed: &changed}
}

// git runs one git subcommand in dir. With out set, stdout goes there and the
// returned string is empty.
func git(ctx context.Context, dir string, out io.Writer, sub string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"--no-optional-locks", sub}, args...)...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	if out != nil {
		cmd.Stdout = out
	}
	cmd.Stderr = &stderr
	err := cmd.Run()
	switch {
	case errors.Is(err, exec.ErrNotFound):
		return "", errors.New("git not found")
	case ctx.Err() != nil:
		return "", fmt.Errorf("git %s timed out after %s", sub, gitBound)
	case err != nil:
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return "", fmt.Errorf("git %s: %s", sub, msg)
	}
	return strings.TrimRight(stdout.String(), "\n"), nil
}

// countingHash hashes what git writes and counts it, so an empty diff is
// known to be clean without a second git call.
type countingHash struct {
	h hash.Hash
	n int64
}

func (c *countingHash) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return c.h.Write(p)
}

// provenanceSection is the report's closing section: the same record in a
// form a reader of a published .md needs no other file for.
func provenanceSection(p Provenance) string {
	var b strings.Builder
	b.WriteString("\n## Provenance\n\n")
	b.WriteString("Source tree: ")
	switch {
	case p.Tree.Unavailable != "":
		fmt.Fprintf(&b, "unavailable (%s).", p.Tree.Unavailable)
	case p.Tree.Dirty != nil && *p.Tree.Dirty:
		fmt.Fprintf(&b, "commit `%s` with tracked files modified (diff sha256 `%s`).", p.Tree.Commit, p.Tree.DiffSHA256)
	default:
		fmt.Fprintf(&b, "commit `%s`, tracked files unmodified.", p.Tree.Commit)
	}
	if p.Tree.ChangedTotal > 0 {
		fmt.Fprintf(&b, " Git status lists %d path(s): `%s`", p.Tree.ChangedTotal, strings.Join(p.Tree.Changed, "`, `"))
		if p.Tree.ChangedTotal > len(p.Tree.Changed) {
			b.WriteString(", …")
		}
		b.WriteString(".")
	}
	switch {
	case p.TreeAtEnd == nil:
	case p.TreeAtEnd.Changed == nil:
		fmt.Fprintf(&b, " Whether it changed during the run could not be checked (%s).", p.TreeAtEnd.Unavailable)
	case *p.TreeAtEnd.Changed:
		b.WriteString(" **The tree changed during the run.**")
	default:
		b.WriteString(" Unchanged during the run.")
	}
	b.WriteString("\n\nSettings: ")
	if len(p.Settings) == 0 {
		b.WriteString("none set, so every value is the harness default.")
	} else {
		names := make([]string, 0, len(p.Settings))
		for name := range p.Settings {
			names = append(names, name)
		}
		sort.Strings(names)
		for i, name := range names {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "`%s=%s`", name, p.Settings[name])
		}
		b.WriteString(".")
	}
	if e := p.Effective; e != nil {
		timings := ""
		if e.TimingSet != "" {
			timings = " (timings: " + e.TimingSet + ")"
		}
		fmt.Fprintf(&b, "\n\nEffective%s: %d clients, lease ttl %d ms, heartbeat %d ms × %d misses, raft tick %d ms, group-commit hold %d ms, replicas %d, stores under `%s`.",
			timings, e.Clients, e.LeaseTTLMs, e.HeartbeatMs, e.HeartbeatMisses, e.RaftTickMs, e.WALGroupDelayMs, e.Replicas, e.TempRoot)
		if e.TakeoverTimeoutMs > 0 {
			fmt.Fprintf(&b, " Takeover timeout (the move grace) %d ms, replication ack timeout %d ms, join timeout %d ms, connect timeout %d s, readiness stable %d ms, drain %d clients/s with timeout %d ms and confirmation %d ms, %d shards.",
				e.TakeoverTimeoutMs, e.AckTimeoutMs, e.JoinTimeoutMs, e.ConnectTimeoutSec, e.ReadinessStableMs, e.DrainRatePerSecond, e.DrainTimeoutMs, e.DrainConfirmTimeoutMs, e.Shards)
		}
		switch {
		case e.ClientSources > 0:
			fmt.Fprintf(&b, " Clients dial from %d source addresses: %s.", e.ClientSources, e.ClientDial)
		case e.ClientDial != "":
			fmt.Fprintf(&b, " Clients dial: %s.", e.ClientDial)
		}
		if e.ClientMode == "remote" {
			fmt.Fprintf(&b, " Clients run in a separate driver process at `%s`; nodes listen on `%s`.", e.ClientHost, strings.Join(e.NodeAddresses, "`, `"))
		}
	}
	b.WriteString("\n\nCommand: `")
	if len(p.Command) > 0 {
		b.WriteString(strings.Join(append([]string{filepath.Base(p.Command[0])}, p.Command[1:]...), " "))
	}
	fmt.Fprintf(&b, "`. Host `%s`, %s.\n", p.Host, p.GoVersion)
	return b.String()
}
