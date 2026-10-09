package sim

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"
)

// artifactEnv names the directory run artifacts are written under. Absent,
// nothing is written and a run behaves exactly as before: the artifact is
// evidence capture, never part of the verdict.
const artifactEnv = "HOARDMQ_SIM_ARTIFACT"

// stepView is a Step in artifact form: the rendered line plus the fields
// the viewer needs to place it on the time axis.
type stepView struct {
	Text   string  `json:"text"`
	Kind   string  `json:"kind"`
	Node   string  `json:"node"`
	AtSec  float64 `json:"at_sec"` // offset from the fault window's start
	DurSec float64 `json:"dur_sec"`
}

// probeView and failClosedView replace error values with strings so the
// artifact marshals faithfully (a bare error marshals as {}).
type probeView struct {
	At   time.Time `json:"at"`
	Node string    `json:"node"`
	Code string    `json:"code"`
	Ref  string    `json:"ref,omitempty"`
	Err  string    `json:"err,omitempty"`
}

type failClosedView struct {
	Node    string `json:"node"`
	Log     string `json:"log"`
	Refused bool   `json:"refused"`
	Err     string `json:"err,omitempty"`
}

// coverage records what the run actually exercised, so a vacuous run is
// visible as vacuous: a property whose input evidence never arrived proved
// nothing, whatever colour the run was. (The FoundationDB code-probe
// pattern; the structural answer to the WI 1308 mis-scoring class.)
type coverage struct {
	Publications int             `json:"publications"`
	Deliveries   int             `json:"deliveries"`
	Events       int             `json:"events"`
	Probes       int             `json:"probes"`
	FailClosed   int             `json:"fail_closed"`
	FaultKinds   map[string]int  `json:"fault_kinds"`
	Exercised    map[string]bool `json:"exercised"`
}

// runArtifact is run.json: everything the run saw, structured.
type runArtifact struct {
	Test       string           `json:"test"`
	Seed       int64            `json:"seed"`
	GoVersion  string           `json:"go_version"`
	Revision   string           `json:"revision,omitempty"`
	Written    time.Time        `json:"written"`
	DurationMS int64            `json:"duration_ms"`
	Nodes      int              `json:"nodes"`
	Replicas   int              `json:"replicas"`
	TTL        string           `json:"ttl"`
	FaultsAt   time.Time        `json:"faults_at"` // when the schedule started executing
	Schedule   []stepView       `json:"schedule"`
	Stats      map[string]int   `json:"stats"`
	Violations []Violation      `json:"violations"`
	Ledger     *Ledger          `json:"ledger"`
	Owners     []OwnerSample    `json:"owner_samples,omitempty"`
	Routes     []RouteSample    `json:"route_samples,omitempty"`
	Probes     []probeView      `json:"probes,omitempty"`
	FailClosed []failClosedView `json:"fail_closed,omitempty"`
	// HealedAt is when every fault was healed and the cluster settled;
	// Recovery is each node's refusing time from then (absent when a node was
	// down and nothing was watched).
	HealedAt *time.Time     `json:"healed_at,omitempty"`
	Recovery []NodeRecovery `json:"recovery,omitempty"`
	// RefusedKeys are the keys peers refused records for, watched from the
	// heal (WI 1531).
	RefusedKeys []KeyBlock   `json:"refused_keys,omitempty"`
	ResendLag   string       `json:"resend_lag"`
	ResendWin   string       `json:"resend_window"`
	Runtime     runtimeDelta `json:"runtime"`
	ProfileMS   int64        `json:"profile_write_ms"`
	ProfileErr  string       `json:"profile_write_err,omitempty"`
}

// vcsRevision reads the build's VCS revision when the toolchain embedded
// one; test binaries built without VCS stamping simply omit it.
func vcsRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			return s.Value
		}
	}
	return ""
}

// writeRunArtifact writes one run's evidence under $HOARDMQ_SIM_ARTIFACT.
// Failures are logged and never affect the run's verdict.
func writeRunArtifact(t testing.TB, rep Report, e Evidence, faultsAt time.Time, rt runtimeDelta) {
	base := os.Getenv(artifactEnv)
	if base == "" {
		return
	}
	name := strings.ReplaceAll(t.Name(), "/", "_")
	dir := filepath.Join(base, name+"-seed"+strconv.FormatInt(rep.Seed, 10)+"-"+time.Now().Format("150405"))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Logf("artifact: %v", err)
		return
	}

	art := runArtifact{
		Test:       name,
		Seed:       rep.Seed,
		GoVersion:  runtime.Version(),
		Revision:   vcsRevision(),
		Written:    time.Now(),
		DurationMS: rep.Duration.Milliseconds(),
		FaultsAt:   faultsAt,
		Stats:      rep.Stats,
		Violations: rep.Violations,
		Ledger:     e.Ledger,
		Owners:     e.Owners,
		Routes:     e.Routes,
		ResendLag:  e.ResendLag.String(),
		ResendWin:  e.ResendWindow.String(),
		Runtime:    rt,
	}
	if rep.Recovery != nil {
		healed := rep.HealedAt
		art.HealedAt, art.Recovery = &healed, rep.Recovery
		art.RefusedKeys = rep.RefusedKeys
	}
	if rep.Cluster != nil {
		art.Nodes = len(rep.Cluster.Nodes)
		art.Replicas = rep.Cluster.o.Replicas
		art.TTL = rep.Cluster.o.TTL.String()
	}
	for _, s := range rep.Schedule {
		art.Schedule = append(art.Schedule, stepView{
			Text: s.String(), Kind: s.Kind, Node: s.Node,
			AtSec: s.At.Seconds(), DurSec: s.Dur.Seconds(),
		})
	}
	for _, p := range e.Probes {
		v := probeView{At: p.At, Node: p.Node, Code: p.Code.String(), Ref: p.Ref}
		if p.Err != nil {
			v.Err = p.Err.Error()
		}
		art.Probes = append(art.Probes, v)
	}
	for _, f := range e.FailClosed {
		v := failClosedView{Node: f.Node, Log: f.Log, Refused: f.Refused}
		if f.Err != nil {
			v.Err = f.Err.Error()
		}
		art.FailClosed = append(art.FailClosed, v)
	}

	if d, err := writeProfiles(dir, "end"); err != nil {
		art.ProfileMS, art.ProfileErr = d.Milliseconds(), err.Error()
	} else {
		art.ProfileMS = d.Milliseconds()
	}

	raw, err := json.MarshalIndent(withCoverage(art), "", " ")
	if err != nil {
		t.Logf("artifact: marshal: %v", err)
		return
	}
	if err := os.WriteFile(filepath.Join(dir, "run.json"), raw, 0o640); err != nil { //nolint:gosec // operator-chosen dir
		t.Logf("artifact: %v", err)
		return
	}
	page := strings.Replace(timelineHTML, "__RUN_JSON__", string(raw), 1)
	if err := os.WriteFile(filepath.Join(dir, "timeline.html"), []byte(page), 0o640); err != nil { //nolint:gosec // operator-chosen dir
		t.Logf("artifact: %v", err)
		return
	}
	t.Logf("artifact written: %s", dir)
}

// artifactWithCoverage wraps the artifact with its derived coverage block.
type artifactWithCoverage struct {
	runArtifact
	Coverage coverage `json:"coverage"`
}

func withCoverage(a runArtifact) artifactWithCoverage {
	c := coverage{FaultKinds: map[string]int{}, Exercised: map[string]bool{}}
	if a.Ledger != nil {
		c.Publications = len(a.Ledger.Publications)
		c.Deliveries = len(a.Ledger.Deliveries)
		c.Events = len(a.Ledger.Events)
	}
	c.Probes = len(a.Probes)
	c.FailClosed = len(a.FailClosed)
	for _, s := range a.Schedule {
		c.FaultKinds[s.Kind]++
	}
	c.Exercised["traffic"] = c.Publications > 0 && c.Deliveries > 0
	c.Exercised["connections"] = c.Events > 0
	c.Exercised["minority-probes"] = c.Probes > 0
	c.Exercised["fail-closed"] = c.FailClosed > 0
	c.Exercised["faults"] = len(a.Schedule) > 0
	return artifactWithCoverage{runArtifact: a, Coverage: c}
}
