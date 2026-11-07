// Package walcheck reports what a node's start would repair or refuse in its
// write-ahead logs and checkpoints, by the start's own code, and makes the
// repairs a start would make, keeping what they drop.
package walcheck

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/happydave/hoardmq/internal/cluster/consensus/etcdraft"
	"github.com/happydave/hoardmq/internal/cluster/consensus/raftlog"
	"github.com/happydave/hoardmq/internal/datadir"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/wal"
)

// Log kinds: how a start opens the log after its survey.
const (
	KindState = "state" // store.Open and Recover
	KindRaft  = "raft"  // raftlog.Open and the stored snapshot
)

// LogPath is one log as a start opens it: wal.Open(Parent, WAL).
type LogPath struct {
	Name   string // state, replica, raft
	Kind   string
	Parent string
	WAL    string
}

// Dir is the log's directory.
func (p LogPath) Dir() string { return filepath.Join(p.Parent, p.WAL) }

// Paths are a node's data directory and its logs.
type Paths struct {
	DataDir string
	Logs    []LogPath
}

// NodePaths are the logs a node with this data directory opens, the state
// log under stateParent (store.wal's directory).
func NodePaths(dataDir, stateParent string) Paths {
	return Paths{DataDir: dataDir, Logs: []LogPath{
		{Name: "state", Kind: KindState, Parent: stateParent, WAL: "state"},
		{Name: "replica", Kind: KindState, Parent: filepath.Join(dataDir, "replica"), WAL: "state"},
		{Name: "raft", Kind: KindRaft, Parent: filepath.Join(dataDir, "logs"), WAL: "raft"},
	}}
}

// What a start does about a finding.
const (
	StartRepairs = "repairs"
	StartRefuses = "refuses"
	StartNotes   = "notes"
)

// What repair does about a finding.
const (
	RepairTruncateTail  = "truncate-tail"
	RepairRemoveSegment = "remove-segment"
	RepairNone          = "none"
	RepairRefused       = "refused"
)

// Finding kinds.
const (
	FindTornTail          = "torn-tail"
	FindTornSegment       = "torn-segment"
	FindNoSegment         = "no-segment"
	FindCheckpointAhead   = "checkpoint-ahead"
	FindSkippedCheckpoint = "skipped-checkpoint"
	FindDamagedRecord     = "damaged-record"
	FindBadSegment        = "bad-segment"
	FindGap               = "gap"
	FindRecovery          = "recovery"
	FindSnapshot          = "snapshot-unreadable"
)

// The recoveries a refusal names.
const (
	recoverState = "not repaired by this command (held for the owner): restore the data directory from a backup, " +
		"or remove the member (hoardmq cluster remove NODE) and start it again with an empty data directory"
	recoverGap  = "not repairable in place: restore the data directory from a backup, or remove the member (hoardmq cluster remove NODE) and start it again with an empty data directory"
	recoverRaft = "not repaired by this command: remove the member (hoardmq cluster remove NODE) and start it again with an empty data directory"
)

// Finding is one thing a start would repair, refuse on or note.
type Finding struct {
	Log      string `json:"log"`
	Kind     string `json:"kind"`
	File     string `json:"file,omitempty"`
	Offset   int64  `json:"offset,omitempty"`
	Index    uint64 `json:"index,omitempty"`
	Bytes    int64  `json:"bytes,omitempty"`
	Detail   string `json:"detail"`
	Start    string `json:"start"`
	Repair   string `json:"repair"`
	Recovery string `json:"recovery,omitempty"`
}

// Segment is one segment as a start keeps it.
type Segment struct {
	File  string `json:"file"`
	First uint64 `json:"first"`
	Last  uint64 `json:"last"`
	Bytes int64  `json:"bytes"`
}

// Checkpoint is the checkpoint a start recovers from.
type Checkpoint struct {
	File  string `json:"file"`
	Index uint64 `json:"index"`
}

// LogReport is one log's verification.
type LogReport struct {
	Name       string      `json:"name"`
	Dir        string      `json:"dir"`
	Present    bool        `json:"present"`
	Segments   []Segment   `json:"segments"`
	Checkpoint *Checkpoint `json:"checkpoint"`
	Findings   []Finding   `json:"findings"`
}

// LockState is the data directory lock as the command found it.
type LockState struct {
	State string `json:"state"`
	Path  string `json:"path"`
	PID   int    `json:"pid,omitempty"`
	Error string `json:"error,omitempty"`
}

// Verdicts.
const (
	VerdictClean        = "clean"
	VerdictStartRepairs = "start-repairs"
	VerdictStartRefuses = "start-refuses"
)

// Report is a verification, and for repair what it did.
type Report struct {
	DataDir string      `json:"dataDir"`
	Lock    LockState   `json:"lock"`
	Logs    []LogReport `json:"logs"`
	Verdict string      `json:"verdict"`
	Repair  *RepairPart `json:"repair,omitempty"`
}

// Findings is every log's findings in order.
func (r *Report) Findings() []Finding {
	var out []Finding
	for _, l := range r.Logs {
		out = append(out, l.Findings...)
	}
	return out
}

func lockState(l *datadir.Lock) LockState {
	s := LockState{State: l.Outcome.String(), Path: l.Path, PID: l.PID}
	if l.Err != nil {
		s.Error = l.Err.Error()
	}
	return s
}

// Verify reports every log in paths, writing nothing.
func Verify(paths Paths, lock *datadir.Lock) *Report {
	r := &Report{DataDir: paths.DataDir, Lock: lockState(lock), Logs: make([]LogReport, 0, len(paths.Logs))}
	for _, p := range paths.Logs {
		r.Logs = append(r.Logs, verifyLog(p))
	}
	r.Verdict = verdict(r.Findings())
	return r
}

func verdict(findings []Finding) string {
	v := VerdictClean
	for _, f := range findings {
		switch f.Start {
		case StartRefuses:
			return VerdictStartRefuses
		case StartRepairs:
			v = VerdictStartRepairs
		}
	}
	return v
}

var quiet = slog.New(slog.DiscardHandler)

func verifyLog(p LogPath) LogReport {
	lr := LogReport{Name: p.Name, Dir: p.Dir(), Segments: []Segment{}, Findings: []Finding{}}
	if info, err := os.Stat(p.Dir()); err != nil || !info.IsDir() {
		return lr
	}
	lr.Present = true
	add := func(f Finding) {
		f.Log = p.Name
		if f.Start == StartRefuses {
			f.Repair = RepairRefused
			switch {
			case p.Kind == KindRaft:
				f.Recovery = recoverRaft
			case f.Kind == FindGap:
				f.Recovery = recoverGap
			default:
				f.Recovery = recoverState
			}
		} else if f.Repair == "" {
			f.Repair = RepairNone
		}
		lr.Findings = append(lr.Findings, f)
	}
	sv, l := wal.Inspect(p.Parent, p.WAL, wal.Options{Logger: quiet})
	if t := sv.TornSegment; t != nil {
		add(Finding{Kind: FindTornSegment, File: t.Segment, Bytes: t.Bytes, Start: StartRepairs, Repair: RepairRemoveSegment,
			Detail: fmt.Sprintf("segment %s was left before its header reached the disk (%d bytes, no record)", t.Segment, t.Bytes)})
	}
	if sv.Err != nil {
		f := Finding{Kind: FindBadSegment, Detail: sv.Err.Error(), Start: StartRefuses}
		var ce *wal.CorruptError
		if errors.As(sv.Err, &ce) {
			f.Kind, f.File, f.Offset, f.Index = FindDamagedRecord, ce.Segment, ce.Offset, ce.Index
		}
		add(f)
		return lr
	}
	for _, s := range sv.Segments {
		lr.Segments = append(lr.Segments, Segment{File: s.File, First: s.First, Last: s.Last, Bytes: s.Bytes})
	}
	if t := sv.TornTail; t != nil {
		add(Finding{Kind: FindTornTail, File: t.Segment, Offset: t.Offset, Index: t.Index, Bytes: t.Bytes, Start: StartRepairs,
			Repair: RepairTruncateTail, Detail: fmt.Sprintf("record %d cut short (%s); %d bytes after offset %d", t.Index, t.Reason, t.Bytes, t.Offset)})
	}
	if sv.NewSegment > 0 {
		add(Finding{Kind: FindNoSegment, Index: sv.NewSegment, Start: StartNotes,
			Detail: fmt.Sprintf("no segment is left; a start creates one at index %d", sv.NewSegment)})
	}
	idx, file, skipped := l.Checkpoints()
	if file != "" {
		lr.Checkpoint = &Checkpoint{File: file, Index: idx}
	}
	for _, s := range skipped {
		add(Finding{Kind: FindSkippedCheckpoint, File: s.File, Start: StartNotes,
			Detail: fmt.Sprintf("checkpoint %s is passed over (%s)", s.File, s.Reason)})
	}
	var after uint64
	var err error
	switch p.Kind {
	case KindRaft:
		v := raftlog.VerifyLog(l)
		after, err = v.AppendAfter, v.Err
		if err == nil {
			if serr := etcdraft.CheckStoredSnapshot(p.Dir()+"-snapshots", v.Snapshot); serr != nil {
				add(Finding{Kind: FindSnapshot, Detail: serr.Error(), Start: StartRefuses})
			}
		}
	default:
		v := store.VerifyLog(l)
		after, err = v.AppendAfter, v.Err
	}
	if after > 0 {
		add(Finding{Kind: FindCheckpointAhead, Index: after, Start: StartNotes,
			Detail: fmt.Sprintf("the log ends at %d, before its checkpoint's replay point; a start appends after %d", l.LastIndex(), after)})
	}
	if err != nil {
		f := Finding{Kind: FindRecovery, Detail: err.Error(), Start: StartRefuses}
		var gap *wal.GapError
		if errors.As(err, &gap) {
			f.Kind, f.File, f.Index = FindGap, gap.Segment, gap.Missing
		}
		add(f)
	}
	return lr
}
