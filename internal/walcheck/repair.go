package walcheck

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/happydave/hoardmq/internal/datadir"
	"github.com/happydave/hoardmq/internal/wal"
)

// Action is one repair: a start's own, made by the start's code.
type Action struct {
	Log    string `json:"log"`
	Repair string `json:"repair"`
	File   string `json:"file"`
	Offset int64  `json:"offset,omitempty"`
	Bytes  int64  `json:"bytes"`
	// Copy is where what the repair drops was kept, "" with -discard or
	// before it ran.
	Copy string `json:"copy,omitempty"`
}

// Decision is whether repair may proceed, and what it does.
type Decision struct {
	Proceed bool
	DryRun  bool
	// Refused says why repair does nothing, "" when it proceeds, runs dry
	// or has nothing to do.
	Refused string
	Actions []Action
}

// Decide is repair's whole decision to write, from the lock this command
// took, -apply and every finding. It proceeds only with -apply, the
// exclusive lock and no finding a start refuses on.
func Decide(lock datadir.Outcome, apply bool, findings []Finding) Decision {
	switch {
	case apply && lock != datadir.Held:
		return Decision{Refused: fmt.Sprintf("the data directory's lock is not held by this command (%s); repair never writes without it", lock)}
	case lock == datadir.HeldElsewhere, lock == datadir.Failed:
		return Decision{Refused: fmt.Sprintf("the data directory's lock could not be taken (%s)", lock)}
	}
	var actions []Action
	for _, f := range findings {
		if f.Start == StartRefuses {
			return Decision{Refused: fmt.Sprintf("log %s: a start refuses on it (%s); %s", f.Log, f.Detail, f.Recovery)}
		}
		if f.Start != StartRepairs {
			continue
		}
		switch f.Repair {
		case RepairTruncateTail, RepairRemoveSegment:
			actions = append(actions, Action{Log: f.Log, Repair: f.Repair, File: f.File, Offset: f.Offset, Bytes: f.Bytes})
		default:
			return Decision{Refused: fmt.Sprintf("log %s: %s has no repair", f.Log, f.Kind)}
		}
	}
	if len(actions) == 0 {
		return Decision{}
	}
	if !apply {
		return Decision{DryRun: true, Actions: actions}
	}
	return Decision{Proceed: true, Actions: actions}
}

// RepairPart is what repair decided and did.
type RepairPart struct {
	DryRun     bool     `json:"dryRun"`
	Refused    string   `json:"refused,omitempty"`
	Actions    []Action `json:"actions"`
	MovedAside string   `json:"movedAside,omitempty"`
	// After is the verification once the repairs were made.
	After *Report `json:"after,omitempty"`
	Error string  `json:"error,omitempty"`
}

// Options are repair's flags.
type Options struct {
	Apply   bool
	Discard bool
	Now     func() time.Time
}

// Repair decides by Decide and, when it proceeds, keeps what each action
// drops (unless Discard), opens each log the start's way, which makes the
// repair, and verifies again.
func Repair(paths Paths, lock *datadir.Lock, o Options) *Report {
	r := Verify(paths, lock)
	d := Decide(lock.Outcome, o.Apply, r.Findings())
	part := &RepairPart{DryRun: d.DryRun, Refused: d.Refused, Actions: d.Actions}
	if part.Actions == nil {
		part.Actions = []Action{}
	}
	r.Repair = part
	if !d.Proceed {
		return r
	}
	if err := apply(paths, part, o); err != nil {
		part.Error = err.Error()
	}
	part.After = Verify(paths, lock)
	return r
}

// apply makes the decided actions and no other change.
func apply(paths Paths, part *RepairPart, o Options) error {
	byLog := map[string]LogPath{}
	for _, p := range paths.Logs {
		byLog[p.Name] = p
	}
	if !o.Discard {
		now := time.Now
		if o.Now != nil {
			now = o.Now
		}
		part.MovedAside = filepath.Join(paths.DataDir, "wal-repair", now().UTC().Format("20060102T150405Z"))
		for i := range part.Actions {
			a := &part.Actions[i]
			c, err := keep(part.MovedAside, byLog[a.Log], *a)
			if err != nil {
				return fmt.Errorf("keeping what %s drops in %s: %w; nothing was repaired", a.Repair, a.File, err)
			}
			a.Copy = c
		}
	}
	done := map[string]bool{}
	for _, a := range part.Actions {
		if done[a.Log] {
			continue
		}
		done[a.Log] = true
		p := byLog[a.Log]
		if err := unchanged(p, part.Actions); err != nil {
			return err
		}
		l, err := wal.Open(p.Parent, p.WAL, wal.Options{Logger: quiet})
		if err != nil {
			return fmt.Errorf("log %s: %w", a.Log, err)
		}
		if err := l.Close(); err != nil {
			return fmt.Errorf("log %s: %w", a.Log, err)
		}
	}
	return nil
}

// unchanged checks that the log's survey still asks for exactly the decided
// actions, so opening it makes those and nothing else.
func unchanged(p LogPath, actions []Action) error {
	sv, _ := wal.Inspect(p.Parent, p.WAL, wal.Options{Logger: quiet})
	var want []Action
	for _, a := range actions {
		if a.Log == p.Name {
			want = append(want, a)
		}
	}
	var got []Action
	if t := sv.TornSegment; t != nil {
		got = append(got, Action{Repair: RepairRemoveSegment, File: t.Segment, Bytes: t.Bytes})
	}
	if t := sv.TornTail; t != nil && sv.Err == nil {
		got = append(got, Action{Repair: RepairTruncateTail, File: t.Segment, Offset: t.Offset, Bytes: t.Bytes})
	}
	if sv.Err != nil || len(got) != len(want) {
		return fmt.Errorf("log %s changed since it was verified; nothing more was repaired", p.Name)
	}
	for i := range got {
		if got[i].Repair != want[i].Repair || got[i].File != want[i].File || got[i].Offset != want[i].Offset || got[i].Bytes != want[i].Bytes {
			return fmt.Errorf("log %s changed since it was verified; nothing more was repaired", p.Name)
		}
	}
	return nil
}

// keep copies what an action drops into dir/<log>/ and syncs it: a torn
// tail's bytes as <segment>.tail, a removed segment whole.
func keep(dir string, p LogPath, a Action) (string, error) {
	dst := filepath.Join(dir, a.Log)
	if err := os.MkdirAll(dst, 0o750); err != nil {
		return "", err
	}
	src, err := os.Open(filepath.Join(p.Dir(), a.File))
	if err != nil {
		return "", err
	}
	defer func() { _ = src.Close() }()
	name := a.File
	if a.Repair == RepairTruncateTail {
		name += ".tail"
		if _, err := src.Seek(a.Offset, io.SeekStart); err != nil {
			return "", err
		}
	}
	path := filepath.Join(dst, name)
	out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return "", err
	}
	n, err := io.Copy(out, src)
	if err == nil && n != a.Bytes {
		err = fmt.Errorf("copied %d bytes of %d", n, a.Bytes)
	}
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", err
	}
	aside := filepath.Dir(dst)
	return path, syncDirs(dst, aside, filepath.Dir(aside), filepath.Dir(filepath.Dir(aside)))
}

func syncDirs(dirs ...string) error {
	for _, d := range dirs {
		f, err := os.Open(d)
		if err != nil {
			return err
		}
		err = f.Sync()
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
	}
	return nil
}
