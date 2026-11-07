package walcheck

import (
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/datadir"
)

func repairable() []Finding {
	return []Finding{
		{Log: "state", Kind: FindTornTail, File: "00000000000000000003.seg", Offset: 90, Bytes: 4, Start: StartRepairs, Repair: RepairTruncateTail},
		{Log: "raft", Kind: FindTornSegment, File: "00000000000000000009.seg", Bytes: 7, Start: StartRepairs, Repair: RepairRemoveSegment},
		{Log: "state", Kind: FindSkippedCheckpoint, Start: StartNotes, Repair: RepairNone},
	}
}

func refusing(kind, log, recovery string) Finding {
	return Finding{Log: log, Kind: kind, Detail: kind + " here", Start: StartRefuses, Repair: RepairRefused, Recovery: recovery}
}

// TestRepairDecision: every argument set repair exists to refuse, and those
// it proceeds on. The decision is made here, on arguments the test builds;
// no repair runs.
func TestRepairDecision(t *testing.T) {
	refusals := []Finding{
		refusing(FindDamagedRecord, "state", recoverState),
		refusing(FindBadSegment, "replica", recoverState),
		refusing(FindRecovery, "state", recoverState),
		refusing(FindGap, "replica", recoverGap),
		refusing(FindDamagedRecord, "raft", recoverRaft),
		refusing(FindRecovery, "raft", recoverRaft),
		refusing(FindSnapshot, "raft", recoverRaft),
	}
	type want struct {
		proceed, dry bool
		refused      string // a substring; "" none
		actions      int
	}
	cases := []struct {
		name     string
		lock     datadir.Outcome
		apply    bool
		findings []Finding
		want     want
	}{
		{"apply, held elsewhere", datadir.HeldElsewhere, true, repairable(), want{refused: "not held by this command"}},
		{"apply, unsupported", datadir.Unsupported, true, repairable(), want{refused: "not held by this command"}},
		{"apply, failed", datadir.Failed, true, repairable(), want{refused: "not held by this command"}},
		{"apply, no lock file (a shared attempt)", datadir.Absent, true, repairable(), want{refused: "not held by this command"}},
		{"dry run, held elsewhere", datadir.HeldElsewhere, false, repairable(), want{refused: "could not be taken"}},
		{"dry run, failed", datadir.Failed, false, repairable(), want{refused: "could not be taken"}},
		{"apply, a refusing finding beside repairable ones", datadir.Held, true, append(repairable(), refusals[0]), want{refused: "a start refuses on it"}},
		{"apply, a refusing finding first", datadir.Held, true, append([]Finding{refusals[3]}, repairable()...), want{refused: "not repairable in place"}},
		{"apply, a repair of a kind with none", datadir.Held, true, []Finding{{Log: "state", Kind: FindCheckpointAhead, Start: StartRepairs}}, want{refused: "has no repair"}},
		{"apply, nothing to repair", datadir.Held, true, []Finding{repairable()[2]}, want{}},
		{"dry run", datadir.Held, false, repairable(), want{dry: true, actions: 2}},
		{"dry run, no lock file", datadir.Absent, false, repairable(), want{dry: true, actions: 2}},
		{"dry run, unsupported", datadir.Unsupported, false, repairable(), want{dry: true, actions: 2}},
		{"apply", datadir.Held, true, repairable(), want{proceed: true, actions: 2}},
	}
	for _, r := range refusals {
		cases = append(cases, struct {
			name     string
			lock     datadir.Outcome
			apply    bool
			findings []Finding
			want     want
		}{"apply, " + r.Log + " " + r.Kind, datadir.Held, true, []Finding{r}, want{refused: r.Recovery}})
	}
	for _, c := range cases {
		d := Decide(c.lock, c.apply, c.findings)
		bad := d.Proceed != c.want.proceed || d.DryRun != c.want.dry || len(d.Actions) != c.want.actions ||
			(c.want.refused == "") != (d.Refused == "") || !strings.Contains(d.Refused, c.want.refused)
		if bad {
			t.Errorf("%s: %+v, want %+v", c.name, d, c.want)
		}
		if d.Refused != "" && (d.Proceed || len(d.Actions) > 0) {
			t.Errorf("%s: refused yet proceeds or has actions: %+v", c.name, d)
		}
	}
	// The actions are the repairable findings, in order, with their places.
	d := Decide(datadir.Held, true, repairable())
	if a := d.Actions; a[0].Repair != RepairTruncateTail || a[0].Offset != 90 || a[0].Bytes != 4 || a[1].Repair != RepairRemoveSegment || a[1].Log != "raft" {
		t.Errorf("actions %+v", a)
	}
}
