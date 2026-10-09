package sim

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// inBounds is a run in the configuration the time bound holds for, meeting
// every bound: the shape of WI 1618's batch.
func inBounds() boundsInput {
	in := boundsInput{
		Clients: 100_000, RecoverySec: 33.6, NinetyNineSec: 32.9, NineNineNineSec: 33.3,
		Probe:      &probeRecord{Attempts: 9300, Acked: 5900},
		Hold:       &holdRecord{Seconds: 60},
		ClientMode: "in-process", StormTarget: "n2", LeaderAfter: "n2",
	}
	in.Provenance.Host = "ai2"
	takeover := 5000
	in.Provenance.Effective = &boundsEffective{TimingSet: "product", TakeoverTimeoutMs: &takeover}
	return in
}

func mustRow(t *testing.T, v boundsVerdict, name string) boundRow {
	t.Helper()
	for _, r := range v.Rows {
		if r.Name == name {
			return r
		}
	}
	t.Fatalf("no row %q in %+v", name, v.Rows)
	return boundRow{}
}

// A1.
func TestBoundsPassInConfiguration(t *testing.T) {
	v := judgeRun(inBounds())
	if v.Verdict != boundPass || len(v.Failures) != 0 {
		t.Fatalf("verdict %s %v, want pass", v.Verdict, v.Failures)
	}
	for _, name := range []string{rowRecovery, rowDowngraded, rowUndelivered, rowSessionLost} {
		if r := mustRow(t, v, name); r.Status != boundPass {
			t.Errorf("%s: %s (%s), want pass", name, r.Status, r.Note)
		}
	}
	if line := boundsVerdictLine(v); line != "**PASS** on every gating bound" {
		t.Errorf("verdict line %q", line)
	}
}

// A2: each gating row, one over its bound, fails the row and the verdict, and
// the failure names the bound and the figure.
func TestBoundsEachGatingRowFails(t *testing.T) {
	cases := []struct {
		row    string
		break_ func(*boundsInput)
		figure string
		bound  string
	}{
		{rowRecovery, func(in *boundsInput) { in.RecoverySec = 55.001 }, "55.001 s", "≤ 55 s"},
		{rowDowngraded, func(in *boundsInput) { in.SubDowngraded = 1 }, "1", "0"},
		{rowUndelivered, func(in *boundsInput) { in.Probe.AckedNotDelivered = 1 }, "1 of 5900 acknowledged", "0"},
		{rowSessionLost, func(in *boundsInput) { in.SessionLost, in.SessionLostClients = 11, 11 }, "11 across 11 clients", "0"},
	}
	for _, c := range cases {
		t.Run(c.row, func(t *testing.T) {
			in := inBounds()
			c.break_(&in)
			v := judgeRun(in)
			if r := mustRow(t, v, c.row); r.Status != boundFail {
				t.Fatalf("row %s, want fail", r.Status)
			}
			if v.Verdict != boundFail || len(v.Failures) != 1 {
				t.Fatalf("verdict %s, failures %v; want one failure", v.Verdict, v.Failures)
			}
			want := c.row + ": " + c.figure + ", bound " + c.bound
			if v.Failures[0] != want {
				t.Errorf("failure %q, want %q", v.Failures[0], want)
			}
			if line := boundsVerdictLine(v); !strings.Contains(line, "FAIL") || !strings.Contains(line, want) {
				t.Errorf("verdict line %q does not carry %q", line, want)
			}
		})
	}
}

// The bound itself is inclusive.
func TestBoundsRecoveryAtTheBoundPasses(t *testing.T) {
	in := inBounds()
	in.RecoverySec = 55
	if r := mustRow(t, judgeRun(in), rowRecovery); r.Status != boundPass {
		t.Fatalf("55 s: %s, want pass", r.Status)
	}
}

// A3: each condition of the configuration alone leaves the time row not
// judged, with the condition named, even far over the bound.
func TestBoundsTimeJudgedOnlyInConfiguration(t *testing.T) {
	cases := []struct {
		name  string
		move  func(*boundsInput)
		named string
	}{
		{"clients", func(in *boundsInput) { in.Clients = 10_000 }, "10000 clients, not 100000"},
		{"host", func(in *boundsInput) { in.Provenance.Host = "ai" }, `host "ai", not ai2`},
		{"timings", func(in *boundsInput) { in.Provenance.Effective.TimingSet = "harness (stress)" }, `timings "harness (stress)", not product`},
		{"no timings", func(in *boundsInput) { in.Provenance.Effective = nil }, `timings "", not product`},
		{"remote clients", func(in *boundsInput) { in.ClientMode = "remote" }, `clients "remote", not in-process`},
		{"hold", func(in *boundsInput) { in.Hold.Seconds = 180 }, "hold 180 s, not 60 s"},
		{"no hold", func(in *boundsInput) { in.Hold = nil }, "no hold recorded"},
		{"placement", func(in *boundsInput) { in.LeaderAfter = "" }, "placement unknown"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := inBounds()
			in.RecoverySec = 90
			c.move(&in)
			v := judgeRun(in)
			r := mustRow(t, v, rowRecovery)
			if r.Status != boundNotJudged {
				t.Fatalf("status %s, want not judged", r.Status)
			}
			if !strings.Contains(r.Note, c.named) {
				t.Errorf("note %q does not name %q", r.Note, c.named)
			}
			if r.Figure != "90.000 s" {
				t.Errorf("figure %q: a run out of configuration still reports it", r.Figure)
			}
			if v.Verdict != boundPass {
				t.Errorf("verdict %s: the other rows pass, so the run does", v.Verdict)
			}
			if line := boundsVerdictLine(v); !strings.Contains(line, "not judged") {
				t.Errorf("verdict line %q does not say what it did not judge", line)
			}
		})
	}
}

// A4: the acknowledged row has no figure without a probe, and a gating row
// with no figure does not pass.
func TestBoundsNoProbeFigureFails(t *testing.T) {
	for name, p := range map[string]*probeRecord{"absent": nil, "published nothing": {Pairs: 100}} {
		t.Run(name, func(t *testing.T) {
			in := inBounds()
			in.Probe = p
			v := judgeRun(in)
			r := mustRow(t, v, rowUndelivered)
			if r.Status != boundFail || r.Figure != "no figure" {
				t.Fatalf("row %s %q, want fail with no figure", r.Status, r.Figure)
			}
			if v.Verdict != boundFail {
				t.Fatalf("verdict %s, want fail", v.Verdict)
			}
			if rr := mustRow(t, v, rowRefusals); rr.Status != boundFail || rr.Figure != "no figure" {
				t.Errorf("refusals row %s %q, want fail with no figure", rr.Status, rr.Figure)
			}
		})
	}
}

// A5: the refusals after the instant gate at 0, since WI 1706 closed (the
// owner's decision of 2026-09-22), and every reason code counts.
func TestBoundsRefusalsGate(t *testing.T) {
	in := inBounds()
	if r := mustRow(t, judgeRun(in), rowRefusals); r.Status != boundPass || r.Figure != "0" {
		t.Fatalf("no refusal: row %s %q, want pass", r.Status, r.Figure)
	}
	in.Probe.RefusedAfterRecovery = map[string]int64{"0x83": 600, "0x97": 51}
	in.Probe.ErrorsAfterRecovery = 3
	v := judgeRun(in)
	r := mustRow(t, v, rowRefusals)
	if r.Status != boundFail {
		t.Fatalf("status %s, want fail", r.Status)
	}
	if r.Figure != "651 (and 3 with no acknowledgement)" {
		t.Errorf("figure %q", r.Figure)
	}
	if v.Verdict != boundFail {
		t.Errorf("verdict %s: a gating row that failed did not decide it", v.Verdict)
	}
	for _, name := range []string{rowNinetyNine, rowNineNine9} {
		if rr := mustRow(t, v, name); rr.Status != boundRecorded {
			t.Errorf("%s: %s, want recorded", name, rr.Status)
		}
	}
}

// A6: in configuration, a run that never recovered fails the time row with
// its reason.
func TestBoundsNotReachedFailsInConfiguration(t *testing.T) {
	in := inBounds()
	in.RecoverySec, in.RecoveryReason = -1, recoveryNeverBack
	v := judgeRun(in)
	r := mustRow(t, v, rowRecovery)
	if r.Status != boundFail || !strings.Contains(r.Figure, recoveryNeverBack) {
		t.Fatalf("row %s %q, want fail carrying the reason", r.Status, r.Figure)
	}
}

func batchOf(ins ...boundsInput) []namedRun {
	out := make([]namedRun, len(ins))
	for i, in := range ins {
		out[i] = namedRun{Name: "r" + string(rune('1'+i)), In: in}
	}
	return out
}

func balanced() []boundsInput {
	var ins []boundsInput
	for i := 0; i < 7; i++ {
		in := inBounds()
		in.RecoverySec = 32.5 + float64(i)*0.5
		if i >= 3 {
			in.LeaderAfter = "n3"
		}
		ins = append(ins, in)
	}
	return ins
}

// B1.
func TestBatchPassesWhenBalanced(t *testing.T) {
	b := judgeBatch(batchOf(balanced()...))
	if b.Verdict != boundPass {
		t.Fatalf("verdict %s %v", b.Verdict, b.Failures)
	}
	r := mustRow(t, b.boundsVerdict, rowRecovery)
	if r.Status != boundPass || r.Figure != "32.50–35.50 s over 7 runs" {
		t.Errorf("time row %s %q (%s)", r.Status, r.Figure, r.Note)
	}
	if b.Placement["target led"] != 3 || b.Placement["other led"] != 4 {
		t.Errorf("placement %v", b.Placement)
	}
	if len(b.Runs) != 7 || b.Runs[0].Verdict != boundPass {
		t.Errorf("per-run verdicts %+v", b.Runs)
	}
}

// B2.
func TestBatchFailsOnOneRunOverTheBound(t *testing.T) {
	ins := balanced()
	ins[5].RecoverySec = 56
	b := judgeBatch(batchOf(ins...))
	r := mustRow(t, b.boundsVerdict, rowRecovery)
	if r.Status != boundFail || r.Figure != "r6 56.000 s" {
		t.Fatalf("time row %s %q", r.Status, r.Figure)
	}
	if b.Verdict != boundFail || !strings.Contains(strings.Join(b.Failures, ";"), "r6 56.000 s, bound ≤ 55 s") {
		t.Errorf("verdict %s %v", b.Verdict, b.Failures)
	}
}

// B3: all of one placement, all within the bound: the time row is not judged.
// The same set with a run over the bound still fails.
func TestBatchTimeNeedsBalanceToPass(t *testing.T) {
	ins := balanced()[:3]
	ins = append(ins, balanced()[:2]...)
	b := judgeBatch(batchOf(ins...))
	r := mustRow(t, b.boundsVerdict, rowRecovery)
	if r.Status != boundNotJudged || !strings.Contains(r.Note, "placement not balanced: target led 5, other led 0") {
		t.Fatalf("time row %s (%s)", r.Status, r.Note)
	}
	if b.Verdict != boundPass {
		t.Errorf("verdict %s: nothing failed", b.Verdict)
	}
	if rr := mustRow(t, b.boundsVerdict, rowSessionLost); rr.Status != boundPass {
		t.Errorf("sessions row %s: the other rows are judged whatever the mix", rr.Status)
	}
	ins[0].RecoverySec = 70
	if r := mustRow(t, judgeBatch(batchOf(ins...)).boundsVerdict, rowRecovery); r.Status != boundFail {
		t.Errorf("an unbalanced set with a run over the bound: %s, want fail", r.Status)
	}
}

// A set with no run in the configuration does not judge the time row.
func TestBatchNoRunInConfiguration(t *testing.T) {
	ins := balanced()
	for i := range ins {
		ins[i].Clients = 10_000
	}
	r := mustRow(t, judgeBatch(batchOf(ins...)).boundsVerdict, rowRecovery)
	if r.Status != boundNotJudged || !strings.Contains(r.Note, "no run in the configuration") {
		t.Fatalf("time row %s (%s)", r.Status, r.Note)
	}
}

// B4.
func TestBatchFailsOnEmptySessions(t *testing.T) {
	ins := balanced()
	ins[1].SessionLost, ins[1].SessionLostClients = 11, 11
	b := judgeBatch(batchOf(ins...))
	r := mustRow(t, b.boundsVerdict, rowSessionLost)
	if r.Status != boundFail || r.Figure != "r2 11 across 11 clients" || r.Note != "1 of 7 runs fail" {
		t.Fatalf("row %s %q (%s)", r.Status, r.Figure, r.Note)
	}
	if b.Verdict != boundFail {
		t.Errorf("verdict %s", b.Verdict)
	}
	if rr := mustRow(t, b.boundsVerdict, rowRefusals); rr.Status != boundPass {
		t.Errorf("refusals row %s (%s), want pass: no run refused after the instant", rr.Status, rr.Note)
	}
}

// B5: the keys boundsInput reads are the keys the artifact writes. A renamed
// field on either side would leave a batch reading a zero where a run wrote a
// figure.
func TestBoundsInputDecodesFromTheArtifact(t *testing.T) {
	want := inBounds()
	want.RecoveryReason = "x"
	want.SubDowngraded, want.SessionLost, want.SessionLostClients = 2, 3, 4
	art := failoverArtifact{
		Clients: want.Clients, RecoverySec: want.RecoverySec, RecoveryReason: want.RecoveryReason,
		NinetyNineSec: want.NinetyNineSec, NineNineNineSec: want.NineNineNineSec,
		SubDowngraded: want.SubDowngraded, SessionLost: want.SessionLost, SessionLostClients: want.SessionLostClients,
		Probe: want.Probe, Hold: want.Hold, ClientMode: want.ClientMode, StormTarget: want.StormTarget,
		LeaderAfter: want.LeaderAfter,
		Provenance:  Provenance{Host: "ai2", Effective: &Effective{TimingSet: "product", TakeoverTimeoutMs: 5000}},
	}
	raw, err := json.Marshal(art)
	if err != nil {
		t.Fatal(err)
	}
	var got boundsInput
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded\n%+v\nwant\n%+v", got, want)
	}
	if !reflect.DeepEqual(judgeRun(got), judgeRun(boundsInputOf(&art))) {
		t.Fatal("the in-memory judgement and the decoded one differ")
	}
}

// artifactFor is the failover artifact a run with these figures would write.
func artifactFor(in boundsInput) failoverArtifact {
	art := failoverArtifact{
		Clients: in.Clients, RecoverySec: in.RecoverySec, RecoveryReason: in.RecoveryReason,
		NinetyNineSec: in.NinetyNineSec, NineNineNineSec: in.NineNineNineSec,
		SubDowngraded: in.SubDowngraded, SessionLost: in.SessionLost, SessionLostClients: in.SessionLostClients,
		Probe: in.Probe, Hold: in.Hold, ClientMode: in.ClientMode, StormTarget: in.StormTarget,
		LeaderAfter: in.LeaderAfter, Provenance: Provenance{Host: in.Provenance.Host},
	}
	if in.Provenance.Effective != nil {
		art.Provenance.Effective = &Effective{TimingSet: in.Provenance.Effective.TimingSet}
		if t := in.Provenance.Effective.TakeoverTimeoutMs; t != nil {
			art.Provenance.Effective.TakeoverTimeoutMs = *t
		}
	}
	return art
}

// The repeat driver's summary judges each run it read and the batch, and its
// one-line summary says so.
func TestRepeatSummaryCarriesTheVerdict(t *testing.T) {
	var runs []repeatRun
	for i, in := range balanced()[:2] {
		if i == 1 {
			in.SessionLost, in.SessionLostClients = 6, 6
		}
		base := t.TempDir()
		inner := filepath.Join(base, "TestFailover-100000-120000")
		if err := os.MkdirAll(inner, 0o750); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(artifactFor(in))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(inner, "run.json"), raw, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := writeComplete(inner, 100000, "33s"); err != nil {
			t.Fatal(err)
		}
		r := readRun(i+1, base)
		if r.Failed || r.Bounds == nil {
			t.Fatalf("run %d: %+v", i+1, r)
		}
		runs = append(runs, r)
	}
	if runs[0].Bounds.Verdict != boundPass || runs[1].Bounds.Verdict != boundFail {
		t.Fatalf("per-run verdicts %s, %s", runs[0].Bounds.Verdict, runs[1].Bounds.Verdict)
	}
	s := summarise(runs)
	if s.Bounds == nil || s.Bounds.Verdict != boundFail {
		t.Fatalf("batch verdict %+v", s.Bounds)
	}
	want := "bounds: FAIL — sessions rebuilt empty on a reconnect: run 2 6 across 6 clients, bound 0"
	if got := s.String(); !strings.Contains(got, want) {
		t.Errorf("summary %q\ndoes not contain %q", got, want)
	}
}

// The command's reading of a result folder: a pass exits 0, a fail 1 and names
// the run and the figure, and a folder with no run in it is an error, never an
// empty pass.
func TestJudgeRunFiles(t *testing.T) {
	write := func(dir string, ins ...boundsInput) {
		for i, in := range ins {
			d := filepath.Join(dir, "r"+string(rune('1'+i)))
			if err := os.MkdirAll(d, 0o750); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(artifactFor(in))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(d, "run.json"), raw, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	pass := t.TempDir()
	write(pass, balanced()...)
	var out strings.Builder
	if code := JudgeRunFiles(&out, []string{pass}); code != 0 {
		t.Fatalf("a passing set exited %d:\n%s", code, out.String())
	}
	if !strings.Contains(out.String(), "**PASS** on every gating bound") || !strings.Contains(out.String(), "| r7 | PASS |") {
		t.Errorf("output:\n%s", out.String())
	}

	fail := t.TempDir()
	ins := balanced()
	ins[4].SessionLost, ins[4].SessionLostClients = 1, 1
	write(fail, ins...)
	out.Reset()
	if code := JudgeRunFiles(&out, []string{fail}); code != 1 {
		t.Fatalf("a failing set exited %d:\n%s", code, out.String())
	}
	if !strings.Contains(out.String(), "r5 fails sessions rebuilt empty on a reconnect: 1 across 1 client, bound 0") {
		t.Errorf("output does not name the failing run and figure:\n%s", out.String())
	}

	out.Reset()
	if code := JudgeRunFiles(&out, []string{t.TempDir()}); code != 2 {
		t.Errorf("an empty folder exited %d, want 2:\n%s", code, out.String())
	}
	if code := JudgeRunFiles(&out, nil); code != 2 {
		t.Errorf("no argument exited %d, want 2", code)
	}
}

// WI 1805: a run labelled product but recorded before the label covered the
// takeover and ack timeouts is noted as such, and the note decides nothing.
func TestAProductRecordWithoutItsTimeoutsIsNoted(t *testing.T) {
	var old boundsInput
	raw := []byte(`{"clients":100000,"recovery_sec":33.6,"probe":{"attempts":9300,"acked":5900},"hold":{"seconds":60},
		"client_mode":"in-process","storm_target":"n2","leader_after":"n2",
		"provenance":{"host":"ai2","effective":{"timing_set":"product","heartbeat_ms":1000}}}`)
	if err := json.Unmarshal(raw, &old); err != nil {
		t.Fatal(err)
	}
	current := inBounds()
	oldV, curV := judgeRun(old), judgeRun(current)
	if row := mustRow(t, oldV, rowRecovery); row.Status != boundPass || !strings.Contains(row.Note, "before WI 1805") {
		t.Errorf("an old product record: time row %+v, want pass with the note", row)
	}
	if row := mustRow(t, curV, rowRecovery); strings.Contains(row.Note, "WI 1805") {
		t.Errorf("a record that carries its takeover timeout needs no note: %+v", row)
	}
	if oldV.Verdict != curV.Verdict {
		t.Errorf("the note changed the verdict: %s against %s", oldV.Verdict, curV.Verdict)
	}
	harness := old
	harness.Provenance.Effective = &boundsEffective{TimingSet: "harness (stress)"}
	if row := mustRow(t, judgeRun(harness), rowRecovery); strings.Contains(row.Note, "WI 1805") {
		t.Errorf("a harness record already says it is not the product: %+v", row)
	}
	b := judgeBatch([]namedRun{{"r1", old}, {"r2", current}})
	if row := mustRow(t, b.boundsVerdict, rowRecovery); !strings.Contains(row.Note, "1 of 2 runs recorded before WI 1805") {
		t.Errorf("the set's time row should count the old records: %+v", row)
	}
}
