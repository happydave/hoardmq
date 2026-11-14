package sim

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestParseEmptyServes(t *testing.T) {
	cases := []struct {
		name string
		body string
		want float64
	}{
		{"no series: never served one empty", "hoardmq_sessions_active{namespace=\"default\"} 5\n", 0},
		{"one reason", "# TYPE hoardmq_sessions_rebuilt_empty_total counter\nhoardmq_sessions_rebuilt_empty_total{reason=\"takeover\"} 1\n", 1},
		{"summed over reasons", "hoardmq_sessions_rebuilt_empty_total{reason=\"takeover\"} 1\nhoardmq_sessions_rebuilt_empty_total{reason=\"adoption\"} 4\n", 5},
		{"a longer name is another series", "hoardmq_sessions_rebuilt_empty_total_other{reason=\"x\"} 9\n", 0},
	}
	for _, c := range cases {
		if got := parseEmptyServes(c.body); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func TestKeepRunDirs(t *testing.T) {
	read := func(point, node string, n float64) lockReading {
		return lockReading{Point: point, Node: node, EmptyServes: n}
	}
	none := []lockReading{read("storm-end", "n1", 0), read("storm-end", "n3", 0), read("end", "n1", 0), read("end", "n2", 0), read("end", "n3", 0)}
	if keep, why := keepRunDirs(none, nil); keep || len(why) != 0 {
		t.Errorf("every count 0: keep %v %v, want the directories removed", keep, why)
	}
	one := append(append([]lockReading(nil), none...), read("census", "n3", 1))
	keep, why := keepRunDirs(one, nil)
	if !keep {
		t.Fatalf("n3 counted 1: keep false, want the directories kept")
	}
	if want := []string{"n3 served 1 sessions empty"}; !reflect.DeepEqual(why, want) {
		t.Errorf("why %q, want %q", why, want)
	}
	// A node read only at one point still counts: the restarted victim's
	// counter starts again at 0, and an earlier reading keeps what it saw.
	restarted := []lockReading{read("storm-start", "n2", 2), read("end", "n2", 0)}
	if keep, why := keepRunDirs(restarted, nil); !keep || !strings.Contains(strings.Join(why, ";"), "n2 served 2") {
		t.Errorf("n2 counted 2 before its restart: keep %v %v, want kept naming n2", keep, why)
	}
	if keep, _ := keepRunDirs(nil, nil); keep {
		t.Errorf("no readings: keep true, want the directories removed")
	}
}

func TestScaleReportNamesKeptDirectories(t *testing.T) {
	rec := &scaleRecord{}
	rec.Teardown.KeptWhy = []string{"n3 served 1 sessions empty"}
	rec.Teardown.Kept = map[string]int64{"host-b:/runs/scale-20261112-000000-abcdef": 12345}
	out := scaleReport(rec)
	for _, want := range []string{"## Kept run directories", "n3 served 1 sessions empty", "host-b:/runs/scale-20261112-000000-abcdef (12345 bytes)"} {
		if !strings.Contains(out, want) {
			t.Errorf("report lacks %q:\n%s", want, out)
		}
	}
}

// TestFinishRunDirsKeepsOnAnEmptyServe runs teardown's last step on a run
// directory the test makes, through the local shell: with every count 0 it is
// removed; with a node counting 1 it stays, recorded with its size.
func TestFinishRunDirsKeepsOnAnEmptyServe(t *testing.T) {
	id := "scale-20261112-000000-a1b2c3"
	setup := func() (string, string) {
		root := filepath.Join(t.TempDir(), "hoardmq-scale", "runs")
		dir := filepath.Join(root, id, "n3", "data")
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "journal"), []byte("0123456789"), 0o600); err != nil {
			t.Fatal(err)
		}
		return root, filepath.Join(root, id)
	}
	ctx := context.Background()

	root, base := setup()
	td := &scaleTeardown{}
	finishRunDirs(ctx, td, []runDir{{host: "local", root: root, sh: localShell{}}}, id,
		[]lockReading{{Point: "end", Node: "n3", EmptyServes: 0}}, nil)
	if _, err := os.Stat(base); !os.IsNotExist(err) || len(td.Kept) != 0 || len(td.Removed) != 1 {
		t.Errorf("count 0: stat %v, kept %v, removed %v; want the directory removed", err, td.Kept, td.Removed)
	}

	root, base = setup()
	td = &scaleTeardown{}
	finishRunDirs(ctx, td, []runDir{{host: "local", root: root, sh: localShell{}}, {host: "local", root: root, sh: localShell{}}}, id,
		[]lockReading{{Point: "end", Node: "n3", EmptyServes: 1}}, nil)
	if _, err := os.Stat(filepath.Join(base, "n3", "data", "journal")); err != nil {
		t.Fatalf("count 1: the kept directory lost its file: %v", err)
	}
	if size, ok := td.Kept["local:"+base]; !ok || size < 10 || len(td.Kept) != 1 || len(td.Removed) != 0 {
		t.Errorf("count 1: kept %v, removed %v; want the one directory kept with its size", td.Kept, td.Removed)
	}
	if want := []string{"n3 served 1 sessions empty"}; !reflect.DeepEqual(td.KeptWhy, want) {
		t.Errorf("why %q, want %q", td.KeptWhy, want)
	}
}

// TestReadLocksRecordsEmptyServes reads two fake nodes over HTTP at one point:
// the one whose /metrics counts a session served empty is recorded with it,
// the other with 0, and the predicate keeps the directories.
func TestReadLocksRecordsEmptyServes(t *testing.T) {
	r := censusRun(t,
		censusNode(t, "n1", func(int) string { return censusBody(9, 9) }),
		censusNode(t, "n3", func(int) string {
			return censusBody(9, 9) + "hoardmq_sessions_rebuilt_empty_total{reason=\"takeover\"} 1\n"
		}))
	r.readLocks(context.Background(), "end")
	got := map[string]float64{}
	for _, rd := range r.rec.LockFetch {
		got[rd.Node] = rd.EmptyServes
	}
	if got["n1"] != 0 || got["n3"] != 1 {
		t.Fatalf("empty serves read %v, want n1 0 and n3 1", got)
	}
	if keep, _ := keepRunDirs(r.rec.LockFetch, nil); !keep {
		t.Fatal("n3 read 1: the directories would be removed")
	}
}
