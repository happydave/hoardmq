package sim

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestDropLogKeepsTheLatest: six drops leave the latest four; a window
// returns the kept drops at or after its start, oldest first, and says when
// every kept drop is in it and older ones were overwritten.
func TestDropLogKeepsTheLatest(t *testing.T) {
	var l dropLog
	for i := 1; i <= 6; i++ {
		l.add(dropRec{atNs: int64(i) * int64(time.Second), node: uint8(i % 2), cause: uint8(i % dropCauseCount)})
	}
	if l.seen != 6 {
		t.Fatalf("seen %d, want 6", l.seen)
	}
	at := func(rs []dropRec) []int64 {
		var out []int64
		for _, r := range rs {
			out = append(out, r.atNs/int64(time.Second))
		}
		return out
	}
	late, earlier := l.since(5 * int64(time.Second))
	if want := []int64{5, 6}; !reflect.DeepEqual(at(late), want) || earlier {
		t.Errorf("since 5 s: %v, earlier %v; want %v and no earlier drop missing", at(late), earlier, want)
	}
	late, earlier = l.since(int64(time.Second))
	if want := []int64{3, 4, 5, 6}; !reflect.DeepEqual(at(late), want) || !earlier {
		t.Errorf("since 1 s: %v, earlier %v; want the latest four %v with earlier drops missing", at(late), earlier, want)
	}
	if late, _ := l.since(7 * int64(time.Second)); len(late) != 0 {
		t.Errorf("since 7 s: %v, want none", at(late))
	}
	var two dropLog
	two.add(dropRec{atNs: 1})
	two.add(dropRec{atNs: 2})
	if late, earlier := two.since(0); len(late) != 2 || earlier {
		t.Errorf("two drops since 0: %d, earlier %v; want both and none missing", len(late), earlier)
	}
}

// TestClientKeepsItsDrops: a dropped event is kept with its node and cause;
// a failed connect and a DISCONNECT are not drops.
func TestClientKeepsItsDrops(t *testing.T) {
	nodes := []string{"10.0.0.1:1", "10.0.0.2:1"}
	c := NewClient("f000001", nodes, 60, 1)
	at := time.Unix(100, 0)
	c.record(Event{Client: c.ID, Node: nodes[0], Kind: "failed", At: at})
	c.record(Event{Client: c.ID, Node: nodes[1], Kind: "disconnect", Code: packet.SessionTakenOver, At: at})
	c.record(Event{Client: c.ID, Node: nodes[1], Kind: "dropped", Code: packet.SessionTakenOver, At: at.Add(time.Second)})
	c.record(Event{Client: c.ID, Node: "10.0.0.9:1", Kind: "dropped", At: at.Add(2 * time.Second)})
	if c.drops.seen != 2 {
		t.Fatalf("the client kept %d drops, want the 2 dropped events", c.drops.seen)
	}
	first, second := c.drops.last[0], c.drops.last[1]
	if first.atNs != at.Add(time.Second).UnixNano() || first.node != 1 || int(first.cause) != dropCauseTakenOver {
		t.Errorf("first drop %+v, want at 101 s on node 1, session taken over", first)
	}
	if second.node != connackNodeOther || int(second.cause) != dropCauseNone {
		t.Errorf("second drop %+v, want a node outside the list, no DISCONNECT", second)
	}
}

func TestBackWatch(t *testing.T) {
	t0 := time.Unix(1000, 0)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	cases := []struct {
		name  string
		steps [][2]bool // at target, crashed
		want  int       // index of the reading that sets the instant, -1 for none
	}{
		{"back after the crash", [][2]bool{{true, false}, {false, false}, {false, true}, {true, true}, {false, true}, {true, true}}, 3},
		{"never below", [][2]bool{{true, false}, {true, true}, {true, true}}, -1},
		{"never back", [][2]bool{{true, false}, {false, true}, {false, true}}, -1},
		{"back before the crash is known", [][2]bool{{false, false}, {true, false}, {true, true}}, 2},
		{"first reading below, the crash known at once", [][2]bool{{false, true}, {true, true}}, 1},
	}
	for _, c := range cases {
		var w backWatch
		for i, s := range c.steps {
			w.observe(s[0], s[1], at(i))
		}
		want := time.Time{}
		if c.want >= 0 {
			want = at(c.want)
		}
		if !w.at.Equal(want) {
			t.Errorf("%s: instant %v, want %v", c.name, w.at, want)
		}
	}
}

// lateClients is four clients: one that dropped only before the window, one
// dropped once in it (session taken over on its second node) after a
// CONNACK, one that never dropped, and one dropped five times in it.
func lateClients(since time.Time) []*Client {
	nodes := []string{"10.0.0.1:1", "10.0.0.2:1"}
	before := NewClient("f000001", nodes, 60, 1)
	before.record(Event{Node: nodes[0], Kind: "dropped", At: since.Add(-time.Second)})
	once := NewClient("f000002", nodes, 60, 2)
	once.noteConnect(nodes[1], since.Add(-2*time.Second), &packet.Connack{ReasonCode: packet.Success, SessionPresent: true}, nil)
	once.record(Event{Node: nodes[0], Kind: "dropped", At: since.Add(-3 * time.Second)})
	once.record(Event{Node: nodes[1], Kind: "dropped", Code: packet.SessionTakenOver, At: since.Add(8 * time.Second)})
	never := NewClient("f000003", nodes, 60, 3)
	often := NewClient("f000004", nodes, 60, 4)
	for i := 0; i < 5; i++ {
		often.record(Event{Node: nodes[0], Kind: "dropped", At: since.Add(time.Duration(i) * time.Second)})
	}
	return []*Client{before, once, never, often}
}

func TestLateDropsOf(t *testing.T) {
	since := time.Unix(2000, 0)
	all := lateClients(since)
	ld := lateDropsOf(all, since)
	if ld.SinceMs != since.UnixMilli() {
		t.Errorf("since %d ms, want %d", ld.SinceMs, since.UnixMilli())
	}
	var ids []string
	for _, lc := range ld.Named {
		ids = append(ids, lc.ID)
	}
	if want := []string{"f000002", "f000004"}; !reflect.DeepEqual(ids, want) || ld.Clients != 2 {
		t.Fatalf("named %v (%d clients), want %v: the clients with a drop in the window, in order", ids, ld.Clients, want)
	}
	once := ld.Named[0]
	if len(once.Drops) != 1 || once.Drops[0].Cause != "session-taken-over" || once.Drops[0].Node != "10.0.0.2:1" || once.Drops[0].AtMs != since.Add(8*time.Second).UnixMilli() || once.EarlierNotKept {
		t.Errorf("f000002's drops %+v (earlier %v), want one session taken over on 10.0.0.2:1 at +8 s", once.Drops, once.EarlierNotKept)
	}
	if !once.Connacks.Found || once.Connacks.Seen != 1 || len(once.Connacks.Last) != 1 || !once.Connacks.Last[0].SessionPresent {
		t.Errorf("f000002's CONNACK record %+v, want its one CONNACK with Session Present", once.Connacks)
	}
	often := ld.Named[1]
	if len(often.Drops) != dropRing || !often.EarlierNotKept || often.Drops[0].Cause != "none" {
		t.Errorf("f000004's drops %d (earlier %v), want the latest %d with earlier ones marked missing", len(often.Drops), often.EarlierNotKept, dropRing)
	}
	if ld.ByCause["session-taken-over"] != 1 || ld.ByCause["none"] != dropRing || len(ld.ByCause) != dropCauseCount {
		t.Errorf("by cause %v, want 1 session taken over and %d none, every cause present", ld.ByCause, dropRing)
	}
	none := lateDropsOf(all, time.Time{})
	if none.SinceMs != 0 || none.Clients != 0 || len(none.Named) != 0 || none.Named == nil {
		t.Errorf("no instant: %+v, want nothing named and an empty list", none)
	}
}

func TestDateLateDrops(t *testing.T) {
	since := time.Unix(2000, 0)
	ld := lateDropsOf(lateClients(since), since)
	crashMs := since.Add(-100 * time.Second).UnixMilli()
	dateLateDrops(ld, map[string]string{"10.0.0.1:1": "n1", "10.0.0.2:1": "n2"}, crashMs)
	if ld.SinceCrashSec == nil || *ld.SinceCrashSec != 100 {
		t.Fatalf("window dated %v, want +100 s", ld.SinceCrashSec)
	}
	d := ld.Named[0].Drops[0]
	if d.NodeName != "n2" || d.SinceCrashSec == nil || *d.SinceCrashSec != 108 {
		t.Errorf("drop %+v, want n2 at +108 s", d)
	}
	if e := ld.Named[0].Connacks.Last[0]; e.NodeName != "n2" || e.SinceCrashSec == nil {
		t.Errorf("CONNACK %+v, want n2 and dated", e)
	}
	dateLateDrops(nil, nil, crashMs)
}

func TestLateDropReport(t *testing.T) {
	since := time.Unix(2000, 0)
	ld := lateDropsOf(lateClients(since), since)
	dateLateDrops(ld, map[string]string{"10.0.0.1:1": "n1", "10.0.0.2:1": "n2"}, since.Add(-100*time.Second).UnixMilli())
	out := lateDropReport(ld)
	for _, want := range []string{"## Late drops", "from +100.000 s", "2 clients dropped", "none 4, session-taken-over 1",
		"- f000002: dropped +108.000 s session-taken-over on n2; 1 CONNACKs: ", "Session Present true", "f000004", "(earlier drops not kept)"} {
		if !strings.Contains(out, want) {
			t.Errorf("report lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "f000001") || strings.Contains(out, "f000003") {
		t.Errorf("report names a client with no late drop:\n%s", out)
	}
	if !strings.Contains(lateDropReport(lateDropsOf(nil, time.Time{})), "never back at once") {
		t.Error("a storm never back reads no late-drop window")
	}
	if lateDropReport(nil) != "" {
		t.Error("no record: a section anyway")
	}
	rec := &scaleRecord{Storm: &scaleStorm{Summary: &driverSummary{LateDrops: ld}}}
	if !strings.Contains(scaleReport(rec), "- f000002: dropped") {
		t.Error("the run's report has no late drops")
	}
}

func TestKeepRunDirsOnLateDrops(t *testing.T) {
	clean := []lockReading{{Point: "end", Node: "n1"}, {Point: "end", Node: "n3"}}
	late := &lateDrops{SinceMs: 1, Clients: 2, ByCause: map[string]int64{"session-taken-over": 1, "none": 1}}
	keep, why := keepRunDirs(clean, late)
	if !keep {
		t.Fatal("2 late-dropped clients: the directories would be removed")
	}
	if want := []string{"2 clients dropped after every client was back (none 1, session-taken-over 1)"}; !reflect.DeepEqual(why, want) {
		t.Errorf("why %q, want %q", why, want)
	}
	if keep, why := keepRunDirs(clean, &lateDrops{SinceMs: 1, ByCause: map[string]int64{}}); keep || len(why) != 0 {
		t.Errorf("no late drop: keep %v %v, want the directories removed", keep, why)
	}
	if keep, _ := keepRunDirs(clean, nil); keep {
		t.Error("no late-drop record: keep true, want the directories removed")
	}
	both := append(append([]lockReading(nil), clean...), lockReading{Point: "end", Node: "n3", EmptyServes: 1})
	if _, why := keepRunDirs(both, late); len(why) != 2 {
		t.Errorf("an empty serve and late drops: why %q, want both", why)
	}
}

// TestFinishRunDirsKeepsOnALateDrop runs teardown's last step on a directory
// the test makes: with a late drop and no empty serve it stays.
func TestFinishRunDirsKeepsOnALateDrop(t *testing.T) {
	id := "scale-20261112-000000-d4e5f6"
	root := filepath.Join(t.TempDir(), "hoardmq-scale", "runs")
	dir := filepath.Join(root, id, "n1", "data")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "journal"), []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	td := &scaleTeardown{}
	finishRunDirs(context.Background(), td, []runDir{{host: "local", root: root, sh: localShell{}}}, id,
		[]lockReading{{Point: "end", Node: "n1"}}, &lateDrops{SinceMs: 1, Clients: 1, ByCause: map[string]int64{"session-taken-over": 1}})
	if _, err := os.Stat(filepath.Join(dir, "journal")); err != nil {
		t.Fatalf("the kept directory lost its file: %v", err)
	}
	if len(td.Kept) != 1 || len(td.Removed) != 0 || len(td.KeptWhy) != 1 {
		t.Errorf("kept %v (%v), removed %v; want the one directory kept for the late drop", td.Kept, td.KeptWhy, td.Removed)
	}
}

// TestRunKeepsDirsOnItsLateDrops: the run's teardown keeps its directory when
// its storm's summary names a late drop, and removes it when the storm had
// none.
func TestRunKeepsDirsOnItsLateDrops(t *testing.T) {
	id := "scale-20261112-000000-a7b8c9"
	for _, tc := range []struct {
		clients int
		kept    bool
	}{{1, true}, {0, false}} {
		root := filepath.Join(t.TempDir(), "hoardmq-scale", "runs")
		if err := os.MkdirAll(filepath.Join(root, id, "n1"), 0o750); err != nil {
			t.Fatal(err)
		}
		r := &scaleRun{rec: &scaleRecord{RunID: id, Storm: &scaleStorm{Summary: &driverSummary{
			LateDrops: &lateDrops{SinceMs: 1, Clients: tc.clients, ByCause: map[string]int64{"session-taken-over": int64(tc.clients)}}}}}}
		r.finishDirs(context.Background(), []runDir{{host: "local", root: root, sh: localShell{}}})
		_, err := os.Stat(filepath.Join(root, id))
		if kept := err == nil; kept != tc.kept || len(r.rec.Teardown.Kept) != boolInt(tc.kept) {
			t.Errorf("%d late-dropped clients: directory kept %v (%v), want %v", tc.clients, kept, r.rec.Teardown.Kept, tc.kept)
		}
	}
}

// TestLateDropRecordSize: the drop record's size a client, which a million
// clients multiply.
func TestLateDropRecordSize(t *testing.T) {
	size := reflect.TypeOf(dropLog{}).Size()
	t.Logf("a client's drop record: %d bytes; %d MB at 1,000,000 clients", size, size)
	if size > 72 {
		t.Fatalf("a client's drop record is %d bytes, over the 72 its cost was stated at", size)
	}
}
