package lease

import (
	"bytes"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// viewBytes writes a captured view and stages it into a fresh table, then
// returns that table's snapshot bytes.
func viewBytes(t *testing.T, v consensus.SectionView) []byte {
	t.Helper()
	var recs [][]byte
	if err := v.WriteRecords(func(b []byte) error { recs = append(recs, append([]byte(nil), b...)); return nil }); err != nil {
		t.Fatal(err)
	}
	i := 0
	next := func() ([]byte, bool, error) {
		if i == len(recs) {
			return nil, false, nil
		}
		i++
		return recs[i-1], true, nil
	}
	fresh := NewTable()
	st, err := fresh.Stage(v.Encoding(), next)
	if err != nil {
		t.Fatal(err)
	}
	st.Install()
	b, err := fresh.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func snapWill(topic string) *store.WillRecord {
	return &store.WillRecord{Message: &message.Message{Topic: topic, Payload: []byte("bye")}, Delay: 3}
}

// TestCapturedViewIsUnchangedByLaterApplies: a capture is the table at its
// index. Every operation kind then applied to the same leases and nodes
// leaves what the capture writes equal to the table as it was.
func TestCapturedViewIsUnchangedByLaterApplies(t *testing.T) {
	tb := NewTable()
	now := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
	applyAt(t, tb, 10,
		Op{Kind: OpAlive, Node: "n1", Now: now, Advertise: "a1", Level: 1},
		Op{Kind: OpAlive, Node: "n2", Now: now, Advertise: "a2", Level: 1},
		Op{Kind: OpAlive, Node: "n3", Now: now, Advertise: "a3", Level: 1},
		Op{Kind: OpAcquire, ClientID: "a", Node: "n1", Will: snapWill("wa"), ExpiryInterval: 60, Now: now},
		Op{Kind: OpAcquire, ClientID: "b", Node: "n1", Will: snapWill("wb"), ExpiryInterval: 60, Now: now},
		Op{Kind: OpAcquire, ClientID: "c", Node: "n2", ExpiryInterval: 60, Now: now},
		Op{Kind: OpAcquire, ClientID: "d", Node: "n2", Will: snapWill("wd"), ExpiryInterval: 60, Now: now},
		Op{Kind: OpAcquire, ClientID: "e", Node: "n2", Will: snapWill("we"), ExpiryInterval: 60, Now: now},
		Op{Kind: OpAcquire, ClientID: "gone", Node: "n3", Now: now},
	)
	applyAt(t, tb, 11, Op{Kind: OpRelease, ClientID: "gone", Node: "n3", Epoch: 10})
	applyAt(t, tb, 12, Op{Kind: OpSetLevel, Level: 2}, Op{Kind: OpServed, Node: "n1", Served: 11, Routes: 4})
	before, err := tb.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	view, err := tb.Capture()
	if err != nil {
		t.Fatal(err)
	}

	later := now.Add(time.Minute)
	r := applyAt(t, tb, 20,
		Op{Kind: OpAcquire, ClientID: "new", Node: "n1", Now: later},
		Op{Kind: OpAcquire, ClientID: "a", Node: "n2", Will: snapWill("wa2"), ExpiryInterval: 30, Now: later},
		Op{Kind: OpAcquire, ClientID: "c", Node: "n2", Epoch: 10, ExpiryInterval: 99, Now: later},
		Op{Kind: OpDisconnected, ClientID: "b", Node: "n1", Epoch: 10, ExpiresAt: later.Add(time.Hour), WillArmed: true, Now: later},
		Op{Kind: OpDisconnected, ClientID: "d", Node: "n2", Epoch: 10, ExpiresAt: later.Add(time.Hour), WillArmed: false, Now: later},
		Op{Kind: OpWillDone, ClientID: "e", Node: "n2", Epoch: 10},
		Op{Kind: OpRelease, ClientID: "c", Node: "n2", Epoch: 10},
		Op{Kind: OpAlive, Node: "n1", Now: later, Advertise: "a1b", Level: 3},
		Op{Kind: OpSetLevel, Level: 3},
		Op{Kind: OpServed, Node: "n2", Served: 19, Routes: 7, Cover: 18, CoverRoutes: 6},
	)
	for i, res := range r {
		if !res.OK {
			t.Fatalf("operation %d refused: %+v", i, res)
		}
	}
	r = applyAt(t, tb, 21,
		Op{Kind: OpReassign, Node: "n2", To: "n3", SeenAliveIndex: 10, Now: later},
		Op{Kind: OpHandoff, Node: "n1", To: "n3", Now: later},
	)
	if !r[0].OK || r[0].Moved == 0 || !r[1].OK || r[1].Moved == 0 {
		t.Fatalf("reassign and hand-off: %+v", r)
	}
	after, _ := tb.Snapshot()
	if bytes.Equal(after, before) {
		t.Fatal("the operations changed nothing, so the test shows nothing")
	}
	if got := viewBytes(t, view); !bytes.Equal(got, before) {
		t.Fatalf("the captured view changed with later applies:\nwant %s\ngot  %s", before, got)
	}
}

// TestLeaseSectionRoundTrips: a captured table restores whole.
func TestLeaseSectionRoundTrips(t *testing.T) {
	tb := NewTable()
	now := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
	applyAt(t, tb, 5, Op{Kind: OpAlive, Node: "n1", Now: now},
		Op{Kind: OpAcquire, Namespace: "ns", ClientID: "x", Node: "n1", Will: snapWill("w"), ExpiryInterval: 60, Now: now})
	applyAt(t, tb, 6, Op{Kind: OpDisconnected, Namespace: "ns", ClientID: "x", Node: "n1", Epoch: 5, ExpiresAt: now.Add(time.Hour), WillArmed: true, Now: now})
	want, _ := tb.Snapshot()
	view, err := tb.Capture()
	if err != nil {
		t.Fatal(err)
	}
	if got := viewBytes(t, view); !bytes.Equal(got, want) {
		t.Fatalf("round trip:\nwant %s\ngot  %s", want, got)
	}
}

// TestEmptyTableRoundTrips: a table with no lease writes its meta record
// alone and restores empty.
func TestEmptyTableRoundTrips(t *testing.T) {
	tb := NewTable()
	want, _ := tb.Snapshot()
	view, err := tb.Capture()
	if err != nil {
		t.Fatal(err)
	}
	if got := viewBytes(t, view); !bytes.Equal(got, want) {
		t.Fatalf("empty table:\nwant %s\ngot  %s", want, got)
	}
}
