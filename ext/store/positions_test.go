package store

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/happydave/hoardmq/ext/mqtt"
)

// checkPositions fails unless the index and the positions agree with a scan
// of Sessions.
func checkPositions(t *testing.T, st *State, step string) {
	t.Helper()
	if st.index != nil {
		if len(st.index) != len(st.Sessions) {
			t.Fatalf("%s: index holds %d sessions, the slice %d", step, len(st.index), len(st.Sessions))
		}
		for _, s := range st.Sessions {
			if st.index[sessionKey{s.Namespace, s.ClientID}] != s {
				t.Fatalf("%s: the index does not name %s at its place", step, s.ClientID)
			}
		}
	}
	if st.pos != nil {
		if len(st.pos) != len(st.Sessions) {
			t.Fatalf("%s: positions hold %d sessions, the slice %d", step, len(st.pos), len(st.Sessions))
		}
		for k, i := range st.pos {
			if i < 0 || i >= len(st.Sessions) || st.Sessions[i].Namespace != k.Namespace || st.Sessions[i].ClientID != k.ClientID {
				t.Fatalf("%s: position %d of %s does not hold it", step, i, k.ClientID)
			}
		}
	}
}

// removeByScan deletes by scanning the slice: the reference the positions'
// delete must agree with.
func removeByScan(st *State, ns, id string) {
	if st.find(ns, id) == nil {
		return
	}
	delete(st.index, sessionKey{ns, id})
	for i, s := range st.Sessions {
		if s.Namespace == ns && s.ClientID == id {
			st.Sessions = append(st.Sessions[:i], st.Sessions[i+1:]...)
			return
		}
	}
}

func upsertRecord(id string, epoch uint64) Record {
	payload, _ := json.Marshal(SessionMeta{Namespace: "default", ClientID: id, Epoch: epoch})
	return Record{Type: RecordSessionUpsert, Payload: payload}
}

// heldSession is a whole session as an import or a snapshot carries it.
func heldSession(id string, epoch uint64, entries int) *SessionState {
	ss := &SessionState{SessionMeta: SessionMeta{Namespace: "default", ClientID: id, Epoch: epoch},
		Subscriptions: []SubscriptionRecord{{Filter: "t/" + id, QoS: 1}}, InboundQoS2: make([]uint16, 0)}
	for i := 0; i < entries; i++ {
		ss.Entries = append(ss.Entries, Entry{Seq: uint64(i + 1), Message: &mqtt.Message{Namespace: "default", Topic: "t/" + id, Payload: []byte("m")}})
	}
	return ss
}

// TestPositionsFollowEveryPathThatChangesTheSessions drives every way a
// state's sessions change, against a reference state that deletes by
// scanning: after every step the index and the positions agree with the
// slice, the two states hold the same sessions, and the positions are rebuilt
// only by the first delete after Normalize.
func TestPositionsFollowEveryPathThatChangesTheSessions(t *testing.T) {
	rng := rand.New(rand.NewSource(1852))
	st, ref := NewState(), NewState()
	var held []string // the model: what both states must hold
	next := 0
	wantRebuilds := 0
	discarded := 0 // rebuilds counted by a state the round trip replaced
	normalized := false
	applyBoth := func(rec Record) {
		t.Helper()
		if _, err := st.ApplyReport(rec.Type, rec.Payload); err != nil {
			t.Fatal(err)
		}
		if rec.Type == RecordSessionDelete {
			var k sessionKey
			_ = json.Unmarshal(rec.Payload, &k)
			removeByScan(ref, k.Namespace, k.ClientID)
			return
		}
		if _, err := ref.ApplyReport(rec.Type, rec.Payload); err != nil {
			t.Fatal(err)
		}
	}
	dropHeld := func(i int) string {
		id := held[i]
		held = append(held[:i], held[i+1:]...)
		return id
	}
	// a delete of a held session reaches the positions, so the first one
	// after Normalize rebuilds them.
	deleting := func() {
		if normalized && st.index != nil {
			wantRebuilds++
		}
		normalized = false
	}
	for step := 0; step < 800; step++ {
		var name string
		switch op := rng.Intn(14); {
		case step == 250 || step == 500:
			name = "normalize"
			if len(held) < 20 {
				t.Fatalf("step %d: only %d sessions held at Normalize; a delete then must move a session", step, len(held))
			}
			st.Normalize()
			ref.Normalize()
			normalized = true
		case step == 380:
			name = "json round trip"
			data, err := json.Marshal(st)
			if err != nil {
				t.Fatal(err)
			}
			discarded += st.rebuilds
			st = NewState()
			if err := json.Unmarshal(data, st); err != nil {
				t.Fatal(err)
			}
			normalized = false // a fresh state builds its positions with its index
		case op < 6 || op == 12 || len(held) == 0:
			name = "upsert new"
			id := fmt.Sprintf("c%04d", next)
			next++
			applyBoth(upsertRecord(id, 1))
			held = append(held, id)
		case op == 6:
			name = "upsert held"
			applyBoth(upsertRecord(held[rng.Intn(len(held))], 2))
		case op == 7:
			name = "delete first"
			deleting()
			applyBoth(DeleteRecord("default", dropHeld(0)))
		case op == 8:
			name = "delete last"
			deleting()
			applyBoth(DeleteRecord("default", dropHeld(len(held)-1)))
		case op == 9:
			name = "delete middle"
			deleting()
			applyBoth(DeleteRecord("default", dropHeld(rng.Intn(len(held)))))
		case op == 10:
			name = "delete absent"
			applyBoth(DeleteRecord("default", "absent"))
		case op == 11:
			name = "snapshot install"
			id := held[rng.Intn(len(held))]
			deleting()
			applyBoth(DeleteRecord("default", id))
			for _, rec := range RecordsFor(heldSession(id, 3, 1+rng.Intn(3))) {
				applyBoth(rec)
			}
		default:
			name = "exported Remove"
			deleting()
			i := rng.Intn(len(held))
			st.Remove("default", held[i])
			removeByScan(ref, "default", held[i])
			dropHeld(i)
		}
		label := fmt.Sprintf("step %d (%s)", step, name)
		checkPositions(t, st, label)
		if discarded+st.rebuilds != wantRebuilds {
			t.Fatalf("%s: positions rebuilt %d times, want %d", label, discarded+st.rebuilds, wantRebuilds)
		}
		if len(st.Sessions) != len(held) {
			t.Fatalf("%s: holds %d sessions, the model %d", label, len(st.Sessions), len(held))
		}
		for _, id := range held {
			if st.Find("default", id) == nil {
				t.Fatalf("%s: %s is missing", label, id)
			}
		}
	}
	if wantRebuilds < 2 {
		t.Fatalf("the sequence rebuilt the positions %d times; it must reach a delete after each Normalize", wantRebuilds)
	}
	st.Normalize()
	ref.Normalize()
	a, _ := json.Marshal(st)
	b, _ := json.Marshal(ref)
	if string(a) != string(b) {
		t.Fatal("the state differs from the reference that deletes by scanning")
	}
}

// TestAStalePositionNeverDeletesAnotherSession: a delete checks the place it
// is about to use, and a place that does not hold the session is rebuilt.
func TestAStalePositionNeverDeletesAnotherSession(t *testing.T) {
	st := NewState()
	for i := 0; i < 10; i++ {
		st.add(&SessionState{SessionMeta: SessionMeta{Namespace: "default", ClientID: fmt.Sprintf("c%d", i)}})
	}
	a, b := sessionKey{"default", "c3"}, sessionKey{"default", "c7"}
	st.pos[a], st.pos[b] = st.pos[b], st.pos[a]
	st.Remove("default", "c3")
	// Read the slice, not the index: a delete at the wrong place leaves the
	// index naming a session the slice no longer holds.
	inSlice := map[string]bool{}
	for _, s := range st.Sessions {
		inSlice[s.ClientID] = true
	}
	if inSlice["c3"] || st.Find("default", "c3") != nil {
		t.Fatal("c3 is still held")
	}
	for i := 0; i < 10; i++ {
		if id := fmt.Sprintf("c%d", i); id != "c3" && !inSlice[id] {
			t.Fatalf("%s was deleted in c3's place", id)
		}
	}
	if len(st.Sessions) != 9 || st.rebuilds != 1 {
		t.Fatalf("holds %d, rebuilds %d; want 9 and 1", len(st.Sessions), st.rebuilds)
	}
	checkPositions(t, st, "after the delete")
}

// TestDeletingAHeldSessionScansNothing: deletes spread across a large state
// never fall back to a scan. The time a delete takes is logged, not judged.
func TestDeletingAHeldSessionScansNothing(t *testing.T) {
	const n, k = 66000, 2000
	st := NewState()
	for i := 0; i < n; i++ {
		st.add(&SessionState{SessionMeta: SessionMeta{Namespace: "default", ClientID: fmt.Sprintf("c%06d", i)}})
	}
	began := time.Now()
	for i := 0; i < k; i++ {
		rec := DeleteRecord("default", fmt.Sprintf("c%06d", i*(n/k)))
		if _, err := st.ApplyReport(rec.Type, rec.Payload); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("%v per delete at %d sessions", time.Since(began)/k, n)
	if st.rebuilds != 0 || len(st.Sessions) != n-k {
		t.Fatalf("rebuilds %d, sessions %d; want 0 and %d", st.rebuilds, len(st.Sessions), n-k)
	}
}
