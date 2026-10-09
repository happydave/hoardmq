package store

import (
	"encoding/json"
	"slices"
	"testing"
)

func inboundState(t *testing.T, records ...string) *SessionState {
	t.Helper()
	st := &State{}
	st.add(&SessionState{SessionMeta: SessionMeta{Namespace: "d", ClientID: "c"}, InboundQoS2: []uint16{}})
	for _, r := range records {
		typ := RecordInboundAdd
		if r[0] == '-' {
			typ, r = RecordInboundRemove, r[1:]
		}
		if err := st.Apply(typ, []byte(r)); err != nil {
			t.Fatal(err)
		}
	}
	return st.Find("d", "c")
}

// TestAcceptanceMarksAnOpenIdentifier: an inbound-add carrying the acceptance
// marks its identifier accepted, and opens it first when it is absent; one
// without it, as an older build writes it, only opens; a remove clears both.
func TestAcceptanceMarksAnOpenIdentifier(t *testing.T) {
	const open9 = `{"ns":"d","id":"c","packetId":9}`
	const accept9 = `{"ns":"d","id":"c","packetId":9,"accepted":true}`
	for _, c := range []struct {
		name           string
		records        []string
		open, accepted []uint16
	}{
		{"an opening only", []string{open9}, []uint16{9}, nil},
		{"opened, then accepted", []string{open9, accept9}, []uint16{9}, []uint16{9}},
		{"accepted while absent", []string{accept9}, []uint16{9}, []uint16{9}},
		{"accepted, then released", []string{open9, accept9, "-" + open9}, nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			ss := inboundState(t, c.records...)
			if !slices.Equal(ss.InboundQoS2, c.open) {
				t.Errorf("open %v, want %v", ss.InboundQoS2, c.open)
			}
			if !slices.Equal(ss.InboundAccepted, c.accepted) {
				t.Errorf("accepted %v, want %v", ss.InboundAccepted, c.accepted)
			}
		})
	}
}

// TestRecordsForKeepsTheAcceptance: a session rendered as records for a whole
// write, a resync or a hand-over, and applied to an empty state, reads back
// with the same identifiers open and the same ones accepted.
func TestRecordsForKeepsTheAcceptance(t *testing.T) {
	ss := &SessionState{SessionMeta: SessionMeta{Namespace: "d", ClientID: "c"}, InboundQoS2: []uint16{9, 12}, InboundAccepted: []uint16{9}}
	st := &State{}
	for _, r := range RecordsFor(ss) {
		if err := st.Apply(r.Type, r.Payload); err != nil {
			t.Fatal(err)
		}
	}
	got := st.Find("d", "c")
	if got == nil {
		t.Fatal("the session was not rebuilt from its records")
	}
	if !slices.Equal(got.InboundQoS2, []uint16{9, 12}) || !slices.Equal(got.InboundAccepted, []uint16{9}) {
		t.Fatalf("rebuilt open %v accepted %v, want open [9 12] accepted [9]", got.InboundQoS2, got.InboundAccepted)
	}
}

// TestStateWithoutAcceptancesReadsNoneAccepted: a session state written
// before the acceptance existed unmarshals with its identifiers open and none
// accepted.
func TestStateWithoutAcceptancesReadsNoneAccepted(t *testing.T) {
	var ss SessionState
	if err := json.Unmarshal([]byte(`{"ns":"d","id":"c","subscriptions":[],"inboundQoS2":[9],"entries":[]}`), &ss); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ss.InboundQoS2, []uint16{9}) || len(ss.InboundAccepted) != 0 {
		t.Fatalf("open %v accepted %v, want open [9] and none accepted", ss.InboundQoS2, ss.InboundAccepted)
	}
}
