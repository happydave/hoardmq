package store

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/happydave/hoardmq/ext/mqtt"
)

// TestEncodeCheckpointIsMarshal: the assembled payload is byte for byte what
// json.Marshal gives for the same state, in every shape the state takes.
func TestEncodeCheckpointIsMarshal(t *testing.T) {
	full := func(id string) *SessionState {
		return &SessionState{
			SessionMeta: SessionMeta{Namespace: "default", ClientID: id, Epoch: 7, Version: 5, Username: "u",
				ExpiryInterval: 3600, ExpiresAt: time.Unix(1700000000, 123456789).UTC(),
				Will: &WillRecord{Message: &mqtt.Message{Topic: "will/" + id, Payload: []byte("bye")}, Delay: 5}},
			Subscriptions:   []SubscriptionRecord{{Filter: "a/#", QoS: 1, NoLocal: true}},
			InboundQoS2:     []uint16{3, 4},
			InboundAccepted: []uint16{3},
			Entries: []Entry{{Seq: 1, Message: &mqtt.Message{Topic: "a/b", Payload: []byte{0, 1, 2}}, QoS: 1,
				Identifiers: []uint32{9}, Retain: true, Group: "g", PacketID: 11, State: 1}},
		}
	}
	escaped := full("<tag> & \"quoted\" \x01 é")
	retained := []*mqtt.Message{{Topic: "r/1", Payload: []byte("x"), Retain: true}}
	cases := []struct {
		name string
		st   func() *State
	}{
		{"no sessions, nil retained", func() *State { return &State{} }},
		{"empty slices", func() *State { return NewState() }},
		{"one full session", func() *State {
			st := NewState()
			st.Sessions = []*SessionState{full("c1")}
			return st
		}},
		{"strings that need escaping", func() *State {
			st := NewState()
			st.Sessions = []*SessionState{escaped}
			return st
		}},
		{"three sessions, retained, through", func() *State {
			st := NewState()
			st.Sessions = []*SessionState{full("c1"), full("c2"), full("c3")}
			st.Retained = retained
			st.Through = []SessionIndex{{Namespace: "default", ClientID: "c2", Index: 42}}
			return st
		}},
		{"versioned retained and tombstones", func() *State {
			st := NewState()
			st.Sessions = []*SessionState{full("c1")}
			st.Retained = retained
			st.RetainedVersions = []RetainedVersion{{Namespace: "default", Topic: "r/1", RV: Version{E: 3, N: 1 << 60}}}
			st.RetainedTombstones = []Tombstone{{Namespace: "default", Topic: "r/2", RV: Version{E: 3, N: 1<<60 + 1}}}
			st.Through = []SessionIndex{{Namespace: "default", ClientID: "c1", Index: 7}}
			return st
		}},
		{"a nil session", func() *State {
			st := NewState()
			st.Sessions = []*SessionState{full("c1"), nil}
			return st
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			want, err := json.Marshal(c.st())
			if err != nil {
				t.Fatal(err)
			}
			st := c.st()
			got, err := encodeCheckpoint(st, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("payload differs from json.Marshal:\n got %s\nwant %s", got, want)
			}
			for i, ss := range st.Sessions {
				if ss != nil {
					t.Fatalf("session %d still referenced after its encoding", i)
				}
			}
		})
	}
}
