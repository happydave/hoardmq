package store

import (
	"slices"
	"testing"
)

// TestAnAcceptanceRecovers: the store's own append of an acceptance survives a
// restart; an identifier opened and not accepted recovers open only.
func TestAnAcceptanceRecovers(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{})
	k := Key{Namespace: "d", ClientID: "c"}
	for _, err := range []error{
		s.SessionUpsert(SessionMeta{Namespace: "d", ClientID: "c", ExpiryInterval: 600}),
		s.InboundQoS2(k, 13, true),
		s.InboundAccepted(k, 13),
		s.InboundQoS2(k, 14, true),
		s.Sync(),
		s.Close(),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	s2 := openStore(t, dir, Options{})
	defer func() { _ = s2.Close() }()
	st, err := s2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	ss := st.Find("d", "c")
	if ss == nil {
		t.Fatal("the session did not recover")
	}
	if !slices.Equal(ss.InboundQoS2, []uint16{13, 14}) || !slices.Equal(ss.InboundAccepted, []uint16{13}) {
		t.Fatalf("recovered open %v accepted %v, want open [13 14] accepted [13]", ss.InboundQoS2, ss.InboundAccepted)
	}
}
