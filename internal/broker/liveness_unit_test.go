package broker

import (
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// TestLivenessNamesTheFirstCause: each cause alone fails liveness with its
// reason; with all three the consensus loop is named; with none the node is
// live.
func TestLivenessNamesTheFirstCause(t *testing.T) {
	loop, own, replica := errors.New("raft log save: x"), errors.New("checksum"), errors.New("gap")
	cases := []struct {
		name               string
		loop, own, replica error
		live               bool
		reason             string
	}{
		{"none", nil, nil, nil, true, ""},
		{"the loop", loop, nil, nil, false, "consensus-stopped: raft log save: x"},
		{"the journal", nil, own, nil, false, "journal-unrecoverable: checksum"},
		{"the replica journal", nil, nil, replica, false, "replica-journal-unrecoverable: gap"},
		{"all three", loop, own, replica, false, "consensus-stopped: raft log save: x"},
	}
	for _, c := range cases {
		live, reason := liveness(c.loop, c.own, c.replica)
		if live != c.live || reason != c.reason {
			t.Errorf("%s: %v %q, want %v %q", c.name, live, reason, c.live, c.reason)
		}
	}
}

// TestLivenessReadsTheReplicaJournal: a replica store whose rewrite fails
// over a damaged record makes the node not live, named as the replica's.
func TestLivenessReadsTheReplicaJournal(t *testing.T) {
	dir := t.TempDir()
	d := faultdisk.New()
	st, err := store.Open(dir, store.Options{Sync: d.Sync, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if err := st.SessionUpsert(store.SessionMeta{Namespace: "ns", ClientID: "a", Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	damageFirstRecord(t, filepath.Join(dir, "state", "00000000000000000001.seg"))
	if err := st.SessionUpsert(store.SessionMeta{Namespace: "ns", ClientID: "b", Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	d.FailNext(1)
	if err := st.Sync(); err == nil {
		t.Fatal("precondition: the armed fsync did not fail")
	}
	if err := st.Sync(); err == nil {
		t.Fatal("precondition: the rewrite over a damaged record succeeded")
	}
	s := &Server{replica: st}
	if live, reason := s.Liveness(); live || !strings.HasPrefix(reason, "replica-journal-unrecoverable: ") {
		t.Fatalf("Liveness() = %v %q, want not live naming the replica journal", live, reason)
	}
}
