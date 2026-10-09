package replication

import (
	"testing"
	"time"
)

func TestBatchCountersCountOnlyBigBatchesAndKeepTheLargest(t *testing.T) {
	var c batchCounters
	small := make([]frame, bigBatch-1)
	c.note("n1", small, time.Second)
	if st := c.stats(); st.Batches != 0 || len(st.ByKind) != 0 {
		t.Fatalf("a batch below the threshold was counted by kind: %+v", st)
	}
	if st := c.stats(); st.MaxLen != bigBatch-1 || st.MaxPeer != "n1" {
		t.Fatalf("the largest batch is kept whatever its length: %+v", st)
	}
	c.note("n9", make([]frame, 3), time.Second)
	if st := c.stats(); st.MaxPeer != "n1" {
		t.Fatalf("a smaller batch replaced the largest: %+v", st)
	}
	big := make([]frame, bigBatch)
	for i := range big {
		big[i].Kind = kindAppend
	}
	big[0].Kind, big[1].Kind, big[2].Kind = kindSnapshot, kindSnapshot, 200 // 200: a kind this build does not know
	c.note("n2", big, 2*time.Second)
	bigger := make([]frame, bigBatch+5)
	for i := range bigger {
		bigger[i].Kind = kindSnapshot
	}
	c.note("n3", bigger, 3*time.Second)
	c.note("n2", big, time.Second) // not the largest: leaves the record alone
	st := c.stats()
	if st.Batches != 3 || st.Frames != int64(3*bigBatch+5) || st.Ms != 6000 {
		t.Fatalf("totals %+v", st)
	}
	wantKinds := map[string]int64{"append": 2 * (bigBatch - 3), "snapshot": 4 + bigBatch + 5, "other": 2}
	for k, want := range wantKinds {
		if st.ByKind[k] != want {
			t.Errorf("frames of kind %s: %d, want %d", k, st.ByKind[k], want)
		}
	}
	if len(st.ByKind) != len(wantKinds) {
		t.Errorf("kinds counted %v, want exactly %v", st.ByKind, wantKinds)
	}
	if st.MaxLen != bigBatch+5 || st.MaxPeer != "n3" || st.MaxMs != 3000 || st.MaxByKind["snapshot"] != bigBatch+5 || len(st.MaxByKind) != 1 {
		t.Fatalf("the largest batch: %+v", st)
	}
}
