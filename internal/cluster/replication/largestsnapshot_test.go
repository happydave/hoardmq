package replication

import (
	"encoding/json"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// TestLargestSnapshotHold measures the follower's hold of its lock while it
// applies one snapshot just under a frame limit, and what that hold makes a
// fetch and another peer's record wait, then projects both to the 16 MiB
// default by bytes. It reports and does not judge: the rule (half an
// acknowledgement timeout) is read from its output. A measurement, so it runs
// only with HOARDMQ_REPL_LARGEST set to the limit in bytes.
func TestLargestSnapshotHold(t *testing.T) {
	limit, _ := strconv.Atoi(os.Getenv("HOARDMQ_REPL_LARGEST"))
	if limit <= 0 {
		t.Skip("a measurement; set HOARDMQ_REPL_LARGEST to the frame limit in bytes")
	}
	rounds := 10
	h, _, _ := threeAt(t, false, nil)
	f := h.nodes["n2"].rep.follower
	id := clientIn(1, 0)
	payload := []byte(strings.Repeat("p", 64<<10))
	st := &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 7, Version: 5, ExpiryInterval: 300},
		Subscriptions: []store.SubscriptionRecord{{Filter: "a/b", QoS: 1}}, InboundQoS2: []uint16{}, Entries: []store.Entry{}}
	for i := 1; ; i++ {
		st.Entries = append(st.Entries, store.Entry{Seq: uint64(i), QoS: 1, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: payload}})
		raw, _ := json.Marshal(st)
		if snapshotFrameSize("default", id, 7, len(raw)) > limit {
			st.Entries = st.Entries[:len(st.Entries)-1]
			break
		}
	}
	raw, _ := json.Marshal(st)
	size := snapshotFrameSize("default", id, 7, len(raw))
	recs := append([]store.Record{store.DeleteRecord("default", id)}, store.RecordsFor(st)...)
	other := store.RecordsFor(&store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "other", Epoch: 7, Version: 5}})[0]
	var holds, finds, workers []time.Duration
	for range rounds {
		started := make(chan struct{})
		var once sync.Once
		f.betweenSnapshotRecords = func() { once.Do(func() { close(started) }) }
		var wg sync.WaitGroup
		var held, findWait, workerWait time.Duration
		wg.Add(3)
		go func() {
			defer wg.Done()
			began := time.Now()
			if err := f.applyWhole(recs, nil); err != nil {
				t.Error(err)
			}
			held = time.Since(began)
		}()
		go func() {
			defer wg.Done()
			<-started
			began := time.Now()
			f.find("default", id)
			findWait = time.Since(began)
		}()
		go func() {
			defer wg.Done()
			<-started
			began := time.Now()
			if _, err := f.apply(other); err != nil {
				t.Error(err)
			}
			workerWait = time.Since(began)
		}()
		wg.Wait()
		holds, finds, workers = append(holds, held), append(finds, findWait), append(workers, workerWait)
	}
	f.betweenSnapshotRecords = nil
	scale := float64(16<<20) / float64(size)
	report := func(name string, d []time.Duration) {
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		t.Logf("%s over %d rounds: min %s, median %s, max %s; projected to 16 MiB: max %s",
			name, len(d), d[0], d[len(d)/2], d[len(d)-1], time.Duration(float64(d[len(d)-1])*scale))
	}
	t.Logf("snapshot: %d entries, %d bytes framed (limit %d), %d records; scale to 16 MiB ×%.2f", len(st.Entries), size, limit, len(recs), scale)
	report("apply hold", holds)
	report("fetch wait", finds)
	report("other peer's record wait", workers)
}
