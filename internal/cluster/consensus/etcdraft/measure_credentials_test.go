package etcdraft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/records"
)

// TestCredentialSnapshotMeasure measures the in-broker credential store at a
// given size: the heap the records hold, the time to apply them, and what a
// snapshot of them costs on each side (capture and file on the builder; stage
// and install on a follower that needs it). One process, one side at a time.
// Each credential is a PBKDF2 PHC string, as auth.db stores, under a 36-byte
// username.
func TestCredentialSnapshotMeasure(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("HOARDMQ_CREDENTIALS_MEASURE"))
	if n == 0 {
		t.Skip("set HOARDMQ_CREDENTIALS_MEASURE to the credential count")
	}
	phc, err := auth.HashPassword([]byte("measure"))
	if err != nil {
		t.Fatal(err)
	}
	value, err := json.Marshal(auth.StoredCredential{Password: phc})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 2, 1, 0, 0, 0, time.UTC)
	fill := func() (*consensus.Machine, time.Duration) {
		m := consensus.NewMachine()
		m.Register(records.Kind, records.New(n))
		began := time.Now()
		for i := 0; i < n; i++ {
			op := records.Op{Collection: auth.CredentialsCollection, Key: fmt.Sprintf("%036d", i), Value: value, By: "admin", At: at}
			payload, err := json.Marshal(op)
			if err != nil {
				t.Fatal(err)
			}
			res, err := m.Apply(uint64(i+1), consensus.Encode(records.Kind, payload))
			if err != nil {
				t.Fatal(err)
			}
			if r, ok := res.(records.Result); !ok || !r.OK {
				t.Fatalf("credential %d not applied: %+v", i, res)
			}
		}
		return m, time.Since(began)
	}
	base := heapNow()
	m, applied := fill()
	heap := startHeapPeaks()
	defer heap.close()
	held := heapNow()
	heap.reset()
	began := time.Now()
	capture, err := m.Capture()
	if err != nil {
		t.Fatal(err)
	}
	captured := time.Since(began)
	dir := t.TempDir()
	c := &Core{snapDir: dir, stop: make(chan struct{})}
	began = time.Now()
	d, err := c.writeSnapshotFile(capture, consensus.FileHeader{Index: m.Applied(), Term: 1, Applied: m.Applied()}, "measure.snap")
	if err != nil {
		t.Fatal(err)
	}
	wrote := time.Since(began)
	buildPeak := heap.reset()

	other, _ := fill()
	c.machine = other
	held2 := heapNow()
	heap.reset()
	began = time.Now()
	st, err := c.stageSnapshot(d)
	if err != nil {
		t.Fatal(err)
	}
	staged := time.Since(began)
	stagePeak := heap.reset()
	began = time.Now()
	if err := st.Install(); err != nil {
		t.Fatal(err)
	}
	installed := time.Since(began)
	fi, err := os.Stat(filepath.Join(dir, "measure.snap"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MEASURE-CREDENTIALS n=%d value=%dB base=%d held=%d (+%d, %d B each) apply=%v file=%dB (%d B each) capture=%v write=%v buildPeak=%d (+%d) heldTwo=%d stage=%v stagePeak=%d (+%d) install=%v",
		n, len(value), base, held, int64(held)-int64(base), (int64(held)-int64(base))/int64(n), applied.Round(time.Millisecond),
		fi.Size(), fi.Size()/int64(n), captured.Round(time.Millisecond), wrote.Round(time.Millisecond), buildPeak, int64(buildPeak)-int64(held),
		held2, staged.Round(time.Millisecond), stagePeak, int64(stagePeak)-int64(held2), installed.Round(time.Millisecond))
}
