package lease

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/ext/mqtt"
	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

func randString(r *rand.Rand) string {
	if r.Intn(4) == 0 {
		return ""
	}
	const chars = "abcdefghij/+#-ü"
	n := 1 + r.Intn(40)
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteString(string([]rune(chars)[r.Intn(len([]rune(chars)))]))
	}
	return b.String()
}

func randTime(r *rand.Rand) time.Time {
	switch r.Intn(4) {
	case 0:
		return time.Time{}
	case 1:
		return time.Unix(0, r.Int63()).UTC()
	case 2:
		return time.Unix(0, r.Int63()).In(time.FixedZone("", -6*3600))
	}
	return time.Unix(0, r.Int63()).In(time.FixedZone("MDT", -6*3600))
}

func randBytes(r *rand.Rand) []byte {
	switch r.Intn(3) {
	case 0:
		return nil
	case 1:
		return []byte{}
	}
	b := make([]byte, 1+r.Intn(64))
	r.Read(b)
	return b
}

func randWill(r *rand.Rand) *store.WillRecord {
	if r.Intn(3) == 0 {
		return nil
	}
	w := &store.WillRecord{Delay: uint32(r.Intn(1000)), Armed: r.Intn(2) == 0}
	if r.Intn(8) == 0 {
		return w
	}
	m := &message.Message{Namespace: randString(r), Topic: randString(r), Payload: randBytes(r), QoS: byte(r.Intn(3)), Retain: r.Intn(2) == 0,
		CorrelationData: randBytes(r), Expiry: randTime(r), Seq: r.Uint64(), Arrived: randTime(r), OriginCluster: randString(r), Hops: byte(r.Intn(255)),
		Publisher: mqtt.Publisher{Namespace: randString(r), ClientID: randString(r), Username: randString(r), Node: randString(r)}}
	if r.Intn(2) == 0 {
		v := byte(r.Intn(2))
		m.PayloadFormat = &v
	}
	if r.Intn(2) == 0 {
		v := randString(r)
		m.ContentType = &v
	}
	if r.Intn(2) == 0 {
		v := randString(r)
		m.ResponseTopic = &v
	}
	switch r.Intn(3) {
	case 1:
		m.UserProperties = []mqtt.UserProperty{}
	case 2:
		for i := 0; i < 1+r.Intn(3); i++ {
			m.UserProperties = append(m.UserProperties, mqtt.UserProperty{Name: randString(r), Value: randString(r)})
		}
	}
	w.Message = m
	return w
}

func randLease(r *rand.Rand) *Lease {
	return &Lease{Namespace: randString(r), ClientID: randString(r), Owner: randString(r), Epoch: r.Uint64() >> uint(r.Intn(64)), Online: r.Intn(2) == 0,
		ExpiryInterval: r.Uint32(), ExpiresAt: randTime(r), DisconnectedAt: randTime(r), Will: randWill(r)}
}

// TestCompactLeaseReadsBackAsJSONDoes: over generated leases (every flag,
// empty and set strings, nil and empty byte slices, UTC and offset zones), a
// compact record reads back to exactly what the JSON record gives.
func TestCompactLeaseReadsBackAsJSONDoes(t *testing.T) {
	r := rand.New(rand.NewSource(2454))
	for i := 0; i < 5000; i++ {
		l := randLease(r)
		js, err := json.Marshal(l)
		if err != nil {
			t.Fatal(err)
		}
		var viaJSON Lease
		if err := json.Unmarshal(js, &viaJSON); err != nil {
			t.Fatal(err)
		}
		viaCompact, err := decodeCompactLease(encodeCompactLease(l))
		if err != nil {
			t.Fatalf("lease %d: %v", i, err)
		}
		a, _ := json.Marshal(&viaJSON)
		b, _ := json.Marshal(viaCompact)
		if !bytes.Equal(a, b) {
			t.Fatalf("lease %d differs:\njson    %s\ncompact %s", i, a, b)
		}
		if viaJSON.ExpiresAt.Location() == time.UTC && viaCompact.ExpiresAt.Location() != time.UTC {
			t.Fatalf("lease %d: a UTC time read back in %v", i, viaCompact.ExpiresAt.Location())
		}
		if !viaJSON.ExpiresAt.Equal(viaCompact.ExpiresAt) || !viaJSON.DisconnectedAt.Equal(viaCompact.DisconnectedAt) {
			t.Fatalf("lease %d: expiry %v against %v", i, viaJSON.ExpiresAt, viaCompact.ExpiresAt)
		}
	}
}

// TestCompactLeaseIsSmaller: a lease with a 64-byte Will costs a fraction of
// its JSON form.
func TestCompactLeaseIsSmaller(t *testing.T) {
	now := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
	l := &Lease{ClientID: strings.Repeat("c", 36), Owner: "n1", Epoch: 123456, ExpiryInterval: 3600, DisconnectedAt: now, ExpiresAt: now.Add(time.Hour),
		Will: &store.WillRecord{Delay: 5, Armed: true, Message: &message.Message{Topic: "devices/" + strings.Repeat("c", 36) + "/status", Payload: make([]byte, 64),
			Publisher: mqtt.Publisher{ClientID: strings.Repeat("c", 36), Username: "user", Node: "n1"}, Seq: 99, Arrived: now}}}
	js, _ := json.Marshal(l)
	c := encodeCompactLease(l)
	t.Logf("JSON %d B, compact %d B", len(js), len(c))
	if len(c)*3 > len(js) {
		t.Fatalf("compact %d B is not under a third of JSON's %d B", len(c), len(js))
	}
}

// TestCompactLeaseRefusesTruncation: every prefix of a record, and a record
// with bytes after its end, is refused.
func TestCompactLeaseRefusesTruncation(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	var l *Lease
	for l == nil || l.Will == nil || l.Will.Message == nil {
		l = randLease(r)
	}
	b := encodeCompactLease(l)
	for i := 0; i < len(b); i++ {
		if _, err := decodeCompactLease(b[:i]); err == nil {
			t.Fatalf("a %d-byte prefix of %d decoded", i, len(b))
		}
	}
	if _, err := decodeCompactLease(append(append([]byte(nil), b...), 0)); err == nil {
		t.Fatal("a record with a trailing byte decoded")
	}
}

// TestJSONLeaseSectionStillInstalls: a section of the JSON encoding, as the
// build before the compact record wrote it, installs.
func TestJSONLeaseSectionStillInstalls(t *testing.T) {
	tb := NewTable()
	now := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
	applyAt(t, tb, 5, Op{Kind: OpAlive, Node: "n1", Now: now},
		Op{Kind: OpAcquire, ClientID: "x", Node: "n1", Will: snapWill("w"), Now: now},
		Op{Kind: OpAcquire, ClientID: "y", Node: "n1", Now: now})
	want, _ := tb.Snapshot()
	meta, _ := json.Marshal(tableMeta{Nodes: tb.nodes, LostAt: tb.lostAt})
	recs := [][]byte{meta}
	for _, l := range tb.sortedLeases() {
		b, _ := json.Marshal(l)
		recs = append(recs, b)
	}
	i := 0
	fresh := NewTable()
	st, err := fresh.Stage(consensus.EncodingJSON, func() ([]byte, bool, error) {
		if i == len(recs) {
			return nil, false, nil
		}
		i++
		return recs[i-1], true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	st.Install()
	if got, _ := fresh.Snapshot(); !bytes.Equal(got, want) {
		t.Fatalf("installed:\n%s\nwant\n%s", got, want)
	}
}
