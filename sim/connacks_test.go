package sim

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestConnackLogKeepsTheLatest: six CONNACKs leave the latest four, oldest
// first, with the count of all six; the last of two handshakes without a
// CONNACK is kept with its class; a node outside the client's list reads
// "other".
func TestConnackLogKeepsTheLatest(t *testing.T) {
	nodes := []string{"10.0.0.1:1", "10.0.0.2:1"}
	var l connackLog
	for i := 0; i < 6; i++ {
		r := connackRec{atNs: int64(i+1) * int64(time.Millisecond), tookMs: uint32(10 * i), node: uint8(i % 2), code: uint8(packet.Success)}
		if i == 4 {
			r.code = uint8(packet.ServerBusy)
		}
		if i == 5 {
			r.flags = connackPresent
			r.node = connackNodeOther
		}
		l.add(r)
	}
	l.addNoAck(connackRec{atNs: int64(time.Second), node: 1, code: uint8(failureIndex(&connectError{stage: "handshake", err: os.ErrDeadlineExceeded}))})
	l.addNoAck(connackRec{atNs: 2 * int64(time.Second), node: 0, code: uint8(failureIndex(&connectError{stage: "handshake", err: os.ErrDeadlineExceeded}))})
	got := l.record("f000001", nodes)
	if got.Seen != 6 || len(got.Last) != connackRing {
		t.Fatalf("seen %d, kept %d; want 6 seen and the latest %d kept", got.Seen, len(got.Last), connackRing)
	}
	var at []int64
	for _, e := range got.Last {
		at = append(at, e.AtMs)
	}
	if want := []int64{3, 4, 5, 6}; !reflect.DeepEqual(at, want) {
		t.Errorf("kept the CONNACKs at %v ms, want the latest four oldest first %v", at, want)
	}
	if e := got.Last[2]; e.Code != "Server busy" || e.Node != nodes[0] || e.SessionPresent {
		t.Errorf("the refused CONNACK reads %+v, want Server busy from %s without Session Present", e, nodes[0])
	}
	if e := got.Last[3]; e.Code != "Success" || !e.SessionPresent || e.Node != "other" {
		t.Errorf("the last CONNACK reads %+v, want Success with Session Present from a node outside the list", e)
	}
	if got.NoConnacks != 2 || got.LastNoConnack == nil || got.LastNoConnack.AtMs != 2000 || got.LastNoConnack.NoConnack != failHandshakeDeadline || got.LastNoConnack.Node != nodes[0] {
		t.Errorf("no-CONNACK handshakes read %d, last %+v; want 2, the last at 2000 ms from %s, %s", got.NoConnacks, got.LastNoConnack, nodes[0], failHandshakeDeadline)
	}
}

// TestConnackLogSize: the record's size a client, which a million clients
// multiply.
func TestConnackLogSize(t *testing.T) {
	size := reflect.TypeOf(connackLog{}).Size()
	t.Logf("a client's CONNACK record: %d bytes; %d MB at 1,000,000 clients", size, size)
	if size > 96 {
		t.Fatalf("a client's record is %d bytes, over the 96 its cost was stated at", size)
	}
}

// scriptedBroker answers each connection by the next of its scripted answers:
// a CONNACK, or silence until the client gives up. It counts connections.
type scriptedBroker struct {
	ln    net.Listener
	conns atomic.Int64
}

func startScriptedBroker(t *testing.T, answers []packet.Connack, silentAt int) *scriptedBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &scriptedBroker{ln: ln}
	var mu sync.Mutex
	var open []net.Conn
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, nc := range open {
			_ = nc.Close()
		}
	})
	go func() {
		for i := 0; ; i++ {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			b.conns.Add(1)
			mu.Lock()
			open = append(open, nc)
			mu.Unlock()
			go func(i int, nc net.Conn) {
				dec := packet.NewDecoder(nc)
				dec.Version = packet.V5
				if _, err := dec.Read(); err != nil {
					_ = nc.Close()
					return
				}
				if i == silentAt || i-boolInt(i > silentAt) >= len(answers) {
					// Held open and silent: the client's handshake deadline ends it.
					buf := make([]byte, 1)
					_, _ = nc.Read(buf)
					_ = nc.Close()
					return
				}
				ack := answers[i-boolInt(i > silentAt)]
				enc := &packet.Encoder{Version: packet.V5}
				_ = enc.Encode(nc, &ack)
				if ack.ReasonCode != packet.Success {
					_ = nc.Close()
				}
			}(i, nc)
		}
	}()
	return b
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// TestClientKeepsItsConnacks runs a client against a broker that refuses it,
// accepts it with Session Present 0, stays silent through one handshake, and
// accepts it with Session Present 1: the client keeps the three CONNACKs in
// order and the silent handshake apart, each naming the node it dialled.
func TestClientKeepsItsConnacks(t *testing.T) {
	b := startScriptedBroker(t, []packet.Connack{
		{ReasonCode: packet.ServerBusy},
		{ReasonCode: packet.Success},
		{ReasonCode: packet.Success, SessionPresent: true},
	}, 2)
	addr := b.ln.Addr().String()
	c := NewClient("f000007", []string{addr}, 3600, 7)
	t.Cleanup(c.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	// The first acceptance's connection stays open; drop it so the client
	// reconnects into the silent handshake and then the last answer.
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	_ = conn.Close()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && (b.conns.Load() < 4 || !c.Online()) {
		time.Sleep(50 * time.Millisecond)
	}
	if b.conns.Load() < 4 || !c.Online() {
		t.Fatalf("the client made %d connections, online %v: the script was not played out", b.conns.Load(), c.Online())
	}
	got := connackRecords([]*Client{c}, []string{"f000007"})[0]
	type seen struct {
		code    string
		present bool
		node    string
	}
	var have []seen
	for _, e := range got.Last {
		have = append(have, seen{e.Code, e.SessionPresent, e.Node})
	}
	want := []seen{{"Server busy", false, addr}, {"Success", false, addr}, {"Success", true, addr}}
	if !got.Found || got.Seen != 3 || !reflect.DeepEqual(have, want) {
		t.Fatalf("the client kept %d CONNACKs %v (found %v), want %v", got.Seen, have, got.Found, want)
	}
	if got.NoConnacks != 1 || got.LastNoConnack == nil || got.LastNoConnack.NoConnack != failHandshakeDeadline || got.LastNoConnack.Node != addr || got.LastNoConnack.TookMs < 4000 {
		t.Fatalf("the silent handshake reads %d, %+v; want one, %s, from %s, after the client's 5 s deadline", got.NoConnacks, got.LastNoConnack, failHandshakeDeadline, addr)
	}
	for i := 1; i < len(got.Last); i++ {
		if got.Last[i].AtMs < got.Last[i-1].AtMs {
			t.Fatalf("the CONNACKs are out of order: %+v", got.Last)
		}
	}
}

// TestConnackRecordsByName: named clients are returned in the order asked;
// a name no client carries is returned not found.
func TestConnackRecordsByName(t *testing.T) {
	a, b := NewClient("f000001", []string{"10.0.0.1:1"}, 0, 1), NewClient("f000002", []string{"10.0.0.1:1"}, 0, 2)
	a.acks.add(connackRec{atNs: int64(time.Millisecond), code: uint8(packet.Success)})
	b.acks.add(connackRec{atNs: 2 * int64(time.Millisecond), code: uint8(packet.Success), flags: connackPresent})
	got := connackRecords([]*Client{a, b}, []string{"f000002", "nobody", "f000001"})
	if len(got) != 3 || got[0].ID != "f000002" || !got[0].Found || len(got[0].Last) != 1 || !got[0].Last[0].SessionPresent ||
		got[1].ID != "nobody" || got[1].Found || got[2].ID != "f000001" || !got[2].Found || got[2].Last[0].SessionPresent {
		t.Fatalf("records %+v: want f000002 with Session Present, nobody not found, then f000001", got)
	}
}

// The first two lines are captured from a 1,000,000-client scale run's node
// logs. The reconciler's lines carry the attributes
// internal/session/reconcile.go logs, written in the JSON form the scale
// nodes log in; the session package's tests log them as text.
const (
	logLineServedEmpty = `{"time":"2026-11-12T13:18:22.474073312-07:00","level":"WARN","msg":"session served empty although its lease says one existed","component":"lease","client":"f222667","namespace":"default","reason":"takeover","owner":"n1","token":35675}`
	logLineFetched     = `{"time":"2026-11-12T13:18:18.847475664-07:00","level":"INFO","msg":"replica fetched from a copy a former holder kept","component":"replication","client":"f634720","from":"n3","entries":0}`
	logLineCorrected   = `{"time":"2026-11-12T10:48:11.072-07:00","level":"WARN","msg":"route reconciler corrected a difference seen at two passes","direction":"withdraw","kind":"dispatched-unheld","namespace":"default","filterBytes":8,"filterStart":"\"f/f000012\"","client":"f000012"}`
	logLineConnected   = `{"time":"2026-11-12T10:48:11.072-07:00","level":"WARN","msg":"route reconciler left a connected client's subscriber that no map holds","kind":"dispatched-unheld","namespace":"default","filterBytes":8,"filterStart":"\"f/f000013\"","client":"f000013"}`
	logLineNoClient    = `{"time":"2026-11-12T10:48:11.072-07:00","level":"WARN","msg":"route reconciler corrected a difference seen at two passes","direction":"announce","kind":"unadvertised","namespace":"default","filterBytes":8,"filterStart":"\"f/f000014\""}`
)

// TestNamedLogScript runs the log scan through the local shell over a node
// log holding each line it looks for among others: the three that name a
// client are found, and only they.
func TestNamedLogScript(t *testing.T) {
	dir := t.TempDir()
	log := strings.Join([]string{logLineFetched, logLineServedEmpty, "not json", logLineCorrected, logLineNoClient, logLineConnected, ""}, "\n")
	if err := os.WriteFile(filepath.Join(dir, "node.log"), []byte(log), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := runScript(context.Background(), localShell{}, namedLogScript(dir))
	if err != nil {
		t.Fatal(err)
	}
	got := logNamedClients("n3", out)
	want := [][2]string{
		{"f222667", "n3 served it empty (takeover)"},
		{"f000012", "n3's route reconciler corrected it (withdraw dispatched-unheld)"},
		{"f000013", "n3's route reconciler left its connected subscriber (dispatched-unheld)"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the scan named %q, want %q (its output:\n%s)", got, want, out)
	}
	empty := t.TempDir()
	if err := os.WriteFile(filepath.Join(empty, "node.log"), []byte(logLineFetched+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := runScript(context.Background(), localShell{}, namedLogScript(empty)); err != nil || out != "" {
		t.Fatalf("a log naming no client: %q, %v; want nothing and no error", out, err)
	}
}

// TestNamedClients: the delivery check's clients come first, in its lists'
// order, then the logs' by node; a client named twice is asked once with
// both reasons.
func TestNamedClients(t *testing.T) {
	d := &deliveryResult{
		Lost:     []deliveryClient{{ID: "f000126", Class: classLost, NodeName: "n1"}},
		Controls: []deliveryClient{{ID: "f000004", Class: classLost, NodeName: "n3"}},
	}
	logs := map[string]string{
		"n3": logLineServedEmpty + "\n" + strings.ReplaceAll(logLineServedEmpty, "f222667", "f000126") + "\n",
		"n1": logLineCorrected + "\n",
	}
	ids, why := namedClients(d, logs)
	if want := []string{"f000126", "f000004", "f000012", "f222667"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("named %v, want %v", ids, want)
	}
	if got := why["f000126"]; len(got) != 2 || !strings.Contains(got[0], "delivery check: lost session") || got[1] != "n3 served it empty (takeover)" {
		t.Errorf("f000126's reasons %q: want the delivery check's and n3's", got)
	}
	if got := why["f000004"]; len(got) != 1 || !strings.Contains(got[0], "negative control") {
		t.Errorf("f000004's reasons %q: want the negative control", got)
	}
	if ids, _ := namedClients(nil, nil); len(ids) != 0 {
		t.Errorf("nothing to name named %v", ids)
	}
}

// TestDateEntry: an entry is named by its node's address and dated against
// the crash.
func TestDateEntry(t *testing.T) {
	e := connackEntry{AtMs: 1_044_250, Node: "10.0.0.3:7067"}
	dateEntry(&e, map[string]string{"10.0.0.3:7067": "n3"}, 1_000_000)
	if e.NodeName != "n3" || e.SinceCrashSec == nil || *e.SinceCrashSec != 44.25 {
		t.Fatalf("entry %+v: want n3 at +44.25 s", e)
	}
	e = connackEntry{AtMs: 5}
	dateEntry(&e, nil, 0)
	if e.SinceCrashSec != nil {
		t.Fatalf("no crash: dated %v", *e.SinceCrashSec)
	}
}

// BenchmarkNoteConnect is the cost a client pays per connect for its record.
func BenchmarkNoteConnect(b *testing.B) {
	c := NewClient("f000001", []string{"10.0.0.1:1", "10.0.0.2:1", "10.0.0.3:1"}, 0, 1)
	ack := &packet.Connack{ReasonCode: packet.Success, SessionPresent: true}
	began := time.Now()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.noteConnect("10.0.0.3:1", began, ack, nil)
	}
}
