package session

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

func ids(rs []Record) string {
	var out []string
	for _, r := range rs {
		out = append(out, r.ClientID)
	}
	return strings.Join(out, ",")
}

// The scan filters by substring, orders by identifier and pages by cursor.
func TestScanRecords(t *testing.T) {
	m, _ := newManager(t, nil)
	for _, id := range []string{"b1", "a2", "a1", "xa"} {
		connectV5(m, newFake(id, packet.V5, 10), true, 600, nil)
	}
	cases := []struct {
		search, after string
		limit         int
		want          string
		more          bool
	}{
		{"", "", 100, "a1,a2,b1,xa", false},
		{"a", "", 100, "a1,a2,xa", false},
		{"a", "", 1, "a1", true},
		{"a", "a1", 1, "a2", true},
		{"a", "a2", 1, "xa", false},
		{"a", "xa", 1, "", false},
		{"", "", 4, "a1,a2,b1,xa", false},
		{"zz", "", 10, "", false},
	}
	for _, c := range cases {
		got, more := m.ScanRecords("default", c.search, c.after, c.limit)
		if ids(got) != c.want || more != c.more {
			t.Errorf("search %q after %q limit %d: %q more=%v, want %q more=%v", c.search, c.after, c.limit, ids(got), more, c.want, c.more)
		}
	}
	if got, _ := m.ScanRecords("other", "", "", 10); len(got) != 0 {
		t.Errorf("another namespace's scan found %s", ids(got))
	}
}

// CountPersistent counts the sessions with an expiry that the predicate
// admits, and no session without one.
func TestCountPersistent(t *testing.T) {
	m, _ := newManager(t, nil)
	for _, id := range []string{"a1", "a2", "b1"} {
		connectV5(m, newFake(id, packet.V5, 10), true, 600, nil)
	}
	connectV5(m, newFake("a3", packet.V5, 10), true, 0, nil)
	if n := m.CountPersistent(func(_, id string) bool { return strings.HasPrefix(id, "a") }); n != 2 {
		t.Fatalf("persistent sessions under a: %d, want 2", n)
	}
	if n := m.CountPersistent(func(string, string) bool { return true }); n != 3 {
		t.Fatalf("persistent sessions: %d, want 3", n)
	}
}

// keyOnShard finds a client identifier whose key maps to the given shard.
func keyOnShard(t *testing.T, m *Manager, shard int) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		id := fmt.Sprint("k", i)
		if m.shardFor(key{m.opts.Namespace, id}) == m.shards[shard] {
			return id
		}
	}
	t.Fatalf("no identifier maps to shard %d", shard)
	return ""
}

// The scan holds one shard at a time: while the last shard's lock is held,
// the scan waits, and a connect on the first shard completes meanwhile. A
// scan that took the shards in index order and held them (lockAll) would
// hold the first while waiting for the last.
func TestScanRecordsHoldsOneShardAtATime(t *testing.T) {
	m, _ := newManager(t, nil)
	if len(m.shards) < 2 {
		t.Fatalf("the fixture needs several shards, has %d", len(m.shards))
	}
	last := m.shards[len(m.shards)-1]
	firstID := keyOnShard(t, m, 0)
	connectV5(m, newFake(keyOnShard(t, m, len(m.shards)-1), packet.V5, 10), true, 600, nil)
	last.mu.Lock()
	scanned := make(chan []Record, 1)
	go func() {
		got, _ := m.ScanRecords("default", "", "", 100)
		scanned <- got
	}()
	connected := make(chan struct{})
	go func() {
		connectV5(m, newFake(firstID, packet.V5, 10), true, 600, nil)
		close(connected)
	}()
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		last.mu.Unlock()
		t.Fatal("a connect on the first shard waited for the scan: the scan holds shards it has passed")
	}
	select {
	case got := <-scanned:
		last.mu.Unlock()
		t.Fatalf("the scan returned %s while the last shard was locked", ids(got))
	case <-time.After(50 * time.Millisecond):
	}
	last.mu.Unlock()
	if got := <-scanned; len(got) == 0 {
		t.Fatal("the scan found nothing")
	}
}

func TestSubscriptionsAndConn(t *testing.T) {
	m, _ := newManager(t, nil)
	c := newFake("c", packet.V5, 10)
	connectV5(m, c, true, 600, nil)
	subscribe(m, c, 1, "b/#", "a/+")
	subs, ok := m.Subscriptions("default", "c")
	if !ok || len(subs) != 2 || subs[0].Filter != "a/+" || subs[1].Filter != "b/#" || subs[0].QoS != 1 {
		t.Fatalf("subscriptions %+v %v", subs, ok)
	}
	if got, ok := m.Conn("default", "c"); !ok || got != c {
		t.Fatalf("conn %v %v", got, ok)
	}
	m.Disconnect(c, true, packet.NormalDisconnection, false)
	if _, ok := m.Conn("default", "c"); ok {
		t.Fatal("an offline session reports a connection")
	}
	if _, ok := m.Subscriptions("default", "nobody"); ok {
		t.Fatal("an unknown client has subscriptions")
	}
}

// Invalidating a connected session closes it with the reason, discards the
// state with its journal delete, reports the end (the lease release) before
// the close, and publishes the Will because the session ended.
func TestInvalidateOnline(t *testing.T) {
	j := newFakeJournal()
	var events []string
	var c *fakeConn
	m, _ := newManager(t, func(o *Options) {
		o.Journal = j
		o.Ended = func(ns, id string, epoch uint64) {
			c.mu.Lock()
			closedYet := len(c.closed) > 0
			c.mu.Unlock()
			events = append(events, fmt.Sprintf("ended %s %d closed=%v", id, epoch, closedYet))
		}
	})
	watcher := newFake("w", packet.V5, 10)
	connectV5(m, watcher, true, 0, nil)
	subscribe(m, watcher, 0, "will/c")
	c = newFake("c", packet.V5, 10)
	pkt := &packet.Connect{ProtocolLevel: packet.V5, ClientID: "c", CleanStart: true, Will: &packet.Will{Topic: "will/c", Payload: []byte("gone")}}
	exp := uint32(600)
	pkt.Properties.SessionExpiryInterval = &exp
	m.Connect(c, pkt, 7)
	m.Resume(c)
	subscribe(m, c, 1, "t")
	res := m.Invalidate("default", "c", packet.AdministrativeAction, func(token uint64) bool { return token == 7 })
	if !res.Found || res.Refused || !res.WasOnline || !res.WillPublished || res.Scope != (store.Scope{Namespace: "default", ClientID: "c", Epoch: 7}) {
		t.Fatalf("result %+v", res)
	}
	if len(c.closed) != 1 || c.closed[0] != packet.AdministrativeAction {
		t.Fatalf("closed %v", c.closed)
	}
	if len(events) != 1 || events[0] != "ended c 7 closed=false" {
		t.Fatalf("end events %v: the release must be submitted before the close", events)
	}
	if _, ok := m.Record("c"); ok {
		t.Fatal("the session is still held")
	}
	pubs := watcher.publishes()
	if len(pubs) != 1 || pubs[0].Topic != "will/c" || string(pubs[0].Payload) != "gone" {
		t.Fatalf("will deliveries %v", pubs)
	}
	if !strings.Contains(strings.Join(j.calls, ";"), "delete c") {
		t.Fatalf("journal %v", j.calls)
	}
	// The connection's own disconnect, arriving after the close, finds nothing.
	m.Disconnect(c, false, packet.AdministrativeAction, true)
	if len(watcher.publishes()) != 1 {
		t.Fatal("the Will went out twice")
	}
}

// Offline: a session whose Will is armed (the connection dropped, the Will
// Delay still running) has it published now; one that disconnected normally
// has none to publish.
func TestInvalidateOffline(t *testing.T) {
	m, _ := newManager(t, nil)
	watcher := newFake("w", packet.V5, 10)
	connectV5(m, watcher, true, 0, nil)
	subscribe(m, watcher, 0, "will/armed", "will/normal")
	delay := uint32(300)
	for _, id := range []string{"armed", "normal"} {
		c := newFake(id, packet.V5, 10)
		pkt := &packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: true, Will: &packet.Will{Topic: "will/" + id, Payload: []byte(id), Properties: packet.Properties{WillDelayInterval: &delay}}}
		exp := uint32(600)
		pkt.Properties.SessionExpiryInterval = &exp
		m.Connect(c, pkt, 3)
		m.Resume(c)
		m.Disconnect(c, id == "normal", packet.NormalDisconnection, id == "armed")
	}
	if len(watcher.publishes()) != 0 {
		t.Fatalf("a Will went out before the delay: %v", watcher.publishes())
	}
	res := m.Invalidate("default", "armed", packet.AdministrativeAction, nil)
	if !res.Found || res.WasOnline || !res.WillPublished {
		t.Fatalf("armed %+v", res)
	}
	res = m.Invalidate("default", "normal", packet.AdministrativeAction, nil)
	if !res.Found || res.WasOnline || res.WillPublished {
		t.Fatalf("normal %+v", res)
	}
	pubs := watcher.publishes()
	if len(pubs) != 1 || pubs[0].Topic != "will/armed" {
		t.Fatalf("wills %v", pubs)
	}
	if res := m.Invalidate("default", "nobody", packet.AdministrativeAction, nil); res.Found {
		t.Fatalf("unknown %+v", res)
	}
}

// A refusal changes nothing: the session, its connection and its Will stay.
func TestInvalidateRefused(t *testing.T) {
	ended := 0
	m, _ := newManager(t, func(o *Options) { o.Ended = func(string, string, uint64) { ended++ } })
	c := newFake("c", packet.V5, 10)
	pkt := &packet.Connect{ProtocolLevel: packet.V5, ClientID: "c", CleanStart: true, Will: &packet.Will{Topic: "w", Payload: []byte("x")}}
	exp := uint32(600)
	pkt.Properties.SessionExpiryInterval = &exp
	m.Connect(c, pkt, 4)
	m.Resume(c)
	var saw uint64
	res := m.Invalidate("default", "c", packet.AdministrativeAction, func(token uint64) bool { saw = token; return false })
	if !res.Found || !res.Refused || saw != 4 {
		t.Fatalf("result %+v saw %d", res, saw)
	}
	if rec, ok := m.Record("c"); !ok || !rec.Online || !rec.HasWill {
		t.Fatalf("record after a refusal %+v %v", rec, ok)
	}
	if len(c.closed) != 0 || ended != 0 {
		t.Fatalf("closed %v ended %d", c.closed, ended)
	}
}

// DeleteRetained removes the entry as an empty retained publish does and
// delivers nothing to a subscriber of the topic.
func TestDeleteRetained(t *testing.T) {
	j := newFakeJournal()
	m, _ := newManager(t, func(o *Options) { o.Journal = j })
	pub := newFake("pub", packet.V5, 10)
	connectV5(m, pub, true, 0, nil)
	m.Publish(pub, &packet.Publish{Topic: "r/1", Retain: true, Payload: []byte("keep")})
	m.Publish(pub, &packet.Publish{Topic: "r/2", Retain: true, Payload: []byte("drop")})
	sub := newFake("sub", packet.V5, 10)
	connectV5(m, sub, true, 0, nil)
	subscribe(m, sub, 0, "r/1", "r/2")
	before := len(sub.publishes())
	if len(m.QueryRetained("default", "r/1")) != 1 || len(m.QueryRetained("default", "r/2")) != 1 {
		t.Fatal("the fixture's retained messages are not held")
	}
	scope, appended := m.DeleteRetained("default", "r/2")
	if scope != (store.Scope{Namespace: "default", Topic: "r/2"}) || !appended {
		t.Fatalf("scope %+v, appended %v", scope, appended)
	}
	if len(m.QueryRetained("default", "r/1")) != 1 || len(m.QueryRetained("default", "r/2")) != 0 {
		t.Fatal("the delete removed the wrong entry or none")
	}
	if after := len(sub.publishes()); after != before {
		t.Fatalf("the delete delivered %d messages to a subscriber", after-before)
	}
}
