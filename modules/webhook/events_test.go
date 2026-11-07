package webhook

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/ext/mqtt"
)

// sink is an endpoint that keeps each body it receives, as raw JSON.
type sink struct {
	mu     sync.Mutex
	bodies []json.RawMessage
	status int
	hold   chan struct{} // when set, each request waits for it to close
}

func (s *sink) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The body is read first: the server notices a client that went away,
	// ending r.Context(), only once the body has been read.
	data, _ := io.ReadAll(r.Body)
	if s.hold != nil {
		select {
		case <-s.hold:
		case <-r.Context().Done():
			return
		}
	}
	s.mu.Lock()
	s.bodies = append(s.bodies, data)
	status := s.status
	s.mu.Unlock()
	if status != 0 {
		w.WriteHeader(status)
	}
}

func (s *sink) received() []json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]json.RawMessage(nil), s.bodies...)
}

func decodeEvent(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("%s: %v", raw, err)
	}
	return v
}

func message() *mqtt.Message {
	return &mqtt.Message{
		Namespace: "default", Topic: "s/1", Payload: []byte("21.5"), QoS: 1, Retain: true,
		Publisher: mqtt.Publisher{Namespace: "default", ClientID: "pub", Username: "pu"},
	}
}

var info = ext.ConnectionInfo{
	Namespace: "default", ClientID: "c1", Username: "u1", Version: mqtt.V5, Listener: "tcp", PeerAddr: "192.0.2.7:51000",
}

// raiseAll calls each hook method as the broker calls it.
func raiseAll(e *eventsHTTP) {
	ctx := context.Background()
	e.Connected(ctx, info)
	e.Disconnected(ctx, info, mqtt.ReasonCode(0x8E), false)
	if out := e.InterceptSubscribe(ctx, info, &mqtt.Subscription{Filter: "s/#", QoS: 1}); out != ext.OK {
		panic(fmt.Sprint("subscribe outcome ", out))
	}
	if out := e.InterceptUnsubscribe(ctx, info, "s/#"); out != ext.OK {
		panic(fmt.Sprint("unsubscribe outcome ", out))
	}
	e.OnPublishAccepted(ctx, ext.SourceClient, message())
	for _, ev := range []ext.DeliveryEvent{ext.Delivered, ext.Acknowledged, ext.Queued, ext.Dropped, ext.Expired} {
		e.OnDelivery(ctx, ev, "default", "sub", message(), 0)
	}
	e.OnRetained(ctx, message())
	e.OnWill(ctx, "default", "c1", message())
	at := time.UnixMilli(1_700_000_000_456)
	e.OnClusterEvent(ctx, ext.ClusterEvent{Kind: ext.MemberJoined, Time: at, Member: "n4"})
	e.OnClusterEvent(ctx, ext.ClusterEvent{Kind: ext.MemberLeft, Time: at, Member: "n3"})
	e.OnClusterEvent(ctx, ext.ClusterEvent{Kind: ext.LeaseMoved, Time: at, Namespace: "t1", ClientID: "c9", From: "n2", To: "n1"})
	e.OnClusterEvent(ctx, ext.ClusterEvent{Kind: ext.DrainStarted, Member: "n1"})
}

// TestEventFamilies: each hook posts its event with its fields, in sync
// mode in the order raised.
func TestEventFamilies(t *testing.T) {
	s := &sink{}
	srv := httptest.NewServer(s)
	defer srv.Close()
	e, m, _ := newEvents(t, `{"url":"`+srv.URL+`","mode":"sync"}`)
	raiseAll(e)
	payload := base64.StdEncoding.EncodeToString([]byte("21.5"))
	common := "namespace:default node:n1 timestamp:1.800000000123e+12"
	conn := "clientid:c1 listener:tcp peername:192.0.2.7:51000 proto_ver:5 username:u1"
	want := []string{
		"map[" + conn + " event:client.connected " + common + "]",
		"map[" + conn + " event:client.disconnected graceful:false " + common + " reason:142]",
		"map[clientid:c1 event:session.subscribe " + common + " qos:1 topic:s/# username:u1]",
		"map[clientid:c1 event:session.unsubscribe " + common + " topic:s/# username:u1]",
		"map[clientid:pub event:message.publish " + common + " payload:" + payload + " qos:1 retain:true source:client topic:s/1 username:pu]",
	}
	for _, name := range []string{"delivered", "acked", "queued", "dropped", "expired"} {
		want = append(want, "map[clientid:sub event:message."+name+" "+common+" payload:"+payload+" qos:0 topic:s/1]")
	}
	want = append(want,
		"map[event:retained.changed "+common+" payload:"+payload+" topic:s/1]",
		"map[clientid:c1 event:will.published "+common+" payload:"+payload+" qos:1 topic:s/1]",
		"map[event:cluster.member.joined member:n4 namespace:default node:n1 timestamp:1.700000000456e+12]",
		"map[event:cluster.member.left member:n3 namespace:default node:n1 timestamp:1.700000000456e+12]",
		"map[clientid:c9 event:cluster.lease.moved from:n2 namespace:t1 node:n1 timestamp:1.700000000456e+12 to:n1]",
		"map[event:cluster.drain.started member:n1 "+common+"]")
	got := s.received()
	if len(got) != len(want) {
		t.Fatalf("%d events, want %d", len(got), len(want))
	}
	for i, raw := range got {
		if g := sortedMap(decodeEvent(t, raw)); g != sortedMap(parseWant(want[i])) {
			t.Errorf("event %d\n got %s\nwant %s", i, g, want[i])
		}
	}
	if n := m.get("hoardmq_events_http_sent_total{}"); n != float64(len(want)) {
		t.Errorf("sent %v, want %d", n, len(want))
	}

	filtered := &sink{}
	fsrv := httptest.NewServer(filtered)
	defer fsrv.Close()
	f, _, _ := newEvents(t, `{"url":"`+fsrv.URL+`","mode":"sync","events":["client.connected","message.publish"],"payload":"omit"}`)
	raiseAll(f)
	fgot := filtered.received()
	if len(fgot) != 2 || decodeEvent(t, fgot[0])["event"] != "client.connected" || decodeEvent(t, fgot[1])["event"] != "message.publish" {
		t.Fatalf("filtered events %s", fgot)
	}
	if _, ok := decodeEvent(t, fgot[1])["payload"]; ok {
		t.Error("payload omit sent the payload")
	}

	picked := &sink{}
	psrv := httptest.NewServer(picked)
	defer psrv.Close()
	p, _, _ := newEvents(t, `{"url":"`+psrv.URL+`","mode":"sync","events":["session.unsubscribe","cluster.lease.moved"]}`)
	raiseAll(p)
	pgot := picked.received()
	if len(pgot) != 2 || decodeEvent(t, pgot[0])["event"] != "session.unsubscribe" || decodeEvent(t, pgot[1])["event"] != "cluster.lease.moved" {
		t.Fatalf("picked events %s", pgot)
	}
}

// sortedMap renders a decoded event with sorted keys and Go's formatting.
func sortedMap(v map[string]any) string { return fmt.Sprint(v) }

// parseWant turns "map[k:v ...]" into the map fmt would print it from;
// values are compared as fmt prints them.
func parseWant(s string) map[string]any {
	out := map[string]any{}
	for _, kv := range strings.Fields(strings.TrimSuffix(strings.TrimPrefix(s, "map["), "]")) {
		k, v, _ := strings.Cut(kv, ":")
		out[k] = v
	}
	return out
}

// TestBatch: batch.maxEvents above 1 posts a JSON array.
func TestBatch(t *testing.T) {
	s := &sink{}
	srv := httptest.NewServer(s)
	defer srv.Close()
	e, m, _ := newEvents(t, `{"url":"`+srv.URL+`","batch":{"maxEvents":3,"maxDelayMs":10000}}`)
	for range 3 {
		e.Connected(context.Background(), info)
	}
	eventually(t, "a batch", func() bool { return len(s.received()) == 1 })
	var batch []map[string]any
	if err := json.Unmarshal(s.received()[0], &batch); err != nil || len(batch) != 3 || batch[2]["event"] != "client.connected" {
		t.Fatalf("batch %s: %v", s.received()[0], err)
	}
	if n := m.get("hoardmq_events_http_sent_total{}"); n != 3 {
		t.Errorf("sent %v", n)
	}
}

// TestAsyncDrops: a full buffer drops at once and counts buffer_full, a
// failing endpoint counts failed after its attempts, Stop counts stopped,
// and each reason is logged.
func TestAsyncDrops(t *testing.T) {
	held := &sink{hold: make(chan struct{})}
	srv := httptest.NewServer(held)
	defer srv.Close()
	e, m, log := newEvents(t, `{"url":"`+srv.URL+`","buffer":{"maxEvents":2},"timeoutMs":30000}`)
	e.Connected(context.Background(), info)
	// The worker holds the first event in its request; two more fill the buffer.
	eventually(t, "the worker takes the first event", func() bool { return len(e.ch) == 0 })
	var slowest time.Duration
	for range 10 {
		start := time.Now()
		e.Connected(context.Background(), info)
		slowest = max(slowest, time.Since(start))
	}
	if slowest > 50*time.Millisecond {
		t.Errorf("a hook with the endpoint held took %v", slowest)
	}
	if got := m.get("hoardmq_events_http_dropped_total{reason=buffer_full}"); got != 8 {
		t.Errorf("buffer_full %v, want 8", got)
	}
	if got := m.get("hoardmq_events_http_buffered{}"); got != 2 {
		t.Errorf("buffered %v, want 2", got)
	}
	if !strings.Contains(log.String(), "reason=buffer_full count=") {
		t.Errorf("no buffer_full Warn:\n%s", log.String())
	}
	close(held.hold)
	eventually(t, "the three delivered", func() bool { return m.get("hoardmq_events_http_sent_total{}") == 3 })

	failing := &sink{status: http.StatusBadRequest}
	fsrv := httptest.NewServer(failing)
	defer fsrv.Close()
	f, fm, flog := newEvents(t, `{"url":"`+fsrv.URL+`","retry":{"maxAttempts":3,"backoffMs":10}}`)
	f.Connected(context.Background(), info)
	eventually(t, "failed", func() bool { return fm.get("hoardmq_events_http_dropped_total{reason=failed}") == 1 })
	if n := len(failing.received()); n != 3 {
		t.Errorf("%d attempts, want 3 (a 4xx is retried)", n)
	}
	if !strings.Contains(flog.String(), "reason=failed") || !strings.Contains(flog.String(), "status 400") {
		t.Errorf("no failed Warn:\n%s", flog.String())
	}

	stuck := &sink{hold: make(chan struct{})}
	ssrv := httptest.NewServer(stuck)
	defer ssrv.Close()
	defer close(stuck.hold)
	st, sm, stlog := newEvents(t, `{"url":"`+ssrv.URL+`","buffer":{"maxEvents":5},"timeoutMs":30000}`)
	for range 3 {
		st.Connected(context.Background(), info)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := st.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Errorf("Stop took %v", el)
	}
	st.Connected(context.Background(), info)
	if got := sm.get("hoardmq_events_http_dropped_total{reason=stopped}"); got != 4 {
		t.Errorf("stopped %v, want 4 (three held at Stop, one after)\n%s", got, sm.keys())
	}
	if !strings.Contains(stlog.String(), "reason=stopped") {
		t.Errorf("no stopped Warn:\n%s", stlog.String())
	}
}

// TestStopSendsTheBuffer: Stop with a reachable endpoint sends what the
// buffer holds, a batch gathering and events still queued alike.
func TestStopSendsTheBuffer(t *testing.T) {
	for _, conf := range []string{`"batch":{"maxEvents":10,"maxDelayMs":10000}`, `"batch":{"maxEvents":1}`} {
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			time.Sleep(50 * time.Millisecond)
		}))
		e, m, _ := newEvents(t, `{"url":"`+srv.URL+`",`+conf+`}`)
		for range 4 {
			e.Connected(context.Background(), info)
		}
		if err := e.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		if n := m.get("hoardmq_events_http_sent_total{}"); n != 4 {
			t.Errorf("%s: sent %v at Stop, want 4\n%s", conf, n, m.keys())
		}
		srv.Close()
	}
}

// TestSyncBackPressure: in sync mode the hook returns only once the
// endpoint answers, and after the attempts when it fails.
func TestSyncBackPressure(t *testing.T) {
	var answered time.Time
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		mu.Lock()
		answered = time.Now()
		mu.Unlock()
	}))
	defer srv.Close()
	e, m, _ := newEvents(t, `{"url":"`+srv.URL+`","mode":"sync"}`)
	start := time.Now()
	e.OnPublishAccepted(context.Background(), ext.SourceClient, message())
	returned := time.Now()
	mu.Lock()
	defer mu.Unlock()
	if returned.Sub(start) < 300*time.Millisecond || returned.Before(answered) {
		t.Errorf("the hook returned after %v, before the endpoint answered", returned.Sub(start))
	}
	if n := m.get("hoardmq_events_http_sent_total{}"); n != 1 {
		t.Errorf("sent %v", n)
	}

	failing := &sink{status: http.StatusServiceUnavailable}
	fsrv := httptest.NewServer(failing)
	defer fsrv.Close()
	f, fm, _ := newEvents(t, `{"url":"`+fsrv.URL+`","mode":"sync","retry":{"maxAttempts":2,"backoffMs":10}}`)
	f.Connected(context.Background(), info)
	if n := len(failing.received()); n != 2 {
		t.Errorf("%d attempts before the hook returned, want 2", n)
	}
	if got := fm.get("hoardmq_events_http_dropped_total{reason=failed}"); got != 1 {
		t.Errorf("failed %v", got)
	}

	// The hook's context bounds it, as hooks.timeoutMs does.
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		<-r.Context().Done()
	}))
	defer slow.Close()
	h, hm, _ := newEvents(t, `{"url":"`+slow.URL+`","mode":"sync","timeoutMs":30000}`)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start = time.Now()
	h.Connected(ctx, info)
	if el := time.Since(start); el > 5*time.Second {
		t.Errorf("the hook's context did not bound it: %v", el)
	}
	if got := hm.get("hoardmq_events_http_dropped_total{reason=failed}"); got != 1 {
		t.Errorf("failed %v\n%s", got, hm.keys())
	}
}

// TestNextBackoff: the wait doubles up to 5 s, or up to a longer first wait.
func TestNextBackoff(t *testing.T) {
	for _, c := range []struct{ wait, first, want time.Duration }{
		{200 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond},
		{4 * time.Second, 200 * time.Millisecond, 5 * time.Second},
		{5 * time.Second, 200 * time.Millisecond, 5 * time.Second},
		{8 * time.Second, 8 * time.Second, 8 * time.Second},
	} {
		if got := nextBackoff(c.wait, c.first); got != c.want {
			t.Errorf("nextBackoff(%v, %v) = %v, want %v", c.wait, c.first, got, c.want)
		}
	}
}
