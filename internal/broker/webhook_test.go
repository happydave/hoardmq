package broker

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
	_ "github.com/happydave/hoardmq/modules/webhook" // auth.http and events.http
)

// TestWebhooks runs auth.http and events.http on a node: the endpoint
// decides a CONNECT and a publish, an answer is held within the decision TTL
// the module reads from its context, the events reach the other endpoint,
// and an unreachable auth.http refuses.
func TestWebhooks(t *testing.T) {
	var mu sync.Mutex
	ttlAsks := 0
	authSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req["topic"] == "w/ttl" {
			mu.Lock()
			ttlAsks++
			mu.Unlock()
			w.Header().Set("Cache-Control", "max-age=100")
		}
		result := "allow"
		if req["username"] == "bad" || req["topic"] == "w/denied" {
			result = "deny"
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"result": result})
	}))
	defer authSrv.Close()
	var events []map[string]any
	eventSrv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		var ev map[string]any
		if json.Unmarshal(data, &ev) == nil {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		}
	}))
	defer eventSrv.Close()

	f := start(t, func(d *config.Document) {
		d.Auth.DecisionTTLSeconds = 1
		d.Modules["auth.http"] = json.RawMessage(`{"url":"` + authSrv.URL + `"}`)
		d.Modules["events.http"] = json.RawMessage(`{"url":"` + eventSrv.URL + `","events":["client.connected","message.publish"]}`)
	})
	good, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "good", CleanStart: true, HasUsername: true, Username: "good"}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("an allowed CONNECT: %s", ack.ReasonCode)
	}
	if _, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "bad", CleanStart: true, HasUsername: true, Username: "bad"}, false); ack.ReasonCode != packet.BadUserNameOrPassword {
		t.Fatalf("a denied CONNECT: %s, want 0x86", ack.ReasonCode)
	}
	good.send(&packet.Publish{Topic: "w/ok", QoS: 1, PacketID: 1, Payload: []byte("x")})
	// With no subscriber an accepted publish is answered No Matching
	// Subscribers (0x10); any code below 0x80 accepted it.
	if c := good.read().(*packet.Puback).ReasonCode; c >= 0x80 {
		t.Fatalf("an allowed publish: %s", c)
	}
	good.send(&packet.Publish{Topic: "w/denied", QoS: 1, PacketID: 2, Payload: []byte("x")})
	if c := good.read().(*packet.Puback).ReasonCode; c != packet.NotAuthorized {
		t.Fatalf("a denied publish: %s, want 0x87", c)
	}
	// The endpoint's max-age of 100 s is held only auth.decisionTtlSeconds.
	asks := func() int {
		mu.Lock()
		defer mu.Unlock()
		return ttlAsks
	}
	for i := range 3 {
		if i == 2 {
			time.Sleep(1100 * time.Millisecond)
		}
		good.send(&packet.Publish{Topic: "w/ttl", QoS: 1, PacketID: uint16(10 + i), Payload: []byte("x")})
		if c := good.read().(*packet.Puback).ReasonCode; c >= 0x80 {
			t.Fatalf("publish %d to w/ttl: %s", i, c)
		}
	}
	if n := asks(); n != 2 {
		t.Fatalf("w/ttl asked %d times over three publishes a second apart, want 2", n)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		var connected, published bool
		for _, ev := range events {
			connected = connected || (ev["event"] == "client.connected" && ev["clientid"] == "good")
			published = published || (ev["event"] == "message.publish" && ev["topic"] == "w/ok")
			if ev["topic"] == "w/denied" {
				t.Errorf("a refused publish was posted: %v", ev)
			}
		}
		mu.Unlock()
		if connected && published {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("events.http posted %v", events)
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.stop()

	dead := start(t, func(d *config.Document) {
		d.Modules["auth.http"] = json.RawMessage(`{"url":"http://` + deadPort(t) + `/auth"}`)
	})
	if _, ack := dead.connect("tcp", packet.V5, &packet.Connect{ClientID: "c", CleanStart: true, HasUsername: true, Username: "good"}, false); ack.ReasonCode != packet.BadUserNameOrPassword {
		t.Fatalf("an unreachable auth.http: %s, want 0x86", ack.ReasonCode)
	}
}
