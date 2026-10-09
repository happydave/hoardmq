package broker

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// reloadNode is a node whose configuration source is a file holding its
// running document. rewrite changes a copy of the file's document and writes
// it; raw writes the file verbatim.
type reloadNode struct {
	*fixture
	path  string
	level *slog.LevelVar
	doc   config.Document
}

func startReloadable(t *testing.T, mutate func(*config.Document)) *reloadNode {
	t.Helper()
	f := start(t, mutate)
	n := &reloadNode{fixture: f, path: filepath.Join(f.dir, "hoardmq.json"), level: new(slog.LevelVar), doc: copyDoc(t, *f.srv.config())}
	n.level.Set(n.doc.Log.SlogLevel())
	n.write(t, n.doc)
	f.srv.SetConfigSource(ConfigSource{Path: n.path, Level: n.level})
	return n
}

// copyDoc is a deep copy: a Document shares its slices and maps.
func copyDoc(t *testing.T, d config.Document) config.Document {
	t.Helper()
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var out config.Document
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func (n *reloadNode) write(t *testing.T, d config.Document) {
	t.Helper()
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	n.raw(t, string(data))
}

func (n *reloadNode) raw(t *testing.T, doc string) {
	t.Helper()
	if err := os.WriteFile(n.path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (n *reloadNode) rewrite(t *testing.T, mutate func(*config.Document)) {
	t.Helper()
	d := copyDoc(t, n.doc)
	mutate(&d)
	n.doc = d
	n.write(t, d)
}

// running is the node's document in force, as GET /config serves it.
func (n *reloadNode) running(t *testing.T) config.Document {
	t.Helper()
	data, err := adminBackend{s: n.srv}.RunningConfig()
	if err != nil {
		t.Fatal(err)
	}
	var d config.Document
	if err := json.Unmarshal(data, &d); err != nil {
		t.Fatal(err)
	}
	return d
}

func pointers(ps []config.Problem) []string {
	var out []string
	for _, p := range ps {
		out = append(out, p.Pointer)
	}
	return out
}

// queuedFor is how many messages an offline client's session holds.
func queuedFor(t *testing.T, f *fixture, id string) int {
	t.Helper()
	rec, ok := f.srv.sessions.Record(id)
	if !ok {
		t.Fatalf("precondition: no session %q", id)
	}
	return rec.Queued
}

// offlineSubscriber leaves a durable session subscribed to filter at QoS 1.
func offlineSubscriber(t *testing.T, f *fixture, id, filter string) {
	t.Helper()
	expiry := uint32(3600)
	cl, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &expiry}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("precondition: %s connack %s", id, ack.ReasonCode)
	}
	cl.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: filter, QoS: 1}}})
	if _, ok := cl.read().(*packet.Suback); !ok {
		t.Fatalf("precondition: %s not subscribed", id)
	}
	cl.send(&packet.Disconnect{})
	_ = cl.c.Close()
	adminWait(t, id+" offline", func() bool {
		rec, ok := f.srv.sessions.Record(id)
		return ok && !rec.Online
	})
}

// publishQoS1 publishes and waits for the PUBACK on one connection.
func publishQoS1(t *testing.T, cl *client, topic string, id uint16) {
	t.Helper()
	cl.send(&packet.Publish{Topic: topic, QoS: 1, PacketID: id, Payload: []byte("m")})
	for {
		switch p := cl.read().(type) {
		case *packet.Puback:
			if p.PacketID == id {
				return
			}
		case *packet.Publish:
			if p.QoS == 1 {
				cl.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
			}
		}
	}
}

// TestReloadAppliesWithoutDroppingAConnection: a reload changing only
// reloadable members puts each into force, the connected client keeps its
// connection, and GET /config serves the new document.
func TestReloadAppliesWithoutDroppingAConnection(t *testing.T) {
	n := startReloadable(t, nil)
	cl, ack := n.connect("tcp", packet.V5, &packet.Connect{ClientID: "stays", CleanStart: true}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatal(ack.ReasonCode)
	}
	cl.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "r/#", QoS: 1}}})
	if _, ok := cl.read().(*packet.Suback); !ok {
		t.Fatal("precondition: no SUBACK")
	}
	offlineSubscriber(t, n.fixture, "away", "q/#")

	n.rewrite(t, func(d *config.Document) {
		d.Log.Level = "debug"
		d.Limits.ClientMessageRate = 5
		d.Limits.MaxQueuedMessages = 3
		d.Listeners[0].MaxConnections = 100
	})
	r := n.srv.Reload()
	want := []string{"/limits/clientMessageRate", "/limits/maxQueuedMessages", "/listeners/0/maxConnections", "/log/level"}
	if !r.Applied || !reflect.DeepEqual(r.Changed, want) || len(r.Problems) != 0 {
		t.Fatalf("reload: %+v; want applied with %q", r, want)
	}

	// The connection that was open stays open: a round trip on it works.
	publishQoS1(t, cl, "r/1", 2)

	got := n.running(t)
	if got.Log.Level != "debug" || got.Limits.ClientMessageRate != 5 || got.Limits.MaxQueuedMessages != 3 || got.Listeners[0].MaxConnections != 100 {
		t.Fatalf("GET /config serves %+v %+v %+v", got.Log, got.Limits, got.Listeners[0])
	}
	if n.level.Level() != slog.LevelDebug {
		t.Errorf("the loggers' level is %v, want debug", n.level.Level())
	}
	if l := n.srv.policy.ClientLimits("default"); l == nil || l.MessageRate != 5 {
		t.Errorf("client limits in force %+v, want a message rate of 5", l)
	}
	if max := n.srv.listeners[0].MaxConnections(); max != 100 {
		t.Errorf("listener tcp's bound is %d, want 100", max)
	}
	for i := uint16(1); i <= 5; i++ {
		publishQoS1(t, cl, "q/1", 10+i)
	}
	if q := queuedFor(t, n.fixture, "away"); q != 3 {
		t.Errorf("the offline session holds %d messages, want the new bound of 3", q)
	}
}

// TestReloadWithARestartOnlyChangeAppliesNothing: one restart-only member
// refuses the whole reload, naming it; the reloadable change beside it is not
// put into force.
func TestReloadWithARestartOnlyChangeAppliesNothing(t *testing.T) {
	n := startReloadable(t, nil)
	n.rewrite(t, func(d *config.Document) {
		d.Limits.MaxPacketSize = 4096
		d.Limits.ClientMessageRate = 7
	})
	r := n.srv.Reload()
	if r.Applied || !r.RestartRequired || !reflect.DeepEqual(pointers(r.Problems), []string{"/limits/maxPacketSize"}) {
		t.Fatalf("reload: %+v; want refused naming /limits/maxPacketSize", r)
	}
	if !strings.Contains(r.Problems[0].Message, "restart") {
		t.Errorf("the refusal says %q, not that a restart applies it", r.Problems[0].Message)
	}
	if got := n.running(t); got.Limits.ClientMessageRate != 0 || got.Limits.MaxPacketSize == 4096 {
		t.Errorf("the running document changed: %+v", got.Limits)
	}
	if l := n.srv.policy.ClientLimits("default"); l == nil || l.MessageRate != 0 {
		t.Errorf("client limits in force %+v, want the rate unchanged at 0", l)
	}
}

// TestReloadOfAnInvalidDocumentReportsEveryProblem: every problem comes back
// together, in check's order, and nothing changes.
func TestReloadOfAnInvalidDocumentReportsEveryProblem(t *testing.T) {
	n := startReloadable(t, nil)
	d := copyDoc(t, n.doc)
	d.Log.Level = "debug"
	d.Listeners[0].ConnectBurst = 5
	data, _ := json.Marshal(d)
	n.raw(t, strings.Replace(string(data), `"level":"debug"`, `"level":"loud"`, 1))
	r := n.srv.Reload()
	if r.Applied || r.RestartRequired || !reflect.DeepEqual(pointers(r.Problems), []string{"/listeners/0/connectBurst", "/log/level"}) {
		t.Fatalf("reload: %+v; want invalid with both problems", r)
	}
	if got := n.running(t); got.Log.Level != "info" || got.Listeners[0].ConnectBurst != 0 {
		t.Errorf("the running document changed: %+v %+v", got.Log, got.Listeners[0])
	}
	n.raw(t, "{")
	if r := n.srv.Reload(); r.Applied || len(r.Problems) != 1 || r.Problems[0].Pointer != "/" {
		t.Errorf("a malformed file: %+v", r)
	}
}

// TestReloadRebuildsTheGateForAListenerRateAlone: a listener's own connect
// rate changing, with the node's unchanged, still rebuilds the gate.
func TestReloadRebuildsTheGateForAListenerRateAlone(t *testing.T) {
	n := startReloadable(t, nil)
	if n.srv.rateGate() != nil {
		t.Fatal("precondition: a gate with no rate set")
	}
	n.rewrite(t, func(d *config.Document) { d.Listeners[0].ConnectRate = 1 })
	if r := n.srv.Reload(); !r.Applied {
		t.Fatalf("reload: %+v", r)
	}
	g := n.srv.rateGate()
	if g == nil {
		t.Fatal("no gate after a listener rate was set")
	}
	if b := g.listeners["tcp"]; b.Rate != 1 {
		t.Fatalf("listener tcp's bucket rate is %v, want 1", b.Rate)
	}
}

// TestReloadKeepsTheAdministeredOverride: the administered record keeps its
// precedence over the reloaded configuration, and the reloaded value applies
// once the record no longer sets the member.
func TestReloadKeepsTheAdministeredOverride(t *testing.T) {
	n := startReloadable(t, nil)
	nine := 9
	putLimits := func(o config.LimitsOverride, want float64) {
		t.Helper()
		v, _ := json.Marshal(o)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if res, err := n.srv.records.Load().Put(ctx, config.LimitsCollection, config.DefaultLimitsKey, v, nil, "test", time.Now()); err != nil || !res.OK {
			t.Fatalf("%+v %v", res, err)
		}
		adminWait(t, "the record in force", func() bool {
			l := n.srv.policy.ClientLimits("default")
			return l != nil && l.MessageRate == want
		})
	}
	putLimits(config.LimitsOverride{ClientMessageRate: &nine}, 9)
	n.rewrite(t, func(d *config.Document) { d.Limits.ClientMessageRate = 5 })
	if r := n.srv.Reload(); !r.Applied {
		t.Fatalf("reload: %+v", r)
	}
	if l := n.srv.policy.ClientLimits("default"); l.MessageRate != 9 {
		t.Fatalf("the rate in force after the reload is %v, want the administered 9", l.MessageRate)
	}
	putLimits(config.LimitsOverride{}, 5)
}

// TestReloadPutsEachComponentIntoForce: the expiry cap, the overload limits
// and the flapping detector each take the reloaded values.
func TestReloadPutsEachComponentIntoForce(t *testing.T) {
	n := startReloadable(t, nil)
	n.rewrite(t, func(d *config.Document) {
		d.Limits.MaxSessionExpiry = 60
		d.Limits.QueueDrop = "newest"
		d.Overload.QueuedMessages = 7
		d.Flapping.Enabled = false
		d.Flapping.MaxCount = 4
	})
	if r := n.srv.Reload(); !r.Applied {
		t.Fatalf("reload: %+v", r)
	}
	expiry := uint32(3600)
	_, ack := n.connect("tcp", packet.V5, &packet.Connect{ClientID: "capped", Properties: packet.Properties{SessionExpiryInterval: &expiry}}, false)
	if got := ack.Properties.SessionExpiryInterval; got == nil || *got != 60 {
		t.Errorf("CONNACK expiry %v, want the new cap of 60", got)
	}
	if l := n.srv.sessions.CurrentLimits(); !l.DropNewest || l.MaxSessionExpiry != 60 {
		t.Errorf("session limits in force %+v", l)
	}
	if lim := n.srv.overload.lim.Load(); lim.Queued != 7 {
		t.Errorf("overload queued limit %d, want 7", lim.Queued)
	}
	if levelOf(Indicators{Queued: 7}, *n.srv.overload.lim.Load()) == 0 {
		t.Error("a queue at the new limit leaves the level at 0")
	}
	if f := n.srv.policy.flap.cfg.Load(); f.Enabled || f.MaxCount != 4 {
		t.Errorf("flapping in force %+v", *f)
	}
}

// TestReloadWithNoSource: a node built in code has no file to re-read.
func TestReloadWithNoSource(t *testing.T) {
	f := start(t, nil)
	r := f.srv.Reload()
	if r.Applied || len(r.Problems) != 1 || r.Problems[0].Pointer != "/" || !strings.Contains(r.Problems[0].Message, "not started from a configuration file") {
		t.Fatalf("reload: %+v", r)
	}
}

// TestReloadAfterShutdownBegan: a reload reaching a node that is shutting
// down changes nothing.
func TestReloadAfterShutdownBegan(t *testing.T) {
	n := startReloadable(t, nil)
	n.rewrite(t, func(d *config.Document) { d.Log.Level = "debug" })
	n.stop()
	if r := n.srv.Reload(); r.Applied || len(r.Problems) != 1 || !strings.Contains(r.Problems[0].Message, "not serving") {
		t.Fatalf("reload: %+v", r)
	}
}

// TestReloadRereadsModuleFiles: an applied reload has auth.file read its
// users file again. A file it cannot read fails validation, as it fails
// check: nothing is applied and the users it holds stay.
func TestReloadRereadsModuleFiles(t *testing.T) {
	users := filepath.Join(t.TempDir(), "users.json")
	writeUsers := func(password string) {
		t.Helper()
		phc, err := auth.HashSCRAM([]byte(password), auth.MinSCRAMIterations)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(users, []byte(`{"users":[{"username":"u","password":"`+phc+`"}]}`), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeUsers("old")
	n := startReloadable(t, func(d *config.Document) {
		d.Auth.Anonymous = false
		d.Modules["auth.file"] = json.RawMessage(`{"path":"` + users + `"}`)
	})
	login := func(password string) packet.ReasonCode {
		_, ack := n.connect("tcp", packet.V5, &packet.Connect{ClientID: "u1", CleanStart: true, HasUsername: true, Username: "u", HasPassword: true, Password: []byte(password)}, false)
		return ack.ReasonCode
	}
	writeUsers("new")
	if code := login("new"); code == packet.Success {
		t.Fatal("precondition: the new password was accepted before a reload")
	}
	if r := n.srv.Reload(); !r.Applied || len(r.Problems) != 0 {
		t.Fatalf("reload: %+v", r)
	}
	if code := login("new"); code != packet.Success {
		t.Fatalf("the new password after the reload: %s", code)
	}
	if err := os.Chmod(users, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(users, 0o600) })
	r := n.srv.Reload()
	if r.Applied || !reflect.DeepEqual(pointers(r.Problems), []string{"/modules/auth.file"}) {
		t.Fatalf("reload with an unreadable users file: %+v", r)
	}
	if code := login("new"); code != packet.Success {
		t.Fatalf("the users kept after a failed re-read: %s", code)
	}
}

// TestReloadCountsEachOutcome: the reload counter reads one of each.
func TestReloadCountsEachOutcome(t *testing.T) {
	n := startReloadable(t, nil)
	n.rewrite(t, func(d *config.Document) { d.Log.Level = "warn" })
	n.srv.Reload()
	n.rewrite(t, func(d *config.Document) { d.Node.WALSegmentBytes *= 2 })
	n.srv.Reload()
	n.raw(t, "{")
	n.srv.Reload()
	for _, outcome := range []string{"applied", "refused", "invalid"} {
		if v := metricSum(t, n.fixture, "hoardmq_config_reloads_total", map[string]string{"outcome": outcome}); v != 1 {
			t.Errorf("reloads %s: %v, want 1", outcome, v)
		}
	}
}

// TestReloadRacesNothing: reloads alternating two reloadable documents run
// while clients connect and publish and the administered record changes.
func TestReloadRacesNothing(t *testing.T) {
	n := startReloadable(t, nil)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			rate := 0
			if i%2 == 0 {
				rate = 1000
			}
			v, _ := json.Marshal(config.LimitsOverride{ConnectRate: &rate})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = n.srv.records.Load().Put(ctx, config.LimitsCollection, config.DefaultLimitsKey, v, nil, "test", time.Now())
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			nc := n.dial("tcp", false)
			_ = nc.Close()
		}
	}()
	cl, _ := n.connect("tcp", packet.V5, &packet.Connect{ClientID: "busy", CleanStart: true}, false)
	for i := 0; i < 20; i++ {
		n.rewrite(t, func(d *config.Document) {
			d.Limits.ClientMessageRate = 1000 + i
			d.Limits.MaxQueuedMessages = 10 + i
			d.Flapping.MaxCount = 5 + i
			d.Overload.QueuedMessages = 100 + i
			d.Listeners[0].MaxConnections = 1000 + i
		})
		if r := n.srv.Reload(); !r.Applied {
			t.Fatalf("reload %d: %+v", i, r)
		}
		publishQoS1(t, cl, "busy", uint16(i+1))
	}
	close(stop)
	wg.Wait()
}

// TestReloadDuringADrain: a draining node still serves, so a reload applies;
// the drain reads only restart-only members.
func TestReloadDuringADrain(t *testing.T) {
	n := startReloadable(t, nil)
	n.srv.Drain()
	if !n.srv.Draining() {
		t.Fatal("precondition: the node is not draining")
	}
	n.rewrite(t, func(d *config.Document) { d.Limits.MaxQueuedMessages = 4 })
	if r := n.srv.Reload(); !r.Applied {
		t.Fatalf("reload during a drain: %+v", r)
	}
	if l := n.srv.sessions.CurrentLimits(); l.MaxQueued != 4 {
		t.Fatalf("queue bound in force %d, want 4", l.MaxQueued)
	}
}
