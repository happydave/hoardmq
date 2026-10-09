package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/audit"
	"github.com/happydave/hoardmq/internal/bans"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/conn"
	"github.com/happydave/hoardmq/internal/packet"
)

func TestLevelOf(t *testing.T) {
	lim := overloadLimits{Queued: 100, Lag: 100, Heap: 100}
	for _, c := range []struct {
		q    int64
		want uint8
	}{{59, 0}, {60, 1}, {79, 1}, {80, 2}, {94, 2}, {95, 3}} {
		if got := levelOf(Indicators{Queued: c.q}, lim); got != c.want {
			t.Errorf("queued at %d%%: level %d, want %d", c.q, got, c.want)
		}
	}
	if got := levelOf(Indicators{Queued: 10, Lag: 85, Heap: 20}, lim); got != 2 {
		t.Errorf("the highest indicator decides: %d, want 2", got)
	}
	if got := levelOf(Indicators{Queued: 1000, Lag: 1000, Heap: 1000}, overloadLimits{}); got != 0 {
		t.Errorf("limits of 0 leave every indicator out: %d", got)
	}
}

func TestShedPlan(t *testing.T) {
	var rates []int64
	for i := 1; i <= 20; i++ {
		rates = append(rates, int64(i)*1000)
	}
	rates = append(rates, 0, 0, 0, 0, 0)
	for _, c := range []struct {
		level uint8
		n     int
		pause time.Duration
	}{{0, 0, 0}, {1, 2, 100 * time.Millisecond}, {2, 10, 250 * time.Millisecond}, {3, 20, 500 * time.Millisecond}} {
		chosen, pause := shedPlan(rates, c.level, nil)
		if len(chosen) != c.n || pause != c.pause {
			t.Fatalf("level %d: %d chosen for %v, want %d for %v", c.level, len(chosen), pause, c.n, c.pause)
		}
		for _, i := range chosen {
			if rates[i] == 0 || rates[i] <= int64(20-c.n)*1000 {
				t.Fatalf("level %d chose a reader at %d B/s: not among the %d noisiest", c.level, rates[i], c.n)
			}
		}
	}
}

// A connection already paused keeps its place though the pause makes it
// quiet: the loudest client, paused at level 1, is still paused when a quiet
// sample follows, and at level 2 the next noisiest join it.
func TestShedPlanHoldsThePaused(t *testing.T) {
	// 0 is the loud client, now at 100 B/s under its pause; 1-4 at 4,000.
	rates := []int64{100, 4000, 4000, 4000, 4000, 400, 400, 400, 400, 400}
	chosen, _ := shedPlan(rates, 1, []int{0})
	if len(chosen) != 1 || chosen[0] != 0 {
		t.Fatalf("level 1 with the loud client held: %v, want [0]", chosen)
	}
	chosen, _ = shedPlan(rates, 2, []int{0})
	if len(chosen) != 5 || chosen[0] != 0 {
		t.Fatalf("level 2: %v, want the held one first and four more", chosen)
	}
	for _, i := range chosen[1:] {
		if rates[i] != 4000 {
			t.Fatalf("level 2 added a reader at %d B/s ahead of one at 4,000: %v", rates[i], chosen)
		}
	}
	// Falling back to level 1 keeps the first chosen.
	chosen, _ = shedPlan(rates, 1, []int{0, 1, 2, 3, 4})
	if len(chosen) != 1 || chosen[0] != 0 {
		t.Fatalf("back at level 1: %v, want [0]", chosen)
	}
}

func TestClusterLevelIsTheHighest(t *testing.T) {
	peers := []transport.PeerInfo{{Name: "a", State: transport.Up, Stress: 2}, {Name: "b", State: transport.Up, Stress: 1}}
	if got := clusterLevel(0, peers); got != 2 {
		t.Fatalf("a peer at 2: %d", got)
	}
	peers[0].State = transport.Down
	if got := clusterLevel(0, peers); got != 1 {
		t.Fatalf("the peer at 2 gone: %d, want the other's 1", got)
	}
	if got := clusterLevel(3, nil); got != 3 {
		t.Fatalf("own 3: %d", got)
	}
}

// withHooks sets the seams every node started next is built with, for this
// test.
func withHooks(t *testing.T, h Hooks) {
	prev := fixtureHooks
	fixtureHooks = h
	t.Cleanup(func() { fixtureHooks = prev })
}

// publisher keeps publishing QoS 0 at a rate until stopped.
func publisher(t *testing.T, addr, id string, perSecond int, stop <-chan struct{}, wg *sync.WaitGroup) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	enc := &packet.Encoder{Version: packet.V5}
	dec := packet.NewDecoder(nc)
	dec.Version = packet.V5
	_ = enc.Encode(nc, &packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: true})
	_ = nc.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := dec.Read(); err != nil {
		t.Error(err)
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { _ = nc.Close() }()
		tick := time.NewTicker(time.Second / time.Duration(perSecond))
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				_ = nc.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if enc.Encode(nc, &packet.Publish{Topic: "load/" + id, Payload: make([]byte, 100)}) != nil {
					return
				}
			}
		}
	}()
}

// Under an injected level the node sheds in the design's order: the noisiest
// readers first, more of them as the level rises, and new connections only at
// level 3. The assertion reads the pause set on each connection.
func TestShedsInTheDesignsOrder(t *testing.T) {
	var level atomic.Uint32
	// Half a second between samples, so the quietest publisher (4 a second)
	// reads in every sample and the ranking is the rates', not the ticks'.
	const interval = 500 * time.Millisecond
	withHooks(t, Hooks{Stress: func(string) uint8 { return uint8(level.Load()) }, OverloadInterval: interval})
	f := start(t, nil)
	addr := f.srv.Addr("tcp").String()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	rates := map[string]int{"loud": 400}
	for i := 0; i < 4; i++ {
		rates[fmt.Sprintf("mid%d", i)] = 40
	}
	for i := 0; i < 5; i++ {
		rates[fmt.Sprintf("low%d", i)] = 4
	}
	for id, r := range rates {
		publisher(t, addr, id, r, stop, &wg)
	}
	t.Cleanup(func() { close(stop); wg.Wait() })
	pauses := func() map[string]time.Duration {
		out := map[string]time.Duration{}
		for id := range rates {
			if c := f.srv.connected(id); c != nil {
				out[id] = c.ShedPause()
			}
		}
		return out
	}
	settle := func(want uint8) map[string]time.Duration {
		t.Helper()
		level.Store(uint32(want))
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if st := f.srv.overload.Stats(); st.ClusterLevel == want && st.Samples > 0 {
				time.Sleep(3 * interval) // three samples at the level
				return pauses()
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("level %d never enforced", want)
		return nil
	}
	count := func(p map[string]time.Duration, d time.Duration, ids ...string) {
		t.Helper()
		for _, id := range ids {
			if p[id] != d {
				t.Fatalf("level %d: %s paused %v, want %v (all: %v)", level.Load(), id, p[id], d, p)
			}
		}
	}
	mids := []string{"mid0", "mid1", "mid2", "mid3"}
	lows := []string{"low0", "low1", "low2", "low3", "low4"}
	admitted := func() packet.ReasonCode {
		code, err := connectOnce(addr, fmt.Sprintf("new-%d", level.Load()), "", "")
		if err != nil {
			t.Fatal(err)
		}
		return code
	}

	p := settle(0)
	count(p, 0, append(append([]string{"loud"}, mids...), lows...)...)
	if code := admitted(); code != packet.Success {
		t.Fatalf("level 0: a new CONNECT answered %s", code)
	}
	p = settle(1)
	count(p, 100*time.Millisecond, "loud")
	count(p, 0, append(mids, lows...)...)
	if code := admitted(); code != packet.Success {
		t.Fatalf("level 1: a new CONNECT answered %s", code)
	}
	p = settle(2)
	count(p, 250*time.Millisecond, append([]string{"loud"}, mids...)...)
	count(p, 0, lows...)
	if code := admitted(); code != packet.Success {
		t.Fatalf("level 2: a new CONNECT answered %s", code)
	}
	p = settle(3)
	count(p, 500*time.Millisecond, append(append([]string{"loud"}, mids...), lows...)...)
	if code := admitted(); code != packet.ServerBusy {
		t.Fatalf("level 3: a new CONNECT answered %s, want Server busy", code)
	}
	for id := range rates {
		if f.srv.connected(id) == nil {
			t.Fatalf("%s's connection was closed while the node shed load", id)
		}
	}
	settle(0)
	count(pauses(), 0, "loud")
	if st := f.srv.overload.Stats(); st.MaxNodeLevel != 3 || st.MaxCluster != 3 || st.MaxClusterAt.IsZero() {
		t.Fatalf("the run's record: node %d, cluster %d at %v; want 3 and 3", st.MaxNodeLevel, st.MaxCluster, st.MaxClusterAt)
	}
}

// A node's level reaches its peers through the heartbeat.
func TestStressReachesThePeers(t *testing.T) {
	withHooks(t, Hooks{Stress: func(node string) uint8 {
		if node == "n1" {
			return 2
		}
		return 0
	}, OverloadInterval: 50 * time.Millisecond})
	nodes := startTrio(t, nil)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		seen := 0
		for _, p := range nodes["n2"].srv.Cluster().Peers() {
			if p.Name == "n1" && p.Stress == 2 {
				seen++
			}
		}
		if seen == 1 && nodes["n2"].srv.overload.Level() == 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("n2 sees n1's stress as %+v and enforces %d", nodes["n2"].srv.Cluster().Peers(), nodes["n2"].srv.overload.Level())
}

// putBan writes a ban through the node's records store and waits until the
// node's index holds it.
func putBan(t *testing.T, f *fixture, key string, e bans.Entry) {
	t.Helper()
	v, _ := json.Marshal(e)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := f.srv.records.Load().Put(ctx, bans.Collection, key, v, nil, "test", time.Now())
	if err != nil || !res.OK {
		t.Fatalf("ban %s: %+v %v", key, res, err)
	}
	waitIndexed(t, f, key, true)
}

// The ban index is compiled again for a write to the ban list only.
func TestBanIndexBuildsOnBanWrites(t *testing.T) {
	f := start(t, nil)
	p := f.srv.policy
	adminWait(t, "the first refresh", func() bool { return p.reads.Load() > 0 })
	built, read := p.rebuilds.Load(), p.reads.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if res, err := f.srv.records.Load().Put(ctx, "other", "k", json.RawMessage(`{"a":1}`), nil, "test", time.Now()); err != nil || !res.OK {
		t.Fatalf("a write to another collection: %+v %v", res, err)
	}
	adminWait(t, "the refresh after it", func() bool { return p.reads.Load() > read })
	if got := p.rebuilds.Load(); got != built {
		t.Fatalf("a write to another collection built the ban index (%d builds, was %d)", got, built)
	}
	putBan(t, f, "clientid:x", bans.Entry{Source: bans.SourceAdmin})
	adminWait(t, "the ban write's build", func() bool { return p.rebuilds.Load() > built })
	if got := p.rebuilds.Load(); got != built+1 {
		t.Fatalf("a ban write: %d builds, want %d", got, built+1)
	}
}

func waitIndexed(t *testing.T, f *fixture, key string, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		items := banItems(f.srv.records.Load())
		found := false
		for _, it := range items {
			found = found || it.Key == key
		}
		if found == want && f.srv.policy.rebuilds.Load() > 0 {
			time.Sleep(50 * time.Millisecond) // the watcher's rebuild follows the apply
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("ban %s present=%v not reached", key, want)
}

// connectAs connects over plain TCP at a level with the given identity and
// returns the CONNACK's reason.
func connectAs(t *testing.T, addr string, v packet.Version, id, user string) packet.ReasonCode {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	enc := &packet.Encoder{Version: v}
	dec := packet.NewDecoder(nc)
	dec.Version = v
	c := &packet.Connect{ProtocolLevel: v, ClientID: id, CleanStart: true}
	if user != "" {
		c.HasUsername, c.Username = true, user
	}
	if err := enc.Encode(nc, c); err != nil {
		t.Fatal(err)
	}
	p, err := dec.Read()
	if err != nil {
		t.Fatal(err)
	}
	return p.(*packet.Connack).ReasonCode
}

// A ban of each kind refuses a matching CONNECT: 0x8A at level 5, Not
// authorized at level 4; a CONNECT that matches none is admitted.
func TestBanRefusesEachKind(t *testing.T) {
	f := start(t, nil)
	addr := f.srv.Addr("tcp").String()
	for _, key := range []string{"clientid:b-id", "username:b-user", "clientid-regex:rx-[0-9]+", "username-regex:bot.*"} {
		putBan(t, f, key, bans.Entry{Source: bans.SourceAdmin})
	}
	for _, c := range []struct{ id, user string }{{"b-id", ""}, {"x1", "b-user"}, {"rx-12", ""}, {"x2", "botnet"}} {
		if code := connectAs(t, addr, packet.V5, c.id, c.user); code != packet.Banned {
			t.Errorf("%+v at level 5: %s, want Banned", c, code)
		}
		if code := connectAs(t, addr, packet.V4, c.id, c.user); code != packet.NotAuthorized {
			t.Errorf("%+v at level 4: %s, want Not authorized", c, code)
		}
	}
	if code := connectAs(t, addr, packet.V5, "fine", ""); code != packet.Success {
		t.Fatalf("a CONNECT no ban matches: %s", code)
	}
	// The address kinds: the test's clients connect from 127.0.0.1.
	putBan(t, f, "cidr:127.0.0.0/8", bans.Entry{Source: bans.SourceAdmin})
	if code := connectAs(t, addr, packet.V5, "fine", ""); code != packet.Banned {
		t.Fatalf("a client inside a banned prefix: %s", code)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := f.srv.records.Load().Delete(ctx, bans.Collection, "cidr:127.0.0.0/8", nil, "test", time.Now()); err != nil {
		t.Fatal(err)
	}
	waitIndexed(t, f, "cidr:127.0.0.0/8", false)
	putBan(t, f, "address:127.0.0.1", bans.Entry{Source: bans.SourceAdmin})
	if code := connectAs(t, addr, packet.V5, "fine", ""); code != packet.Banned {
		t.Fatalf("a client from a banned address: %s", code)
	}
}

// A ban refuses until its expiry and not after; the leader's sweeper then
// deletes the record.
func TestBanExpiresOnSchedule(t *testing.T) {
	withHooks(t, Hooks{BanSweepInterval: 100 * time.Millisecond})
	f := start(t, nil)
	addr := f.srv.Addr("tcp").String()
	until := time.Now().Add(1500 * time.Millisecond)
	putBan(t, f, "clientid:brief", bans.Entry{Source: bans.SourceAdmin, ExpiresAt: &until})
	if code := connectAs(t, addr, packet.V5, "brief", ""); code != packet.Banned {
		t.Fatalf("before its expiry: %s", code)
	}
	time.Sleep(time.Until(until) + 50*time.Millisecond)
	if code := connectAs(t, addr, packet.V5, "brief", ""); code != packet.Success {
		t.Fatalf("after its expiry: %s", code)
	}
	waitIndexed(t, f, "clientid:brief", false)
	if !hasEvent(auditEvents(t, f), "sweeper", "ban.delete", "clientid:brief", "deleted") {
		t.Fatal("the sweeper's delete is not audited")
	}
}

// hasEvent reports whether events hold one matching every given field.
func hasEvent(events []audit.Event, actor, action, target, outcome string) bool {
	for _, e := range events {
		if e.Actor == actor && e.Action == action && e.Target == target && e.Outcome == outcome {
			return true
		}
	}
	return false
}

// A ban written through one node refuses on the others once applied there.
func TestBanIsClusterWide(t *testing.T) {
	nodes := startTrio(t, nil)
	putBan(t, nodes["n1"], "clientid:everywhere", bans.Entry{Source: bans.SourceAdmin})
	for _, n := range []string{"n2", "n3"} {
		waitIndexed(t, nodes[n], "clientid:everywhere", true)
		if code := connectAs(t, nodes[n].srv.Addr("tcp").String(), packet.V5, "everywhere", ""); code != packet.Banned {
			t.Fatalf("%s: %s, want Banned", n, code)
		}
	}
}

func TestTripped(t *testing.T) {
	t0 := time.Now()
	w := 10 * time.Second
	two := []time.Time{t0, t0.Add(time.Second)}
	if tripped(two, t0.Add(2*time.Second), w, 3) {
		t.Fatal("two ends at a threshold of three")
	}
	three := append(two, t0.Add(2*time.Second))
	if !tripped(three, t0.Add(2*time.Second), w, 3) {
		t.Fatal("three ends within the window at a threshold of three")
	}
	spread := []time.Time{t0, t0.Add(6 * time.Second), t0.Add(12 * time.Second)}
	if tripped(spread, t0.Add(12*time.Second), w, 3) {
		t.Fatal("three ends spread over more than the window")
	}
}

// dropOnce connects and closes the socket without a DISCONNECT.
func dropOnce(t *testing.T, addr, id string) {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	enc := &packet.Encoder{Version: packet.V5}
	dec := packet.NewDecoder(nc)
	dec.Version = packet.V5
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	_ = enc.Encode(nc, &packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: true})
	if p, err := dec.Read(); err != nil || p.(*packet.Connack).ReasonCode != packet.Success {
		t.Fatalf("connect %s: %v %v", id, p, err)
	}
	_ = nc.Close()
}

// flaps waits for the node to have seen n abnormal ends.
func waitTrips(t *testing.T, f *fixture, n int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for f.srv.policy.flap.trips.Load() < n && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

// A client whose connections drop three times within the window is banned by
// the detector through the ban list, and admitted again once the ban lapses.
func TestFlappingTripsToABan(t *testing.T) {
	f := start(t, func(d *config.Document) {
		d.Flapping = config.Flapping{Enabled: true, WindowSeconds: 10, MaxCount: 3, BanSeconds: 2}
	})
	addr := f.srv.Addr("tcp").String()
	for i := 0; i < 3; i++ {
		dropOnce(t, addr, "flappy")
		time.Sleep(100 * time.Millisecond) // each end reaches the detector before the next connect
	}
	waitTrips(t, f, 1)
	waitIndexed(t, f, "clientid:flappy", true)
	rec, ok := f.srv.records.Load().Get(bans.Collection, "clientid:flappy")
	var e bans.Entry
	if !ok || json.Unmarshal(rec.Value, &e) != nil || e.Source != bans.SourceFlapping || e.ExpiresAt == nil {
		t.Fatalf("the verdict's record: %+v %s", ok, rec.Value)
	}
	if code := connectAs(t, addr, packet.V5, "flappy", ""); code != packet.Banned {
		t.Fatalf("a flapping client's next CONNECT: %s", code)
	}
	if !hasEvent(auditEvents(t, f), "flapping", "ban.create", "clientid:flappy", "written") {
		t.Fatal("the detector's ban is not audited")
	}
	time.Sleep(time.Until(*e.ExpiresAt) + 50*time.Millisecond)
	if code := connectAs(t, addr, packet.V5, "flappy", ""); code != packet.Success {
		t.Fatalf("after the ban lapsed: %s", code)
	}
}

// A client DISCONNECT, a refusal of the broker's own and a takeover are not
// flaps, however many.
func TestNotFlaps(t *testing.T) {
	f := start(t, func(d *config.Document) {
		d.Flapping = config.Flapping{Enabled: true, WindowSeconds: 30, MaxCount: 3, BanSeconds: 30}
		d.Listeners[0].ConnectRate, d.Listeners[0].ConnectBurst = 1, 1
	})
	addr := f.srv.Addr("tcp").String()
	for i := 0; i < 20; i++ {
		_, _ = connectOnce(addr, "polite", "", "") // DISCONNECTs after the CONNACK; most are refused for rate
	}
	// Five connections of one client, each taken over by the next; the client
	// closes the one taken over, as a real one does on DISCONNECT 0x8E.
	var prev *client
	for i := 0; i < 5; i++ {
		cl, ack := f.connect("tls", packet.V5, &packet.Connect{ClientID: "taken", CleanStart: true}, false)
		if ack.ReasonCode != packet.Success {
			t.Fatal(ack)
		}
		if prev != nil {
			_ = prev.c.Close()
		}
		prev = cl
	}
	// The ends must have reached the detector before its silence means
	// anything: one graceful end at least, and the four takeovers.
	flap := f.srv.policy.flap
	deadline := time.Now().Add(5 * time.Second)
	for (flap.ends[conn.EndedGraceful].Load() < 1 || flap.ends[conn.EndedByServer].Load() < 4) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if g, s := flap.ends[conn.EndedGraceful].Load(), flap.ends[conn.EndedByServer].Load(); g < 1 || s < 4 {
		t.Fatalf("precondition: the detector saw %d graceful and %d server ends, want at least 1 and 4", g, s)
	}
	if n := flap.trips.Load(); n != 0 {
		t.Fatalf("%d trips from graceful, refused and taken-over connections", n)
	}
	if _, ok := f.srv.records.Load().Get(bans.Collection, "clientid:polite"); ok {
		t.Fatal("a client that disconnects politely was banned")
	}
}

// With the detector off, no count of abnormal ends trips it.
func TestFlappingOff(t *testing.T) {
	f := start(t, func(d *config.Document) {
		d.Flapping = config.Flapping{Enabled: false, WindowSeconds: 10, MaxCount: 2, BanSeconds: 2}
	})
	for i := 0; i < 5; i++ {
		f.srv.policy.flap.ended(fakeEnded(t, f, "off"), conn.EndedAbnormal)
	}
	if n := f.srv.policy.flap.ends[conn.EndedAbnormal].Load(); n != 5 {
		t.Fatalf("precondition: %d abnormal ends seen, want 5", n)
	}
	if n := f.srv.policy.flap.trips.Load(); n != 0 {
		t.Fatalf("%d trips with the detector off", n)
	}
}

// A subscriber that stops reading does not hold a publisher: 500 QoS 1
// PUBLISHes are all acknowledged while it reads nothing, and once it reads
// it receives every one, in order, once.
func TestSlowSubscriberDoesNotBlockPublisher(t *testing.T) {
	f := start(t, func(d *config.Document) { d.Limits.MaxQueuedMessages = 1000 })
	addr := f.srv.Addr("tcp").String()
	dial := func(id string, v packet.Version) (net.Conn, *packet.Encoder, *packet.Decoder) {
		nc, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = nc.Close() })
		enc, dec := &packet.Encoder{Version: v}, packet.NewDecoder(nc)
		dec.Version = v
		_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
		recv := uint16(65535)
		_ = enc.Encode(nc, &packet.Connect{ProtocolLevel: v, ClientID: id, CleanStart: true, Properties: packet.Properties{ReceiveMaximum: &recv}})
		if p, err := dec.Read(); err != nil || p.(*packet.Connack).ReasonCode != packet.Success {
			t.Fatalf("%s: %v %v", id, p, err)
		}
		return nc, enc, dec
	}
	for _, qos := range []byte{1, 0} {
		t.Run(fmt.Sprintf("QoS %d", qos), func(t *testing.T) {
			snc, senc, sdec := dial(fmt.Sprintf("slow-sub-%d", qos), packet.V5)
			_ = senc.Encode(snc, &packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: fmt.Sprintf("slow/%d", qos), QoS: qos}}})
			if _, err := sdec.Read(); err != nil {
				t.Fatal(err)
			}
			pnc, penc, pdec := dial(fmt.Sprintf("fast-pub-%d", qos), packet.V5)
			// The subscriber reads nothing from here. 500 messages of 32 KiB
			// fill its socket buffers and its outbound queue many times over.
			began := time.Now()
			_ = pnc.SetDeadline(time.Now().Add(10 * time.Second))
			go func() {
				for i := 0; i < 500; i++ {
					if penc.Encode(pnc, &packet.Publish{Topic: fmt.Sprintf("slow/%d", qos), QoS: 1, PacketID: uint16(i + 1), Payload: append([]byte(fmt.Sprintf("%04d", i)), make([]byte, 32<<10)...)}) != nil {
						return
					}
				}
			}()
			for i := 0; i < 500; i++ {
				p, err := pdec.Read()
				if err != nil {
					t.Fatalf("PUBACK %d: %v after %v", i+1, err, time.Since(began))
				}
				if a, ok := p.(*packet.Puback); !ok || a.ReasonCode.IsError() {
					t.Fatalf("PUBACK %d: %+v", i+1, p)
				}
			}
			if took := time.Since(began); took > 5*time.Second {
				t.Fatalf("500 PUBACKs took %v with a subscriber reading nothing", took)
			}
			_ = snc.SetDeadline(time.Now().Add(10 * time.Second))
			for i := 0; i < 500; i++ {
				p, err := sdec.Read()
				if err != nil {
					t.Fatalf("delivery %d: %v", i, err)
				}
				pub, ok := p.(*packet.Publish)
				if !ok || string(pub.Payload[:4]) != fmt.Sprintf("%04d", i) {
					t.Fatalf("delivery %d: %+v, want message %04d", i, p, i)
				}
				if pub.QoS == 1 {
					_ = senc.Encode(snc, &packet.Puback{Ack: packet.Ack{PacketID: pub.PacketID}})
				}
			}
		})
	}
}

// The administered record lays over the configuration member by member.
func TestEffectiveLimits(t *testing.T) {
	cfg := config.Limits{ClientMessageRate: 10, ClientByteRate: 1000, ConnectRate: 5}
	one := 1
	e := effective(cfg, config.LimitsOverride{ClientMessageRate: &one})
	if e.ClientMessageRate != 1 || e.ClientByteRate != 1000 || e.ConnectRate != 5 {
		t.Fatalf("%+v", e)
	}
	if l := clientLimitsOf(e); l.MessageRate != 1 || l.MessageBurst != 2 || l.ByteRate != 1000 || l.ByteBurst != 2000 {
		t.Fatalf("client limits %+v", l)
	}
}

// A limits record written through the store is in force on the node at once:
// a client's next PUBLISHes are paced, and the node's connect rate applies.
func TestLimitsRecordApplies(t *testing.T) {
	f := start(t, nil)
	if l := f.srv.policy.ClientLimits("default"); l == nil || l.MessageRate != 0 {
		t.Fatalf("before: %+v", l)
	}
	one := 1
	v, _ := json.Marshal(config.LimitsOverride{ClientMessageRate: &one, ConnectRate: &one})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if res, err := f.srv.records.Load().Put(ctx, config.LimitsCollection, config.DefaultLimitsKey, v, nil, "test", time.Now()); err != nil || !res.OK {
		t.Fatalf("%+v %v", res, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if l := f.srv.policy.ClientLimits("default"); l != nil && l.MessageRate == 1 && f.srv.rateGate() != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("after: %+v, gate %v", f.srv.policy.ClientLimits("default"), f.srv.rateGate())
}

// A client's PUBLISHes are paced by the administered rate: at one a second
// with its burst of two, the fourth is read only after a second.
func TestLimitsRecordPacesAClient(t *testing.T) {
	f := start(t, nil)
	one := 1
	v, _ := json.Marshal(config.LimitsOverride{ClientMessageRate: &one})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if res, err := f.srv.records.Load().Put(ctx, config.LimitsCollection, config.DefaultLimitsKey, v, nil, "test", time.Now()); err != nil || !res.OK {
		t.Fatal(res, err)
	}
	for deadline := time.Now().Add(5 * time.Second); f.srv.policy.ClientLimits("default").MessageRate != 1; {
		if time.Now().After(deadline) {
			t.Fatal("the record never applied")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cl, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "paced", CleanStart: true}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatal(ack)
	}
	began := time.Now()
	for i := 0; i < 4; i++ {
		cl.send(&packet.Publish{Topic: "p", QoS: 1, PacketID: uint16(i + 1), Payload: []byte("x")})
	}
	_ = cl.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	for i := 0; i < 4; i++ {
		if p, err := cl.dec.Read(); err != nil {
			t.Fatalf("PUBACK %d: %v", i+1, err)
		} else if _, ok := p.(*packet.Puback); !ok {
			t.Fatalf("PUBACK %d: %+v", i+1, p)
		}
	}
	if took := time.Since(began); took < 900*time.Millisecond {
		t.Fatalf("four PUBLISHes at one a second with a burst of two were acknowledged in %v", took)
	}
}

// The detector writes only where no ban exists: an operator's ban on the
// same client stands as the operator wrote it.
func TestFlappingLeavesAnExistingBan(t *testing.T) {
	f := start(t, func(d *config.Document) {
		d.Flapping = config.Flapping{Enabled: true, WindowSeconds: 10, MaxCount: 2, BanSeconds: 2}
	})
	addr := f.srv.Addr("tcp").String()
	for i := 0; i < 2; i++ {
		dropOnce(t, addr, "flappy2")
		time.Sleep(100 * time.Millisecond)
	}
	waitTrips(t, f, 1)
	waitIndexed(t, f, "clientid:flappy2", true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, _ := json.Marshal(bans.Entry{Reason: "operator", Source: bans.SourceAdmin})
	if res, err := f.srv.records.Load().Put(ctx, bans.Collection, "clientid:flappy2", v, nil, "op", time.Now()); err != nil || !res.OK {
		t.Fatal(res, err)
	}
	// The flapping ban has been replaced by the operator's; flap again past
	// the detector's own ban time so it would propose once more.
	time.Sleep(2100 * time.Millisecond)
	before := f.srv.policy.flap.trips.Load()
	for i := 0; i < 2; i++ {
		f.srv.policy.flap.ended(fakeEnded(t, f, "flappy2"), conn.EndedAbnormal)
	}
	if f.srv.policy.flap.trips.Load() != before+1 {
		t.Fatalf("trips %d, want %d", f.srv.policy.flap.trips.Load(), before+1)
	}
	time.Sleep(300 * time.Millisecond)
	rec, _ := f.srv.records.Load().Get(bans.Collection, "clientid:flappy2")
	var e bans.Entry
	_ = json.Unmarshal(rec.Value, &e)
	if e.Source != bans.SourceAdmin || e.Reason != "operator" || e.ExpiresAt != nil {
		t.Fatalf("the operator's ban was overwritten: %+v", e)
	}
}

// fakeEnded is a connection whose client identifier is id, for driving the
// detector directly.
func fakeEnded(t *testing.T, f *fixture, id string) *conn.Conn {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	c := conn.New(a, "tcp", f.srv.Options(), f.srv.core)
	conn.SetClientIDForTest(c, id)
	return c
}

// A change of the administered limits keeps each listener's bucket where it
// was: a listener emptied before the change is still empty after it.
func TestLimitsChangeKeepsListenerBuckets(t *testing.T) {
	f := start(t, func(d *config.Document) { d.Listeners[0].ConnectRate, d.Listeners[0].ConnectBurst = 1, 1 })
	addr := f.srv.Addr("tcp").String()
	if code, _ := connectOnce(addr, "first", "", ""); code != packet.Success {
		t.Fatalf("the burst's CONNECT: %s", code)
	}
	began := time.Now()
	five := 5
	v, _ := json.Marshal(config.LimitsOverride{ClientMessageRate: &five})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if res, err := f.srv.records.Load().Put(ctx, config.LimitsCollection, config.DefaultLimitsKey, v, nil, "test", time.Now()); err != nil || !res.OK {
		t.Fatal(res, err)
	}
	for time.Now().Before(began.Add(3*time.Second)) && f.srv.policy.ClientLimits("default").MessageRate != 5 {
		time.Sleep(5 * time.Millisecond)
	}
	if time.Since(began) > 500*time.Millisecond {
		t.Fatalf("precondition: the change took %v, past the half second the listener's bucket stays empty", time.Since(began))
	}
	if code, _ := connectOnce(addr, "second", "", ""); code != packet.ConnectionRateExceeded {
		t.Fatalf("a CONNECT right after the change: %s, want the listener still empty (0x9F)", code)
	}
}
