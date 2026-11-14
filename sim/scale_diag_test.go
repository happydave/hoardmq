package sim

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestStormSeriesKeepsOneReadingASecond: of readings every 50 ms the series
// keeps the first, one a second after it, and the last.
func TestStormSeriesKeepsOneReadingASecond(t *testing.T) {
	crash := time.Now()
	var series []stormReading
	var last popSample
	for i := 0; i < 60; i++ {
		last = popSample{Online: 1000 + i, Recovered: 900 + i, At: crash.Add(time.Duration(i) * 50 * time.Millisecond)}
		series = appendReading(series, crash, last, false)
	}
	series = appendReading(series, crash, last, true)
	if len(series) != 4 {
		t.Fatalf("kept %d readings, want 4 (0 s, 1 s, 2 s and the last): %+v", len(series), series)
	}
	end := series[len(series)-1]
	if end.Online != 1059 || end.Recovered != 959 || end.AtSec < 2.94 || end.AtSec > 2.96 {
		t.Fatalf("the last reading kept is %+v, want the final sample at 2.95 s", end)
	}
}

// TestNotRecoveredNamesTheClients: the clients not recovered when the summary
// is taken are counted and named, and the names cross the driver's wire.
func TestNotRecoveredNamesTheClients(t *testing.T) {
	p := &localPopulation{all: []*Client{
		NewClient("f000001", []string{"127.0.0.1:1"}, 3600, 1),
		NewClient("f000002", []string{"127.0.0.1:1"}, 3600, 2),
	}}
	sum, err := p.finish()
	if err != nil {
		t.Fatal(err)
	}
	if sum.NotRecovered != 2 || len(sum.NotRecoveredSample) != 2 || sum.NotRecoveredSample[1] != "f000002" {
		t.Fatalf("not recovered %d, named %v; want both clients", sum.NotRecovered, sum.NotRecoveredSample)
	}
	got := summaryFromDriver(driverSummary{NotRecovered: 2, NotRecoveredSample: []string{"f000001", "f000002"}})
	if got.NotRecovered != 2 || len(got.NotRecoveredSample) != 2 {
		t.Fatalf("the conversion lost the clients not recovered: %d %v", got.NotRecovered, got.NotRecoveredSample)
	}
}

// TestProbeRefusalsGroupedByNodeReasonAndPair: each refused publish is placed
// by the node it went to, its reason and its pair, with its first and last.
func TestProbeRefusalsGroupedByNodeReasonAndPair(t *testing.T) {
	crash := time.Now()
	p := &probe{}
	p.noteRefusal("n3", "0x83", 4, crash.Add(2*time.Second))
	p.noteRefusal("n1", "0x83", 4, crash.Add(time.Second))
	p.noteRefusal("n1", "0x83", 4, crash.Add(5*time.Second))
	p.noteRefusal("n1", "0x97", 7, crash.Add(3*time.Second))
	g := refusalGroups(p.refusedBy, crash)
	if len(g) != 3 {
		t.Fatalf("%d groups, want 3: %+v", len(g), g)
	}
	if g[0].Node != "n1" || g[0].Code != "0x83" || g[0].Pair != 4 || g[0].Count != 2 || g[0].FirstSec != 1 || g[0].LastSec != 5 {
		t.Fatalf("first group %+v, want n1 0x83 pair 4: 2 refusals from 1 s to 5 s", g[0])
	}
	if g[2].Node != "n3" || g[2].Count != 1 {
		t.Fatalf("last group %+v, want n3's one refusal", g[2])
	}
}

// TestScaleScrapeSetting: the scrape interval defaults to 10 s, takes the
// setting, and refuses a value outside 1 to 60 s.
func TestScaleScrapeSetting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "layout.json")
	data, _ := json.Marshal(goodLayout())
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{scaleLayoutEnv: path, scaleClientsEnv: "1000000"}
	get := func(k string) string { return env[k] }
	if c, _, err := readScaleConfig(get); err != nil || c.scrapeEvery() != 10*time.Second {
		t.Fatalf("default scrape interval %v, %v", c.scrapeEvery(), err)
	}
	env[scaleScrapeEnv] = "30"
	if c, _, err := readScaleConfig(get); err != nil || c.scrapeEvery() != 30*time.Second {
		t.Fatalf("scrape interval 30: %v, %v", c.scrapeEvery(), err)
	}
	env[scaleScrapeEnv] = "0.5"
	if _, _, err := readScaleConfig(get); err == nil {
		t.Fatal("a half-second scrape interval accepted")
	}
}

// TestScaleIdleHoldSetting: the idle hold is unset by default, takes the
// setting, and refuses a value outside 1 to 3600 s.
func TestScaleIdleHoldSetting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "layout.json")
	data, _ := json.Marshal(goodLayout())
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{scaleLayoutEnv: path, scaleClientsEnv: "100000"}
	get := func(k string) string { return env[k] }
	if c, _, err := readScaleConfig(get); err != nil || c.IdleHoldSec != 0 {
		t.Fatalf("default idle hold %v, %v", c.IdleHoldSec, err)
	}
	env[scaleIdleHoldEnv] = "600"
	if c, _, err := readScaleConfig(get); err != nil || c.IdleHoldSec != 600 {
		t.Fatalf("idle hold 600: %v, %v", c.IdleHoldSec, err)
	}
	for _, bad := range []string{"0", "3601", "x"} {
		env[scaleIdleHoldEnv] = bad
		if _, _, err := readScaleConfig(get); err == nil {
			t.Fatalf("idle hold %q accepted", bad)
		}
	}
}

// TestHoldForLastsTheHold: a phase that ended at once still lasts the hold
// from its start, scraping as it waits; a hold already passed takes no
// scrape and returns at once.
func TestHoldForLastsTheHold(t *testing.T) {
	start := time.Now()
	n := holdFor(context.Background(), start, 300*time.Millisecond, 100*time.Millisecond, func() {})
	if took := time.Since(start); took < 300*time.Millisecond || n < 3 {
		t.Fatalf("held %v with %d scrapes, want at least 300ms and 3", took, n)
	}
	again := time.Now()
	if n := holdFor(context.Background(), start, 300*time.Millisecond, 100*time.Millisecond, func() {}); n != 0 || time.Since(again) > 50*time.Millisecond {
		t.Fatalf("a passed hold took %d scrapes and %v", n, time.Since(again))
	}
}

// TestScaleDriverKeepAliveSetting: the clients' TCP keepalive is Go's by
// default, takes "off", and refuses anything else.
func TestScaleDriverKeepAliveSetting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "layout.json")
	data, _ := json.Marshal(goodLayout())
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{scaleLayoutEnv: path, scaleClientsEnv: "100000"}
	get := func(k string) string { return env[k] }
	if c, _, err := readScaleConfig(get); err != nil || c.DriverTCPKeepAlive != "default" {
		t.Fatalf("default keepalive %q, %v", c.DriverTCPKeepAlive, err)
	}
	env[scaleKeepAliveEnv] = "off"
	if c, _, err := readScaleConfig(get); err != nil || c.DriverTCPKeepAlive != "off" {
		t.Fatalf("keepalive off: %q, %v", c.DriverTCPKeepAlive, err)
	}
	for _, bad := range []string{"on", "15", "OFF"} {
		env[scaleKeepAliveEnv] = bad
		if _, _, err := readScaleConfig(get); err == nil {
			t.Fatalf("keepalive %q accepted", bad)
		}
	}
}

// TestDriverKeepAliveProblem: a run refuses a driver that applies another
// keepalive than it asked for.
func TestDriverKeepAliveProblem(t *testing.T) {
	if p := driverKeepAliveProblem("off", DriverInfo{TCPKeepAlive: "off"}); p != "" {
		t.Fatalf("matching keepalive refused: %s", p)
	}
	for _, info := range []DriverInfo{{TCPKeepAlive: "default"}, {}} {
		if p := driverKeepAliveProblem("off", info); p == "" {
			t.Fatalf("a driver applying %q passed a run asking for off", info.TCPKeepAlive)
		}
	}
}

// TestPopulationClientsTakeKeepAlive: every client the fill makes carries the
// population's keepalive setting.
func TestPopulationClientsTakeKeepAlive(t *testing.T) {
	for _, off := range []bool{false, true} {
		p := newLocalPopulation(3, []string{"127.0.0.1:1"}, nil, nil)
		p.keepAliveOff = off
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var completed atomic.Int64
		if _, err := p.fill(ctx, &completed); err != nil {
			t.Fatal(err)
		}
		all := p.clientsSnapshot()
		for _, cl := range all {
			cl.Close()
		}
		if len(all) != 3 {
			t.Fatalf("%d clients, want 3", len(all))
		}
		for _, cl := range all {
			if cl.KeepAliveOff != off {
				t.Fatalf("client %s keepalive off %v, want %v", cl.ID, cl.KeepAliveOff, off)
			}
		}
	}
}

// TestHelloCarriesKeepAlive: the hello asks the driver for the keepalive, and
// the driver reports the one it applies.
func TestHelloCarriesKeepAlive(t *testing.T) {
	c := New(t, Options{})
	d := startPipedDriver(t, c, 10, testDriverOptions())
	t.Cleanup(d.pop.close)
	d.pop.settings.TCPKeepAliveOff = true
	info, err := d.pop.hello()
	if err != nil {
		t.Fatal(err)
	}
	if info.TCPKeepAlive != "off" {
		t.Fatalf("the driver applies keepalive %q, want off", info.TCPKeepAlive)
	}
}
