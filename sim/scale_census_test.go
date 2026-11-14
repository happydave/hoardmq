package sim

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// censusBody is a node holding subs subscriptions, one a session, and
// advertising routes own routes.
func censusBody(subs, routes int) string {
	return fmt.Sprintf("# TYPE hoardmq_subscriptions_active gauge\nhoardmq_subscriptions_active{namespace=\"default\"} %d\nhoardmq_subscriptions_active{namespace=\"other\"} 3\nhoardmq_routes_active{scope=\"local\"} %d\nhoardmq_routes_active{scope=\"remote\"} 999\nhoardmq_sessions_active{namespace=\"default\"} %d\n", subs-3, routes, subs)
}

func TestParseRouteFigures(t *testing.T) {
	f, ok := parseRouteFigures(censusBody(556930, 556913))
	if !ok || f.Subscriptions != 556930 || f.OwnRoutes != 556913 {
		t.Fatalf("figures %+v ok %v: want 556930 subscriptions over two namespaces and 556913 own routes", f, ok)
	}
	if _, ok := parseRouteFigures("hoardmq_routes_active{scope=\"local\"} 4\n"); ok {
		t.Fatal("a body without the subscriptions gauge read as a census")
	}
}

func TestCensusGaps(t *testing.T) {
	fig := func(s, r float64) *routeFigures { return &routeFigures{Subscriptions: s, OwnRoutes: r} }
	for _, c := range []struct {
		name string
		in   []lockReading
		gaps []string
	}{
		{"every node agrees", []lockReading{{Node: "n1", Routes: fig(5, 5)}, {Node: "n3", Routes: fig(7, 7)}}, nil},
		{"one node holds 17 more than it advertises", []lockReading{{Node: "n1", Routes: fig(556930, 556913)}, {Node: "n3", Routes: fig(7, 7)}}, []string{"n1: subscriptions and own routes differ"}},
		{"a node advertising more than it holds", []lockReading{{Node: "n2", Routes: fig(4, 6)}}, []string{"n2: subscriptions and own routes differ"}},
		{"a node not read", []lockReading{{Node: "n1", Error: "refused"}, {Node: "n3", Routes: fig(7, 7)}}, []string{"n1: not read: refused"}},
		{"a node without the counters", []lockReading{{Node: "n1"}}, []string{"n1: no census counters"}},
		{"nothing read", nil, []string{": no node read"}},
	} {
		gaps := censusGaps(c.in)
		var got []string
		for _, g := range gaps {
			got = append(got, g.Node+": "+g.Reason)
		}
		if strings.Join(got, ",") != strings.Join(c.gaps, ",") {
			t.Errorf("%s: gaps %q, want %q", c.name, got, c.gaps)
		}
	}
}

// censusNode serves a /metrics body from body(n), n counting the requests.
func censusNode(t *testing.T, name string, body func(n int) string) scaleNode {
	t.Helper()
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body(int(n.Add(1)))))
	}))
	t.Cleanup(srv.Close)
	host, port, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := strconv.Atoi(port)
	return scaleNode{Name: name, Address: host, ObsPort: p}
}

func censusRun(t *testing.T, nodes ...scaleNode) *scaleRun {
	t.Helper()
	wait, pause := censusWait, censusPause
	censusWait, censusPause = 500*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { censusWait, censusPause = wait, pause })
	return &scaleRun{cfg: scaleConfig{Layout: scaleLayout{Nodes: nodes}}, rec: &scaleRecord{}, logf: t.Logf}
}

// TestScaleCensus: a gap that persists fails the run and is named in the
// record; one that clears within the census's bound passes.
func TestScaleCensus(t *testing.T) {
	steady := func(int) string { return censusBody(9, 9) }
	r := censusRun(t, censusNode(t, "n1", func(int) string { return censusBody(556930, 556913) }), censusNode(t, "n3", steady))
	note := r.census(context.Background())
	if len(r.failed) != 1 || !strings.Contains(r.failed[0], "n1: subscriptions and own routes differ (556930 subscriptions, 556913 own routes)") {
		t.Fatalf("a lasting gap: failed %q, note %q", r.failed, note)
	}
	c := r.rec.Census
	if c == nil || len(c.Gaps) != 1 || c.Gaps[0].Node != "n1" || c.Readings < 2 {
		t.Fatalf("the record names %+v", c)
	}

	r = censusRun(t, censusNode(t, "n1", func(n int) string {
		if n == 1 {
			return censusBody(10, 11)
		}
		return censusBody(10, 10)
	}), censusNode(t, "n3", steady))
	note = r.census(context.Background())
	if len(r.failed) != 0 || len(r.rec.Census.Gaps) != 0 || r.rec.Census.Readings != 2 {
		t.Fatalf("a gap that cleared: failed %q, note %q, census %+v", r.failed, note, r.rec.Census)
	}
}
