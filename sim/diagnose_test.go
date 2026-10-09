package sim

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// refusalDiagnosis is what a refused publish has to carry for the refusal to
// be diagnosable afterwards: whether the cluster was still in a state where a
// publish is taken at that moment, what each node saw of its peers, the
// barriers that failed by peer and reason, and how long the refused publish
// took. A reason code alone cannot tell a state that changed since `New`
// from a refusal with every predicate still holding, and it was the second
// that turned out to be happening (WI 1661).
func refusalDiagnosis(c *Cluster, took time.Duration) string {
	var b strings.Builder
	serving := c.notServing()
	if serving == "" {
		serving = "the cluster was still in a state where a publish is taken"
	}
	fmt.Fprintf(&b, "refused after %s; %s", took.Round(time.Millisecond), serving)
	// The broker's own reasons, which no predicate over the cluster's state
	// can supply: which barrier could not make progress, and what every
	// refused forward was refused for.
	if failures := barrierFailuresByNode(c); len(failures) > 0 {
		nodes := make([]string, 0, len(failures))
		for name := range failures {
			nodes = append(nodes, name)
		}
		sort.Strings(nodes)
		for _, name := range nodes {
			if failures[name] == "" {
				continue
			}
			fmt.Fprintf(&b, "; %s barriers %s", name, failures[name])
		}
	}
	if reasons := refusalsByReason(c); len(reasons) > 0 {
		keys := make([]string, 0, len(reasons))
		for k := range reasons {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s=%d", k, reasons[k]))
		}
		fmt.Fprintf(&b, "; forwards refused %s", strings.Join(parts, ","))
	}
	for _, n := range c.Running() {
		s := n.Server()
		if s == nil {
			continue
		}
		peers := make([]string, 0, 2)
		for _, p := range s.Cluster().Peers() {
			peers = append(peers, p.Name+"="+p.State.String())
		}
		fmt.Fprintf(&b, "; %s sees %s", n.Name, strings.Join(peers, ","))
		if holed := s.Replication().HoledStreams(); len(holed) > 0 {
			fmt.Fprintf(&b, ", holed to %v", holed)
		}
	}
	return b.String()
}

// TestRefusalDiagnosisNamesWhatChanged: the diagnosis is only worth carrying
// if it distinguishes a cluster that stopped being able to take a publish
// from one that refused while it could. It is asserted in that order — the
// healthy reading first, so the second assertion cannot pass by the string
// being empty.
func TestRefusalDiagnosisNamesWhatChanged(t *testing.T) {
	c := New(t, Options{})
	healthy := refusalDiagnosis(c, 1500*time.Millisecond)
	if !strings.Contains(healthy, "refused after 1.5s") {
		t.Errorf("the diagnosis does not say how long the publish took: %q", healthy)
	}
	if !strings.Contains(healthy, "still in a state where a publish is taken") {
		t.Errorf("on a healthy cluster the diagnosis reads %q; it must say every predicate held, which is the finding", healthy)
	}
	if !strings.Contains(healthy, "n1 sees") || !strings.Contains(healthy, "=up") {
		t.Errorf("the diagnosis does not carry the peer states: %q", healthy)
	}

	c.Crash(c.Nodes[2])
	changed := refusalDiagnosis(c, time.Second)
	if strings.Contains(changed, "still in a state where a publish is taken") {
		t.Errorf("with %s gone the diagnosis still reads healthy: %q", c.Nodes[2].Name, changed)
	}
}
