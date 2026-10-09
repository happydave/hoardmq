package sim

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestStaleRouteAfterOwnerRestart drains the node holding a durable
// subscriber, so the session moves, and then restarts that node. Once the
// cluster has settled, every node's route table must name exactly the node
// that holds the subscriber. A table that still names the restarted node
// sends every publish for that topic to a node with no subscriber, where the
// forward fails, the routing barrier fails, and the publish is refused —
// indefinitely, because the only node that could withdraw the entry no longer
// knows it published one.
func TestStaleRouteAfterOwnerRestart(t *testing.T) {
	c := New(t, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	addrs := make([]string, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		addrs = append(addrs, n.TCP)
	}
	const topic = "routes/t"

	sub := NewClient("sub-routes", addrs, 600, 1)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.Subscribe(ctx, topic, 1); err != nil {
		t.Fatal(err)
	}

	owner := c.NodeAt(sub.Node())
	if owner == nil {
		t.Fatalf("the subscriber is not attached to a known node (%q)", sub.Node())
	}
	if err := waitStableRoutes(ctx, c, topic, owner.Name); err != nil {
		t.Fatalf("routes did not converge before the drain: %v", err)
	}

	// The session moves off its node, and the node it came from returns.
	c.Drain(owner)
	if err := waitMoved(ctx, sub, owner.Name, c); err != nil {
		t.Fatalf("the subscriber did not move off %s: %v", owner.Name, err)
	}
	c.Start(owner)

	moved := c.NodeAt(sub.Node())
	if moved == nil {
		t.Fatalf("the subscriber is not attached to a known node after the move (%q)", sub.Node())
	}
	t.Logf("subscriber moved %s -> %s", owner.Name, moved.Name)

	if err := waitStableRoutes(ctx, c, topic, moved.Name); err != nil {
		t.Fatalf("stale route after the owner restarted: %v", err)
	}
}

// waitStableRoutes waits until every running node's route table names, for
// the topic, exactly the holder — no more and no less — and then holds that
// for a settle window so a table that is merely slow is not mistaken for one
// that is correct.
func waitStableRoutes(ctx context.Context, c *Cluster, topic, holder string) error {
	const settle = 3 * time.Second
	var stableSince time.Time
	var last string
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w (last disagreement: %s)", err, last)
		}
		last = routeDisagreement(c, topic, holder)
		if last == "" {
			if stableSince.IsZero() {
				stableSince = time.Now()
			}
			if time.Since(stableSince) >= settle {
				return nil
			}
		} else {
			stableSince = time.Time{}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// routeDisagreement returns "" when every running node's table names exactly
// the holder for the topic, or a description of the first disagreement. A
// node's own table reports remote holders only, so the holder itself must
// name nobody.
func routeDisagreement(c *Cluster, topic, holder string) string {
	for _, n := range c.Running() {
		srv := n.Server()
		if srv == nil || srv.Routes() == nil {
			continue
		}
		nodes, _ := srv.Routes().Table().Match("default", topic)
		got := append([]string(nil), nodes...)
		sort.Strings(got)
		var want []string
		if n.Name != holder {
			want = []string{holder}
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			return fmt.Sprintf("%s names [%s] for %q, holder is %s", n.Name, strings.Join(got, ","), topic, holder)
		}
	}
	return ""
}

// waitMoved waits until the subscriber is attached to a node other than the
// one named, which is what the drain's redirect should produce.
func waitMoved(ctx context.Context, sub *Client, from string, c *Cluster) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if at := c.NodeAt(sub.Node()); at != nil && at.Name != from {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}
