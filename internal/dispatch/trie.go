// Package dispatch matches messages to subscribers: the subscription trie
// with the specification's wildcard and $-topic rules, the retained store,
// and shared subscription groups with their distribution strategies. It
// implements the session package's Router.
package dispatch

import (
	"sort"
	"strings"

	"github.com/happydave/hoardmq/internal/session"
)

// node is one topic level in the trie. Children are keyed by level, with
// "+" and "#" as ordinary keys; subs holds the sessions subscribed to the
// filter that ends here; groups holds shared groups whose inner filter ends
// here, keyed by share name.
type node struct {
	children map[string]*node
	subs     map[*session.Session]session.Subscription
	groups   map[string]*group
}

func newNode() *node { return &node{children: map[string]*node{}} }

func (n *node) empty() bool {
	return len(n.children) == 0 && len(n.subs) == 0 && len(n.groups) == 0
}

// walk descends to the node for a filter, creating nodes when create is set.
func (n *node) walk(filter string, create bool) *node {
	cur := n
	for _, level := range strings.Split(filter, "/") {
		next, ok := cur.children[level]
		if !ok {
			if !create {
				return nil
			}
			next = newNode()
			cur.children[level] = next
		}
		cur = next
	}
	return cur
}

// prune removes empty nodes along a filter's path.
func (n *node) prune(filter string) {
	levels := strings.Split(filter, "/")
	var path []*node
	cur := n
	for _, level := range levels {
		next, ok := cur.children[level]
		if !ok {
			return
		}
		path = append(path, cur)
		cur = next
	}
	for i := len(levels) - 1; i >= 0; i-- {
		if !cur.empty() {
			return
		}
		delete(path[i].children, levels[i])
		cur = path[i]
	}
}

// hit is one subscription or group reached by a topic.
type hit struct {
	n      *node
	filter string
}

// match collects the nodes a topic reaches. A filter beginning with a
// wildcard does not match a topic beginning with '$'.
func (n *node) match(topic string) []hit {
	levels := strings.Split(topic, "/")
	var out []hit
	var rec func(cur *node, i int, prefix []string, dollarRoot bool)
	rec = func(cur *node, i int, prefix []string, dollarRoot bool) {
		if i == len(levels) {
			if len(cur.subs) > 0 || len(cur.groups) > 0 {
				out = append(out, hit{n: cur, filter: strings.Join(prefix, "/")})
			}
			// "a/#" also matches "a".
			if h, ok := cur.children["#"]; ok && (!dollarRoot || i != 0) {
				out = append(out, hit{n: h, filter: strings.Join(append(prefix, "#"), "/")})
			}
			return
		}
		level := levels[i]
		if next, ok := cur.children[level]; ok {
			rec(next, i+1, append(prefix, level), dollarRoot)
		}
		if dollarRoot && i == 0 {
			return
		}
		if next, ok := cur.children["+"]; ok {
			rec(next, i+1, append(prefix, "+"), dollarRoot)
		}
		if h, ok := cur.children["#"]; ok {
			out = append(out, hit{n: h, filter: strings.Join(append(prefix, "#"), "/")})
		}
	}
	rec(n, 0, nil, strings.HasPrefix(topic, "$"))
	sort.Slice(out, func(i, j int) bool { return out[i].filter < out[j].filter })
	return out
}
