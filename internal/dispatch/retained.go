package dispatch

import (
	"sort"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/topic"
)

// retained holds the last retained message per topic, per namespace.
type retained struct {
	mu   sync.RWMutex
	byNS map[string]map[string]*message.Message
}

func newRetained() *retained { return &retained{byNS: map[string]map[string]*message.Message{}} }

// set stores a message or, for an empty payload, removes the entry.
func (r *retained) set(m *message.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	topics := r.byNS[m.Namespace]
	if len(m.Payload) == 0 {
		delete(topics, m.Topic)
		if len(topics) == 0 {
			delete(r.byNS, m.Namespace)
		}
		return
	}
	if topics == nil {
		topics = map[string]*message.Message{}
		r.byNS[m.Namespace] = topics
	}
	topics[m.Topic] = m
}

// matching returns the live retained messages a filter matches, in topic
// order. Expired entries are dropped as they are met.
func (r *retained) matching(ns, filter string, now time.Time) []*message.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	topics := r.byNS[ns]
	if len(topics) == 0 {
		return nil
	}
	if !topic.HasWildcard(filter) {
		m, ok := topics[filter]
		if !ok {
			return nil
		}
		if m.Expired(now) {
			delete(topics, filter)
			return nil
		}
		return []*message.Message{m}
	}
	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	sort.Strings(names)
	var out []*message.Message
	for _, name := range names {
		if !topic.Match(filter, name) {
			continue
		}
		m := topics[name]
		if m.Expired(now) {
			delete(topics, name)
			continue
		}
		out = append(out, m)
	}
	return out
}

// sweep drops every expired entry.
func (r *retained) sweep(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for ns, topics := range r.byNS {
		for name, m := range topics {
			if m.Expired(now) {
				delete(topics, name)
			}
		}
		if len(topics) == 0 {
			delete(r.byNS, ns)
		}
	}
}

// all lists every entry.
func (r *retained) all() []*message.Message {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*message.Message
	for _, topics := range r.byNS {
		for _, m := range topics {
			out = append(out, m)
		}
	}
	return out
}

// count is the number of retained messages, for tests and metrics.
// counts is the number of retained messages in each namespace holding one.
func (r *retained) counts() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]int, len(r.byNS))
	for ns, topics := range r.byNS {
		out[ns] = len(topics)
	}
	return out
}

func (r *retained) count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, topics := range r.byNS {
		n += len(topics)
	}
	return n
}
