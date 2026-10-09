package lease

import (
	"hash/fnv"
	"sync"
)

// Tokens is the highest grant token this node has seen for each client, from
// any source: its lease table, the result of its own acquire, a replication
// frame, a takeover request. A component deciding whether a holder is current
// compares against this rather than against the lease table, whose view lags
// the grant by design — a node is told its acquire succeeded before its own
// table has applied it, and a follower hears from a new owner before its table
// has. The highest wins, whichever path delivered it. It is read on the
// replication and takeover paths, so it takes no lock of the lease table's.
type Tokens struct {
	shards [tokenShards]tokenShard
}

const tokenShards = 64

type tokenShard struct {
	mu sync.Mutex
	m  map[key]uint64
}

// NewTokens returns an empty registry.
func NewTokens() *Tokens {
	t := &Tokens{}
	for i := range t.shards {
		t.shards[i].m = map[key]uint64{}
	}
	return t
}

func (t *Tokens) shard(k key) *tokenShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(k.ns))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(k.id))
	return &t.shards[h.Sum32()%tokenShards]
}

// Note records a token seen for a client; a lower one than is held changes
// nothing. Zero is no token.
func (t *Tokens) Note(ns, id string, token uint64) {
	if t == nil || token == 0 {
		return
	}
	k := key{ns, id}
	sh := t.shard(k)
	sh.mu.Lock()
	if token > sh.m[k] {
		sh.m[k] = token
	}
	sh.mu.Unlock()
}

// Seen is the highest token seen for a client; zero when none has been.
func (t *Tokens) Seen(ns, id string) uint64 {
	if t == nil {
		return 0
	}
	k := key{ns, id}
	sh := t.shard(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.m[k]
}

// released forgets a client whose lease was released at this token or a
// higher one. A release below what has been seen is an old holder's, and the
// entry stays.
func (t *Tokens) released(ns, id string, token uint64) {
	if t == nil {
		return
	}
	k := key{ns, id}
	sh := t.shard(k)
	sh.mu.Lock()
	if token >= sh.m[k] {
		delete(sh.m, k)
	}
	sh.mu.Unlock()
}

// Len is the number of clients held, for tests and a gauge.
func (t *Tokens) Len() int {
	n := 0
	for i := range t.shards {
		t.shards[i].mu.Lock()
		n += len(t.shards[i].m)
		t.shards[i].mu.Unlock()
	}
	return n
}
