package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
)

// VerifyCache keeps the password verifications that matched, for its TTL, so
// a client's repeated CONNECT derives no key. An entry
// is a MAC, under a secret made with the cache and kept nowhere else, of the
// stored credential and the password, and the time it lapses: never the
// password, the stored string or a derived key. The stored credential is in
// the key, so a changed credential is a new key and derives. Only a match is
// kept, so a client that knows no password can neither add an entry nor crowd
// one out. A nil cache verifies every time.
type VerifyCache struct {
	ttl    time.Duration
	max    int
	secret [32]byte
	now    func() time.Time

	mu        sync.Mutex
	entries   map[[32]byte]time.Time
	nextSweep time.Time

	derivations atomic.Int64
	hits        atomic.Int64
	size        atomic.Int64
}

// NewVerifyCache keeps a match for ttl, at most max at once; a ttl of 0
// keeps nothing and still counts the derivations.
func NewVerifyCache(ttl time.Duration, max int) *VerifyCache {
	return newVerifyCache(ttl, max, time.Now)
}

func newVerifyCache(ttl time.Duration, max int, now func() time.Time) *VerifyCache {
	c := &VerifyCache{ttl: ttl, max: max, now: now, entries: map[[32]byte]time.Time{}}
	if _, err := rand.Read(c.secret[:]); err != nil {
		panic("crypto/rand: " + err.Error())
	}
	return c
}

// cacheKey is the entry's key: HMAC-SHA256 under the cache's secret of the
// stored credential and the password, each length-prefixed.
func cacheKey(secret *[32]byte, stored string, password []byte) [32]byte {
	m := hmac.New(sha256.New, secret[:])
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(stored)))
	m.Write(n[:])
	m.Write([]byte(stored))
	binary.BigEndian.PutUint64(n[:], uint64(len(password)))
	m.Write(n[:])
	m.Write(password)
	var k [32]byte
	copy(k[:], m.Sum(nil))
	return k
}

// fresh is whether an entry lapsing at expires still answers at now.
func fresh(expires, now time.Time) bool { return now.Before(expires) }

// Verify is VerifyPassword, answered from the cache when this password
// matched this stored credential within the TTL. The derivation runs outside
// the cache's lock.
func (c *VerifyCache) Verify(stored string, password []byte) (bool, error) {
	if c == nil {
		return VerifyPassword(stored, password)
	}
	if c.ttl <= 0 {
		c.derivations.Add(1)
		return VerifyPassword(stored, password)
	}
	key := cacheKey(&c.secret, stored, password)
	c.mu.Lock()
	expires, ok := c.entries[key]
	hit := ok && fresh(expires, c.now())
	if ok && !hit {
		delete(c.entries, key)
		c.size.Store(int64(len(c.entries)))
	}
	c.mu.Unlock()
	if hit {
		c.hits.Add(1)
		return true, nil
	}
	c.derivations.Add(1)
	match, err := VerifyPassword(stored, password)
	if err != nil || !match {
		return match, err
	}
	c.keep(key)
	return true, nil
}

// keep stores a match until now + ttl. A full cache first drops what has
// lapsed, at most once a quarter TTL; still full, the match is not kept.
func (c *VerifyCache) keep(key [32]byte) {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.max && !now.Before(c.nextSweep) {
		for k, exp := range c.entries {
			if !fresh(exp, now) {
				delete(c.entries, k)
			}
		}
		c.nextSweep = now.Add(c.ttl / 4)
	}
	if len(c.entries) < c.max {
		c.entries[key] = now.Add(c.ttl)
	}
	c.size.Store(int64(len(c.entries)))
}

// VerifyStats is what the cache has done, read without its lock.
type VerifyStats struct {
	Derivations int64 // verifications that derived a key
	Hits        int64 // verifications answered from the cache
	Entries     int64 // matches held, lapsed ones not yet dropped included
}

// Stats reads the counters.
func (c *VerifyCache) Stats() VerifyStats {
	if c == nil {
		return VerifyStats{}
	}
	return VerifyStats{Derivations: c.derivations.Load(), Hits: c.hits.Load(), Entries: c.size.Load()}
}
