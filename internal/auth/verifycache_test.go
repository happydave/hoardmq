package auth

import (
	"bytes"
	"context"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// clock is a settable time for the cache.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newClock() *clock { return &clock{t: time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)} }

func pbkdf2Form(t testing.TB, pw string) string {
	t.Helper()
	s, err := hashPassword([]byte(pw), 1000)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func scramForm(t testing.TB, pw string) string {
	t.Helper()
	s, err := HashSCRAM([]byte(pw), MinSCRAMIterations)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func mustVerify(t *testing.T, c *VerifyCache, stored, pw string, want bool) {
	t.Helper()
	got, err := c.Verify(stored, []byte(pw))
	if err != nil || got != want {
		t.Fatalf("Verify(%q) = %v, %v; want %v", pw, got, err, want)
	}
}

func wantStats(t *testing.T, c *VerifyCache, derivations, hits, entries int64) {
	t.Helper()
	if s := c.Stats(); s.Derivations != derivations || s.Hits != hits || s.Entries != entries {
		t.Fatalf("stats %+v; want %d derivations, %d hits, %d entries", s, derivations, hits, entries)
	}
}

// A repeat against either stored form the authenticators accept is answered
// without a second derivation.
func TestVerifyCacheServesARepeat(t *testing.T) {
	for name, stored := range map[string]string{"pbkdf2": pbkdf2Form(t, "pw1"), "scram": scramForm(t, "pw1")} {
		t.Run(name, func(t *testing.T) {
			c := newVerifyCache(time.Minute, 10, newClock().now)
			mustVerify(t, c, stored, "pw1", true)
			mustVerify(t, c, stored, "pw1", true)
			wantStats(t, c, 1, 1, 1)
		})
	}
}

// A match answers until, and not at, its TTL.
func TestVerifyCacheTTL(t *testing.T) {
	clk := newClock()
	c := newVerifyCache(60*time.Second, 10, clk.now)
	stored := pbkdf2Form(t, "pw1")
	mustVerify(t, c, stored, "pw1", true)
	clk.add(60*time.Second - time.Millisecond)
	mustVerify(t, c, stored, "pw1", true)
	wantStats(t, c, 1, 1, 1)
	clk.add(time.Millisecond)
	mustVerify(t, c, stored, "pw1", true)
	wantStats(t, c, 2, 1, 1)
}

// A mismatch is not kept: the same wrong password derives again.
func TestVerifyCacheMismatchNotCached(t *testing.T) {
	c := newVerifyCache(time.Minute, 10, newClock().now)
	stored := pbkdf2Form(t, "pw1")
	mustVerify(t, c, stored, "wrong", false)
	mustVerify(t, c, stored, "wrong", false)
	wantStats(t, c, 2, 0, 0)
}

// The stored credential is in the key: a changed credential derives, and the
// old password is refused at once.
func TestVerifyCacheKeyCoversTheCredential(t *testing.T) {
	c := newVerifyCache(time.Hour, 10, newClock().now)
	s1 := pbkdf2Form(t, "pw1")
	mustVerify(t, c, s1, "pw1", true)
	s2 := pbkdf2Form(t, "pw2")
	mustVerify(t, c, s2, "pw1", false)
	mustVerify(t, c, s2, "pw2", true)
	wantStats(t, c, 3, 0, 2)
	// The same password under a new salt is a new credential too.
	s3 := pbkdf2Form(t, "pw1")
	mustVerify(t, c, s3, "pw1", true)
	wantStats(t, c, 4, 0, 3)
}

// A full cache keeps no more; once its entries lapse, a sweep makes room.
func TestVerifyCacheBound(t *testing.T) {
	clk := newClock()
	c := newVerifyCache(60*time.Second, 3, clk.now)
	var stored []string
	for _, pw := range []string{"a", "b", "c", "d"} {
		stored = append(stored, pbkdf2Form(t, pw))
	}
	for i, pw := range []string{"a", "b", "c"} {
		mustVerify(t, c, stored[i], pw, true)
	}
	mustVerify(t, c, stored[3], "d", true)
	wantStats(t, c, 4, 0, 3)
	mustVerify(t, c, stored[3], "d", true)
	wantStats(t, c, 5, 0, 3)
	clk.add(61 * time.Second)
	mustVerify(t, c, stored[3], "d", true)
	wantStats(t, c, 6, 0, 1)
	mustVerify(t, c, stored[3], "d", true)
	wantStats(t, c, 6, 1, 1)
}

// The cache holds a MAC key and a time per entry: not the password, a hash
// of it, the stored string or any key derived from it. Two caches given the
// same inputs make different keys, so a key is no function of the password.
func TestVerifyCacheRetainsNoSecret(t *testing.T) {
	const pw = "correct horse"
	pb, sc := pbkdf2Form(t, pw), scramForm(t, pw)
	c := newVerifyCache(time.Minute, 10, newClock().now)
	mustVerify(t, c, pb, pw, true)
	mustVerify(t, c, sc, pw, true)
	if len(c.entries) != 2 {
		t.Fatalf("the cache holds %d entries, want the 2 just verified", len(c.entries))
	}
	if got, want := reflect.TypeOf(c.entries), reflect.TypeOf(map[[32]byte]time.Time{}); got != want {
		t.Fatalf("entries are %v; an entry must be a MAC key and a time (%v)", got, want)
	}
	parts := strings.Split(pb, "$")
	pbHash, err := b64.DecodeString(parts[4])
	if err != nil {
		t.Fatal(err)
	}
	cred, err := ParseSCRAM(sc)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(pw))
	saltedPassword, err := pbkdf2.Key(sha256.New, pw, cred.Salt, cred.Iterations, sha256.Size)
	if err != nil {
		t.Fatal(err)
	}
	secrets := map[string][]byte{
		"the password": []byte(pw), "its SHA-256": digest[:], "the PBKDF2 stored string": []byte(pb),
		"the SCRAM stored string": []byte(sc), "the PBKDF2 hash": pbHash, "the SCRAM SaltedPassword": saltedPassword,
		"the StoredKey": cred.StoredKey, "the ServerKey": cred.ServerKey,
	}
	for k, exp := range c.entries {
		when, _ := exp.MarshalBinary()
		for what, b := range secrets {
			if bytes.Contains(k[:], b) || bytes.Contains(when, b) || bytes.Equal(k[:], b) {
				t.Errorf("an entry holds %s", what)
			}
		}
	}
	other := newVerifyCache(time.Minute, 10, newClock().now)
	if cacheKey(&c.secret, pb, []byte(pw)) == cacheKey(&other.secret, pb, []byte(pw)) {
		t.Error("two caches keyed the same credential and password alike: the key is a function of the password")
	}
}

// A TTL of 0 keeps nothing and still counts every derivation.
func TestVerifyCacheOff(t *testing.T) {
	c := newVerifyCache(0, 10, newClock().now)
	stored := pbkdf2Form(t, "pw1")
	for i := 0; i < 3; i++ {
		mustVerify(t, c, stored, "pw1", true)
	}
	wantStats(t, c, 3, 0, 0)
}

// Many goroutines verifying a few users: every answer right, and every
// verification either derived or was answered from the cache.
func TestVerifyCacheConcurrent(t *testing.T) {
	c := NewVerifyCache(time.Minute, 100)
	const users, workers, rounds = 16, 64, 50
	stored := make([]string, users)
	for i := range stored {
		stored[i] = pbkdf2Form(t, "pw"+string(rune('a'+i)))
	}
	var wg sync.WaitGroup
	var bad sync.Once
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				i := (w + r) % users
				if ok, err := c.Verify(stored[i], []byte("pw"+string(rune('a'+i)))); !ok || err != nil {
					bad.Do(func() { t.Errorf("user %d: %v %v", i, ok, err) })
				}
			}
		}(w)
	}
	wg.Wait()
	s := c.Stats()
	if s.Derivations < users || s.Derivations > users*workers || s.Derivations+s.Hits != workers*rounds || s.Entries != users {
		t.Fatalf("stats %+v: want %d to %d derivations, derivations + hits = %d, %d entries", s, users, users*workers, workers*rounds, users)
	}
}

func writeUsers(t *testing.T, path string, users ...fileUser) {
	t.Helper()
	b, err := json.Marshal(usersFile{Users: users})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func pwReq(user, pw string) *Request {
	return &Request{Username: user, HasUsername: true, Password: []byte(pw), HasPassword: true}
}

// A users file reloaded with a new password refuses the old one at once,
// inside the TTL; attributes come from the file, not the cache; a removed
// user passes.
func TestFileAuthenticatorCredentialChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "users.json")
	s1 := pbkdf2Form(t, "pw1")
	writeUsers(t, path, fileUser{Username: "alice", Password: s1, Attributes: map[string]string{"team": "blue"}})
	f, err := LoadUsersFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cache := newVerifyCache(time.Hour, 10, newClock().now)
	f.Cache = cache
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if r := f.Authenticate(ctx, pwReq("alice", "pw1")); r.Decision != Allow || r.Attributes["team"] != "blue" {
			t.Fatalf("pw1 attempt %d: %+v", i, r)
		}
	}
	wantStats(t, cache, 1, 1, 1)
	// The same stored password, new attributes: a hit, with the new attributes.
	writeUsers(t, path, fileUser{Username: "alice", Password: s1, Attributes: map[string]string{"team": "red"}})
	if err := f.Reload(); err != nil {
		t.Fatal(err)
	}
	if r := f.Authenticate(ctx, pwReq("alice", "pw1")); r.Decision != Allow || r.Attributes["team"] != "red" {
		t.Fatalf("after an attribute change: %+v, want allow with team red", r)
	}
	wantStats(t, cache, 1, 2, 1)
	writeUsers(t, path, fileUser{Username: "alice", Password: pbkdf2Form(t, "pw2")})
	if err := f.Reload(); err != nil {
		t.Fatal(err)
	}
	if r := f.Authenticate(ctx, pwReq("alice", "pw1")); r.Decision != Deny {
		t.Fatalf("the old password after a change: %+v, want deny", r)
	}
	if r := f.Authenticate(ctx, pwReq("alice", "pw2")); r.Decision != Allow {
		t.Fatalf("the new password: %+v, want allow", r)
	}
	writeUsers(t, path, fileUser{Username: "bob", Password: s1})
	if err := f.Reload(); err != nil {
		t.Fatal(err)
	}
	if r := f.Authenticate(ctx, pwReq("alice", "pw2")); r.Decision != Pass {
		t.Fatalf("a removed user: %+v, want pass", r)
	}
}

// The same for the in-broker database: a replaced record refuses the old
// password at once; a removed one passes.
func TestDBCredentialChange(t *testing.T) {
	var mu sync.Mutex
	records := map[string]StoredCredential{}
	put := func(user string, c StoredCredential) { mu.Lock(); records[user] = c; mu.Unlock() }
	dbCache := newVerifyCache(time.Hour, 10, newClock().now)
	db := &DBAuthenticator{Cache: dbCache, Lookup: func(user string) (json.RawMessage, bool, bool) {
		mu.Lock()
		defer mu.Unlock()
		c, ok := records[user]
		if !ok {
			return nil, false, true
		}
		b, _ := json.Marshal(c)
		return b, true, true
	}}
	ctx := context.Background()
	s1 := pbkdf2Form(t, "pw1")
	put("alice", StoredCredential{Password: s1, Attributes: map[string]string{"team": "blue"}})
	for i := 0; i < 2; i++ {
		if r := db.Authenticate(ctx, pwReq("alice", "pw1")); r.Decision != Allow {
			t.Fatalf("pw1 attempt %d: %+v", i, r)
		}
	}
	wantStats(t, dbCache, 1, 1, 1)
	put("alice", StoredCredential{Password: s1, Attributes: map[string]string{"team": "red"}})
	if r := db.Authenticate(ctx, pwReq("alice", "pw1")); r.Decision != Allow || r.Attributes["team"] != "red" {
		t.Fatalf("after an attribute change: %+v, want allow with team red", r)
	}
	put("alice", StoredCredential{Password: pbkdf2Form(t, "pw2")})
	if r := db.Authenticate(ctx, pwReq("alice", "pw1")); r.Decision != Deny {
		t.Fatalf("the old password after a change: %+v, want deny", r)
	}
	if r := db.Authenticate(ctx, pwReq("alice", "pw2")); r.Decision != Allow {
		t.Fatalf("the new password: %+v, want allow", r)
	}
	mu.Lock()
	delete(records, "alice")
	mu.Unlock()
	if r := db.Authenticate(ctx, pwReq("alice", "pw2")); r.Decision != Pass {
		t.Fatalf("a removed user: %+v, want pass", r)
	}
}
