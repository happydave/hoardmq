package broker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/admin"
	"github.com/happydave/hoardmq/internal/config"
)

// TestKeysReady: the decision a node makes before authenticating an API key.
func TestKeysReady(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name    string
		index   uint64
		at      time.Time
		applied uint64
		ready   bool
		says    string
	}{
		{"never renewed", 0, time.Time{}, 50, false, "no renewal"},
		{"renewed, not applied through it", 40, now.Add(-time.Second), 39, false, "short of its last renewal at 40"},
		{"renewed and applied, within the bound", 40, now.Add(-time.Second), 40, true, ""},
		{"renewed and applied past it", 40, now.Add(-time.Second), 55, true, ""},
		{"renewed longer ago than the bound", 40, now.Add(-16 * time.Second), 55, false, "more than 15s"},
	} {
		ready, why := keysReady(c.index, c.at, now, c.applied, 15*time.Second)
		if ready != c.ready || !strings.Contains(why, c.says) {
			t.Errorf("%s: %v %q", c.name, ready, why)
		}
	}
}

// keyTrio starts three nodes with the admin API.
func keyTrio(t *testing.T, mutate func(name string, d *config.Document)) map[string]*fixture {
	t.Helper()
	dir := t.TempDir()
	nodes := startTrio(t, func(name string, d *config.Document) {
		withAdmin(t, dir)(d)
		d.Cluster.Shards = 8
		d.Cluster.Readiness.StableMs = 0
		if mutate != nil {
			mutate(name, d)
		}
	})
	waitMap(t, nodes, 3)
	return nodes
}

// createKeyOn makes a key on a node with the operator's token.
func createKeyOn(t *testing.T, f *fixture) admin.CreatedAPIKey {
	t.Helper()
	code, data := api(t, f, "POST", "/api/v1/apikeys", adminOps, `{"name":"ci","scopes":["clients:read"]}`)
	if code != 201 {
		t.Fatalf("create: %d %s", code, data)
	}
	return decode[admin.CreatedAPIKey](t, data)
}

// keyWorks reports whether a node authenticates key.
func keyWorks(t *testing.T, f *fixture, key string) bool {
	t.Helper()
	code, _ := api(t, f, "GET", "/api/v1/whoami", key, "")
	return code == 200
}

// TestAPIKeyWorksOnEveryNodeAndItsRevocationEverywhere: a key made on n1
// authenticates on n2 and n3 once they have applied it; revoked on n2, the
// 200 comes only once no node takes it, so each refuses it straight after.
func TestAPIKeyWorksOnEveryNodeAndItsRevocationEverywhere(t *testing.T) {
	nodes := keyTrio(t, nil)
	k := createKeyOn(t, nodes["n1"])
	for _, n := range []string{"n1", "n2", "n3"} {
		adminWait(t, n+" to take the key", func() bool { return keyWorks(t, nodes[n], k.Key) })
	}
	code, data := api(t, nodes["n3"], "GET", "/api/v1/apikeys", adminOps, "")
	if keys := decode[[]admin.APIKey](t, data); code != 200 || len(keys) != 1 || keys[0].ID != k.ID || strings.Contains(string(data), k.Key) {
		t.Fatalf("n3's list: %d %s", code, data)
	}
	if code, data := api(t, nodes["n2"], "DELETE", "/api/v1/apikeys/"+k.ID, adminOps, ""); code != 200 {
		t.Fatalf("revoke on n2: %d %s", code, data)
	}
	for _, n := range []string{"n1", "n2", "n3"} {
		if keyWorks(t, nodes[n], k.Key) {
			t.Errorf("%s took the key after its revocation was acknowledged", n)
		}
	}
}

// TestAPIKeyWorksPastTheLastRecordsWrite: a renewal applies above the
// records component's last entry, and the node still takes the key: a
// renewal's index is compared with everything the node has applied, not
// with the records alone, which advance only on their own writes.
func TestAPIKeyWorksPastTheLastRecordsWrite(t *testing.T) {
	nodes := keyTrio(t, nil)
	k := createKeyOn(t, nodes["n1"])
	n2 := nodes["n2"]
	adminWait(t, "n2 to take the key", func() bool { return keyWorks(t, n2, k.Key) })
	adminWait(t, "a renewal of n2's applied past the records' last entry", func() bool {
		index, _ := n2.srv.leases.Contact()
		return index > n2.srv.records.Load().Applied() && n2.srv.leases.Applied() >= index
	})
	if !keyWorks(t, n2, k.Key) {
		ready, why := adminBackend{s: n2.srv}.KeysReady()
		t.Fatalf("n2 refused the key after a renewal past the records' last entry: ready %v, %s", ready, why)
	}
}

// TestAPIKeyRevocationWaitsForALiveNode: a revocation that n3, alive by the
// log, has not confirmed answers 503 naming it; once n3 answers, revoking
// again answers 200.
func TestAPIKeyRevocationWaitsForALiveNode(t *testing.T) {
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var holding sync.Mutex
	held := true
	fixtureHooks = Hooks{AdminWriteTimeout: time.Second, AdminAppliedGate: func(node string) {
		holding.Lock()
		h := held
		holding.Unlock()
		if node == "n3" && h {
			<-release
		}
	}}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes := keyTrio(t, nil)
	t.Cleanup(unblock)
	k := createKeyOn(t, nodes["n1"])
	adminWait(t, "n3 to take the key", func() bool { return keyWorks(t, nodes["n3"], k.Key) })
	code, data := api(t, nodes["n1"], "DELETE", "/api/v1/apikeys/"+k.ID, adminOps, "")
	if p := decode[admin.Problem](t, data); code != 503 || !strings.Contains(p.Detail, "n3") || strings.Contains(p.Detail, "n2") {
		t.Fatalf("revoke with n3 held: %d %s", code, data)
	}
	holding.Lock()
	held = false
	holding.Unlock()
	unblock()
	if code, data := api(t, nodes["n1"], "DELETE", "/api/v1/apikeys/"+k.ID, adminOps, ""); code != 200 {
		t.Fatalf("revoke again: %d %s", code, data)
	}
}

// TestAPIKeyRevocationSkipsADeadNode: with n3 crashed and declared dead the
// revocation answers 200 without it; restarted, n3 refuses the revoked key
// and takes a live one once caught up.
func TestAPIKeyRevocationSkipsADeadNode(t *testing.T) {
	nodes := keyTrio(t, nil)
	n1 := nodes["n1"]
	revoked, live := createKeyOn(t, n1), createKeyOn(t, n1)
	for _, n := range []string{"n2", "n3"} {
		adminWait(t, n+" to take the keys", func() bool { return keyWorks(t, nodes[n], revoked.Key) && keyWorks(t, nodes[n], live.Key) })
	}
	doc := *nodes["n3"].srv.config()
	nodes["n3"].srv.Crash()
	adminWait(t, "n1's table to declare n3 dead", func() bool { return !n1.srv.Leases().Table().Live("n3") })
	if code, data := api(t, n1, "DELETE", "/api/v1/apikeys/"+revoked.ID, adminOps, ""); code != 200 {
		t.Fatalf("revoke with n3 dead: %d %s", code, data)
	}
	srv, err := NewWithHooks(doc, nil, fixtureHooks)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	n3 := &fixture{t: t, srv: srv}
	t.Cleanup(n3.stop)
	adminWait(t, "the restarted n3 to take the live key", func() bool { return keyWorks(t, n3, live.Key) })
	if keyWorks(t, n3, revoked.Key) {
		t.Fatal("the restarted n3 took the revoked key")
	}
}

// TestAPIKeysImportedFromANodesFile: a node started with a key file from a
// build that kept keys per node puts them into the cluster's collection, so
// its live key works on another node and its revoked key on none.
func TestAPIKeysImportedFromANodesFile(t *testing.T) {
	liveID, liveKey := "0123456789ab", "hmq_0123456789ab_"+strings.Repeat("A", 43)
	goneID, goneKey := "ba9876543210", "hmq_ba9876543210_"+strings.Repeat("B", 43)
	when := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	digest := func(k string) string { return keyDigestForTest(k) }
	file := map[string]any{"version": 1, "keys": []map[string]any{
		{"id": liveID, "name": "live", "scopes": []string{"clients:read"}, "createdBy": "token:ops", "createdAt": when, "digest": digest(liveKey)},
		{"id": goneID, "name": "gone", "scopes": []string{"clients:read"}, "createdBy": "token:ops", "createdAt": when, "revokedAt": when, "revokedBy": "token:ops", "digest": digest(goneKey)},
	}}
	data, _ := json.Marshal(file)
	nodes := keyTrio(t, func(name string, d *config.Document) {
		if name != "n1" {
			return
		}
		dir := filepath.Join(d.Node.DataDir, "admin")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "apikeys.json"), data, 0o600); err != nil {
			t.Fatal(err)
		}
	})
	adminWait(t, "n2 to take n1's imported key", func() bool { return keyWorks(t, nodes["n2"], liveKey) })
	for _, n := range []string{"n1", "n2", "n3"} {
		if keyWorks(t, nodes[n], goneKey) {
			t.Errorf("%s took the key n1's file holds revoked", n)
		}
	}
}

// keyDigestForTest is the digest a key file holds: SHA-256 of the key, hex.
func keyDigestForTest(key string) string {
	d := sha256.Sum256([]byte(key))
	return hex.EncodeToString(d[:])
}
