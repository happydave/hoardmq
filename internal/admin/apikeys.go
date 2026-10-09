package admin

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/records"
)

// An API key is apiKeyPrefix, a key identifier of apiKeyIDLen lowercase hex
// characters, "_", and a secret of 32 random bytes in unpadded base64url.
// Its digest is the SHA-256 of the whole key; the store holds nothing else
// derived from it.
const (
	apiKeyPrefix    = "hmq_"
	apiKeyIDLen     = 12
	apiKeySecretLen = 43
	apiKeyFile      = "apikeys.json"
)

// newAPIKey returns a fresh key identifier and key.
func newAPIKey() (id, key string) {
	idb := make([]byte, apiKeyIDLen/2)
	secret := make([]byte, 32)
	_, _ = rand.Read(idb)
	_, _ = rand.Read(secret)
	id = hex.EncodeToString(idb)
	return id, apiKeyPrefix + id + "_" + base64.RawURLEncoding.EncodeToString(secret)
}

// apiKeyID returns the identifier of a value shaped as an API key.
func apiKeyID(v string) (string, bool) {
	rest, ok := strings.CutPrefix(v, apiKeyPrefix)
	if !ok || len(rest) != apiKeyIDLen+1+apiKeySecretLen || rest[apiKeyIDLen] != '_' {
		return "", false
	}
	id := rest[:apiKeyIDLen]
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", false
		}
	}
	if _, err := base64.RawURLEncoding.DecodeString(rest[apiKeyIDLen+1:]); err != nil {
		return "", false
	}
	return id, true
}

func keyDigest(key string) string {
	d := sha256.Sum256([]byte(key))
	return hex.EncodeToString(d[:])
}

// APIKey is a key's record: everything but the key, which is shown once, at
// creation, and never stored.
type APIKey struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Scopes    []string   `json:"scopes"`
	Roles     []string   `json:"roles,omitempty"`
	CreatedBy string     `json:"createdBy"`
	CreatedAt time.Time  `json:"createdAt"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
	RevokedAt *time.Time `json:"revokedAt,omitempty"`
	RevokedBy string     `json:"revokedBy,omitempty"`
}

// storedKey is a record as the key file holds it: the record and the key's
// digest.
type storedKey struct {
	APIKey
	Digest string `json:"digest"`
}

type keyFileDoc struct {
	Version int         `json:"version"`
	Keys    []storedKey `json:"keys"`
}

// APIKeysCollection is the records collection holding the cluster's API
// keys, keyed by key identifier; each value is a storedKey.
const APIKeysCollection = "admin.apikeys"

// readKeyFile reads a node's key file, written by builds that kept keys per
// node; a missing file holds no keys.
func readKeyFile(dir string) ([]storedKey, error) {
	path := filepath.Join(dir, apiKeyFile)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var doc keyFileDoc
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if doc.Version != 1 {
		return nil, fmt.Errorf("%s: version %d, want 1", path, doc.Version)
	}
	for _, k := range doc.Keys {
		if d, err := hex.DecodeString(k.Digest); err != nil || len(d) != sha256.Size || len(k.ID) != apiKeyIDLen {
			return nil, fmt.Errorf("%s: key %q is malformed", path, k.ID)
		}
	}
	return doc.Keys, nil
}

// getKey reads one key and its record's version.
func getKey(st RecordStore, id string) (storedKey, uint64, bool) {
	rec, ok := st.Get(APIKeysCollection, id)
	if !ok {
		return storedKey{}, 0, false
	}
	var k storedKey
	if json.Unmarshal(rec.Value, &k) != nil || k.ID != id {
		return storedKey{}, 0, false
	}
	return k, rec.Version, true
}

// listKeys is every key's record, oldest first.
func listKeys(st RecordStore) []APIKey {
	out := []APIKey{}
	after := ""
	for {
		items, more := st.List(APIKeysCollection, after, 1000)
		for _, e := range items {
			var k storedKey
			if json.Unmarshal(e.Value, &k) == nil {
				out = append(out, k.APIKey)
			}
			after = e.Key
		}
		if !more || len(items) == 0 {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// keyWriteError is the answer for a key write the collection refused.
func keyWriteError(res records.Result, err error, id string) error {
	if err != nil {
		if errors.Is(err, records.ErrUnavailable) {
			return &Error{Status: http.StatusServiceUnavailable, Detail: "the cluster did not confirm the write: " + err.Error()}
		}
		return err
	}
	switch res.Reason {
	case "":
		return nil
	case records.RefusedVersion:
		return Conflict("API key " + id + " changed while it was being written; try again")
	case records.RefusedFull:
		return Conflict("the cluster holds as many API keys as it takes")
	}
	return fmt.Errorf("API key %s was refused: %s", id, res.Reason)
}

// matchAPIKey is the decision on a presented bearer value against this
// node's keys at now. A value that is not shaped as a key, or names no key
// here, is not an API key's to decide; one that names a key authenticates
// only with that key's digest, unrevoked and unexpired.
func matchAPIKey(presented string, keys map[string]storedKey, now time.Time) (Principal, Verdict, string) {
	id, ok := apiKeyID(presented)
	if !ok {
		return Principal{}, NotMine, ""
	}
	k, ok := keys[id]
	if !ok {
		return Principal{}, NotMine, ""
	}
	if subtle.ConstantTimeCompare([]byte(keyDigest(presented)), []byte(k.Digest)) != 1 {
		return Principal{}, Refused, "apikey: wrong secret"
	}
	if k.RevokedAt != nil {
		return Principal{}, Refused, "apikey: revoked"
	}
	if k.ExpiresAt != nil && !now.Before(*k.ExpiresAt) {
		return Principal{}, Refused, "apikey: expired"
	}
	scopes, err := Expand(k.Scopes)
	if err != nil || len(scopes) == 0 {
		return Principal{}, Refused, "apikey: no usable scope"
	}
	p := Principal{Name: "apikey:" + id, Method: "apikey", Scopes: scopes, Roles: k.Roles}
	if k.ExpiresAt != nil {
		p.Expires = *k.ExpiresAt
	}
	return p, Authenticated, ""
}

// mayGrant is the decision on a key creation: an API key cannot create keys
// (a key it made would outlive its own revocation), and no identity grants a
// scope it does not hold. It returns the refusal, or "".
func mayGrant(creator Principal, want map[Scope]bool) string {
	if creator.Method == "apikey" {
		return "an API key cannot create API keys"
	}
	var missing []string
	for s := range want {
		if !creator.Scopes[s] {
			missing = append(missing, string(s))
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return "this identity cannot grant scopes it does not hold: " + strings.Join(missing, ", ")
	}
	return ""
}

// apiKeyAuthenticator decides with the cluster's key collection, once this
// node's consensus core is open, and only while the node is caught up: a
// node behind the log could still hold a key its revocation has not reached.
type apiKeyAuthenticator struct{ s *Server }

func (a apiKeyAuthenticator) Authenticate(r *http.Request) (Principal, Verdict, string) {
	st := a.s.backend.Records()
	if st == nil {
		return Principal{}, NotMine, ""
	}
	value, ok := bearer(r)
	if !ok {
		return Principal{}, NotMine, ""
	}
	id, ok := apiKeyID(string(value))
	if !ok {
		return Principal{}, NotMine, ""
	}
	if ready, _ := a.s.backend.KeysReady(); !ready {
		return Principal{}, Refused, "apikey: this node is not caught up with the cluster"
	}
	k, _, found := getKey(st, id)
	if !found {
		return Principal{}, NotMine, ""
	}
	return matchAPIKey(string(value), map[string]storedKey{id: k}, a.s.now())
}

// APIKeyRequest creates a key.
type APIKeyRequest struct {
	Name   string   `json:"name"`
	Scopes []string `json:"scopes,omitempty"`
	Roles  []string `json:"roles,omitempty"`
	// ExpiresInSeconds is 0 or absent for a key that does not expire.
	ExpiresInSeconds int64 `json:"expiresInSeconds,omitempty"`
}

// CreatedAPIKey is a new key's record and the key, shown this once.
type CreatedAPIKey struct {
	APIKey
	Key string `json:"key"`
}

// WhoAmI is the principal a request authenticated as.
type WhoAmI struct {
	Name    string     `json:"name"`
	Method  string     `json:"method"`
	Roles   []string   `json:"roles,omitempty"`
	Scopes  []string   `json:"scopes"`
	Groups  []string   `json:"groups,omitempty"`
	Expires *time.Time `json:"expires,omitempty"`
}
