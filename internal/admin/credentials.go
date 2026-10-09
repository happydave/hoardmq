package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/cluster/records"
)

// RecordStore is the replicated records store as the credential routes use
// it.
type RecordStore interface {
	Get(collection, key string) (records.Record, bool)
	List(collection, after string, limit int) (items []records.Entry, more bool)
	Put(ctx context.Context, collection, key string, value json.RawMessage, ifVersion *uint64, by string, at time.Time) (records.Result, error)
	Delete(ctx context.Context, collection, key string, ifVersion *uint64, by string, at time.Time) (records.Result, error)
}

// Credential is a client credential as the API shows it: never the password
// or its hash.
type Credential struct {
	Username string `json:"username"`
	// Form is the stored hash's form: pbkdf2-sha256 or scram-sha-256.
	Form       string            `json:"form"`
	Attributes map[string]string `json:"attributes,omitempty"`
	// Version is the log index of the credential's last write; ifVersion
	// makes a later write conditional on it.
	Version   uint64    `json:"version"`
	UpdatedAt time.Time `json:"updatedAt"`
	UpdatedBy string    `json:"updatedBy,omitempty"`
}

// CredentialPage is a page of credentials by username.
type CredentialPage struct {
	Items []Credential `json:"items"`
	Next  string       `json:"next,omitempty"`
}

// CredentialRequest creates a credential: exactly one of password (the
// node hashes it) and passwordHash (a PHC string hash-password wrote).
type CredentialRequest struct {
	Username     string  `json:"username"`
	Password     *string `json:"password,omitempty"`
	PasswordHash *string `json:"passwordHash,omitempty"`
	// SCRAM stores a password in the SCRAM-SHA-256 form rather than PBKDF2.
	SCRAM      bool              `json:"scram,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// CredentialUpdate changes a credential: a new password (or hash), the
// attributes (replaced whole when present), or both.
type CredentialUpdate struct {
	Password     *string           `json:"password,omitempty"`
	PasswordHash *string           `json:"passwordHash,omitempty"`
	SCRAM        bool              `json:"scram,omitempty"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	IfVersion    *uint64           `json:"ifVersion,omitempty"`
}

var (
	errNoRecords = &Error{Status: http.StatusServiceUnavailable, Detail: "the credential database is not open on this node"}
	// credentialProblems documents the failures the routes add.
	credentialProblems = map[int]string{409: "The credential exists, or its version is not ifVersion.", 503: "The cluster did not confirm the write, or the database is not open on this node."}
)

// passwordProblem is the decision on a request's password fields: exactly
// one of a password and a hash when one is required, never both, scram only
// with a password, never an empty password.
func passwordProblem(password, hash *string, scram, required bool) string {
	switch {
	case password != nil && hash != nil:
		return "set one of password and passwordHash, not both"
	case required && password == nil && hash == nil:
		return "one of password and passwordHash is required"
	case scram && password == nil:
		return "scram applies to a password the node hashes"
	case password != nil && *password == "":
		return "the password is empty"
	}
	return ""
}

// storedPassword is the PHC string a request's password fields store.
func storedPassword(password, hash *string, scram bool) (string, error) {
	if hash != nil {
		return *hash, nil
	}
	if scram {
		return auth.HashSCRAM([]byte(*password), auth.DefaultSCRAMIterations)
	}
	return auth.HashPassword([]byte(*password))
}

// credentialView is what a stored record shows.
func credentialView(username string, r records.Record) (Credential, error) {
	var c auth.StoredCredential
	if err := json.Unmarshal(r.Value, &c); err != nil {
		return Credential{}, err
	}
	return Credential{Username: username, Form: auth.CredentialForm(c.Password), Attributes: c.Attributes,
		Version: r.Version, UpdatedAt: r.At, UpdatedBy: r.By}, nil
}

// DefaultWriteTimeout bounds a credential write: waiting for a leader, the
// commit, and this node's apply.
const DefaultWriteTimeout = 10 * time.Second

// writeError maps a records write's refusal or failure to the API's answer.
func writeError(res records.Result, err error, username string) error {
	if err != nil {
		if errors.Is(err, records.ErrUnavailable) {
			return &Error{Status: http.StatusServiceUnavailable, Detail: "the cluster did not confirm the write: " + err.Error()}
		}
		return err
	}
	switch res.Reason {
	case "":
		return nil
	case records.RefusedExists:
		return Conflict("a credential for " + username + " exists")
	case records.RefusedMissing:
		return NotFound("no credential for " + username)
	case records.RefusedVersion:
		return Conflict("the credential for " + username + " is at version " + strconv.FormatUint(res.Version, 10))
	case records.RefusedFull:
		return Conflict("the credential database is full")
	}
	return BadRequest("the credential was refused: " + res.Reason)
}

func (s *Server) records() (RecordStore, error) {
	st := s.backend.Records()
	if st == nil {
		return nil, errNoRecords
	}
	return st, nil
}

func (s *Server) listCredentials(r *http.Request) (any, error) {
	st, err := s.records()
	if err != nil {
		return nil, err
	}
	after, limit, err := page(r)
	if err != nil {
		return nil, err
	}
	entries, more := st.List(auth.CredentialsCollection, after, limit)
	out := CredentialPage{Items: make([]Credential, 0, len(entries))}
	for _, e := range entries {
		c, err := credentialView(e.Key, e.Record)
		if err != nil {
			return nil, err
		}
		out.Items = append(out.Items, c)
	}
	if more && len(out.Items) > 0 {
		out.Next = out.Items[len(out.Items)-1].Username
	}
	return out, nil
}

func (s *Server) getCredential(r *http.Request) (any, error) {
	st, err := s.records()
	if err != nil {
		return nil, err
	}
	name := r.PathValue("username")
	rec, ok := st.Get(auth.CredentialsCollection, name)
	if !ok {
		return nil, NotFound("no credential for " + name)
	}
	return credentialView(name, rec)
}

func (s *Server) createCredential(r *http.Request) (any, error) {
	st, err := s.records()
	if err != nil {
		return nil, err
	}
	var req CredentialRequest
	if err := decodeBody(r, &req); err != nil {
		return nil, err
	}
	n := note(r)
	n.target = req.Username
	if msg := passwordProblem(req.Password, req.PasswordHash, req.SCRAM, true); msg != "" {
		return nil, BadRequest(msg)
	}
	phc, err := storedPassword(req.Password, req.PasswordHash, req.SCRAM)
	if err != nil {
		return nil, err
	}
	cred := auth.StoredCredential{Password: phc, Attributes: req.Attributes}
	if err := auth.CheckCredential(req.Username, cred); err != nil {
		return nil, BadRequest(err.Error())
	}
	n.detail = map[string]string{"form": auth.CredentialForm(phc), "changed": changed(true, req.Attributes != nil)}
	value, _ := json.Marshal(cred)
	absent := uint64(0)
	ctx, cancel := context.WithTimeout(r.Context(), s.writeTimeout)
	defer cancel()
	res, err := st.Put(ctx, auth.CredentialsCollection, req.Username, value, &absent, principal(r).Name, s.now().UTC())
	if err := writeError(res, err, req.Username); err != nil {
		return nil, err
	}
	rec, _ := st.Get(auth.CredentialsCollection, req.Username)
	return credentialView(req.Username, rec)
}

func (s *Server) updateCredential(r *http.Request) (any, error) {
	st, err := s.records()
	if err != nil {
		return nil, err
	}
	name := r.PathValue("username")
	var req CredentialUpdate
	if err := decodeBody(r, &req); err != nil {
		return nil, err
	}
	if msg := passwordProblem(req.Password, req.PasswordHash, req.SCRAM, false); msg != "" {
		return nil, BadRequest(msg)
	}
	newPassword := req.Password != nil || req.PasswordHash != nil
	if !newPassword && req.Attributes == nil {
		return nil, BadRequest("nothing to change: set a password, a passwordHash or attributes")
	}
	rec, ok := st.Get(auth.CredentialsCollection, name)
	if !ok {
		return nil, NotFound("no credential for " + name)
	}
	var cred auth.StoredCredential
	if err := json.Unmarshal(rec.Value, &cred); err != nil {
		return nil, err
	}
	if newPassword {
		if cred.Password, err = storedPassword(req.Password, req.PasswordHash, req.SCRAM); err != nil {
			return nil, err
		}
	}
	if req.Attributes != nil {
		cred.Attributes = req.Attributes
	}
	if err := auth.CheckCredential(name, cred); err != nil {
		return nil, BadRequest(err.Error())
	}
	note(r).detail = map[string]string{"form": auth.CredentialForm(cred.Password), "changed": changed(newPassword, req.Attributes != nil)}
	// The write is conditional on the version read, or on the caller's, so
	// a concurrent change is refused rather than overwritten.
	at := rec.Version
	if req.IfVersion != nil {
		at = *req.IfVersion
	}
	value, _ := json.Marshal(cred)
	ctx, cancel := context.WithTimeout(r.Context(), s.writeTimeout)
	defer cancel()
	res, err := st.Put(ctx, auth.CredentialsCollection, name, value, &at, principal(r).Name, s.now().UTC())
	if err := writeError(res, err, name); err != nil {
		return nil, err
	}
	rec, _ = st.Get(auth.CredentialsCollection, name)
	return credentialView(name, rec)
}

func (s *Server) deleteCredential(r *http.Request) (any, error) {
	st, err := s.records()
	if err != nil {
		return nil, err
	}
	name := r.PathValue("username")
	var ifVersion *uint64
	if v := r.URL.Query().Get("ifVersion"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil || n == 0 {
			return nil, BadRequest("ifVersion must be a positive integer")
		}
		ifVersion = &n
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.writeTimeout)
	defer cancel()
	res, err := st.Delete(ctx, auth.CredentialsCollection, name, ifVersion, principal(r).Name, s.now().UTC())
	if err := writeError(res, err, name); err != nil {
		return nil, err
	}
	if res.Previous == nil {
		return Credential{Username: name}, nil
	}
	return credentialView(name, *res.Previous)
}

// changed names what a write changed, for its audit record.
func changed(password, attributes bool) string {
	var parts []string
	if password {
		parts = append(parts, "password")
	}
	if attributes {
		parts = append(parts, "attributes")
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
