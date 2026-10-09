package admin

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/records"
	"github.com/happydave/hoardmq/internal/config"
)

// Server is the admin API's HTTP handler.
type Server struct {
	backend Backend
	chain   []Authenticator
	roles   Roles
	// fileKeys are the keys read from a node's key file, for ImportKeys.
	fileKeys atomic.Pointer[[]storedKey]
	now      func() time.Time
	maxBody  int64
	// writeTimeout bounds a credential write.
	writeTimeout time.Duration
	log          *slog.Logger
	table        []Route
	mux          *http.ServeMux
	// forwardable is the mux patterns of the routes a forwarded request may
	// reach.
	forwardable map[string]bool
	openapi     []byte
	// ids are request identifiers: a random prefix, then a count.
	idBase string
	idSeq  atomic.Uint64
}

// New builds the API from the admin configuration: it expands the roles,
// reads the token files and generates the OpenAPI document. It binds
// nothing, and contacts no identity provider.
func New(cfg config.Admin, b Backend, log *slog.Logger) (*Server, error) {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	roles, err := NewRoles(cfg.Roles)
	if err != nil {
		return nil, err
	}
	chain, err := Authenticators(cfg, roles, log)
	if err != nil {
		return nil, err
	}
	return newServer(chain, roles, cfg.MaxBodyBytes, b, log)
}

func newServer(chain []Authenticator, roles Roles, maxBody int64, b Backend, log *slog.Logger) (*Server, error) {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if maxBody <= 0 {
		maxBody = 1 << 20
	}
	s := &Server{backend: b, roles: roles, now: time.Now, maxBody: maxBody, writeTimeout: DefaultWriteTimeout, log: log, mux: http.NewServeMux(),
		idBase: randomID()}
	// API keys decide right after certificates: a key is a bearer value no
	// other authenticator claims.
	at := 0
	if len(chain) > 0 {
		if _, ok := chain[0].(certificateAuthenticator); ok {
			at = 1
		}
	}
	s.chain = append(append(append([]Authenticator{}, chain[:at]...), apiKeyAuthenticator{s: s}), chain[at:]...)
	s.table = s.routes()
	s.forwardable = map[string]bool{}
	for _, rt := range s.table {
		pattern := rt.Method + " " + Prefix + rt.Path
		s.mux.Handle(pattern, s.wrap(rt))
		if rt.forward {
			s.forwardable[pattern] = true
		}
	}
	doc, err := OpenAPI(s.table)
	if err != nil {
		return nil, err
	}
	s.openapi = doc
	return s, nil
}

// randomID is a request identifier prefix that says nothing about the
// process, since a 401 carries it too.
func randomID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// SetWriteTimeout replaces how long a write to the cluster's records, and a
// revocation's confirmation, may take.
func (s *Server) SetWriteTimeout(d time.Duration) { s.writeTimeout = d }

// ReadKeyFile reads the key file a build that kept API keys per node wrote
// in dir, so ImportKeys can put its keys into the cluster's collection. A
// missing file holds none; one that does not parse is an error.
func (s *Server) ReadKeyFile(dir string) error {
	keys, err := readKeyFile(dir)
	if err != nil {
		return err
	}
	s.fileKeys.Store(&keys)
	return nil
}

// ImportKeys puts each key the key file held into the cluster's collection
// where no record of its identifier exists, its revocation included; a key
// already there is left as it is. It returns how many it put and kept.
func (s *Server) ImportKeys(ctx context.Context) (imported, kept int, err error) {
	keys := s.fileKeys.Load()
	st := s.backend.Records()
	if keys == nil || st == nil {
		return 0, 0, nil
	}
	absent := uint64(0)
	for _, k := range *keys {
		value, merr := json.Marshal(k)
		if merr != nil {
			return imported, kept, merr
		}
		res, werr := st.Put(ctx, APIKeysCollection, k.ID, value, &absent, "import:"+s.backend.Node(), s.now().UTC())
		switch {
		case werr != nil:
			return imported, kept, werr
		case res.OK:
			imported++
		case res.Reason == records.RefusedExists:
			kept++
		default:
			return imported, kept, fmt.Errorf("API key %s: %s", k.ID, res.Reason)
		}
	}
	return imported, kept, nil
}

// Routes is the table the server dispatches.
func (s *Server) Routes() []Route { return s.table }

// Mux is the router built from the table, for the derivation test.
func (s *Server) Mux() *http.ServeMux { return s.mux }

type principalKey struct{}
type requestKey struct{}
type noteKey struct{}

// auditNote lets a handler name what its audit record is about when the
// request does not: the identifier of a key it created, and detail beside.
type auditNote struct {
	target string
	detail map[string]string
}

// principal is the request's authenticated principal.
func principal(r *http.Request) Principal {
	p, _ := r.Context().Value(principalKey{}).(Principal)
	return p
}

// note is the request's audit note.
func note(r *http.Request) *auditNote {
	n, _ := r.Context().Value(noteKey{}).(*auditNote)
	if n == nil {
		return &auditNote{}
	}
	return n
}

// ServeHTTP authenticates every request before routing it, so an
// unauthenticated caller learns nothing about which paths exist.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := s.idBase + "-" + strconv.FormatUint(s.idSeq.Add(1), 10)
	w.Header().Set("X-Request-Id", id)
	p, ok, reason := Authenticate(s.chain, r)
	if !ok {
		detail := map[string]string{"request": id, "remote": r.RemoteAddr, "method": r.Method}
		if reason != "" {
			detail["reason"] = reason
		}
		s.backend.Audit(AuditEvent{Actor: "anonymous", Action: "admin.auth.failure", Target: r.URL.Path, Outcome: "unauthenticated",
			Detail: detail}, true)
		w.Header().Set("WWW-Authenticate", `Bearer realm="hoardmq-admin"`)
		writeProblem(w, &Error{Status: http.StatusUnauthorized, Detail: "authentication required"}, "")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBody)
	ctx := context.WithValue(context.WithValue(r.Context(), principalKey{}, p), requestKey{}, id)
	s.mux.ServeHTTP(w, r.WithContext(ctx))
}

// statusWriter keeps the status a handler wrote, for the audit record.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

// wrap is the only way a route is mounted: authorization by the route's
// scope, then the handler, then the audit record. A client route whose
// client another node holds is forwarded there, once: a forwarded request is
// never forwarded again.
func (s *Server) wrap(rt Route) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := principal(r)
		sw := &statusWriter{ResponseWriter: w}
		n := &auditNote{}
		from := forwardedFrom(r)
		if from != "" {
			n.detail = map[string]string{"forwardedFrom": from}
		}
		r = r.WithContext(context.WithValue(r.Context(), noteKey{}, n))
		if !Allowed(p, rt.Scope) {
			action := rt.Action
			if action == "" {
				action = "admin.denied"
			}
			s.audit(r, rt, p, action, http.StatusForbidden, n)
			writeProblem(w, &Error{Status: http.StatusForbidden, Detail: "this identity lacks the scope " + string(rt.Scope)}, string(rt.Scope))
			return
		}
		var sent []byte
		if rt.forward && from == "" && r.Body != nil {
			b, err := io.ReadAll(r.Body)
			if err != nil {
				writeProblem(sw, s.problem(err), "")
				if rt.Action != "" {
					s.audit(r, rt, p, rt.Action, sw.status, n)
				}
				return
			}
			sent = b
			r.Body = io.NopCloser(bytes.NewReader(b))
		}
		body, err := rt.handle(r)
		switch owner := heldElsewhere(err); {
		case err != nil && rt.forward && from == "" && owner != "" && owner != s.backend.Node():
			s.forward(sw, r, owner, sent, n)
		case err != nil:
			writeProblem(sw, s.problem(err), "")
		default:
			writeJSON(sw, rt.Status, body)
		}
		if rt.Action != "" {
			s.audit(r, rt, p, rt.Action, sw.status, n)
		}
	})
}

func (s *Server) audit(r *http.Request, rt Route, p Principal, action string, status int, n *auditNote) {
	id, _ := r.Context().Value(requestKey{}).(string)
	target := n.target
	if target == "" && rt.target != nil {
		target = rt.target(r)
	}
	actor := p.Name
	if actor == "" {
		actor = "anonymous"
	}
	detail := map[string]string{"request": id, "remote": r.RemoteAddr, "method": r.Method, "path": r.URL.Path, "credential": p.Method}
	for k, v := range n.detail {
		detail[k] = v
	}
	s.backend.Audit(AuditEvent{Actor: actor, Action: action, Target: target, Outcome: outcome(status),
		Detail: detail}, false)
}

// outcome names a status for an audit record.
func outcome(status int) string {
	switch {
	case status >= 200 && status < 300:
		return "ok"
	case status == http.StatusForbidden:
		return "denied"
	case status == http.StatusBadRequest:
		return "bad-request"
	case status == http.StatusNotFound:
		return "not-found"
	case status == http.StatusConflict:
		return "conflict"
	case status == http.StatusRequestEntityTooLarge:
		return "too-large"
	default:
		return "error"
	}
}

func (s *Server) problem(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		return &Error{Status: http.StatusRequestEntityTooLarge, Detail: fmt.Sprintf("the request body exceeds %d bytes", maxErr.Limit)}
	}
	s.log.Error("admin request failed", "err", err)
	return &Error{Status: http.StatusInternalServerError, Detail: "internal error"}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeProblem(w http.ResponseWriter, e *Error, scope string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(e.Status)
	_ = json.NewEncoder(w).Encode(Problem{Type: "about:blank", Title: http.StatusText(e.Status), Status: e.Status, Detail: e.Detail, Owner: e.Owner, Scope: scope})
}
