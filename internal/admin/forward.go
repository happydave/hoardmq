package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// NodeHeader names the node whose answer a forwarded request carries.
const NodeHeader = "X-Hoardmq-Node"

// ForwardRequest is an admin call sent to the node that holds the client it
// acts on: the route, the body, and the principal that authenticated on the
// calling node.
type ForwardRequest struct {
	Method string `json:"method"`
	// Path is below Prefix, escaped as the caller received it, so a client
	// identifier holding a slash stays one segment.
	Path      string           `json:"path"`
	Query     string           `json:"query,omitempty"`
	Body      []byte           `json:"body,omitempty"`
	Principal ForwardPrincipal `json:"principal"`
	// Request is the calling node's request identifier, so both nodes'
	// audit records name the same request.
	Request string `json:"request"`
}

// ForwardPrincipal is the part of a principal a forwarded request carries.
type ForwardPrincipal struct {
	Name   string   `json:"name"`
	Method string   `json:"method"`
	Scopes []string `json:"scopes"`
	Roles  []string `json:"roles,omitempty"`
}

// ForwardAnswer is the owner's answer to a forwarded request.
type ForwardAnswer struct {
	Status      int    `json:"status"`
	ContentType string `json:"contentType"`
	Body        []byte `json:"body,omitempty"`
}

// Why a forward was not answered.
const (
	ForwardUnreachable = "unreachable"
	ForwardUnsupported = "unsupported"
	ForwardAdminOff    = "admin-off"
	ForwardTooLarge    = "too-large"
	ForwardTimeout     = "timeout"
)

// ForwardError says why a forward has no answer from its owner.
type ForwardError struct {
	Outcome string
	Reason  string
}

func (e *ForwardError) Error() string { return e.Outcome + ": " + e.Reason }

type forwardedKey struct{}

// forwardedFrom is the calling node of a forwarded request, empty for one
// that arrived over HTTP.
func forwardedFrom(r *http.Request) string {
	from, _ := r.Context().Value(forwardedKey{}).(string)
	return from
}

// heldElsewhere is the owner a handler's error names when it is the 409 for
// a client another node holds, or empty.
func heldElsewhere(err error) string {
	var e *Error
	if errors.As(err, &e) && e.Status == http.StatusConflict {
		return e.Owner
	}
	return ""
}

func forwardPrincipal(p Principal) ForwardPrincipal {
	return ForwardPrincipal{Name: p.Name, Method: p.Method, Scopes: p.ScopeList(), Roles: p.Roles}
}

// forward sends a request whose handler found its client on owner and writes
// the owner's answer, or the problem that says why there is none.
func (s *Server) forward(w *statusWriter, r *http.Request, owner string, body []byte, n *auditNote) {
	id, _ := r.Context().Value(requestKey{}).(string)
	req := ForwardRequest{Method: r.Method, Path: strings.TrimPrefix(r.URL.EscapedPath(), Prefix), Query: r.URL.RawQuery, Body: body,
		Principal: forwardPrincipal(principal(r)), Request: id}
	if n.detail == nil {
		n.detail = map[string]string{}
	}
	n.detail["forwardedTo"] = owner
	ans, err := s.backend.Forward(r.Context(), owner, req)
	if err == nil && (ans.Status < 100 || ans.Status > 599) {
		err = &ForwardError{Outcome: ForwardUnreachable, Reason: fmt.Sprintf("node %s answered with status %d", owner, ans.Status)}
	}
	if err == nil {
		w.Header().Set(NodeHeader, owner)
		if ans.ContentType != "" {
			w.Header().Set("Content-Type", ans.ContentType)
		}
		w.WriteHeader(ans.Status)
		_, _ = w.Write(ans.Body)
		return
	}
	var fe *ForwardError
	if !errors.As(err, &fe) {
		fe = &ForwardError{Outcome: ForwardUnreachable, Reason: err.Error()}
	}
	n.detail["forward"] = fe.Outcome
	held := HeldElsewhere(r.PathValue("clientId"), owner)
	switch fe.Outcome {
	case ForwardTooLarge:
		writeProblem(w, &Error{Status: http.StatusBadGateway, Owner: owner,
			Detail: "node " + owner + " ran the request, but its answer could not be carried back: " + fe.Reason}, "")
	case ForwardTimeout:
		writeProblem(w, &Error{Status: http.StatusGatewayTimeout, Owner: owner,
			Detail: "node " + owner + " did not answer in time; the request may have been applied there"}, "")
	default:
		held.Detail += " (it could not be forwarded: " + fe.Reason + ")"
		writeProblem(w, held, "")
	}
}

// ServeForwarded serves a request another node forwarded to this one, the
// owner of the client it acts on: through the same scope check, handler and
// audit as a request over HTTP, as the principal that authenticated there.
// Only the client routes are served; any other is refused, since the
// principal is not authenticated here.
func (s *Server) ServeForwarded(ctx context.Context, from string, req ForwardRequest) ForwardAnswer {
	scopes := map[Scope]bool{}
	for _, sc := range req.Principal.Scopes {
		scopes[Scope(sc)] = true
	}
	p := Principal{Name: req.Principal.Name, Method: req.Principal.Method, Scopes: scopes, Roles: req.Principal.Roles}
	target := Prefix + req.Path
	if req.Query != "" {
		target += "?" + req.Query
	}
	u, err := url.ParseRequestURI(target)
	if err != nil || !strings.HasPrefix(req.Path, "/") {
		return s.refuseForwarded(from, req, p)
	}
	r, err := http.NewRequestWithContext(ctx, req.Method, u.String(), bytes.NewReader(req.Body))
	if err != nil {
		return s.refuseForwarded(from, req, p)
	}
	r.RemoteAddr = from
	if _, pattern := s.mux.Handler(r); !s.forwardable[pattern] {
		return s.refuseForwarded(from, req, p)
	}
	ctx = context.WithValue(context.WithValue(context.WithValue(ctx, principalKey{}, p), requestKey{}, req.Request), forwardedKey{}, from)
	rec := &recorder{header: http.Header{}}
	r.Body = http.MaxBytesReader(rec, io.NopCloser(bytes.NewReader(req.Body)), s.maxBody)
	s.mux.ServeHTTP(rec, r.WithContext(ctx))
	if rec.status == 0 {
		rec.status = http.StatusOK
	}
	return ForwardAnswer{Status: rec.status, ContentType: rec.header.Get("Content-Type"), Body: rec.body.Bytes()}
}

// refuseForwarded answers a forwarded request for a route this node does not
// serve for another, and audits it.
func (s *Server) refuseForwarded(from string, req ForwardRequest, p Principal) ForwardAnswer {
	actor := p.Name
	if actor == "" {
		actor = "anonymous"
	}
	s.backend.Audit(AuditEvent{Actor: actor, Action: "admin.forward.refused", Target: req.Method + " " + req.Path, Outcome: "denied",
		Detail: map[string]string{"request": req.Request, "remote": from, "forwardedFrom": from, "credential": p.Method}}, false)
	e := &Error{Status: http.StatusForbidden, Detail: "a forwarded request may act only on a client: " + req.Method + " " + req.Path + " is not served for another node"}
	body, _ := json.Marshal(Problem{Type: "about:blank", Title: http.StatusText(e.Status), Status: e.Status, Detail: e.Detail})
	return ForwardAnswer{Status: e.Status, ContentType: "application/problem+json", Body: body}
}

// recorder keeps a forwarded request's answer.
type recorder struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (r *recorder) Header() http.Header { return r.header }
func (r *recorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
}
func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.body.Write(b)
}
