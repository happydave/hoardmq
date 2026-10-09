// Package otlptest is an in-process OTLP/HTTP traces endpoint for tests. It
// checks every request against what a stock OpenTelemetry Collector's OTLP/HTTP
// receiver refuses and against the rules OTLP 1.11.0 sets for a sender's JSON,
// records the spans, and names each violation.
package otlptest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/happydave/hoardmq/internal/otel"
)

// Span is one span as the endpoint decoded it.
type Span struct {
	Resource      map[string]string
	Scope         string
	TraceID       string
	SpanID        string
	ParentSpanID  string
	TraceState    string
	Name          string
	Kind          int
	Start, End    uint64
	Attributes    map[string]string
	StatusCode    int
	StatusMessage string
}

// Violation is one rule a request broke. Refused marks the rules a stock
// Collector enforces with an error status; the others are rules OTLP sets for
// a sender that the Collector tolerates.
type Violation struct {
	Rule    string
	Detail  string
	Refused bool
}

func (v Violation) String() string { return v.Rule + ": " + v.Detail }

// Check applies the rules to one request: its method, path, media type and
// body. It returns the spans it could read, every violation, and the status a
// Collector would answer.
func Check(method, path, contentType string, body []byte) ([]Span, []Violation, int) {
	var vs []Violation
	if method != http.MethodPost {
		return nil, []Violation{{Rule: "method", Detail: method, Refused: true}}, http.StatusMethodNotAllowed
	}
	if path != "/v1/traces" {
		vs = append(vs, Violation{Rule: "path", Detail: path})
	}
	if mt, _, err := mime.ParseMediaType(contentType); err != nil || mt != "application/json" {
		return nil, append(vs, Violation{Rule: "media type", Detail: contentType, Refused: true}), http.StatusUnsupportedMediaType
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var root any
	if err := dec.Decode(&root); err != nil {
		return nil, append(vs, Violation{Rule: "body", Detail: err.Error(), Refused: true}), http.StatusBadRequest
	}
	c := &checker{}
	spans := c.request(root)
	vs = append(vs, c.vs...)
	for _, v := range vs {
		if v.Refused {
			return spans, vs, http.StatusBadRequest
		}
	}
	return spans, vs, http.StatusOK
}

type checker struct{ vs []Violation }

func (c *checker) bad(refused bool, rule, format string, args ...any) {
	c.vs = append(c.vs, Violation{Rule: rule, Detail: fmt.Sprintf(format, args...), Refused: refused})
}

// object reads an object, naming every member outside known. A snake_case
// name is a key OTLP's JSON does not use; the Collector reads it, and a
// sender must not write it.
func (c *checker) object(v any, at string, known ...string) map[string]any {
	m, ok := v.(map[string]any)
	if !ok {
		c.bad(true, "type", "%s is not an object", at)
		return nil
	}
	allowed := map[string]bool{}
	for _, k := range known {
		allowed[k] = true
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !allowed[k] {
			c.bad(false, "member", "%s has a member %q OTLP's JSON does not define in lowerCamelCase", at, k)
		}
	}
	return m
}

func (c *checker) array(v any, at string) []any {
	if v == nil {
		return nil
	}
	a, ok := v.([]any)
	if !ok {
		c.bad(true, "type", "%s is not an array", at)
	}
	return a
}

func (c *checker) request(root any) []Span {
	m := c.object(root, "request", "resourceSpans")
	var out []Span
	for i, rs := range c.array(m["resourceSpans"], "resourceSpans") {
		at := fmt.Sprintf("resourceSpans[%d]", i)
		rm := c.object(rs, at, "resource", "scopeSpans", "schemaUrl")
		res := map[string]string{}
		if r := c.object(orEmpty(rm["resource"]), at+".resource", "attributes", "droppedAttributesCount"); r != nil {
			res = c.attributes(r["attributes"], at+".resource.attributes")
		}
		for j, ss := range c.array(rm["scopeSpans"], at+".scopeSpans") {
			sat := fmt.Sprintf("%s.scopeSpans[%d]", at, j)
			sm := c.object(ss, sat, "scope", "spans", "schemaUrl")
			scope := ""
			if sc := c.object(orEmpty(sm["scope"]), sat+".scope", "name", "version", "attributes", "droppedAttributesCount"); sc != nil {
				scope, _ = sc["name"].(string)
			}
			for k, sp := range c.array(sm["spans"], sat+".spans") {
				if s, ok := c.span(sp, fmt.Sprintf("%s.spans[%d]", sat, k)); ok {
					s.Resource, s.Scope = res, scope
					out = append(out, s)
				}
			}
		}
	}
	return out
}

func orEmpty(v any) any {
	if v == nil {
		return map[string]any{}
	}
	return v
}

var lowerHex = regexp.MustCompile(`^[0-9a-f]*$`)
var anyHex = regexp.MustCompile(`^[0-9a-fA-F]*$`)

// id checks a hex identifier of n bytes. The Collector refuses a wrong length
// or a non-hex character and accepts the rest; OTLP asks a sender for
// lower-case, and trace.proto calls an all-zero id invalid.
func (c *checker) id(v any, at string, n int, required bool) string {
	s, ok := v.(string)
	if !ok {
		if v != nil {
			c.bad(true, "id", "%s is not a string", at)
		}
		if required {
			c.bad(false, "id", "%s is missing", at)
		}
		return ""
	}
	if s == "" {
		if required {
			c.bad(false, "id", "%s is empty", at)
		}
		return ""
	}
	if len(s) != 2*n || !anyHex.MatchString(s) {
		c.bad(true, "id", "%s %q is not %d hex characters", at, s, 2*n)
		return s
	}
	if !lowerHex.MatchString(s) {
		c.bad(false, "id", "%s %q is not lower-case", at, s)
	}
	if s == zeros(2*n) {
		c.bad(false, "id", "%s is all zero", at)
	}
	return s
}

func zeros(n int) string { return string(bytes.Repeat([]byte{'0'}, n)) }

// uint64String checks a 64-bit integer, which a sender writes as a decimal
// string and a Collector reads as a string or a number.
func (c *checker) uint64String(v any, at string) uint64 {
	switch x := v.(type) {
	case string:
		n, err := strconv.ParseUint(x, 10, 64)
		if err != nil {
			c.bad(true, "int64", "%s %q is not a decimal integer", at, x)
		}
		return n
	case json.Number:
		c.bad(false, "int64", "%s is a JSON number, not a decimal string", at)
		n, err := strconv.ParseUint(x.String(), 10, 64)
		if err != nil {
			c.bad(true, "int64", "%s %s is not an unsigned integer", at, x)
		}
		return n
	case nil:
		c.bad(false, "int64", "%s is missing", at)
	default:
		c.bad(true, "int64", "%s is neither a string nor a number", at)
	}
	return 0
}

// enum checks an enum, which a sender writes as an integer; the Collector also
// reads a known name.
func (c *checker) enum(v any, at string, max int) int {
	switch x := v.(type) {
	case json.Number:
		n, err := strconv.Atoi(x.String())
		if err != nil || n < 0 || n > max {
			c.bad(true, "enum", "%s %s is not a value from 0 to %d", at, x, max)
		}
		return n
	case string:
		c.bad(false, "enum", "%s is the name %q, not an integer", at, x)
	case nil:
	default:
		c.bad(true, "enum", "%s is not an integer", at)
	}
	return 0
}

func (c *checker) span(v any, at string) (Span, bool) {
	m := c.object(v, at, "traceId", "spanId", "traceState", "parentSpanId", "flags", "name", "kind",
		"startTimeUnixNano", "endTimeUnixNano", "attributes", "droppedAttributesCount", "events",
		"droppedEventsCount", "links", "droppedLinksCount", "status")
	if m == nil {
		return Span{}, false
	}
	s := Span{
		TraceID:      c.id(m["traceId"], at+".traceId", 16, true),
		SpanID:       c.id(m["spanId"], at+".spanId", 8, true),
		ParentSpanID: c.id(m["parentSpanId"], at+".parentSpanId", 8, false),
		Kind:         c.enum(m["kind"], at+".kind", 5),
		Start:        c.uint64String(m["startTimeUnixNano"], at+".startTimeUnixNano"),
		End:          c.uint64String(m["endTimeUnixNano"], at+".endTimeUnixNano"),
		Attributes:   c.attributes(m["attributes"], at+".attributes"),
	}
	s.TraceState, _ = m["traceState"].(string)
	if name, _ := m["name"].(string); name == "" {
		c.bad(false, "name", "%s has no name", at)
	} else {
		s.Name = name
	}
	if s.End < s.Start {
		c.bad(false, "times", "%s ends at %d before it starts at %d", at, s.End, s.Start)
	}
	if st := c.object(orEmpty(m["status"]), at+".status", "message", "code"); st != nil {
		s.StatusCode = c.enum(st["code"], at+".status.code", 2)
		s.StatusMessage, _ = st["message"].(string)
	}
	return s, true
}

// attributes reads a KeyValue list; each value holds exactly one member.
func (c *checker) attributes(v any, at string) map[string]string {
	out := map[string]string{}
	for i, kv := range c.array(v, at) {
		kat := fmt.Sprintf("%s[%d]", at, i)
		m := c.object(kv, kat, "key", "value")
		if m == nil {
			continue
		}
		key, _ := m["key"].(string)
		if key == "" {
			c.bad(false, "attribute", "%s has no key", kat)
		}
		val := c.object(orEmpty(m["value"]), kat+".value", "stringValue", "boolValue", "intValue", "doubleValue",
			"arrayValue", "kvlistValue", "bytesValue")
		if val == nil {
			continue
		}
		if len(val) != 1 {
			c.bad(false, "attribute", "%s value has %d members, not one", kat, len(val))
		}
		for k, x := range val {
			switch k {
			case "intValue":
				switch n := x.(type) {
				case string:
					if _, err := strconv.ParseInt(n, 10, 64); err != nil {
						c.bad(true, "int64", "%s.intValue %q is not a decimal integer", kat, n)
					}
					out[key] = n
				case json.Number:
					c.bad(false, "int64", "%s.intValue is a JSON number, not a decimal string", kat)
					out[key] = n.String()
				default:
					c.bad(true, "int64", "%s.intValue is neither a string nor a number", kat)
				}
			case "stringValue":
				s, ok := x.(string)
				if !ok {
					c.bad(true, "attribute", "%s.stringValue is not a string", kat)
				}
				out[key] = s
			case "boolValue":
				b, ok := x.(bool)
				if !ok {
					c.bad(true, "attribute", "%s.boolValue is not a boolean", kat)
				}
				out[key] = strconv.FormatBool(b)
			case "doubleValue":
				n, ok := x.(json.Number)
				if !ok {
					c.bad(true, "attribute", "%s.doubleValue is not a number", kat)
				}
				out[key] = string(n)
			default:
				out[key] = fmt.Sprint(x)
			}
		}
	}
	return out
}

// Collector is the endpoint: an httptest server at /v1/traces.
type Collector struct {
	srv *httptest.Server

	mu         sync.Mutex
	spans      []Span
	violations []Violation
	requests   int
	bodies     [][]byte
	// respond, when set, chooses the answer to the n-th request (from 1) in
	// place of the status Check gives.
	respond func(n int) (status int, header http.Header, body string)
	hold    chan struct{}
}

// New starts a collector closed at the test's end.
func New(t testing.TB) *Collector {
	c := &Collector{}
	c.srv = httptest.NewServer(http.HandlerFunc(c.serve))
	t.Cleanup(c.Close)
	return c
}

// NewUnmanaged starts a collector the caller closes.
func NewUnmanaged() *Collector {
	c := &Collector{}
	c.srv = httptest.NewServer(http.HandlerFunc(c.serve))
	return c
}

// Close stops the server.
func (c *Collector) Close() {
	c.mu.Lock()
	if c.hold != nil {
		close(c.hold)
		c.hold = nil
	}
	c.mu.Unlock()
	c.srv.Close()
}

// Endpoint is the traces URL.
func (c *Collector) Endpoint() string { return c.srv.URL + "/v1/traces" }

// Respond scripts the answers by request number.
func (c *Collector) Respond(f func(n int) (int, http.Header, string)) {
	c.mu.Lock()
	c.respond = f
	c.mu.Unlock()
}

// Hold makes every request wait until Release.
func (c *Collector) Hold() {
	c.mu.Lock()
	c.hold = make(chan struct{})
	c.mu.Unlock()
}

// Release lets held requests through.
func (c *Collector) Release() {
	c.mu.Lock()
	if c.hold != nil {
		close(c.hold)
		c.hold = nil
	}
	c.mu.Unlock()
}

func (c *Collector) serve(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	hold := c.hold
	c.mu.Unlock()
	if hold != nil {
		<-hold
	}
	body, _ := io.ReadAll(r.Body)
	spans, vs, code := Check(r.Method, r.URL.Path, r.Header.Get("Content-Type"), body)
	c.mu.Lock()
	c.requests++
	n := c.requests
	c.bodies = append(c.bodies, body)
	respond := c.respond
	var header http.Header
	answer := `{}`
	if respond != nil {
		code, header, answer = respond(n)
	}
	if code >= 200 && code < 300 {
		c.spans = append(c.spans, spans...)
	}
	c.violations = append(c.violations, vs...)
	c.mu.Unlock()
	for k, v := range header {
		w.Header()[k] = v
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, answer)
}

// Spans returns the spans accepted so far.
func (c *Collector) Spans() []Span {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Span(nil), c.spans...)
}

// Violations returns every violation seen.
func (c *Collector) Violations() []Violation {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Violation(nil), c.violations...)
}

// Requests is the number of requests served.
func (c *Collector) Requests() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.requests
}

// Bodies returns every request body received.
func (c *Collector) Bodies() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.bodies...)
}

// Recorder collects ended spans from a tracer's sink.
type Recorder struct {
	mu    sync.Mutex
	spans []otel.SpanData
}

// Record is the sink.
func (r *Recorder) Record(d otel.SpanData) {
	r.mu.Lock()
	r.spans = append(r.spans, d)
	r.mu.Unlock()
}

// Spans returns what was recorded.
func (r *Recorder) Spans() []otel.SpanData {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]otel.SpanData(nil), r.spans...)
}

// Named returns the recorded spans with the name.
func (r *Recorder) Named(name string) []otel.SpanData {
	var out []otel.SpanData
	for _, d := range r.Spans() {
		if d.Name == name {
			out = append(out, d)
		}
	}
	return out
}
