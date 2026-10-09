// Package ldaptest is an in-process directory for tests: simple binds,
// searches by equality and presence, StartTLS and LDAPS, and an open mode in
// which every uid under the base exists. It decodes and encodes with
// encoding/asn1 over RFC 4511's ASN.1, sharing no code with the client it
// tests.
package ldaptest

import (
	"bufio"
	"crypto/tls"
	"encoding/asn1"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Entry is a directory entry.
type Entry struct {
	DN       string
	Password string
	Attrs    map[string][]string
}

// Server is the fixture; New and NewTLS start it.
type Server struct {
	// Base is the naming context searches run under.
	Base string
	// TLS is the server configuration for StartTLS and LDAPS.
	TLS *tls.Config

	ln, tlsLn net.Listener
	// URL is ldap://addr; LDAPSURL is ldaps://addr when TLS is set.
	URL, LDAPSURL string

	mu         sync.Mutex
	entries    []Entry
	conns      map[net.Conn]bool
	binds      map[string]int
	tlsBinds   map[string]int
	lastFilter asn1.RawValue
	bindLog    []string
	down       bool
	bindDelay  time.Duration
	open       func(uid string) (string, bool)

	searches, opened, concurrent, maxConcurrent atomic.Int64
	wg                                          sync.WaitGroup
}

// New starts a fixture on loopback; it stops at the test's end.
func New(t testing.TB, base string, entries ...Entry) *Server {
	t.Helper()
	s := &Server{Base: base, entries: entries}
	return s.start(t)
}

// NewTLS starts a fixture that also offers StartTLS and an LDAPS listener.
func NewTLS(t testing.TB, base string, cfg *tls.Config, entries ...Entry) *Server {
	t.Helper()
	s := &Server{Base: base, entries: entries, TLS: cfg}
	return s.start(t)
}

func (s *Server) start(t testing.TB) *Server {
	s.conns, s.binds, s.tlsBinds = map[net.Conn]bool{}, map[string]int{}, map[string]int{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.ln, s.URL = ln, "ldap://"+ln.Addr().String()
	s.wg.Add(1)
	go s.accept(ln, false)
	if s.TLS != nil {
		tl, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		s.tlsLn, s.LDAPSURL = tl, "ldaps://"+tl.Addr().String()
		s.wg.Add(1)
		go s.accept(tl, true)
	}
	t.Cleanup(s.Stop)
	return s
}

// Stop closes the listeners and every connection.
func (s *Server) Stop() {
	_ = s.ln.Close()
	if s.tlsLn != nil {
		_ = s.tlsLn.Close()
	}
	s.DropConnections()
	s.wg.Wait()
}

// SetDown makes the directory refuse to talk: open connections close, and a
// new one is closed as soon as it is accepted.
func (s *Server) SetDown(down bool) {
	s.mu.Lock()
	s.down = down
	s.mu.Unlock()
	if down {
		s.DropConnections()
	}
}

// SetOpen makes uid=<x>,<Base> exist for every x open accepts, with the
// password it returns.
func (s *Server) SetOpen(open func(uid string) (password string, ok bool)) {
	s.mu.Lock()
	s.open = open
	s.mu.Unlock()
}

func (s *Server) opener() func(string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.open
}

// SetBindDelay delays every bind's answer.
func (s *Server) SetBindDelay(d time.Duration) {
	s.mu.Lock()
	s.bindDelay = d
	s.mu.Unlock()
}

// DropConnections closes every open connection, as a server closing idle
// sessions does.
func (s *Server) DropConnections() {
	s.mu.Lock()
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
}

// Binds is how many binds named dn, with any password.
func (s *Server) Binds(dn string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.binds[dn]
}

// TLSBinds counts dn's binds over TLS.
func (s *Server) TLSBinds(dn string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tlsBinds[dn]
}

// BindLog is every bind's name, in order ("" for anonymous).
func (s *Server) BindLog() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.bindLog...)
}

// OpenConnections is how many connections are open now.
func (s *Server) OpenConnections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// Searches, Connections and MaxConcurrent count what the fixture served.
func (s *Server) Searches() int64      { return s.searches.Load() }
func (s *Server) Connections() int64   { return s.opened.Load() }
func (s *Server) MaxConcurrent() int64 { return s.maxConcurrent.Load() }

// LastFilter is the last search's filter as sent.
func (s *Server) LastFilter() asn1.RawValue {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastFilter
}

func (s *Server) accept(ln net.Listener, ldaps bool) {
	defer s.wg.Done()
	for {
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		down := s.down
		if !down {
			s.conns[nc] = true
		}
		s.mu.Unlock()
		if down {
			_ = nc.Close()
			continue
		}
		s.opened.Add(1)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serve(nc, ldaps)
			s.mu.Lock()
			delete(s.conns, nc)
			s.mu.Unlock()
			_ = nc.Close()
		}()
	}
}

type ldapMessage struct {
	ID int
	Op asn1.RawValue
}

type bindRequest struct {
	Version int
	Name    []byte
	Auth    asn1.RawValue
}

type searchRequest struct {
	Base      []byte
	Scope     asn1.Enumerated
	Deref     asn1.Enumerated
	Size      int
	Time      int
	TypesOnly bool
	Filter    asn1.RawValue
	Attrs     [][]byte
}

type extendedRequest struct {
	Name []byte `asn1:"tag:0"`
}

type ldapResult struct {
	Code    asn1.Enumerated
	Matched []byte
	Message []byte
}

type extendedResponse struct {
	Code    asn1.Enumerated
	Matched []byte
	Message []byte
	Name    []byte `asn1:"optional,tag:10"`
}

type attribute struct {
	Type []byte
	Vals [][]byte `asn1:"set"`
}

type searchEntry struct {
	DN    []byte
	Attrs []attribute
}

func (s *Server) serve(nc net.Conn, ldaps bool) {
	conn := nc
	if ldaps {
		tc := tls.Server(nc, s.TLS)
		if tc.Handshake() != nil {
			return
		}
		conn = tc
	}
	r := bufio.NewReader(conn)
	isTLS := ldaps
	for {
		raw, err := readElement(r)
		if err != nil {
			return
		}
		var m ldapMessage
		if _, err := asn1.Unmarshal(raw, &m); err != nil || m.Op.Class != asn1.ClassApplication {
			return
		}
		switch m.Op.Tag {
		case 0:
			var b bindRequest
			if _, err := asn1.UnmarshalWithParams(m.Op.FullBytes, &b, "application,tag:0"); err != nil {
				return
			}
			code := s.bind(string(b.Name), b.Auth.Bytes, isTLS)
			if !s.reply(conn, m.ID, 1, ldapResult{Code: asn1.Enumerated(code)}) {
				return
			}
		case 2:
			return
		case 3:
			var q searchRequest
			if _, err := asn1.UnmarshalWithParams(m.Op.FullBytes, &q, "application,tag:3"); err != nil {
				return
			}
			if !s.search(conn, m.ID, q) {
				return
			}
		case 23:
			var x extendedRequest
			if _, err := asn1.UnmarshalWithParams(m.Op.FullBytes, &x, "application,tag:23"); err != nil {
				return
			}
			if string(x.Name) != "1.3.6.1.4.1.1466.20037" || s.TLS == nil || isTLS {
				if !s.reply(conn, m.ID, 24, extendedResponse{Code: 2}) {
					return
				}
				continue
			}
			if !s.reply(conn, m.ID, 24, extendedResponse{Code: 0, Name: x.Name}) {
				return
			}
			tc := tls.Server(conn, s.TLS)
			if tc.Handshake() != nil {
				return
			}
			conn, r, isTLS = tc, bufio.NewReader(tc), true
		default:
			return
		}
	}
}

func (s *Server) bind(name string, password []byte, isTLS bool) int {
	n := s.concurrent.Add(1)
	defer s.concurrent.Add(-1)
	for {
		m := s.maxConcurrent.Load()
		if n <= m || s.maxConcurrent.CompareAndSwap(m, n) {
			break
		}
	}
	s.mu.Lock()
	s.binds[name]++
	if isTLS {
		s.tlsBinds[name]++
	}
	s.bindLog = append(s.bindLog, name)
	delay := s.bindDelay
	s.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	if name == "" && len(password) == 0 {
		return 0
	}
	e, ok := s.entry(name)
	if !ok {
		return 49
	}
	// An unauthenticated bind (a name, no password) is accepted, as RFC
	// 4513 § 5.1.2 lets a server do.
	if len(password) == 0 || string(password) == e.Password {
		return 0
	}
	return 49
}

// entry finds a DN among the entries or, in open mode, synthesizes it.
func (s *Server) entry(dn string) (Entry, bool) {
	s.mu.Lock()
	for _, e := range s.entries {
		if strings.EqualFold(e.DN, dn) {
			s.mu.Unlock()
			return e, true
		}
	}
	s.mu.Unlock()
	if uid, ok := strings.CutSuffix(dn, ","+s.Base); ok && s.opener() != nil {
		if v, ok := strings.CutPrefix(uid, "uid="); ok && !strings.ContainsAny(v, ",=+\\") {
			return s.openEntry(v)
		}
	}
	return Entry{}, false
}

func (s *Server) openEntry(uid string) (Entry, bool) {
	pw, ok := s.opener()(uid)
	if !ok {
		return Entry{}, false
	}
	return Entry{DN: "uid=" + uid + "," + s.Base, Password: pw, Attrs: map[string][]string{"objectClass": {"person"}, "uid": {uid}}}, true
}

func (s *Server) search(conn net.Conn, id int, q searchRequest) bool {
	s.searches.Add(1)
	s.mu.Lock()
	s.lastFilter = q.Filter
	candidates := append([]Entry(nil), s.entries...)
	s.mu.Unlock()
	if s.opener() != nil {
		for _, v := range equalityValues(q.Filter, "uid") {
			if e, ok := s.openEntry(v); ok {
				candidates = append(candidates, e)
			}
		}
	}
	base := string(q.Base)
	if !strings.EqualFold(base, s.Base) && !strings.HasSuffix(strings.ToLower(base), ","+strings.ToLower(s.Base)) {
		return s.reply(conn, id, 5, ldapResult{Code: 32})
	}
	var found []Entry
	for _, e := range candidates {
		if !inScope(e.DN, base, int(q.Scope)) {
			continue
		}
		ok, err := matches(q.Filter, e)
		if err != nil {
			return s.reply(conn, id, 5, ldapResult{Code: 2})
		}
		if ok {
			found = append(found, e)
		}
	}
	code := 0
	if q.Size > 0 && len(found) > q.Size {
		found, code = found[:q.Size], 4
	}
	for _, e := range found {
		if !s.reply(conn, id, 4, searchEntry{DN: []byte(e.DN), Attrs: []attribute{}}) {
			return false
		}
	}
	return s.reply(conn, id, 5, ldapResult{Code: asn1.Enumerated(code)})
}

func inScope(dn, base string, scope int) bool {
	dn, base = strings.ToLower(dn), strings.ToLower(base)
	if !strings.HasSuffix(dn, ","+base) {
		return scope == 0 && dn == base
	}
	rdns := strings.TrimSuffix(dn, ","+base)
	if scope == 1 {
		return !strings.Contains(rdns, ",")
	}
	return scope == 2
}

// matches evaluates a filter against an entry.
func matches(f asn1.RawValue, e Entry) (bool, error) {
	if f.Class != asn1.ClassContextSpecific {
		return false, errors.New("filter class")
	}
	switch f.Tag {
	case 0, 1:
		children, err := elements(f.Bytes)
		if err != nil {
			return false, err
		}
		for _, c := range children {
			ok, err := matches(c, e)
			if err != nil {
				return false, err
			}
			if f.Tag == 0 && !ok {
				return false, nil
			}
			if f.Tag == 1 && ok {
				return true, nil
			}
		}
		return f.Tag == 0, nil
	case 2:
		children, err := elements(f.Bytes)
		if err != nil || len(children) != 1 {
			return false, errors.New("not")
		}
		ok, err := matches(children[0], e)
		return !ok, err
	case 3:
		var ava struct{ Attr, Value []byte }
		if _, err := asn1.UnmarshalWithParams(f.FullBytes, &ava, "tag:3"); err != nil {
			return false, err
		}
		for k, vs := range e.Attrs {
			if strings.EqualFold(k, string(ava.Attr)) {
				for _, v := range vs {
					if v == string(ava.Value) {
						return true, nil
					}
				}
			}
		}
		return false, nil
	case 7:
		for k := range e.Attrs {
			if strings.EqualFold(k, string(f.Bytes)) {
				return true, nil
			}
		}
		return false, nil
	}
	return false, errors.New("unsupported filter")
}

// equalityValues collects the values asserted for attr anywhere in f.
func equalityValues(f asn1.RawValue, attr string) []string {
	switch f.Tag {
	case 0, 1, 2:
		children, _ := elements(f.Bytes)
		var out []string
		for _, c := range children {
			out = append(out, equalityValues(c, attr)...)
		}
		return out
	case 3:
		var ava struct{ Attr, Value []byte }
		if _, err := asn1.UnmarshalWithParams(f.FullBytes, &ava, "tag:3"); err == nil && strings.EqualFold(string(ava.Attr), attr) {
			return []string{string(ava.Value)}
		}
	}
	return nil
}

func elements(b []byte) ([]asn1.RawValue, error) {
	var out []asn1.RawValue
	for len(b) > 0 {
		var v asn1.RawValue
		rest, err := asn1.Unmarshal(b, &v)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
		b = rest
	}
	return out, nil
}

// FilterEqualities lists a filter's assertions as sent, attr=value and attr=*
// for presence, and counts its equality assertions.
func FilterEqualities(f asn1.RawValue) (count int, values []string) {
	switch f.Tag {
	case 0, 1, 2:
		children, _ := elements(f.Bytes)
		for _, c := range children {
			n, v := FilterEqualities(c)
			count += n
			values = append(values, v...)
		}
	case 3:
		var ava struct{ Attr, Value []byte }
		if _, err := asn1.UnmarshalWithParams(f.FullBytes, &ava, "tag:3"); err == nil {
			return 1, []string{string(ava.Attr) + "=" + string(ava.Value)}
		}
	case 7:
		return 0, []string{string(f.Bytes) + "=*"}
	}
	return count, values
}

func (s *Server) reply(conn net.Conn, id, tag int, v any) bool {
	op, err := asn1.MarshalWithParams(v, "application,tag:"+itoa(tag))
	if err != nil {
		return false
	}
	msg, err := asn1.Marshal(ldapMessage{ID: id, Op: asn1.RawValue{FullBytes: op}})
	if err != nil {
		return false
	}
	_, err = conn.Write(msg)
	return err == nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}

// readElement reads one DER element, as a server would, with io.ReadFull.
func readElement(r *bufio.Reader) ([]byte, error) {
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	n := int(hdr[1])
	if hdr[1]&0x80 != 0 {
		k := int(hdr[1] & 0x7f)
		if k == 0 || k > 4 {
			return nil, errors.New("length")
		}
		lb := make([]byte, k)
		if _, err := io.ReadFull(r, lb); err != nil {
			return nil, err
		}
		hdr = append(hdr, lb...)
		n = 0
		for _, c := range lb {
			n = n<<8 | int(c)
		}
	}
	if n > 1<<20 {
		return nil, errors.New("too large")
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return append(hdr, body...), nil
}
