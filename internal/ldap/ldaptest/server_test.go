package ldaptest

import (
	"bufio"
	"encoding/asn1"
	"net"
	"strings"
	"testing"
	"time"
)

type simpleBind struct {
	Version int
	Name    []byte
	Simple  []byte `asn1:"tag:0"`
}

func send(t *testing.T, c net.Conn, id int, op []byte) {
	t.Helper()
	b, err := asn1.Marshal(ldapMessage{ID: id, Op: asn1.RawValue{FullBytes: op}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(b); err != nil {
		t.Fatal(err)
	}
}

func bindCode(t *testing.T, c net.Conn, r *bufio.Reader, id int, name, pw string) int {
	t.Helper()
	op, err := asn1.MarshalWithParams(simpleBind{Version: 3, Name: []byte(name), Simple: []byte(pw)}, "application,tag:0")
	if err != nil {
		t.Fatal(err)
	}
	send(t, c, id, op)
	raw, err := readElement(r)
	if err != nil {
		t.Fatal(err)
	}
	var m ldapMessage
	if _, err := asn1.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	var res ldapResult
	if _, err := asn1.UnmarshalWithParams(m.Op.FullBytes, &res, "application,tag:1"); err != nil {
		t.Fatal(err)
	}
	return int(res.Code)
}

// The fixture accepts an anonymous bind, and an unauthenticated one — a name
// with an empty password — as RFC 4513 lets a real server, so a client that
// sent an empty password would be told success.
func TestFixtureContract(t *testing.T) {
	s := New(t, "dc=x", Entry{DN: "uid=alice,dc=x", Password: "pw", Attrs: map[string][]string{"uid": {"alice"}}})
	s.SetOpen(func(uid string) (string, bool) { return "pw-" + uid, strings.HasPrefix(uid, "c-") })
	c, err := net.Dial("tcp", strings.TrimPrefix(s.URL, "ldap://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(c)
	for _, x := range []struct {
		name, pw string
		want     int
	}{{"", "", 0}, {"uid=alice,dc=x", "", 0}, {"uid=alice,dc=x", "pw", 0}, {"uid=alice,dc=x", "no", 49},
		{"uid=c-7,dc=x", "pw-c-7", 0}, {"uid=c-7,dc=x", "pw", 49}, {"uid=d-7,dc=x", "pw-d-7", 49}} {
		if got := bindCode(t, c, r, 1, x.name, x.pw); got != x.want {
			t.Errorf("bind %q/%q: %d, want %d", x.name, x.pw, got, x.want)
		}
	}
	type ava struct{ Attr, Value []byte }
	eq, _ := asn1.MarshalWithParams(ava{[]byte("uid"), []byte("c-7")}, "tag:3")
	op, err := asn1.MarshalWithParams(searchRequest{Base: []byte("dc=x"), Scope: 2, Size: 2, Filter: asn1.RawValue{FullBytes: eq}, Attrs: [][]byte{[]byte("1.1")}}, "application,tag:3")
	if err != nil {
		t.Fatal(err)
	}
	send(t, c, 2, op)
	raw, err := readElement(r)
	if err != nil {
		t.Fatal(err)
	}
	var m ldapMessage
	_, _ = asn1.Unmarshal(raw, &m)
	var e searchEntry
	if _, err := asn1.UnmarshalWithParams(m.Op.FullBytes, &e, "application,tag:4"); err != nil || string(e.DN) != "uid=c-7,dc=x" {
		t.Fatalf("open-mode search: %q %v", e.DN, err)
	}
}
