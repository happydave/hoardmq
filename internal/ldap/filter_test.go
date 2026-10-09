package ldap

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

func bufioReader(b []byte) *bufio.Reader { return bufio.NewReader(bytes.NewReader(b)) }

// walk flattens an encoded filter into its assertions, attr=value or attr=*.
func walk(t *testing.T, b []byte) []string {
	t.Helper()
	var out []string
	for len(b) > 0 {
		e, rest, err := readTLV(b)
		if err != nil {
			t.Fatal(err)
		}
		switch e.tag {
		case 0, 1, 2:
			out = append(out, walk(t, e.value)...)
		case 3:
			a, r, err := readTLV(e.value)
			if err != nil {
				t.Fatal(err)
			}
			v, _, err := readTLV(r)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, string(a.value)+"="+string(v.value))
		case 7:
			out = append(out, string(e.value)+"=*")
		default:
			t.Fatalf("filter tag %d", e.tag)
		}
		b = rest
	}
	return out
}

func TestFilterUsernameIsOnlyAValue(t *testing.T) {
	f, err := ParseTemplate("(&(objectClass=person)(uid=${username}))")
	if err != nil {
		t.Fatal(err)
	}
	got := walk(t, f.Encode([]byte("*)(uid=*")))
	if len(got) != 2 || got[0] != "objectClass=person" || got[1] != "uid=*)(uid=*" {
		t.Fatalf("assertions %q, want objectClass=person and uid with the literal value", got)
	}
}

func TestFilterParse(t *testing.T) {
	f, err := ParseTemplate(`(&(uid=${username})(cn=a\2ab)(mail=*)(!(x=y))(|(o=p)(q=r)))`)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(walk(t, f.Encode([]byte("u"))), " ")
	if want := "uid=u cn=a*b mail=* x=y o=p q=r"; got != want {
		t.Fatalf("assertions %q, want %q", got, want)
	}
	enc := f.Encode([]byte("u"))
	outer, _, _ := readTLV(enc)
	if outer.class != classContext || outer.tag != 0 || !outer.cons {
		t.Fatalf("outer %+v", outer)
	}
	for _, bad := range []string{
		"(&(uid=${username})(cn=a*))", "(&(uid=${username})(cn=a${username}))", "(&(uid=${username})(|))",
		"(uid=a*)", "(uid>=${username})", "(uid<=${username})", "(uid~=${username})", "(uid:dn:=${username})",
		"(&(uid=${username})(cn=${username}))", "(uid=x)", "(&(uid=${username})", "(uid=a${username})",
		"", "(&)", "(uid=${username})x", `(uid=${username})(cn=\2)`, `(&(uid=${username})(cn=\zz))`, "(=x)",
	} {
		if _, err := ParseTemplate(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestDNEscaping(t *testing.T) {
	for in, want := range map[string]string{
		"alice": "alice", "a,b": `a\,b`, " x": `\ x`, "x ": `x\ `, "#x": `\#x`, "a=b": `a\=b`,
		`a+b"c\d<e>f;g`: `a\+b\"c\\d\<e\>f\;g`, "a\x00b": `a\00b`, "x#y": "x#y", "a b": "a b",
	} {
		if got := EscapeDNValue(in); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
	tpl, err := ParseDNTemplate("uid=${username},ou=people,dc=x")
	if err != nil {
		t.Fatal(err)
	}
	if got := tpl.Fill("a,b"); got != `uid=a\,b,ou=people,dc=x` {
		t.Fatalf("fill %q", got)
	}
	for _, bad := range []string{"uid=x,dc=y", "uid=${username}${username}", "uid=a${username},dc=x", "${username},dc=x", "uid=${username}x,dc=y"} {
		if _, err := ParseDNTemplate(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
