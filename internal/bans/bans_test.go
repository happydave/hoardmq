package bans

import (
	"net/netip"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

func forever(source string) Entry { return Entry{Source: source} }

func at(t time.Time) *time.Time { return &t }

// One ban of each kind, and a CONNECT that matches only it.
func TestBannedEachKind(t *testing.T) {
	ix := Build([]Item{
		{Key: "clientid:bad-id", Entry: forever(SourceAdmin)},
		{Key: "username:mallory", Entry: forever(SourceAdmin)},
		{Key: "address:192.0.2.7", Entry: forever(SourceAdmin)},
		{Key: "cidr:10.0.0.0/8", Entry: forever(SourceAdmin)},
		{Key: "clientid-regex:dev-[0-9]+", Entry: forever(SourceFlapping)},
		{Key: "username-regex:bot.*", Entry: forever(SourceAdmin)},
	})
	good := netip.MustParseAddr("198.51.100.1")
	for _, c := range []struct {
		name, id, user string
		hasUser        bool
		addr           netip.Addr
		want           string
	}{
		{"client identifier", "bad-id", "", false, good, "clientid:bad-id"},
		{"username", "x", "mallory", true, good, "username:mallory"},
		{"address", "x", "", false, netip.MustParseAddr("192.0.2.7"), "address:192.0.2.7"},
		{"cidr", "x", "", false, netip.MustParseAddr("10.2.3.4"), "cidr:10.0.0.0/8"},
		{"a v4-mapped address in the prefix", "x", "", false, netip.MustParseAddr("::ffff:10.2.3.4"), "cidr:10.0.0.0/8"},
		{"client identifier regex", "dev-12", "", false, good, "clientid-regex:dev-[0-9]+"},
		{"username regex", "x", "botnet", true, good, "username-regex:bot.*"},
		{"nothing", "ok", "alice", true, good, ""},
		{"outside the prefix", "x", "", false, netip.MustParseAddr("11.0.0.1"), ""},
		{"the regex is whole-string", "xdev-12", "", false, good, ""},
		{"a username the CONNECT does not carry", "x", "mallory", false, good, ""},
		{"no peer address", "x", "", false, netip.Addr{}, ""},
	} {
		m, ok := ix.Banned(c.id, c.user, c.hasUser, c.addr, t0)
		if got := map[bool]string{true: m.Key, false: ""}[ok]; got != c.want {
			t.Errorf("%s: banned by %q, want %q", c.name, got, c.want)
		}
		if ok && !strings.HasPrefix(m.Key, m.Kind+":") {
			t.Errorf("%s: kind %q for key %q", c.name, m.Kind, m.Key)
		}
	}
	if ix.Size() != 6 || ix.Invalid() != 0 {
		t.Fatalf("size %d invalid %d", ix.Size(), ix.Invalid())
	}
}

// An entry refuses until its expiry and not at it.
func TestBanExpiry(t *testing.T) {
	ix := Build([]Item{{Key: "clientid:c", Entry: Entry{Source: SourceAdmin, ExpiresAt: at(t0)}}})
	if _, ok := ix.Banned("c", "", false, netip.Addr{}, t0.Add(-time.Millisecond)); !ok {
		t.Fatal("not banned a millisecond before its expiry")
	}
	if _, ok := ix.Banned("c", "", false, netip.Addr{}, t0); ok {
		t.Fatal("still banned at its expiry")
	}
	// Every kind's match honours the expiry.
	good := netip.MustParseAddr("198.51.100.1")
	for _, c := range []struct {
		key, id, user string
		addr          netip.Addr
	}{
		{"username:u", "x", "u", good},
		{"address:192.0.2.7", "x", "", netip.MustParseAddr("192.0.2.7")},
		{"cidr:10.0.0.0/8", "x", "", netip.MustParseAddr("10.0.0.9")},
		{"clientid-regex:d.*", "dev", "", good},
		{"username-regex:b.*", "x", "bob", good},
	} {
		ix := Build([]Item{{Key: c.key, Entry: Entry{Source: SourceAdmin, ExpiresAt: at(t0)}}})
		if _, ok := ix.Banned(c.id, c.user, c.user != "", c.addr, t0.Add(-time.Millisecond)); !ok {
			t.Errorf("%s: not banned before its expiry", c.key)
		}
		if _, ok := ix.Banned(c.id, c.user, c.user != "", c.addr, t0); ok {
			t.Errorf("%s: banned at its expiry", c.key)
		}
	}
	items := []Item{
		{Key: "clientid:a", Entry: Entry{Source: SourceAdmin, ExpiresAt: at(t0)}, Version: 3},
		{Key: "clientid:b", Entry: Entry{Source: SourceAdmin, ExpiresAt: at(t0.Add(time.Millisecond))}, Version: 4},
		{Key: "clientid:c", Entry: Entry{Source: SourceAdmin}, Version: 5},
	}
	got := Lapsed(items, t0)
	if len(got) != 1 || got[0].Key != "clientid:a" || got[0].Version != 3 {
		t.Fatalf("lapsed at t0: %+v, want clientid:a at version 3 alone", got)
	}
}

func TestCheckBan(t *testing.T) {
	ok := forever(SourceAdmin)
	for key, want := range map[string]string{
		"host:x":                   "kind must be one of",
		"clientid:":                "1 to 256 bytes",
		"address:not-an-ip":        "IP address",
		"address:::ffff:192.0.2.1": "write the address as 192.0.2.1",
		"cidr:10.1.2.3/8":          "write the prefix as 10.0.0.0/8",
		"cidr:10.0.0.0/33":         "prefix such as",
		"clientid-regex:(":         "does not compile",
		"username-regex:" + strings.Repeat("a", 257): "1 to 256 bytes",
	} {
		if err := CheckBan(key, ok); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: %v, want %q", key, err, want)
		}
	}
	if err := CheckBan("clientid:x", Entry{Source: "someone"}); err == nil {
		t.Error("an unknown source accepted")
	}
	if err := CheckBan("clientid:x", Entry{Source: SourceAdmin, Reason: strings.Repeat("r", 257)}); err == nil {
		t.Error("a 257-byte reason accepted")
	}
	for _, key := range []string{"clientid:x", "username:y", "address:192.0.2.1", "address:2001:db8::1", "cidr:10.0.0.0/8", "clientid-regex:dev-[0-9]+", "username-regex:.*"} {
		if err := CheckBan(key, ok); err != nil {
			t.Errorf("%q refused: %v", key, err)
		}
	}
	// An entry this build refuses is skipped and counted; the rest still refuse.
	ix := Build([]Item{{Key: "cidr:10.1.2.3/8", Entry: ok}, {Key: "clientid:x", Entry: ok}})
	if ix.Invalid() != 1 || ix.Size() != 1 {
		t.Fatalf("invalid %d size %d", ix.Invalid(), ix.Size())
	}
	if _, banned := ix.Banned("x", "", false, netip.Addr{}, t0); !banned {
		t.Fatal("a valid entry beside an invalid one does not refuse")
	}
}

func TestPeerAddr(t *testing.T) {
	if got := PeerAddr("[::ffff:10.0.0.1]:1883"); got != netip.MustParseAddr("10.0.0.1") {
		t.Errorf("mapped: %v", got)
	}
	if got := PeerAddr("pipe"); got.IsValid() {
		t.Errorf("no address: %v", got)
	}
}
