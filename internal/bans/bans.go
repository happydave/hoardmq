// Package bans is the ban list: its entries' form and rules, an index built
// from them, and the decision whether a CONNECT is banned. The entries live
// in the replicated records collection Collection, keyed "<kind>:<value>";
// every decision here is a pure function of what it is given.
package bans

import (
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Collection is the records collection the ban list lives in.
const Collection = "bans"

// Kinds.
const (
	ClientID      = "clientid"
	Username      = "username"
	Address       = "address"
	CIDR          = "cidr"
	ClientIDRegex = "clientid-regex"
	UsernameRegex = "username-regex"
)

// Kinds lists every kind, in the order a CONNECT is matched against them.
var Kinds = []string{ClientID, Username, Address, CIDR, ClientIDRegex, UsernameRegex}

// Sources.
const (
	SourceAdmin    = "admin"
	SourceFlapping = "flapping"
)

// Bounds.
const (
	MaxValue  = 256
	MaxReason = 256
)

// Entry is a ban's stored value. ExpiresAt nil never expires.
type Entry struct {
	Reason    string     `json:"reason,omitempty"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
	Source    string     `json:"source"`
}

// Key is the record key of a ban.
func Key(kind, value string) string { return kind + ":" + value }

// SplitKey reads a record key back into its kind and value.
func SplitKey(key string) (kind, value string, ok bool) {
	kind, value, ok = strings.Cut(key, ":")
	return kind, value, ok && known(kind)
}

func known(kind string) bool {
	for _, k := range Kinds {
		if k == kind {
			return true
		}
	}
	return false
}

// Live is whether an entry refuses at now: until its expiry, not at it.
func Live(e Entry, now time.Time) bool { return e.ExpiresAt == nil || now.Before(*e.ExpiresAt) }

// CheckBan is the rule every stored ban meets: a known kind, a value of that
// kind's form (an address or a prefix in its canonical form, a regular
// expression that compiles), bounded lengths and a known source.
func CheckBan(key string, e Entry) error {
	kind, value, ok := SplitKey(key)
	if !ok {
		return fmt.Errorf("the kind must be one of %s", strings.Join(Kinds, ", "))
	}
	if value == "" || len(value) > MaxValue || !utf8.ValidString(value) {
		return fmt.Errorf("the value must be 1 to %d bytes of UTF-8", MaxValue)
	}
	switch kind {
	case Address:
		a, err := netip.ParseAddr(value)
		if err != nil {
			return errors.New("an address ban's value must be an IP address")
		}
		if c := a.Unmap().String(); c != value {
			return fmt.Errorf("write the address as %s", c)
		}
	case CIDR:
		p, err := netip.ParsePrefix(value)
		if err != nil {
			return errors.New("a cidr ban's value must be a prefix such as 10.0.0.0/8")
		}
		if c := p.Masked().String(); c != value {
			return fmt.Errorf("write the prefix as %s", c)
		}
	case ClientIDRegex, UsernameRegex:
		if _, err := compile(value); err != nil {
			return fmt.Errorf("the regular expression does not compile: %v", err)
		}
	}
	if len(e.Reason) > MaxReason || !utf8.ValidString(e.Reason) {
		return fmt.Errorf("the reason must be at most %d bytes of UTF-8", MaxReason)
	}
	if e.Source != SourceAdmin && e.Source != SourceFlapping {
		return fmt.Errorf("the source must be %s or %s", SourceAdmin, SourceFlapping)
	}
	return nil
}

// compile anchors a pattern to the whole string.
func compile(pattern string) (*regexp.Regexp, error) { return regexp.Compile(`^(?:` + pattern + `)$`) }

// Item is one stored ban with its record version.
type Item struct {
	Key     string
	Entry   Entry
	Version uint64
}

type prefixBan struct {
	key   string
	p     netip.Prefix
	entry Entry
}

type regexBan struct {
	key   string
	re    *regexp.Regexp
	entry Entry
}

// Index is the ban list compiled for matching. It is never changed once
// built; a change of the list builds a new one.
type Index struct {
	exact   map[string]Entry
	cidrs   []prefixBan
	idRes   []regexBan
	userRes []regexBan
	size    int
	invalid int
}

// Build compiles the items; an item this build's rules refuse is skipped and
// counted, and the others are kept.
func Build(items []Item) *Index {
	ix := &Index{exact: map[string]Entry{}}
	for _, it := range items {
		if CheckBan(it.Key, it.Entry) != nil {
			ix.invalid++
			continue
		}
		ix.size++
		kind, value, _ := SplitKey(it.Key)
		switch kind {
		case ClientID, Username, Address:
			ix.exact[it.Key] = it.Entry
		case CIDR:
			p, _ := netip.ParsePrefix(value)
			ix.cidrs = append(ix.cidrs, prefixBan{key: it.Key, p: p, entry: it.Entry})
		case ClientIDRegex:
			re, _ := compile(value)
			ix.idRes = append(ix.idRes, regexBan{key: it.Key, re: re, entry: it.Entry})
		case UsernameRegex:
			re, _ := compile(value)
			ix.userRes = append(ix.userRes, regexBan{key: it.Key, re: re, entry: it.Entry})
		}
	}
	return ix
}

// Size is the number of bans indexed; Invalid those skipped.
func (ix *Index) Size() int    { return ix.size }
func (ix *Index) Invalid() int { return ix.invalid }

// Match is the ban a CONNECT met.
type Match struct {
	Key   string
	Kind  string
	Entry Entry
}

// Banned is the decision on a CONNECT: the first live ban it matches, in the
// order of Kinds. A username is matched only when the CONNECT carries one;
// an address only when the peer's is known.
func (ix *Index) Banned(clientID, username string, hasUsername bool, addr netip.Addr, now time.Time) (Match, bool) {
	if ix == nil {
		return Match{}, false
	}
	exact := func(kind, value string) (Match, bool) {
		k := Key(kind, value)
		if e, ok := ix.exact[k]; ok && Live(e, now) {
			return Match{Key: k, Kind: kind, Entry: e}, true
		}
		return Match{}, false
	}
	if m, ok := exact(ClientID, clientID); ok {
		return m, true
	}
	if hasUsername {
		if m, ok := exact(Username, username); ok {
			return m, true
		}
	}
	if addr.IsValid() {
		addr = addr.Unmap()
		if m, ok := exact(Address, addr.String()); ok {
			return m, true
		}
		for _, b := range ix.cidrs {
			if b.p.Contains(addr) && Live(b.entry, now) {
				return Match{Key: b.key, Kind: CIDR, Entry: b.entry}, true
			}
		}
	}
	for _, b := range ix.idRes {
		if b.re.MatchString(clientID) && Live(b.entry, now) {
			return Match{Key: b.key, Kind: ClientIDRegex, Entry: b.entry}, true
		}
	}
	if hasUsername {
		for _, b := range ix.userRes {
			if b.re.MatchString(username) && Live(b.entry, now) {
				return Match{Key: b.key, Kind: UsernameRegex, Entry: b.entry}, true
			}
		}
	}
	return Match{}, false
}

// PeerAddr reads the IP address out of a connection's remote address
// ("ip:port"); the zero Addr when it has none.
func PeerAddr(remote string) netip.Addr {
	ap, err := netip.ParseAddrPort(remote)
	if err != nil {
		return netip.Addr{}
	}
	return ap.Addr().Unmap()
}

// Lapsed is the decision on what the sweeper deletes: the items at or past
// their expiry at now, each with the version it was read at.
func Lapsed(items []Item, now time.Time) []Item {
	var out []Item
	for _, it := range items {
		if !Live(it.Entry, now) {
			out = append(out, it)
		}
	}
	return out
}
