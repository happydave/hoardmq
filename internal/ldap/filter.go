package ldap

import (
	"errors"
	"fmt"
	"strings"
)

// Placeholder is the username's place in a filter or DN template.
const Placeholder = "${username}"

// Filter is a parsed RFC 4515 filter restricted to and, or, not, equality
// and presence. An equality node whose value is the placeholder takes the
// username at encode time, as the assertion value's octets, so the username
// can never add or change a clause.
type Filter struct {
	kind     filterKind
	attr     string
	value    []byte
	username bool
	children []*Filter
}

type filterKind int

const (
	filterAnd filterKind = iota
	filterOr
	filterNot
	filterEquality
	filterPresent
)

// ParseTemplate parses a filter that holds the placeholder exactly once, as a
// whole assertion value.
func ParseTemplate(s string) (*Filter, error) {
	f, rest, err := parseFilter(s)
	if err != nil {
		return nil, err
	}
	if rest != "" {
		return nil, fmt.Errorf("filter: %q after the filter", rest)
	}
	if n := f.placeholders(); n != 1 {
		return nil, fmt.Errorf("filter: %s must appear exactly once as a whole value, found %d", Placeholder, n)
	}
	return f, nil
}

func (f *Filter) placeholders() int {
	n := 0
	if f.username {
		n++
	}
	for _, c := range f.children {
		n += c.placeholders()
	}
	return n
}

func parseFilter(s string) (*Filter, string, error) {
	if !strings.HasPrefix(s, "(") {
		return nil, "", errors.New("filter: expected (")
	}
	s = s[1:]
	if s == "" {
		return nil, "", errors.New("filter: unterminated")
	}
	var f *Filter
	var err error
	switch s[0] {
	case '&', '|':
		kind := filterAnd
		if s[0] == '|' {
			kind = filterOr
		}
		f = &Filter{kind: kind}
		s = s[1:]
		for strings.HasPrefix(s, "(") {
			var c *Filter
			c, s, err = parseFilter(s)
			if err != nil {
				return nil, "", err
			}
			f.children = append(f.children, c)
		}
		if len(f.children) == 0 {
			return nil, "", errors.New("filter: an empty and/or")
		}
	case '!':
		var c *Filter
		c, s, err = parseFilter(s[1:])
		if err != nil {
			return nil, "", err
		}
		f = &Filter{kind: filterNot, children: []*Filter{c}}
	default:
		end := strings.IndexByte(s, ')')
		if end < 0 {
			return nil, "", errors.New("filter: unterminated item")
		}
		f, err = parseItem(s[:end])
		if err != nil {
			return nil, "", err
		}
		s = s[end:]
	}
	if !strings.HasPrefix(s, ")") {
		return nil, "", errors.New("filter: expected )")
	}
	return f, s[1:], nil
}

// parseItem reads attr=value or attr=*; every other match is refused.
func parseItem(s string) (*Filter, error) {
	eq := strings.IndexByte(s, '=')
	if eq <= 0 {
		return nil, fmt.Errorf("filter: %q is not attr=value", s)
	}
	attr, value := s[:eq], s[eq+1:]
	for _, c := range attr {
		if !attrChar(c) {
			return nil, fmt.Errorf("filter: attribute %q: only equality and presence are supported", attr)
		}
	}
	if value == "*" {
		return &Filter{kind: filterPresent, attr: attr}, nil
	}
	if value == Placeholder {
		return &Filter{kind: filterEquality, attr: attr, username: true}, nil
	}
	if strings.Contains(value, Placeholder) {
		return nil, fmt.Errorf("filter: %s must be a whole value, not part of %q", Placeholder, value)
	}
	if strings.ContainsAny(value, "*(") {
		return nil, fmt.Errorf("filter: only equality and presence are supported, not %q", s)
	}
	v, err := unescapeValue(value)
	if err != nil {
		return nil, err
	}
	return &Filter{kind: filterEquality, attr: attr, value: v}, nil
}

// unescapeValue decodes RFC 4515's \XX escapes.
func unescapeValue(s string) ([]byte, error) {
	var out []byte
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			out = append(out, s[i])
			continue
		}
		if i+2 >= len(s) {
			return nil, fmt.Errorf("filter: a short escape in %q", s)
		}
		var b byte
		for _, c := range s[i+1 : i+3] {
			b <<= 4
			switch {
			case c >= '0' && c <= '9':
				b |= byte(c - '0')
			case c >= 'a' && c <= 'f':
				b |= byte(c-'a') + 10
			case c >= 'A' && c <= 'F':
				b |= byte(c-'A') + 10
			default:
				return nil, fmt.Errorf("filter: a bad escape in %q", s)
			}
		}
		out = append(out, b)
		i += 2
	}
	return out, nil
}

// Encode is the filter's BER with username as the placeholder's value.
func (f *Filter) Encode(username []byte) []byte {
	switch f.kind {
	case filterAnd, filterOr:
		var parts [][]byte
		for _, c := range f.children {
			parts = append(parts, c.Encode(username))
		}
		tag := 0
		if f.kind == filterOr {
			tag = 1
		}
		return element(classContext, true, tag, concat(parts))
	case filterNot:
		return element(classContext, true, 2, f.children[0].Encode(username))
	case filterPresent:
		return element(classContext, false, 7, []byte(f.attr))
	}
	v := f.value
	if f.username {
		v = username
	}
	return element(classContext, true, 3, concat([][]byte{octets([]byte(f.attr)), octets(v)}))
}

func attrChar(c rune) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '.' || c == ';'
}
