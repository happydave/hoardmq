package ldap

import (
	"fmt"
	"strings"
)

// EscapeDNValue escapes s as an RFC 4514 attribute value: the characters
// that end or change a value (, + " \ < > ; =), NUL, a leading space or #,
// and a trailing space.
func EscapeDNValue(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == 0:
			b.WriteString(`\00`)
			continue
		case strings.IndexByte(`,+"\<>;=`, c) >= 0,
			i == 0 && (c == ' ' || c == '#'),
			i == len(s)-1 && c == ' ':
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	return b.String()
}

// DNTemplate is a DN holding the placeholder exactly once.
type DNTemplate string

// ParseDNTemplate checks a DN template.
func ParseDNTemplate(s string) (DNTemplate, error) {
	if n := strings.Count(s, Placeholder); n != 1 {
		return "", fmt.Errorf("bindDn: %s must appear exactly once, found %d", Placeholder, n)
	}
	at := strings.Index(s, Placeholder)
	after := s[at+len(Placeholder):]
	if at == 0 || s[at-1] != '=' || (after != "" && after[0] != ',' && after[0] != '+') {
		return "", fmt.Errorf("bindDn: %s must be a whole attribute value", Placeholder)
	}
	return DNTemplate(s), nil
}

// Fill is the DN for a username, escaped as a value.
func (t DNTemplate) Fill(username string) string {
	return strings.Replace(string(t), Placeholder, EscapeDNValue(username), 1)
}
