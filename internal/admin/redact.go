package admin

import (
	"bytes"
	"encoding/json"
	"strings"
)

// Redacted replaces a secret's value in the configuration view.
const Redacted = "[redacted]"

// sensitive reports whether a member's value is withheld from the view: its
// name contains password, secret, token or key, in any case. It errs toward
// withholding; a module's configuration is read the same way.
func sensitive(name string) bool {
	n := strings.ToLower(name)
	for _, word := range []string{"password", "secret", "token", "key"} {
		if strings.Contains(n, word) {
			return true
		}
	}
	return false
}

// Redact returns a JSON document with every scalar member whose name is
// sensitive replaced by Redacted, at any depth, arrays and module
// configuration included. Objects and arrays under such a name are walked,
// not dropped, so what is not secret in them stays readable.
func Redact(doc []byte) (json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(doc))
	dec.UseNumber()
	var tree any
	if err := dec.Decode(&tree); err != nil {
		return nil, err
	}
	out, err := json.Marshal(redact(tree))
	return out, err
}

func redact(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			switch child.(type) {
			case map[string]any, []any:
				t[k] = redact(child)
			default:
				if sensitive(k) && child != nil {
					t[k] = Redacted
				}
			}
		}
		return t
	case []any:
		for i := range t {
			t[i] = redact(t[i])
		}
		return t
	default:
		return v
	}
}
