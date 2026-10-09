package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// member is one key of a JSON object, in the order the API wrote it.
type member struct {
	key   string
	value any
}

type object []member

// decodeOrdered decodes JSON keeping object members in order, so the text
// output reads in the order the API documents.
func decodeOrdered(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			var out object
			for dec.More() {
				k, err := dec.Token()
				if err != nil {
					return nil, err
				}
				v, err := decodeOrdered(dec)
				if err != nil {
					return nil, err
				}
				out = append(out, member{key: k.(string), value: v})
			}
			_, err := dec.Token()
			return out, err
		case '[':
			out := []any{}
			for dec.More() {
				v, err := decodeOrdered(dec)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			}
			_, err := dec.Token()
			return out, err
		}
	}
	return tok, nil
}

// inline renders a value on one line: scalars as they are, nested objects
// and lists compactly.
func inline(v any) string {
	switch t := v.(type) {
	case nil:
		return "-"
	case string:
		return t
	case json.Number:
		return t.String()
	case bool:
		return fmt.Sprint(t)
	case object:
		parts := make([]string, 0, len(t))
		for _, m := range t {
			parts = append(parts, m.key+"="+inline(m.value))
		}
		return "{" + strings.Join(parts, " ") + "}"
	case []any:
		parts := make([]string, 0, len(t))
		for _, e := range t {
			parts = append(parts, inline(e))
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	return fmt.Sprint(v)
}

// line renders an object as key=value pairs on one line.
func line(v any) string {
	if o, ok := v.(object); ok {
		parts := make([]string, 0, len(o))
		for _, m := range o {
			parts = append(parts, m.key+"="+inline(m.value))
		}
		return strings.Join(parts, " ")
	}
	return inline(v)
}

// render prints an API answer as text: a list one item per line, a page's
// items then its cursor, an object one member per line.
func render(w io.Writer, data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := decodeOrdered(dec)
	if err != nil {
		return fmt.Errorf("decode the answer: %w", err)
	}
	switch t := v.(type) {
	case []any:
		for _, e := range t {
			fprintln(w, line(e))
		}
		if len(t) == 0 {
			fprintln(w, "(none)")
		}
	case object:
		if items, next, ok := asPage(t); ok {
			for _, e := range items {
				fprintln(w, line(e))
			}
			if len(items) == 0 {
				fprintln(w, "(none)")
			}
			if next != "" {
				fprintln(w, "next: "+next)
			}
			return nil
		}
		for _, m := range t {
			fprintln(w, m.key+": "+inline(m.value))
		}
	default:
		fprintln(w, inline(t))
	}
	return nil
}

// asPage recognises a page: an object whose members are items and, perhaps,
// next.
func asPage(o object) (items []any, next string, ok bool) {
	for _, m := range o {
		switch m.key {
		case "items":
			items, ok = m.value.([]any)
			if !ok {
				return nil, "", false
			}
		case "next":
			next, _ = m.value.(string)
		default:
			return nil, "", false
		}
	}
	return items, next, ok
}
