// Package jsonschema validates JSON documents against the subset of JSON
// Schema draft 2020-12 that the HoardMQ configuration schema uses. The
// keyword set is closed on purpose: type, const, enum, required, properties,
// additionalProperties, propertyNames, items, minimum, maximum, minLength,
// pattern, $ref into $defs. Anything else in a schema is ignored, so a schema
// author must stay inside this set for validation to mean what it says.
package jsonschema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Problem is one validation failure at a JSON Pointer within the instance.
type Problem struct {
	Pointer string
	Message string
}

func (p Problem) String() string { return p.Pointer + ": " + p.Message }

// Schema is a compiled schema. Compile once, validate many times.
type Schema struct {
	root *node
	defs map[string]*node
}

type node struct {
	ref                  string
	types                []string
	hasConst             bool
	constValue           any
	enum                 []any
	required             []string
	properties           map[string]*node
	additionalProperties *bool
	additionalSchema     *node
	propertyNames        *node
	items                *node
	minimum, maximum     *float64
	minLength            *int
	pattern              *regexp.Regexp
}

// Compile parses a schema document. It returns an error for malformed JSON,
// a pattern that does not compile, or a $ref that points outside $defs.
func Compile(data []byte) (*Schema, error) {
	var raw map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("schema: %w", err)
	}
	s := &Schema{defs: map[string]*node{}}
	if defs, ok := raw["$defs"].(map[string]any); ok {
		for name, d := range defs {
			dm, ok := d.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("schema: $defs/%s is not an object", name)
			}
			n, err := compileNode(dm)
			if err != nil {
				return nil, fmt.Errorf("schema: $defs/%s: %w", name, err)
			}
			s.defs[name] = n
		}
	}
	root, err := compileNode(raw)
	if err != nil {
		return nil, fmt.Errorf("schema: %w", err)
	}
	s.root = root
	if err := s.checkRefs(root); err != nil {
		return nil, err
	}
	for name, d := range s.defs {
		if err := s.checkRefs(d); err != nil {
			return nil, fmt.Errorf("schema: $defs/%s: %w", name, err)
		}
	}
	return s, nil
}

func (s *Schema) checkRefs(n *node) error {
	if n == nil {
		return nil
	}
	if n.ref != "" {
		if _, ok := s.defs[n.ref]; !ok {
			return fmt.Errorf("schema: unresolved $ref #/$defs/%s", n.ref)
		}
	}
	for _, c := range n.properties {
		if err := s.checkRefs(c); err != nil {
			return err
		}
	}
	for _, c := range []*node{n.additionalSchema, n.propertyNames, n.items} {
		if err := s.checkRefs(c); err != nil {
			return err
		}
	}
	return nil
}

func compileNode(m map[string]any) (*node, error) {
	n := &node{}
	if ref, ok := m["$ref"].(string); ok {
		const prefix = "#/$defs/"
		if !strings.HasPrefix(ref, prefix) {
			return nil, fmt.Errorf("unsupported $ref %q (only #/$defs/<name>)", ref)
		}
		n.ref = strings.TrimPrefix(ref, prefix)
	}
	switch t := m["type"].(type) {
	case string:
		n.types = []string{t}
	case []any:
		for _, v := range t {
			if s, ok := v.(string); ok {
				n.types = append(n.types, s)
			}
		}
	}
	if c, ok := m["const"]; ok {
		n.hasConst, n.constValue = true, c
	}
	if e, ok := m["enum"].([]any); ok {
		n.enum = e
	}
	if r, ok := m["required"].([]any); ok {
		for _, v := range r {
			if s, ok := v.(string); ok {
				n.required = append(n.required, s)
			}
		}
	}
	if p, ok := m["properties"].(map[string]any); ok {
		n.properties = map[string]*node{}
		for name, v := range p {
			vm, ok := v.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("properties/%s is not an object", name)
			}
			c, err := compileNode(vm)
			if err != nil {
				return nil, fmt.Errorf("properties/%s: %w", name, err)
			}
			n.properties[name] = c
		}
	}
	switch ap := m["additionalProperties"].(type) {
	case bool:
		n.additionalProperties = &ap
	case map[string]any:
		c, err := compileNode(ap)
		if err != nil {
			return nil, fmt.Errorf("additionalProperties: %w", err)
		}
		n.additionalSchema = c
	}
	if pn, ok := m["propertyNames"].(map[string]any); ok {
		c, err := compileNode(pn)
		if err != nil {
			return nil, fmt.Errorf("propertyNames: %w", err)
		}
		n.propertyNames = c
	}
	if it, ok := m["items"].(map[string]any); ok {
		c, err := compileNode(it)
		if err != nil {
			return nil, fmt.Errorf("items: %w", err)
		}
		n.items = c
	}
	var err error
	if n.minimum, err = optFloat(m, "minimum"); err != nil {
		return nil, err
	}
	if n.maximum, err = optFloat(m, "maximum"); err != nil {
		return nil, err
	}
	if v, ok := m["minLength"]; ok {
		f, err := toFloat(v)
		if err != nil {
			return nil, fmt.Errorf("minLength: %w", err)
		}
		i := int(f)
		n.minLength = &i
	}
	if p, ok := m["pattern"].(string); ok {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("pattern %q: %w", p, err)
		}
		n.pattern = re
	}
	return n, nil
}

func optFloat(m map[string]any, key string) (*float64, error) {
	v, ok := m[key]
	if !ok {
		return nil, nil
	}
	f, err := toFloat(v)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	return &f, nil
}

func toFloat(v any) (float64, error) {
	switch x := v.(type) {
	case json.Number:
		return x.Float64()
	case float64:
		return x, nil
	}
	return 0, fmt.Errorf("not a number: %v", v)
}

// Validate checks an instance decoded with encoding/json (using UseNumber so
// integers survive) and returns every problem found, sorted by pointer then
// message. A nil result means the instance conforms.
func (s *Schema) Validate(instance any) []Problem {
	var out []Problem
	s.validate(s.root, instance, "", &out)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Pointer != out[j].Pointer {
			return out[i].Pointer < out[j].Pointer
		}
		return out[i].Message < out[j].Message
	})
	return out
}

// ValidateJSON parses data and validates it. A parse failure is reported as a
// single problem at the root pointer.
func (s *Schema) ValidateJSON(data []byte) []Problem {
	v, err := Parse(data)
	if err != nil {
		return []Problem{{Pointer: "/", Message: err.Error()}}
	}
	return s.Validate(v)
}

// Parse decodes a JSON document into the generic form Validate expects,
// preserving integers as json.Number and rejecting trailing content.
func Parse(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if dec.More() {
		return nil, fmt.Errorf("invalid JSON: trailing content after document")
	}
	return v, nil
}

func (s *Schema) validate(n *node, v any, ptr string, out *[]Problem) {
	if n == nil {
		return
	}
	if n.ref != "" {
		s.validate(s.defs[n.ref], v, ptr, out)
	}
	add := func(msg string) {
		p := ptr
		if p == "" {
			p = "/"
		}
		*out = append(*out, Problem{Pointer: p, Message: msg})
	}
	if len(n.types) > 0 && !typeMatches(n.types, v) {
		add(fmt.Sprintf("expected %s, got %s", strings.Join(n.types, " or "), typeName(v)))
		return
	}
	if n.hasConst && !equal(n.constValue, v) {
		add(fmt.Sprintf("must be %s", render(n.constValue)))
	}
	if n.enum != nil {
		found := false
		for _, e := range n.enum {
			if equal(e, v) {
				found = true
				break
			}
		}
		if !found {
			parts := make([]string, 0, len(n.enum))
			for _, e := range n.enum {
				parts = append(parts, render(e))
			}
			add("must be one of " + strings.Join(parts, ", "))
		}
	}
	switch x := v.(type) {
	case map[string]any:
		for _, r := range n.required {
			if _, ok := x[r]; !ok {
				add("missing required member " + strconvQuote(r))
			}
		}
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			child := ptr + "/" + escape(k)
			if n.propertyNames != nil {
				var nameProblems []Problem
				s.validate(n.propertyNames, k, child, &nameProblems)
				for _, p := range nameProblems {
					*out = append(*out, Problem{Pointer: p.Pointer, Message: "member name " + strconvQuote(k) + ": " + p.Message})
				}
			}
			if c, ok := n.properties[k]; ok {
				s.validate(c, x[k], child, out)
				continue
			}
			if n.additionalSchema != nil {
				s.validate(n.additionalSchema, x[k], child, out)
				continue
			}
			if n.additionalProperties != nil && !*n.additionalProperties {
				*out = append(*out, Problem{Pointer: child, Message: "unknown member"})
			}
		}
	case []any:
		if n.items != nil {
			for i, e := range x {
				s.validate(n.items, e, fmt.Sprintf("%s/%d", ptr, i), out)
			}
		}
	case string:
		if n.minLength != nil && len([]rune(x)) < *n.minLength {
			if *n.minLength == 1 {
				add("must not be empty")
			} else {
				add(fmt.Sprintf("must be at least %d characters", *n.minLength))
			}
		}
		if n.pattern != nil && !n.pattern.MatchString(x) {
			add(fmt.Sprintf("must match %s", n.pattern.String()))
		}
	case json.Number:
		f, err := x.Float64()
		if err == nil {
			if n.minimum != nil && f < *n.minimum {
				add(fmt.Sprintf("must be at least %s", render(*n.minimum)))
			}
			if n.maximum != nil && f > *n.maximum {
				add(fmt.Sprintf("must be at most %s", render(*n.maximum)))
			}
		}
	}
}

func typeMatches(types []string, v any) bool {
	for _, t := range types {
		switch t {
		case "object":
			if _, ok := v.(map[string]any); ok {
				return true
			}
		case "array":
			if _, ok := v.([]any); ok {
				return true
			}
		case "string":
			if _, ok := v.(string); ok {
				return true
			}
		case "boolean":
			if _, ok := v.(bool); ok {
				return true
			}
		case "null":
			if v == nil {
				return true
			}
		case "number":
			if _, ok := v.(json.Number); ok {
				return true
			}
			if _, ok := v.(float64); ok {
				return true
			}
		case "integer":
			if isInteger(v) {
				return true
			}
		}
	}
	return false
}

func isInteger(v any) bool {
	switch x := v.(type) {
	case json.Number:
		if _, err := x.Int64(); err == nil {
			return true
		}
		f, err := x.Float64()
		return err == nil && f == float64(int64(f))
	case float64:
		return x == float64(int64(x))
	}
	return false
}

func typeName(v any) string {
	switch x := v.(type) {
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "string"
	case bool:
		return "boolean"
	case nil:
		return "null"
	case json.Number:
		if isInteger(x) {
			return "integer"
		}
		return "number"
	case float64:
		if isInteger(x) {
			return "integer"
		}
		return "number"
	}
	return fmt.Sprintf("%T", v)
}

func equal(a, b any) bool {
	fa, ea := toFloat(a)
	fb, eb := toFloat(b)
	if ea == nil && eb == nil {
		return fa == fb
	}
	switch x := a.(type) {
	case string:
		y, ok := b.(string)
		return ok && x == y
	case bool:
		y, ok := b.(bool)
		return ok && x == y
	case nil:
		return b == nil
	}
	return false
}

func render(v any) string {
	switch x := v.(type) {
	case string:
		return strconvQuote(x)
	case json.Number:
		return x.String()
	case float64:
		if x == float64(int64(x)) {
			return fmt.Sprintf("%d", int64(x))
		}
		return fmt.Sprintf("%g", x)
	}
	return fmt.Sprintf("%v", v)
}

func strconvQuote(s string) string { return fmt.Sprintf("%q", s) }

func escape(key string) string {
	key = strings.ReplaceAll(key, "~", "~0")
	return strings.ReplaceAll(key, "/", "~1")
}
