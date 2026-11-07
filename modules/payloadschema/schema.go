package payloadschema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// draft2020 is the one $schema a schema may name.
const draft2020 = "https://json-schema.org/draft/2020-12/schema"

// maxMessage bounds a violation's message.
const maxMessage = 256

// annotations are the keywords accepted and not asserted.
var annotations = map[string]bool{
	"$schema": true, "$id": true, "$comment": true, "$defs": true, "title": true, "description": true,
	"default": true, "examples": true, "deprecated": true, "readOnly": true, "writeOnly": true, "format": true,
}

// assertions are the keywords the validator implements.
var assertions = map[string]bool{
	"type": true, "const": true, "enum": true,
	"properties": true, "required": true, "additionalProperties": true, "patternProperties": true,
	"propertyNames": true, "minProperties": true, "maxProperties": true,
	"items": true, "prefixItems": true, "minItems": true, "maxItems": true, "uniqueItems": true,
	"minimum": true, "maximum": true, "exclusiveMinimum": true, "exclusiveMaximum": true, "multipleOf": true,
	"minLength": true, "maxLength": true, "pattern": true,
	"allOf": true, "anyOf": true, "oneOf": true, "not": true, "$ref": true,
}

var jsonTypes = map[string]bool{"null": true, "boolean": true, "object": true, "array": true, "number": true, "string": true, "integer": true}

// node is one compiled schema.
type node struct {
	always *bool // a boolean schema

	types    []string
	hasConst bool
	constV   any
	enum     []any

	properties    map[string]*node
	required      []string
	additional    *node
	patternProps  []patternProp
	propertyNames *node
	minProps      *int
	maxProps      *int

	items       *node
	prefixItems []*node
	minItems    *int
	maxItems    *int
	unique      bool

	minimum, maximum, exMin, exMax, multipleOf *big.Rat

	minLength, maxLength *int
	pattern              *regexp.Regexp

	allOf, anyOf, oneOf []*node
	not                 *node

	ref    string
	target *node
}

type patternProp struct {
	re *regexp.Regexp
	n  *node
}

// Schema is a compiled JSON Schema.
type Schema struct{ root *node }

// Compile parses a schema document. It refuses any keyword the validator
// does not implement, a $schema other than draft 2020-12, a $ref outside
// #/$defs, and a $ref cycle that consumes nothing of the instance.
func Compile(data []byte) (*Schema, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("not JSON: %v", err)
	}
	if dec.More() {
		return nil, errors.New("not one JSON value")
	}
	c := &compiler{defs: map[string]*node{}}
	if m, ok := raw.(map[string]any); ok {
		if s, ok := m["$schema"]; ok {
			str, _ := s.(string)
			if strings.TrimSuffix(str, "#") != draft2020 {
				return nil, fmt.Errorf("/$schema: only %s is supported", draft2020)
			}
		}
		if defs, ok := m["$defs"]; ok {
			dm, ok := defs.(map[string]any)
			if !ok {
				return nil, errors.New("/$defs: not an object")
			}
			// A $ref is resolved once every definition is compiled, so one may
			// name a definition compiled after it.
			for name, d := range dm {
				n, err := c.compile(d, "/$defs/"+name)
				if err != nil {
					return nil, err
				}
				c.defs[name] = n
			}
		}
	}
	root, err := c.compile(raw, "")
	if err != nil {
		return nil, err
	}
	for _, r := range c.refs {
		t, ok := c.defs[r.n.ref]
		if !ok {
			return nil, fmt.Errorf("%s/$ref: no definition %q", r.path, r.n.ref)
		}
		r.n.target = t
	}
	if path := inPlaceCycle(root, c.defs); path != "" {
		return nil, fmt.Errorf("%s: a $ref cycle that consumes nothing of the instance", path)
	}
	return &Schema{root: root}, nil
}

type compiler struct {
	defs map[string]*node
	refs []refAt
}

type refAt struct {
	n    *node
	path string
}

func (c *compiler) compile(v any, path string) (*node, error) {
	switch x := v.(type) {
	case bool:
		return &node{always: &x}, nil
	case map[string]any:
		return c.object(x, path)
	}
	return nil, fmt.Errorf("%s: a schema is an object or a boolean", orRoot(path))
}

func orRoot(path string) string {
	if path == "" {
		return "/"
	}
	return path
}

func (c *compiler) object(m map[string]any, path string) (*node, error) {
	n := &node{}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !assertions[k] && !annotations[k] {
			return nil, fmt.Errorf("%s/%s: keyword not supported", path, k)
		}
	}
	var err error
	sub := func(k string) (*node, error) {
		v, ok := m[k]
		if !ok {
			return nil, nil
		}
		return c.compile(v, path+"/"+k)
	}
	list := func(k string) ([]*node, error) {
		v, ok := m[k]
		if !ok {
			return nil, nil
		}
		arr, ok := v.([]any)
		if !ok || len(arr) == 0 {
			return nil, fmt.Errorf("%s/%s: a non-empty array of schemas", path, k)
		}
		out := make([]*node, len(arr))
		for i, s := range arr {
			if out[i], err = c.compile(s, fmt.Sprintf("%s/%s/%d", path, k, i)); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	count := func(k string) (*int, error) {
		v, ok := m[k]
		if !ok {
			return nil, nil
		}
		num, ok := v.(json.Number)
		if !ok {
			return nil, fmt.Errorf("%s/%s: a non-negative integer", path, k)
		}
		i, err := strconv.Atoi(num.String())
		if err != nil || i < 0 {
			return nil, fmt.Errorf("%s/%s: a non-negative integer", path, k)
		}
		return &i, nil
	}
	number := func(k string) (*big.Rat, error) {
		v, ok := m[k]
		if !ok {
			return nil, nil
		}
		r, ok := ratOf(v)
		if !ok {
			return nil, fmt.Errorf("%s/%s: a number", path, k)
		}
		return r, nil
	}

	if t, ok := m["type"]; ok {
		switch x := t.(type) {
		case string:
			n.types = []string{x}
		case []any:
			for _, e := range x {
				s, _ := e.(string)
				n.types = append(n.types, s)
			}
		}
		if len(n.types) == 0 {
			return nil, fmt.Errorf("%s/type: a type name or a non-empty list of them", path)
		}
		for _, s := range n.types {
			if !jsonTypes[s] {
				return nil, fmt.Errorf("%s/type: unknown type %q", path, s)
			}
		}
	}
	if v, ok := m["const"]; ok {
		n.hasConst, n.constV = true, v
	}
	if v, ok := m["enum"]; ok {
		arr, ok := v.([]any)
		if !ok || len(arr) == 0 {
			return nil, fmt.Errorf("%s/enum: a non-empty array", path)
		}
		n.enum = arr
	}
	if v, ok := m["properties"]; ok {
		pm, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s/properties: an object", path)
		}
		n.properties = map[string]*node{}
		for name, s := range pm {
			if n.properties[name], err = c.compile(s, path+"/properties/"+name); err != nil {
				return nil, err
			}
		}
	}
	if v, ok := m["required"]; ok {
		arr, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("%s/required: an array of names", path)
		}
		for _, e := range arr {
			s, ok := e.(string)
			if !ok {
				return nil, fmt.Errorf("%s/required: an array of names", path)
			}
			n.required = append(n.required, s)
		}
	}
	if v, ok := m["patternProperties"]; ok {
		pm, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s/patternProperties: an object", path)
		}
		pats := make([]string, 0, len(pm))
		for p := range pm {
			pats = append(pats, p)
		}
		sort.Strings(pats)
		for _, p := range pats {
			re, err := regexp.Compile(p)
			if err != nil {
				return nil, fmt.Errorf("%s/patternProperties: %q is not an RE2 pattern", path, p)
			}
			sn, err := c.compile(pm[p], path+"/patternProperties/"+p)
			if err != nil {
				return nil, err
			}
			n.patternProps = append(n.patternProps, patternProp{re: re, n: sn})
		}
	}
	if n.additional, err = sub("additionalProperties"); err != nil {
		return nil, err
	}
	if n.propertyNames, err = sub("propertyNames"); err != nil {
		return nil, err
	}
	if n.minProps, err = count("minProperties"); err != nil {
		return nil, err
	}
	if n.maxProps, err = count("maxProperties"); err != nil {
		return nil, err
	}
	if n.items, err = sub("items"); err != nil {
		return nil, err
	}
	if n.prefixItems, err = list("prefixItems"); err != nil {
		return nil, err
	}
	if n.minItems, err = count("minItems"); err != nil {
		return nil, err
	}
	if n.maxItems, err = count("maxItems"); err != nil {
		return nil, err
	}
	if v, ok := m["uniqueItems"]; ok {
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("%s/uniqueItems: a boolean", path)
		}
		n.unique = b
	}
	for _, kw := range []struct {
		name string
		dst  **big.Rat
	}{{"minimum", &n.minimum}, {"maximum", &n.maximum}, {"exclusiveMinimum", &n.exMin}, {"exclusiveMaximum", &n.exMax}, {"multipleOf", &n.multipleOf}} {
		if *kw.dst, err = number(kw.name); err != nil {
			return nil, err
		}
	}
	if n.multipleOf != nil && n.multipleOf.Sign() <= 0 {
		return nil, fmt.Errorf("%s/multipleOf: greater than 0", path)
	}
	if n.minLength, err = count("minLength"); err != nil {
		return nil, err
	}
	if n.maxLength, err = count("maxLength"); err != nil {
		return nil, err
	}
	if v, ok := m["pattern"]; ok {
		s, _ := v.(string)
		if n.pattern, err = regexp.Compile(s); err != nil {
			return nil, fmt.Errorf("%s/pattern: %q is not an RE2 pattern", path, s)
		}
	}
	if n.allOf, err = list("allOf"); err != nil {
		return nil, err
	}
	if n.anyOf, err = list("anyOf"); err != nil {
		return nil, err
	}
	if n.oneOf, err = list("oneOf"); err != nil {
		return nil, err
	}
	if n.not, err = sub("not"); err != nil {
		return nil, err
	}
	if v, ok := m["$ref"]; ok {
		s, _ := v.(string)
		const prefix = "#/$defs/"
		if !strings.HasPrefix(s, prefix) || strings.Contains(s[len(prefix):], "/") {
			return nil, fmt.Errorf("%s/$ref: only #/$defs/<name> is supported", path)
		}
		n.ref = s[len(prefix):]
		c.refs = append(c.refs, refAt{n: n, path: path})
	}
	return n, nil
}

// inPlaceCycle finds a cycle through keywords that apply to the same
// instance ($ref, allOf, anyOf, oneOf, not), which would never end; it
// returns a path naming it, or "".
func inPlaceCycle(root *node, defs map[string]*node) string {
	const (
		unseen = iota
		active
		done
	)
	state := map[*node]int{}
	names := map[*node]string{}
	for name, d := range defs {
		names[d] = "/$defs/" + name
	}
	var visit func(n *node) string
	visit = func(n *node) string {
		switch state[n] {
		case active:
			if p, ok := names[n]; ok {
				return p
			}
			return "/"
		case done:
			return ""
		}
		state[n] = active
		next := append(append(append([]*node{}, n.allOf...), n.anyOf...), n.oneOf...)
		if n.not != nil {
			next = append(next, n.not)
		}
		if n.target != nil {
			next = append(next, n.target)
		}
		for _, m := range next {
			if p := visit(m); p != "" {
				return p
			}
		}
		state[n] = done
		return ""
	}
	if p := visit(root); p != "" {
		return p
	}
	for _, d := range defs {
		if p := visit(d); p != "" {
			return p
		}
	}
	return ""
}

// Violation is the first way an instance fails its schema.
type Violation struct {
	Path    string // the instance's JSON Pointer, "" for the root
	Keyword string
	Detail  string
}

func (v *Violation) Error() string {
	msg := orRoot(v.Path) + ": " + v.Keyword
	if v.Detail != "" {
		msg += " " + v.Detail
	}
	if len(msg) > maxMessage {
		cut := maxMessage
		for cut > 0 && !utf8.RuneStart(msg[cut]) {
			cut--
		}
		msg = msg[:cut]
	}
	return msg
}

// Decode reads a payload as exactly one JSON value, numbers kept exact.
func Decode(payload []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, errors.New("not JSON")
	}
	if dec.More() {
		return nil, errors.New("not one JSON value")
	}
	return v, nil
}

// Validate returns the instance's first violation, or nil.
func (s *Schema) Validate(v any) *Violation { return check(s.root, v, "") }

func fail(path, kw, format string, a ...any) *Violation {
	return &Violation{Path: path, Keyword: kw, Detail: fmt.Sprintf(format, a...)}
}

func check(n *node, v any, path string) *Violation {
	if n.always != nil {
		if *n.always {
			return nil
		}
		return fail(path, "false", "schema")
	}
	if n.target != nil {
		if bad := check(n.target, v, path); bad != nil {
			return bad
		}
	}
	if len(n.types) > 0 && !typeMatches(n.types, v) {
		return fail(path, "type", "%s, not %s", strings.Join(n.types, " or "), typeName(v))
	}
	if n.hasConst && !equal(v, n.constV) {
		return fail(path, "const", "")
	}
	if n.enum != nil {
		found := false
		for _, e := range n.enum {
			if equal(v, e) {
				found = true
				break
			}
		}
		if !found {
			return fail(path, "enum", "")
		}
	}
	switch x := v.(type) {
	case map[string]any:
		if bad := checkObject(n, x, path); bad != nil {
			return bad
		}
	case []any:
		if bad := checkArray(n, x, path); bad != nil {
			return bad
		}
	case json.Number:
		if bad := checkNumber(n, x, path); bad != nil {
			return bad
		}
	case string:
		count := utf8.RuneCountInString(x)
		if n.minLength != nil && count < *n.minLength {
			return fail(path, "minLength", "%d", *n.minLength)
		}
		if n.maxLength != nil && count > *n.maxLength {
			return fail(path, "maxLength", "%d", *n.maxLength)
		}
		if n.pattern != nil && !n.pattern.MatchString(x) {
			return fail(path, "pattern", "%q", n.pattern.String())
		}
	}
	for _, s := range n.allOf {
		if bad := check(s, v, path); bad != nil {
			return bad
		}
	}
	if n.anyOf != nil {
		matched := false
		for _, s := range n.anyOf {
			if check(s, v, path) == nil {
				matched = true
				break
			}
		}
		if !matched {
			return fail(path, "anyOf", "no branch matches")
		}
	}
	if n.oneOf != nil {
		matched := 0
		for _, s := range n.oneOf {
			if check(s, v, path) == nil {
				matched++
			}
		}
		if matched != 1 {
			return fail(path, "oneOf", "%d branches match, want 1", matched)
		}
	}
	if n.not != nil && check(n.not, v, path) == nil {
		return fail(path, "not", "")
	}
	return nil
}

// pointer escapes a JSON Pointer token.
func pointer(path, token string) string {
	return path + "/" + strings.NewReplacer("~", "~0", "/", "~1").Replace(token)
}

func checkObject(n *node, x map[string]any, path string) *Violation {
	if n.minProps != nil && len(x) < *n.minProps {
		return fail(path, "minProperties", "%d", *n.minProps)
	}
	if n.maxProps != nil && len(x) > *n.maxProps {
		return fail(path, "maxProperties", "%d", *n.maxProps)
	}
	for _, r := range n.required {
		if _, ok := x[r]; !ok {
			return fail(path, "required", "%q", r)
		}
	}
	keys := make([]string, 0, len(x))
	for k := range x {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		val := x[k]
		at := pointer(path, k)
		if n.propertyNames != nil {
			if bad := check(n.propertyNames, k, at); bad != nil {
				bad.Keyword = "propertyNames " + bad.Keyword
				return bad
			}
		}
		covered := false
		if s, ok := n.properties[k]; ok {
			covered = true
			if bad := check(s, val, at); bad != nil {
				return bad
			}
		}
		for _, pp := range n.patternProps {
			if pp.re.MatchString(k) {
				covered = true
				if bad := check(pp.n, val, at); bad != nil {
					return bad
				}
			}
		}
		if !covered && n.additional != nil {
			if bad := check(n.additional, val, at); bad != nil {
				if n.additional.always != nil {
					bad.Keyword, bad.Detail = "additionalProperties", "false"
				}
				return bad
			}
		}
	}
	return nil
}

func checkArray(n *node, x []any, path string) *Violation {
	if n.minItems != nil && len(x) < *n.minItems {
		return fail(path, "minItems", "%d", *n.minItems)
	}
	if n.maxItems != nil && len(x) > *n.maxItems {
		return fail(path, "maxItems", "%d", *n.maxItems)
	}
	for i, e := range x {
		at := path + "/" + strconv.Itoa(i)
		var s *node
		if i < len(n.prefixItems) {
			s = n.prefixItems[i]
		} else {
			s = n.items
		}
		if s != nil {
			if bad := check(s, e, at); bad != nil {
				if s.always != nil {
					bad.Keyword, bad.Detail = "items", "false"
				}
				return bad
			}
		}
	}
	if n.unique {
		for i := range x {
			for j := i + 1; j < len(x); j++ {
				if equal(x[i], x[j]) {
					return fail(path, "uniqueItems", "/%d equals /%d", i, j)
				}
			}
		}
	}
	return nil
}

func checkNumber(n *node, x json.Number, path string) *Violation {
	r, ok := ratOf(x)
	if !ok {
		return fail(path, "type", "number unreadable")
	}
	switch {
	case n.minimum != nil && r.Cmp(n.minimum) < 0:
		return fail(path, "minimum", "%s", n.minimum.RatString())
	case n.maximum != nil && r.Cmp(n.maximum) > 0:
		return fail(path, "maximum", "%s", n.maximum.RatString())
	case n.exMin != nil && r.Cmp(n.exMin) <= 0:
		return fail(path, "exclusiveMinimum", "%s", n.exMin.RatString())
	case n.exMax != nil && r.Cmp(n.exMax) >= 0:
		return fail(path, "exclusiveMaximum", "%s", n.exMax.RatString())
	}
	if n.multipleOf != nil {
		q := new(big.Rat).Quo(r, n.multipleOf)
		if !q.IsInt() {
			return fail(path, "multipleOf", "%s", n.multipleOf.RatString())
		}
	}
	return nil
}

// ratOf reads a JSON number exactly.
func ratOf(v any) (*big.Rat, bool) {
	num, ok := v.(json.Number)
	if !ok {
		return nil, false
	}
	r, ok := new(big.Rat).SetString(num.String())
	return r, ok
}

func typeMatches(types []string, v any) bool {
	got := typeName(v)
	for _, t := range types {
		if t == got || t == "number" && got == "integer" {
			return true
		}
	}
	return false
}

// typeName is an instance's JSON type, "integer" for a number with no
// fraction (1.0 included, as 2020-12 says).
func typeName(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "string"
	case json.Number:
		if r, ok := ratOf(x); ok && r.IsInt() {
			return "integer"
		}
		return "number"
	}
	return fmt.Sprintf("%T", v)
}

// equal is JSON equality: numbers by value, objects and arrays deeply.
func equal(a, b any) bool {
	switch x := a.(type) {
	case json.Number:
		ra, ok1 := ratOf(x)
		rb, ok2 := ratOf(b)
		return ok1 && ok2 && ra.Cmp(rb) == 0
	case string:
		y, ok := b.(string)
		return ok && x == y
	case bool:
		y, ok := b.(bool)
		return ok && x == y
	case nil:
		return b == nil
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !equal(x[i], y[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			w, ok := y[k]
			if !ok || !equal(v, w) {
				return false
			}
		}
		return true
	}
	return false
}
