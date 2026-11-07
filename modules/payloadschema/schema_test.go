package payloadschema

import (
	"strings"
	"testing"
)

func mustCompile(t *testing.T, schema string) *Schema {
	t.Helper()
	s, err := Compile([]byte(schema))
	if err != nil {
		t.Fatalf("%s: %v", schema, err)
	}
	return s
}

// TestKeywords: each keyword accepts one instance and refuses another, each
// chosen so a validator without the keyword, or with a looser one, answers
// differently.
func TestKeywords(t *testing.T) {
	for _, c := range []struct {
		schema, valid, invalid, keyword string
	}{
		{`{"type":"integer"}`, `1.0`, `1.5`, "type"},
		{`{"type":["string","null"]}`, `null`, `1`, "type"},
		{`{"type":"number"}`, `2`, `"2"`, "type"},
		{`{"const":{"a":[1,2]}}`, `{"a":[1.0,2]}`, `{"a":[2,1]}`, "const"},
		{`{"enum":["a",1]}`, `1.00`, `"b"`, "enum"},
		{`{"maximum":100}`, `100`, `100.5`, "maximum"},
		{`{"minimum":-1}`, `-1`, `-1.0001`, "minimum"},
		{`{"exclusiveMaximum":100}`, `99.999`, `100`, "exclusiveMaximum"},
		{`{"exclusiveMinimum":0}`, `0.001`, `0`, "exclusiveMinimum"},
		{`{"multipleOf":0.1}`, `0.3`, `0.35`, "multipleOf"},
		{`{"maximum":1e400}`, `1e399`, `1e401`, "maximum"},
		{`{"maxLength":2}`, `"é€"`, `"abc"`, "maxLength"},
		{`{"minLength":2}`, `"ab"`, `"€"`, "minLength"},
		{`{"pattern":"^[a-z]+$"}`, `"abc"`, `"ab1"`, "pattern"},
		{`{"pattern":"b"}`, `"abc"`, `"ac"`, "pattern"},
		{`{"required":["op"]}`, `{"op":1}`, `{"po":1}`, "required"},
		{`{"properties":{"t":{"type":"number"}}}`, `{"t":1,"x":"y"}`, `{"t":"1"}`, "type"},
		{`{"additionalProperties":false,"properties":{"a":true},"patternProperties":{"^x-":true}}`, `{"a":1,"x-b":2}`, `{"a":1,"b":2}`, "additionalProperties"},
		{`{"additionalProperties":{"type":"string"}}`, `{"a":"s"}`, `{"a":1}`, "type"},
		{`{"patternProperties":{"^n":{"type":"number"}}}`, `{"n1":1,"s":"x"}`, `{"n1":"x"}`, "type"},
		{`{"propertyNames":{"maxLength":3}}`, `{"abc":1}`, `{"abcd":1}`, "propertyNames maxLength"},
		{`{"minProperties":1}`, `{"a":1}`, `{}`, "minProperties"},
		{`{"maxProperties":1}`, `{"a":1}`, `{"a":1,"b":2}`, "maxProperties"},
		{`{"items":{"type":"integer"}}`, `[1,2]`, `[1,"2"]`, "type"},
		{`{"prefixItems":[{"type":"string"}],"items":false}`, `["a"]`, `["a",1]`, "items"},
		{`{"minItems":2}`, `[1,2]`, `[1]`, "minItems"},
		{`{"maxItems":1}`, `[1]`, `[1,2]`, "maxItems"},
		{`{"uniqueItems":true}`, `[1,"1",[1]]`, `[1,1.0]`, "uniqueItems"},
		{`{"allOf":[{"minimum":1},{"maximum":3}]}`, `2`, `4`, "maximum"},
		{`{"anyOf":[{"type":"string"},{"minimum":10}]}`, `11`, `5`, "anyOf"},
		{`{"oneOf":[{"type":"integer"},{"minimum":0}]}`, `-1`, `1`, "oneOf"},
		{`{"not":{"type":"null"}}`, `0`, `null`, "not"},
		{`false`, ``, `{}`, "false"},
		{`{"$defs":{"pos":{"minimum":0}},"properties":{"v":{"$ref":"#/$defs/pos"}}}`, `{"v":0}`, `{"v":-1}`, "minimum"},
		{`{"$defs":{"tree":{"type":"object","properties":{"kids":{"type":"array","items":{"$ref":"#/$defs/tree"}},"v":{"type":"integer"}}}},"$ref":"#/$defs/tree"}`,
			`{"v":1,"kids":[{"v":2,"kids":[]}]}`, `{"v":1,"kids":[{"v":2.5}]}`, "type"},
		{`{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"t","format":"email","type":"string"}`, `"not an email"`, `1`, "type"},
	} {
		s := mustCompile(t, c.schema)
		if c.valid != "" {
			v, err := Decode([]byte(c.valid))
			if err != nil {
				t.Fatalf("%s: %v", c.valid, err)
			}
			if bad := s.Validate(v); bad != nil {
				t.Errorf("%s against %s: %v, want valid", c.schema, c.valid, bad)
			}
		}
		v, err := Decode([]byte(c.invalid))
		if err != nil {
			t.Fatalf("%s: %v", c.invalid, err)
		}
		bad := s.Validate(v)
		if bad == nil {
			t.Errorf("%s against %s: valid, want a %s violation", c.schema, c.invalid, c.keyword)
		} else if bad.Keyword != c.keyword {
			t.Errorf("%s against %s: %v, want keyword %s", c.schema, c.invalid, bad, c.keyword)
		}
	}
}

// TestRefusedAtCompile: what the validator does not implement is refused,
// naming where, so a schema never means less than it says.
func TestRefusedAtCompile(t *testing.T) {
	for _, c := range []struct{ schema, want string }{
		{`{"if":{"type":"string"},"then":{"minLength":1}}`, "/if: keyword not supported"},
		{`{"properties":{"a":{"contains":{"const":1}}}}`, "/properties/a/contains: keyword not supported"},
		{`{"unevaluatedProperties":false}`, "/unevaluatedProperties"},
		{`{"dependentSchemas":{}}`, "/dependentSchemas"},
		{`{"$dynamicRef":"#x"}`, "/$dynamicRef"},
		{`{"$ref":"https://example.com/s.json"}`, "only #/$defs/<name>"},
		{`{"$ref":"#/$defs/missing"}`, `no definition "missing"`},
		{`{"$defs":{"a":{"$ref":"#/$defs/b"},"b":{"allOf":[{"$ref":"#/$defs/a"}]}},"$ref":"#/$defs/a"}`, "consumes nothing"},
		{`{"$schema":"http://json-schema.org/draft-07/schema#"}`, "/$schema"},
		{`{"pattern":"(?<=a)b"}`, "not an RE2 pattern"},
		{`{"type":"float"}`, "unknown type"},
		{`{"multipleOf":0}`, "greater than 0"},
		{`{"minLength":-1}`, "non-negative integer"},
		{`{"allOf":[]}`, "non-empty array"},
		{`"string"`, "an object or a boolean"},
		{`{} {}`, "not one JSON value"},
	} {
		_, err := Compile([]byte(c.schema))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", c.schema, err, c.want)
		}
	}
}

// TestViolationMessage: the instance path and the keyword, escaped as a JSON
// Pointer, within 256 bytes.
func TestViolationMessage(t *testing.T) {
	s := mustCompile(t, `{"properties":{"a/b":{"properties":{"c~d":{"maximum":1}}}}}`)
	v, _ := Decode([]byte(`{"a/b":{"c~d":2}}`))
	if got := s.Validate(v).Error(); got != "/a~1b/c~0d: maximum 1" {
		t.Fatalf("message %q", got)
	}
	long := mustCompile(t, `{"pattern":"^`+strings.Repeat("x", 400)+`$"}`)
	v, _ = Decode([]byte(`"y"`))
	if got := long.Validate(v).Error(); len(got) > maxMessage || !strings.HasPrefix(got, "/: pattern") {
		t.Fatalf("message of %d bytes: %.40q", len(got), got)
	}
}

// TestDecode: one JSON value or nothing.
func TestDecode(t *testing.T) {
	for _, bad := range []string{``, `{} {}`, `{"a":`, "\xef\xbb\xbf{}", `not json`} {
		if _, err := Decode([]byte(bad)); err == nil {
			t.Errorf("%q decoded", bad)
		}
	}
	if _, err := Decode([]byte(" {\"a\":1} \n")); err != nil {
		t.Errorf("whitespace around a value: %v", err)
	}
}
