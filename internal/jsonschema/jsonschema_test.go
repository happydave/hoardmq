package jsonschema

import (
	"strings"
	"testing"
)

const testSchema = `{
  "type": "object",
  "additionalProperties": false,
  "required": ["id", "kind"],
  "properties": {
    "id": {"type": "integer", "minimum": 1, "maximum": 10},
    "kind": {"type": "string", "enum": ["a", "b"]},
    "name": {"type": "string", "minLength": 1, "pattern": "^[a-z]+$"},
    "one": {"const": 1},
    "items": {"type": "array", "items": {"$ref": "#/$defs/item"}},
    "bag": {"type": "object", "propertyNames": {"pattern": "^x"}, "additionalProperties": {"type": "object"}}
  },
  "$defs": {
    "item": {"type": "object", "required": ["v"], "additionalProperties": false, "properties": {"v": {"type": "number"}}}
  }
}`

func compile(t *testing.T) *Schema {
	t.Helper()
	s, err := Compile([]byte(testSchema))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestValidDocument(t *testing.T) {
	s := compile(t)
	doc := `{"id": 3, "kind": "a", "name": "abc", "one": 1, "items": [{"v": 1.5}], "bag": {"x1": {}}}`
	if p := s.ValidateJSON([]byte(doc)); len(p) != 0 {
		t.Fatalf("unexpected problems: %v", p)
	}
}

func TestProblemsAreAllReported(t *testing.T) {
	s := compile(t)
	doc := `{"id": 0, "kind": "c", "name": "", "one": 2, "items": [{"v": "x"}, {}], "bag": {"y": 1}, "extra": true}`
	got := s.ValidateJSON([]byte(doc))
	want := []string{
		"/bag/y: expected object, got integer",
		`/bag/y: member name "y": must match ^x`,
		"/extra: unknown member",
		"/id: must be at least 1",
		"/items/0/v: expected number, got string",
		`/items/1: missing required member "v"`,
		`/kind: must be one of "a", "b"`,
		"/name: must match ^[a-z]+$",
		"/name: must not be empty",
		"/one: must be 1",
	}
	var lines []string
	for _, p := range got {
		lines = append(lines, p.String())
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Errorf("problems differ\n got:\n%s\nwant:\n%s", strings.Join(lines, "\n"), strings.Join(want, "\n"))
	}
}

func TestTypeMismatchStopsDescent(t *testing.T) {
	s := compile(t)
	got := s.ValidateJSON([]byte(`{"id": "3", "kind": "a"}`))
	if len(got) != 1 || got[0].String() != "/id: expected integer, got string" {
		t.Fatalf("got %v", got)
	}
}

func TestMissingRequiredAtRoot(t *testing.T) {
	s := compile(t)
	got := s.ValidateJSON([]byte(`{}`))
	if len(got) != 2 || got[0].Pointer != "/" || got[1].Pointer != "/" {
		t.Fatalf("got %v", got)
	}
}

func TestInvalidJSONIsOneProblem(t *testing.T) {
	s := compile(t)
	for _, doc := range []string{`{`, `{"id":1} {"id":2}`, ``} {
		got := s.ValidateJSON([]byte(doc))
		if len(got) != 1 || got[0].Pointer != "/" {
			t.Errorf("%q: got %v", doc, got)
		}
	}
}

func TestIntegerAcceptsWholeFloats(t *testing.T) {
	s := compile(t)
	if got := s.ValidateJSON([]byte(`{"id": 2.0, "kind": "b"}`)); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
	if got := s.ValidateJSON([]byte(`{"id": 2.5, "kind": "b"}`)); len(got) != 1 {
		t.Fatalf("got %v", got)
	}
}

func TestPointerEscaping(t *testing.T) {
	s, err := Compile([]byte(`{"type":"object","additionalProperties":false}`))
	if err != nil {
		t.Fatal(err)
	}
	got := s.ValidateJSON([]byte(`{"a/b~c": 1}`))
	if len(got) != 1 || got[0].Pointer != "/a~1b~0c" {
		t.Fatalf("got %v", got)
	}
}

func TestCompileErrors(t *testing.T) {
	cases := map[string]string{
		"bad json":    `{`,
		"bad pattern": `{"pattern": "("}`,
		"bad ref":     `{"$ref": "#/$defs/nope"}`,
		"foreign ref": `{"$ref": "http://x/y"}`,
	}
	for name, doc := range cases {
		if _, err := Compile([]byte(doc)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
