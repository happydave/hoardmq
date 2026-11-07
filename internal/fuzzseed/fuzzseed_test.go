package fuzzseed

import "testing"

func TestEncode(t *testing.T) {
	got, err := Encode([]byte("a\x00\"b"), "s\n", true)
	want := "go test fuzz v1\n[]byte(\"a\\x00\\\"b\")\nstring(\"s\\n\")\nbool(true)\n"
	if err != nil || string(got) != want {
		t.Fatalf("%q %v, want %q", got, err, want)
	}
	if _, err := Encode(1); err == nil {
		t.Fatal("an int seed was encoded")
	}
}
