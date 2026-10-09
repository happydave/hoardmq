package topic

import "testing"

func TestValidateFilter(t *testing.T) {
	valid := []string{"a", "a/b", "+", "#", "a/+/b", "a/#", "+/#", "/", "a/", "/a", "$SYS/#", "$share/g/a/#", "$share/g/+"}
	for _, f := range valid {
		if msg := ValidateFilter(f); msg != "" {
			t.Errorf("%q: %s", f, msg)
		}
	}
	invalid := []string{"", "a/#/b", "#/a", "a+", "a#", "a/b+", "+a", "$share/", "$share/g", "$share//a", "$share/+/a", "$share/g/a/#/b"}
	for _, f := range invalid {
		if ValidateFilter(f) == "" {
			t.Errorf("%q accepted", f)
		}
	}
}

func TestValidateName(t *testing.T) {
	if ValidateName("a/b") != "" || ValidateName("/") != "" || ValidateName("$SYS/x") != "" {
		t.Error("valid names rejected")
	}
	if ValidateName("") == "" || ValidateName("a/+") == "" || ValidateName("#") == "" {
		t.Error("invalid names accepted")
	}
}

func TestMatch(t *testing.T) {
	cases := []struct {
		filter, name string
		want         bool
	}{
		{"a/b", "a/b", true}, {"a/b", "a/c", false}, {"a/b", "a/b/c", false}, {"a/b/c", "a/b", false},
		{"+", "a", true}, {"+", "a/b", false}, {"+/b", "a/b", true}, {"a/+", "a/b", true}, {"a/+", "a", false},
		{"#", "a", true}, {"#", "a/b/c", true}, {"a/#", "a", true}, {"a/#", "a/b/c", true}, {"a/#", "b", false},
		{"a/+/#", "a/b", true}, {"a/+/#", "a/b/c/d", true}, {"a/+/#", "a", false},
		{"+/+", "/x", true}, {"/+", "/x", true}, {"+", "/x", false}, {"a/", "a/", true}, {"a/+", "a/", true},
		{"#", "$SYS/a", false}, {"+/a", "$SYS/a", false}, {"$SYS/#", "$SYS/a", true}, {"$SYS/+", "$SYS/a", true},
		{"a", "", false},
		{"sport/tennis/player1/#", "sport/tennis/player1", true},
		{"sport/tennis/player1/#", "sport/tennis/player1/ranking", true},
		{"sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon", true},
		{"sport/tennis/+", "sport/tennis/player1", true},
		{"sport/tennis/+", "sport/tennis/player1/ranking", false},
		{"sport/+", "sport", false}, {"sport/+", "sport/", true},
	}
	for _, c := range cases {
		if got := Match(c.filter, c.name); got != c.want {
			t.Errorf("Match(%q, %q) = %v, want %v", c.filter, c.name, got, c.want)
		}
	}
}

func TestShared(t *testing.T) {
	if !IsShared("$share/g/a") || IsShared("$shares/g/a") || IsShared("a") {
		t.Error("IsShared")
	}
	if s, f := SplitShared("$share/g/a/#"); s != "g" || f != "a/#" {
		t.Errorf("SplitShared: %q %q", s, f)
	}
	if s, f := SplitShared("a/#"); s != "" || f != "a/#" {
		t.Errorf("SplitShared plain: %q %q", s, f)
	}
	if !HasWildcard("a/+") || HasWildcard("a/b") {
		t.Error("HasWildcard")
	}
}
