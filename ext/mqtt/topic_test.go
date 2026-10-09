package mqtt

import "testing"

func TestMatchTopic(t *testing.T) {
	cases := []struct {
		filter, name string
		want         bool
	}{
		{"+/x", "$SYS/x", false},
		{"#", "$SYS", false},
		{"#", "$SYS/x", false},
		{"$SYS/#", "$SYS/x", true},
		{"a/#", "a", true},
		{"a/#", "a/b/c", true},
		{"a/+", "a", false},
		{"a/+/c", "a/b/c", true},
		{"a/+/c", "a/b/d", false},
		{"a/b", "a/b/c", false},
		{"a/b", "a/b", true},
		{"+", "", false},
		{"#", "", false},
		{"sensors/+/temp", "sensors/a/temp", true},
	}
	for _, c := range cases {
		if got := MatchTopic(c.filter, c.name); got != c.want {
			t.Errorf("MatchTopic(%q, %q) = %t, want %t", c.filter, c.name, got, c.want)
		}
	}
}

func TestValidateTopicFilterAndName(t *testing.T) {
	for _, f := range []string{"a", "a/+/b", "a/#", "#", "$share/g/a/#"} {
		if msg := ValidateTopicFilter(f); msg != "" {
			t.Errorf("filter %q refused: %s", f, msg)
		}
	}
	for _, f := range []string{"", "a/#/b", "a+", "$share/g", "$share/+/a"} {
		if ValidateTopicFilter(f) == "" {
			t.Errorf("filter %q accepted", f)
		}
	}
	if ValidateTopicName("a/b") != "" || ValidateTopicName("") == "" || ValidateTopicName("a/+") == "" {
		t.Error("topic name rules")
	}
	if share, inner := SplitSharedFilter("$share/g/a/b"); share != "g" || inner != "a/b" || !IsSharedFilter("$share/g/a") || HasWildcard("a/b") {
		t.Errorf("shared split %q %q", share, inner)
	}
}
