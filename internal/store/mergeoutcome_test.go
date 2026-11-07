package store

import "testing"

func TestMergeOutcome(t *testing.T) {
	v := Version{E: 2, N: 5}
	for _, c := range []struct {
		installed bool
		held, in  Version
		want      string
	}{
		{true, Version{}, v, "newer"},
		{true, Version{E: 1, N: 9}, v, "newer"},
		{false, v, v, "equal"},
		{false, Version{}, Version{}, "equal"},
		{false, Version{E: 3, N: 1}, v, "older"},
	} {
		if got := MergeOutcome(c.installed, c.held, c.in); got != c.want {
			t.Errorf("MergeOutcome(%v, %v, %v) = %q, want %q", c.installed, c.held, c.in, got, c.want)
		}
	}
}

// TestUnversionedRefused: an unversioned write is refused over a stamped
// message or tombstone, and taken into an empty topic or over an unversioned
// entry.
func TestUnversionedRefused(t *testing.T) {
	for _, c := range []struct {
		held   Version
		exists bool
		want   bool
	}{
		{Version{E: 2, N: 5}, true, true},
		{Version{E: 0, N: 1}, true, true},
		{Version{E: 1, N: 0}, true, true},
		{Version{}, true, false},
		{Version{}, false, false},
	} {
		if got := UnversionedRefused(c.held, c.exists); got != c.want {
			t.Errorf("UnversionedRefused(%v, %v) = %v, want %v", c.held, c.exists, got, c.want)
		}
	}
}
