package sim

import (
	"testing"
	"time"
)

func TestCrashAtReadsWholeSecondsOnly(t *testing.T) {
	for _, tc := range []struct {
		v    string
		want time.Duration
		bad  bool
	}{{"", 0, false}, {"26", 26 * time.Second, false}, {"0", 0, true}, {"-3", 0, true}, {"2.5", 0, true}, {"x", 0, true}} {
		got, err := crashAt(func(string) string { return tc.v })
		if (err != nil) != tc.bad || got != tc.want {
			t.Errorf("crashAt(%q) = %v, %v; want %v, error %v", tc.v, got, err, tc.want, tc.bad)
		}
	}
}

func TestCrashWaitHoldsToTheAskedPointOrRefuses(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	if w, err := crashWait(start, start.Add(21500*time.Millisecond), 26*time.Second); err != nil || w != 4500*time.Millisecond {
		t.Fatalf("crashWait at 21.5s for 26s = %v, %v; want 4.5s", w, err)
	}
	if w, err := crashWait(start, start.Add(26*time.Second), 26*time.Second); err != nil || w != 0 {
		t.Fatalf("crashWait exactly at 26s = %v, %v; want 0", w, err)
	}
	if _, err := crashWait(start, start.Add(26100*time.Millisecond), 26*time.Second); err == nil {
		t.Fatal("crashWait past the asked point returned no error")
	}
}
