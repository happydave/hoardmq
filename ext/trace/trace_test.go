package trace

import (
	"math"
	"testing"
)

func TestSampleDecision(t *testing.T) {
	at := func(f float64) uint64 { return uint64(math.Ldexp(f, 64)) }
	cases := []struct {
		name                     string
		hasParent, parentSampled bool
		ratio                    float64
		draw                     uint64
		want                     bool
	}{
		{"sampled parent, ratio 0", true, true, 0, at(0.9), true},
		{"unsampled parent, ratio 1", true, false, 1, 0, false},
		{"no parent, draw below the ratio", false, false, 0.25, at(0.2), true},
		{"no parent, draw above the ratio", false, false, 0.25, at(0.3), false},
		{"no parent, ratio 0, draw 0", false, false, 0, 0, false},
		{"no parent, ratio 1, top draw", false, false, 1, math.MaxUint64, true},
	}
	for _, tc := range cases {
		if got := sampleDecision(tc.hasParent, tc.parentSampled, tc.ratio, tc.draw); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}
