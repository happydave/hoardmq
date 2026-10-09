package routing

import (
	"fmt"
	"reflect"
	"testing"
)

type seqStep struct {
	seq      uint64
	want     seqVerdict
	from, to uint64 // the missing range, for a gap
}

func TestInboundSeq(t *testing.T) {
	cases := []struct {
		name       string
		ignoreGaps bool
		steps      []seqStep
		next       uint64
		holes      []seqRange
	}{
		{"in order", false, []seqStep{{1, seqInOrder, 0, 0}, {2, seqInOrder, 0, 0}, {3, seqInOrder, 0, 0}}, 4, nil},
		{"a late frame does not lower the expectation", false, []seqStep{
			{1, seqInOrder, 0, 0}, {3, seqGap, 2, 2}, {2, seqLate, 0, 0}, {4, seqInOrder, 0, 0},
		}, 5, nil},
		{"two holes filled late, the later first", false, []seqStep{
			{1, seqInOrder, 0, 0}, {3, seqGap, 2, 2}, {5, seqGap, 4, 4}, {4, seqLate, 0, 0}, {2, seqLate, 0, 0}, {6, seqInOrder, 0, 0},
		}, 7, nil},
		{"a late frame splits a wide hole", false, []seqStep{
			{1, seqInOrder, 0, 0}, {10, seqGap, 2, 9}, {5, seqLate, 0, 0}, {2, seqLate, 0, 0}, {9, seqLate, 0, 0},
		}, 11, []seqRange{{3, 4}, {6, 8}}},
		{"a real loss stays a hole", false, []seqStep{{1, seqInOrder, 0, 0}, {3, seqGap, 2, 2}, {4, seqInOrder, 0, 0}}, 5, []seqRange{{2, 2}}},
		{"a first frame above one is a gap and opens no hole", false, []seqStep{
			{50, seqGap, 1, 49}, {51, seqInOrder, 0, 0},
		}, 52, nil},
		{"below a first-frame gap is a new stream, and its losses are reported", false, []seqStep{
			{50, seqGap, 1, 49}, {51, seqInOrder, 0, 0}, {1, seqRestart, 0, 0}, {2, seqInOrder, 0, 0}, {4, seqGap, 3, 3},
		}, 5, []seqRange{{3, 3}}},
		{"below the expectation in no hole is a new stream, and clears the holes", false, []seqStep{
			{1, seqInOrder, 0, 0}, {5, seqGap, 2, 4}, {6, seqInOrder, 0, 0}, {1, seqRestart, 0, 0}, {3, seqGap, 2, 2},
		}, 4, []seqRange{{2, 2}}},
		{"a frame that already arrived is a new stream", false, []seqStep{
			{1, seqInOrder, 0, 0}, {2, seqInOrder, 0, 0}, {1, seqRestart, 0, 0},
		}, 2, nil},
		{"ignoring gaps reports none and opens no hole", true, []seqStep{
			{1, seqInOrder, 0, 0}, {3, seqInOrder, 0, 0}, {2, seqRestart, 0, 0},
		}, 3, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newInboundSeq()
			for i, st := range tc.steps {
				v, from, to := s.observe(st.seq, tc.ignoreGaps)
				if v != st.want || from != st.from || to != st.to {
					t.Fatalf("step %d, seq %d: got %v [%d,%d], want %v [%d,%d]; state %+v", i, st.seq, v, from, to, st.want, st.from, st.to, s)
				}
			}
			if s.next != tc.next {
				t.Errorf("next %d, want %d", s.next, tc.next)
			}
			if len(s.holes) != len(tc.holes) || (len(tc.holes) > 0 && !reflect.DeepEqual(s.holes, tc.holes)) {
				t.Errorf("holes %v, want %v", s.holes, tc.holes)
			}
		})
	}
}

// TestInboundSeqForgetsItsOldestHole: past the cap the oldest hole goes, and
// a late frame for it is then read as a new stream — a rewind, which refuses
// one more publish rather than hiding a loss.
func TestInboundSeqForgetsItsOldestHole(t *testing.T) {
	s := newInboundSeq()
	s.observe(1, false)
	for i := 0; i <= maxHoles; i++ {
		seq := uint64(3 + 2*i) // leaves 2+2i missing
		if v, _, _ := s.observe(seq, false); v != seqGap {
			t.Fatalf("seq %d: %v, want a gap", seq, v)
		}
	}
	if len(s.holes) != maxHoles {
		t.Fatalf("%d holes kept, want %d", len(s.holes), maxHoles)
	}
	if v, _, _ := s.observe(4, false); v != seqLate {
		t.Fatalf("the second-oldest hole: %v, want late", v)
	}
	if v, _, _ := s.observe(2, false); v != seqRestart {
		t.Fatalf("the forgotten hole: %v, want a new stream", v)
	}
}

func (v seqVerdict) String() string {
	switch v {
	case seqInOrder:
		return "in-order"
	case seqGap:
		return "gap"
	case seqLate:
		return "late"
	case seqRestart:
		return "restart"
	}
	return fmt.Sprintf("verdict(%d)", int(v))
}
