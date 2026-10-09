package routing

// maxHoles bounds the ranges a receiver remembers as reported missing. A
// hole forgotten early only costs a late frame its reading: it is taken for
// a new stream, which rewinds the expectation and refuses one more publish.
const maxHoles = 64

// seqVerdict is what a forwarded frame's sequence says about the stream.
type seqVerdict int

const (
	seqInOrder seqVerdict = iota
	// seqGap: the frames from missingFrom to missingTo never arrived and are
	// reported before this frame's acknowledgement can cover them (WI 1337).
	seqGap
	// seqLate: a frame this receiver reported missing arrived after all.
	seqLate
	// seqRestart: a frame below the expectation that no reported hole
	// accounts for — the sender restarted its count.
	seqRestart
)

type seqRange struct{ lo, hi uint64 }

// inboundSeq judges one peer's forward sequence. Its expectation advances
// within a stream: a late frame fills its hole and leaves the expectation
// where it was, since rewinding it made the next frame report a hole that
// never existed (WI 1708). Only a new stream moves it back.
type inboundSeq struct {
	next  uint64 // the sequence the stream's next frame must carry
	seen  bool   // a frame of the current stream has arrived
	holes []seqRange
}

func newInboundSeq() inboundSeq { return inboundSeq{next: 1} }

// observe judges seq and advances the state. With ignoreGaps a frame past the
// expectation is taken as in order and nothing is reported: the defect
// IgnoreForwardGaps injects.
func (s *inboundSeq) observe(seq uint64, ignoreGaps bool) (v seqVerdict, missingFrom, missingTo uint64) {
	switch {
	case seq == s.next:
		v = seqInOrder
	case seq > s.next:
		if ignoreGaps {
			v = seqInOrder
			break
		}
		v, missingFrom, missingTo = seqGap, s.next, seq-1
		// The frames below a stream's first arrival were sent to another
		// incarnation of this node, or before it was listening; a hole over
		// them would swallow the first frames of the sender's next stream.
		if s.seen {
			s.addHole(seqRange{missingFrom, missingTo})
		}
	case s.fill(seq):
		s.seen = true
		return seqLate, 0, 0
	default:
		v = seqRestart
		s.holes = nil
	}
	s.next = seq + 1
	s.seen = true
	return v, missingFrom, missingTo
}

func (s *inboundSeq) addHole(r seqRange) {
	if len(s.holes) == maxHoles {
		s.holes = append(s.holes[:0], s.holes[1:]...)
	}
	s.holes = append(s.holes, r)
}

// fill removes seq from the hole holding it and reports whether one did.
func (s *inboundSeq) fill(seq uint64) bool {
	for i, h := range s.holes {
		if seq < h.lo || seq > h.hi {
			continue
		}
		switch {
		case h.lo == h.hi:
			s.holes = append(s.holes[:i], s.holes[i+1:]...)
		case seq == h.lo:
			s.holes[i].lo++
		case seq == h.hi:
			s.holes[i].hi--
		default:
			s.holes[i].hi = seq - 1
			s.addHole(seqRange{seq + 1, h.hi})
		}
		return true
	}
	return false
}
