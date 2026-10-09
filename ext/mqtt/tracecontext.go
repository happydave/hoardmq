package mqtt

// The W3C trace context a PUBLISH may carry in its user properties, and the
// envelope's record of the span routing it.

// SpanContext identifies a span within a trace, with the W3C trace flags and
// the trace's tracestate.
type SpanContext struct {
	TraceID [16]byte
	SpanID  [8]byte
	Flags   byte
	State   string
}

// Valid reports whether both identifiers are non-zero.
func (c SpanContext) Valid() bool {
	return c.TraceID != [16]byte{} && c.SpanID != [8]byte{}
}

// Sampled reports the W3C sampled flag.
func (c SpanContext) Sampled() bool { return c.Flags&flagSampled != 0 }

const flagSampled = 0x01

// traceparentLen is a version 00 traceparent's length, and the least any
// version may have.
const traceparentLen = 55

// ParseTraceparent reads a W3C traceparent. Version ff, an all-zero trace or
// parent id, upper-case hex and any malformed value are not a parent; a later
// version is read at version 00's offsets and whatever follows is ignored.
func ParseTraceparent(s string) (SpanContext, bool) {
	var c SpanContext
	if len(s) < traceparentLen {
		return c, false
	}
	ver, ok := hexByte(s[0], s[1])
	if !ok || ver == 0xff {
		return c, false
	}
	if ver == 0 && len(s) != traceparentLen {
		return c, false
	}
	if len(s) > traceparentLen && s[traceparentLen] != '-' {
		return c, false
	}
	if s[2] != '-' || s[35] != '-' || s[52] != '-' {
		return c, false
	}
	if !decodeHex(c.TraceID[:], s[3:35]) || !decodeHex(c.SpanID[:], s[36:52]) {
		return SpanContext{}, false
	}
	flags, ok := hexByte(s[53], s[54])
	if !ok || !c.Valid() {
		return SpanContext{}, false
	}
	c.Flags = flags
	return c, true
}

// AppendTraceparent appends the context as a version 00 traceparent. Only the
// sampled flag is written: the other bits are reserved to whoever sets them.
func (c SpanContext) AppendTraceparent(b []byte) []byte {
	b = append(b, "00-"...)
	b = appendHex(b, c.TraceID[:])
	b = append(b, '-')
	b = appendHex(b, c.SpanID[:])
	b = append(b, '-', '0')
	if c.Sampled() {
		return append(b, '1')
	}
	return append(b, '0')
}

// maxTraceState is the length of tracestate a propagator must keep; a longer
// value is dropped rather than cut, since a cut would split a member.
const maxTraceState = 512

// FromUserProperties reads the trace context a PUBLISH carries: the first user
// property named traceparent and, only when that parses, the first named
// tracestate, kept when it is printable ASCII of at most 512 bytes. It reads
// the properties and changes nothing.
func FromUserProperties(props []UserProperty) (SpanContext, bool) {
	var c SpanContext
	found := false
	for i := range props {
		if props[i].Name == "traceparent" {
			c, found = ParseTraceparent(props[i].Value)
			break
		}
	}
	if !found {
		return SpanContext{}, false
	}
	for i := range props {
		if props[i].Name == "tracestate" {
			if validTraceState(props[i].Value) {
				c.State = props[i].Value
			}
			break
		}
	}
	return c, true
}

func validTraceState(s string) bool {
	if len(s) > maxTraceState {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

const hexDigits = "0123456789abcdef"

func appendHex(b []byte, src []byte) []byte {
	for _, v := range src {
		b = append(b, hexDigits[v>>4], hexDigits[v&0x0f])
	}
	return b
}

// lowerHex is the value of a lower-case hex digit; W3C trace context allows
// no other case.
func lowerHex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	}
	return 0, false
}

func hexByte(hi, lo byte) (byte, bool) {
	h, ok1 := lowerHex(hi)
	l, ok2 := lowerHex(lo)
	return h<<4 | l, ok1 && ok2
}

func decodeHex(dst []byte, s string) bool {
	for i := range dst {
		v, ok := hexByte(s[2*i], s[2*i+1])
		if !ok {
			return false
		}
		dst[i] = v
	}
	return true
}
