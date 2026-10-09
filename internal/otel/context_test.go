package otel

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

const (
	validTrace  = "4bf92f3577b34da6a3ce929d0e0e4736"
	validParent = "00f067aa0ba902b7"
	validTP     = "00-" + validTrace + "-" + validParent + "-01"
)

func TestParseTraceparent(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		ok      bool
		sampled bool
	}{
		{"valid, sampled", validTP, true, true},
		{"valid, not sampled", "00-" + validTrace + "-" + validParent + "-00", true, false},
		{"level 2 random bit", "00-" + validTrace + "-" + validParent + "-03", true, true},
		{"upper-case hex", "00-" + strings.ToUpper(validTrace) + "-" + validParent + "-01", false, false},
		{"version 00, 54 characters", validTP[:54], false, false},
		{"version 00, 56 characters", validTP + "0", false, false},
		{"version 00 with more after a dash", validTP + "-x", false, false},
		{"version ff", "ff" + validTP[2:], false, false},
		{"all-zero trace id", "00-" + strings.Repeat("0", 32) + "-" + validParent + "-01", false, false},
		{"all-zero parent id", "00-" + validTrace + "-" + strings.Repeat("0", 16) + "-01", false, false},
		{"a non-hex character", "00-" + validTrace[:31] + "g-" + validParent + "-01", false, false},
		{"a missing dash", "00-" + validTrace + "x" + validParent + "-01", false, false},
		{"a later version with more", "01-" + validTrace + "-" + validParent + "-01-extra", true, true},
		{"a later version run on", "01-" + validTrace + "-" + validParent + "-01extra", false, false},
	}
	for _, tc := range cases {
		c, ok := ParseTraceparent(tc.in)
		if ok != tc.ok {
			t.Errorf("%s: parsed %v, want %v", tc.name, ok, tc.ok)
			continue
		}
		if !ok {
			continue
		}
		if c.Sampled() != tc.sampled {
			t.Errorf("%s: sampled %v, want %v", tc.name, c.Sampled(), tc.sampled)
		}
		if got := string(appendHexString(c.TraceID[:])); got != validTrace {
			t.Errorf("%s: trace id %s", tc.name, got)
		}
		if got := string(appendHexString(c.SpanID[:])); got != validParent {
			t.Errorf("%s: parent id %s", tc.name, got)
		}
	}
	c, _ := ParseTraceparent(validTP)
	if got := string(c.AppendTraceparent(nil)); got != validTP {
		t.Errorf("round trip %q, want %q", got, validTP)
	}
}

func appendHexString(b []byte) []byte { return []byte(hex.EncodeToString(b)) }

func TestFromUserProperties(t *testing.T) {
	long := strings.Repeat("a", 513)
	cases := []struct {
		name  string
		props []packet.UserProperty
		ok    bool
		state string
	}{
		{"traceparent second of three", []packet.UserProperty{{Name: "a", Value: "1"}, {Name: "traceparent", Value: validTP}, {Name: "z", Value: "2"}}, true, ""},
		{"a differently cased name", []packet.UserProperty{{Name: "TraceParent", Value: validTP}}, false, ""},
		{"a valid tracestate", []packet.UserProperty{{Name: "tracestate", Value: "k=v"}, {Name: "traceparent", Value: validTP}}, true, "k=v"},
		{"a 513-character tracestate", []packet.UserProperty{{Name: "traceparent", Value: validTP}, {Name: "tracestate", Value: long}}, true, ""},
		{"a tracestate with a control byte", []packet.UserProperty{{Name: "traceparent", Value: validTP}, {Name: "tracestate", Value: "k=v\n"}}, true, ""},
		{"a tracestate beside an invalid traceparent", []packet.UserProperty{{Name: "traceparent", Value: "junk"}, {Name: "tracestate", Value: "k=v"}}, false, ""},
		{"only the first traceparent counts", []packet.UserProperty{{Name: "traceparent", Value: "junk"}, {Name: "traceparent", Value: validTP}}, false, ""},
	}
	for _, tc := range cases {
		c, ok := FromUserProperties(tc.props)
		if ok != tc.ok || c.State != tc.state {
			t.Errorf("%s: ok %v state %q, want %v %q", tc.name, ok, c.State, tc.ok, tc.state)
		}
	}
	if c, _ := FromUserProperties([]packet.UserProperty{{Name: "traceparent", Value: validTP}, {Name: "tracestate", Value: strings.Repeat("b", 512)}}); len(c.State) != 512 {
		t.Errorf("a 512-character tracestate was not kept: %d", len(c.State))
	}
}
