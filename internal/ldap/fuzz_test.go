package ldap

import (
	"bytes"
	"testing"

	"github.com/happydave/hoardmq/internal/fuzzseed"
)

func fuzzResponses() [][]byte {
	return [][]byte{
		bindResponse(9, 49),
		message(2, element(classApplication, true, opSearchEntry, concat([][]byte{octets([]byte("uid=a,dc=x")), seq()}))),
		message(0, element(classApplication, true, opExtendedResult, concat([][]byte{integer(tagEnumerated, classUniversal, 52), octets(nil), octets(nil), element(classContext, false, 10, []byte(noticeOfDisconnectN))}))),
		message(3, element(classApplication, true, opSearchDone, concat([][]byte{integer(tagEnumerated, classUniversal, 0), octets(nil), octets(nil)}))),
		append([]byte{0x30, 0x83, 0x20, 0x00, 0x00}, make([]byte, 16)...),
		{0x30, 0x84, 0xff, 0xff, 0xff, 0xff},
	}
}

// FuzzReadMessage: an accepted message is a prefix of the stream, within
// the bound.
func FuzzReadMessage(f *testing.F) {
	for _, b := range fuzzResponses() {
		fuzzseed.Add(f, b)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		got, err := readMessage(bufioReader(raw), MaxMessage)
		if err != nil {
			return
		}
		if len(got) > MaxMessage || !bytes.HasPrefix(raw, got) {
			t.Fatalf("read %d bytes, not a prefix of the %d sent within %d", len(got), len(raw), MaxMessage)
		}
	})
}

// FuzzParseResponse: no input panics; an accepted response is one of the
// operations it decodes.
func FuzzParseResponse(f *testing.F) {
	for _, b := range fuzzResponses() {
		fuzzseed.Add(f, b)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		r, err := ParseResponse(raw)
		if err != nil {
			return
		}
		switch r.Op {
		case opSearchEntry, opSearchRef, opBindResponse, opSearchDone, opExtendedResult:
		default:
			t.Fatalf("accepted operation %d", r.Op)
		}
	})
}
