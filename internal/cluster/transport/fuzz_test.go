package transport

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/happydave/hoardmq/internal/fuzzseed"
)

const fuzzMaxFrame = 64 << 10

func encodedFrame(f *testing.F, fr frame) []byte {
	f.Helper()
	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	if err := writeFrame(w, fr); err != nil {
		f.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		f.Fatal(err)
	}
	return b.Bytes()
}

func seedHello() hello {
	h := hello{cluster: "hoardmq", name: "n1", vmin: 1, vmax: Version5}
	copy(h.nonce[:], bytes.Repeat([]byte{7}, len(h.nonce)))
	return h
}

// FuzzReadFrame: an accepted frame is within the limit and re-encodes to
// the bytes it was read from.
func FuzzReadFrame(f *testing.F) {
	fuzzseed.Add(f, encodedFrame(f, frame{class: classHello, payload: seedHello().encode()}), true)
	fuzzseed.Add(f, encodedFrame(f, frame{class: classAuth, payload: bytes.Repeat([]byte{1}, 32)}), true)
	fuzzseed.Add(f, encodedFrame(f, frame{class: Consensus, payload: []byte("entries")}), false)
	fuzzseed.Add(f, encodedFrame(f, frame{class: Heartbeat}), false)
	fuzzseed.Add(f, []byte{0, 0, 0, 1, 0, 0}, false)
	fuzzseed.Add(f, []byte{0xff, 0xff, 0xff, 0xff, byte(Consensus), 0}, false)
	f.Fuzz(func(t *testing.T, raw []byte, handshake bool) {
		fr, err := readFrame(bytes.NewReader(raw), fuzzMaxFrame, handshake)
		if err != nil {
			return
		}
		if len(fr.payload) > fuzzMaxFrame {
			t.Fatalf("a %d-byte payload over the %d limit", len(fr.payload), fuzzMaxFrame)
		}
		var b bytes.Buffer
		w := bufio.NewWriter(&b)
		if err := writeFrame(w, fr); err != nil {
			t.Fatal(err)
		}
		_ = w.Flush()
		if n := headerSize + len(fr.payload); !bytes.Equal(b.Bytes(), raw[:n]) {
			t.Fatalf("re-encoded % x, read from % x", b.Bytes(), raw[:n])
		}
	})
}

// FuzzDecodeHello: an accepted hello re-encodes to its bytes.
func FuzzDecodeHello(f *testing.F) {
	fuzzseed.Add(f, seedHello().encode())
	h := seedHello()
	h.name = ""
	fuzzseed.Add(f, h.encode())
	fuzzseed.Add(f, []byte{0, 1})
	f.Fuzz(func(t *testing.T, raw []byte) {
		h, err := decodeHello(raw)
		if err != nil {
			return
		}
		if again := h.encode(); !bytes.Equal(again, raw) {
			t.Fatalf("re-encoded % x, read from % x", again, raw)
		}
	})
}
