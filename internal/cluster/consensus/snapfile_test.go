package consensus

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// streamComp is a streaming component holding a list of strings.
type streamComp struct {
	items []string
	enc   Encoding
}

func (s *streamComp) Apply(_ uint64, p []byte) (any, error) {
	s.items = append(s.items, string(p))
	return nil, nil
}
func (s *streamComp) Snapshot() ([]byte, error) { return []byte(strings.Join(s.items, ",")), nil }
func (s *streamComp) Restore(d []byte) error {
	s.items = nil
	if len(d) > 0 {
		s.items = strings.Split(string(d), ",")
	}
	return nil
}

type streamView struct {
	items []string
	enc   Encoding
}

func (v *streamView) Encoding() Encoding { return v.enc }
func (v *streamView) WriteRecords(emit func([]byte) error) error {
	for _, it := range v.items {
		if err := emit([]byte(it)); err != nil {
			return err
		}
	}
	return nil
}

func (s *streamComp) Capture() (SectionView, error) {
	enc := s.enc
	if enc == 0 {
		enc = EncodingJSON
	}
	return &streamView{items: append([]string(nil), s.items...), enc: enc}, nil
}

type stagedItems struct {
	s     *streamComp
	items []string
}

func (st *stagedItems) Install() { st.s.items = st.items }

func (s *streamComp) Stage(e Encoding, next func() ([]byte, bool, error)) (Staged, error) {
	if e != EncodingJSON {
		return nil, fmt.Errorf("%w: encoding %d", ErrSnapshotFormat, e)
	}
	st := &stagedItems{s: s}
	for {
		rec, ok, err := next()
		if err != nil {
			return nil, err
		}
		if !ok {
			return st, nil
		}
		st.items = append(st.items, string(rec))
	}
}

// blobComp is a component without the streaming capability.
type blobComp struct{ v string }

func (b *blobComp) Apply(_ uint64, p []byte) (any, error) { b.v = string(p); return nil, nil }
func (b *blobComp) Snapshot() ([]byte, error)             { return []byte(b.v), nil }
func (b *blobComp) Restore(d []byte) error                { b.v = string(d); return nil }

func newTestMachine() (*Machine, *streamComp, *blobComp) {
	m := NewMachine()
	s, b := &streamComp{}, &blobComp{}
	m.Register(KindReserved, s)
	m.Register(KindReserved+1, b)
	return m, s, b
}

// writeCapture captures m and writes it as a file.
func writeCapture(t *testing.T, m *Machine, hd FileHeader) []byte {
	t.Helper()
	c, err := m.Capture()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	fw, err := NewFileWriter(&buf, hd)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Write(fw); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func stageBytes(m *Machine, data []byte) (*StagedMachine, error) {
	fr, hd, err := NewFileReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return m.Stage(fr, hd)
}

// TestSnapshotFileRoundTrip: a streamed section, a blob section, the
// membership, the configuration and the applied index come back as they were
// captured, and a later apply does not change what was captured.
func TestSnapshotFileRoundTrip(t *testing.T) {
	m, s, b := newTestMachine()
	_, _ = m.Apply(1, Encode(KindReserved, []byte("a")))
	_, _ = m.Apply(2, Encode(KindReserved, []byte("b")))
	_, _ = m.Apply(3, Encode(KindReserved+1, []byte("blob")))
	_, _ = m.Apply(4, ConfigCommand("k", "v"))
	m.Membership().Upsert(Meta{Name: "n1", ID: 7, Address: "x"})
	c, err := m.Capture()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = m.Apply(5, Encode(KindReserved, []byte("after")))
	var buf bytes.Buffer
	fw, _ := NewFileWriter(&buf, FileHeader{Index: 4, Term: 2, Applied: c.Applied(), ConfState: []byte{9, 9}})
	if err := c.Write(fw); err != nil {
		t.Fatal(err)
	}
	size, digest, err := fw.Close()
	if err != nil {
		t.Fatal(err)
	}
	if size != uint64(buf.Len()) {
		t.Fatalf("size %d, wrote %d", size, buf.Len())
	}

	m2, s2, b2 := newTestMachine()
	fr, hd, err := NewFileReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if hd.Index != 4 || hd.Term != 2 || hd.Applied != 4 || !bytes.Equal(hd.ConfState, []byte{9, 9}) {
		t.Fatalf("header %+v", hd)
	}
	st, err := m2.Stage(fr, hd)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := fr.Digest(); !ok || got != digest {
		t.Fatal("digest not reported or different")
	}
	if len(s2.items) != 0 {
		t.Fatal("staging touched the live state")
	}
	if err := st.Install(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(s2.items, ",") != "a,b" || b2.v != "blob" || m2.Applied() != 4 {
		t.Fatalf("installed items %v blob %q applied %d; source holds %v", s2.items, b2.v, m2.Applied(), s.items)
	}
	if v, _ := m2.ConfigValue("k"); v != "v" {
		t.Fatalf("config %q", v)
	}
	if mem, ok := m2.Membership().Lookup(7); !ok || mem.Name != "n1" {
		t.Fatalf("membership %+v %v", mem, ok)
	}
	_ = b
}

// TestEmptyRecordsRoundTrip: a component whose bytes are empty and a
// streamed section with no records come back as they were.
func TestEmptyRecordsRoundTrip(t *testing.T) {
	m, _, b := newTestMachine()
	b.v = ""
	data := writeCapture(t, m, FileHeader{Index: 1, Term: 1})
	fresh, s, b2 := newTestMachine()
	s.items, b2.v = []string{"stale"}, "stale"
	st, err := stageBytes(fresh, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Install(); err != nil {
		t.Fatal(err)
	}
	if len(s.items) != 0 || b2.v != "" {
		t.Fatalf("installed %v and %q, want both empty", s.items, b2.v)
	}
}

// TestUnknownSectionRoundTrips: a section no component claims is kept as its
// records and written back, and a component registered later takes it.
func TestUnknownSectionRoundTrips(t *testing.T) {
	m, _, _ := newTestMachine()
	extra := &streamComp{items: []string{"x", "y"}}
	m.Register(KindReserved+5, extra)
	data := writeCapture(t, m, FileHeader{Index: 1, Term: 1})

	lacks, _, _ := newTestMachine()
	st, err := stageBytes(lacks, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Install(); err != nil {
		t.Fatal(err)
	}
	again := writeCapture(t, lacks, FileHeader{Index: 1, Term: 1})
	late := &streamComp{}
	has, _, _ := newTestMachine()
	st, err = stageBytes(has, again)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Install(); err != nil {
		t.Fatal(err)
	}
	has.Register(KindReserved+5, late)
	if strings.Join(late.items, ",") != "x,y" {
		t.Fatalf("late component got %v", late.items)
	}
}

// TestSnapshotFileRefusals: every malformed file is refused before anything
// is installed, each for its own reason.
func TestSnapshotFileRefusals(t *testing.T) {
	m, _, _ := newTestMachine()
	_, _ = m.Apply(1, Encode(KindReserved, []byte("a")))
	good := writeCapture(t, m, FileHeader{Index: 1, Term: 1})
	// The first section starts after the header: magic, format, three
	// integers and an empty configuration state.
	first := 4 + 1 + 24 + 4
	mutate := func(f func(b []byte) []byte) []byte { return f(append([]byte(nil), good...)) }
	cases := []struct {
		name string
		data []byte
		want string
	}{
		{"bad magic", mutate(func(b []byte) []byte { b[0] = 'X'; return b }), "not a HoardMQ snapshot"},
		{"format 3", mutate(func(b []byte) []byte { b[4] = 3; return b }), "format 3 not supported"},
		{"unknown encoding", mutate(func(b []byte) []byte { b[first+1] = 9; return b }), "encoding 9, not supported"},
		{"record over the bound", mutate(func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[first+2:], MaxSnapshotRecord+1)
			return b
		}), "exceeds"},
		{"truncated", good[:len(good)-40], "truncated"},
		{"digest mismatch", mutate(func(b []byte) []byte { b[len(b)-1] ^= 1; return b }), "digest mismatch"},
		{"content changed", mutate(func(b []byte) []byte { b[len(b)-40] ^= 1; return b }), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fresh, s, _ := newTestMachine()
			st, err := stageBytes(fresh, tc.data)
			if err == nil {
				t.Fatalf("staged %v", st)
			}
			if !errors.Is(err, ErrSnapshotFormat) && tc.want != "" {
				t.Fatalf("error %v is not a format refusal", err)
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q, want %q", err, tc.want)
			}
			if len(s.items) != 0 {
				t.Fatal("refusal touched the live state")
			}
		})
	}
}

// TestUnknownEncodingInAnUnknownSectionIsRefused: a section no component
// claims is kept as its records, but only in an encoding this build knows.
func TestUnknownEncodingInAnUnknownSectionIsRefused(t *testing.T) {
	var buf bytes.Buffer
	fw, _ := NewFileWriter(&buf, FileHeader{Index: 1, Term: 1})
	for _, k := range []Kind{KindMembership, KindReserved + 30} {
		if err := fw.Section(k, EncodingBlob); err != nil {
			t.Fatal(err)
		}
		if err := fw.Record([]byte("[]")); err != nil {
			t.Fatal(err)
		}
		if err := fw.EndSection(); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := fw.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	// The second section's encoding byte: after the header, the membership
	// section's kind, encoding, record length, record and terminator, then
	// the second section's kind.
	pos := 4 + 1 + 24 + 4 + 2 + 4 + 2 + 4 + 1
	if Kind(data[pos-1]) != KindReserved+30 || Encoding(data[pos]) != EncodingBlob {
		t.Fatalf("the offset does not find the second section: %d %d", data[pos-1], data[pos])
	}
	data[pos] = 9
	m, _, _ := newTestMachine()
	if _, err := stageBytes(m, data); err == nil || !strings.Contains(err.Error(), "encoding 9, not supported") {
		t.Fatalf("an unknown section's unknown encoding: %v", err)
	}
}

// TestComponentRefusesAnEncodingItDoesNotTake: a known encoding a component
// does not take is refused at staging.
func TestComponentRefusesAnEncodingItDoesNotTake(t *testing.T) {
	m, s, _ := newTestMachine()
	s.enc = EncodingCompact
	_, _ = m.Apply(1, Encode(KindReserved, []byte("a")))
	data := writeCapture(t, m, FileHeader{Index: 1, Term: 1})
	fresh, _, _ := newTestMachine()
	if _, err := stageBytes(fresh, data); err == nil || !strings.Contains(err.Error(), "encoding 3") {
		t.Fatalf("staged an encoding the component does not take: %v", err)
	}
}

// TestFormatOneStillRestores: an older build's inline document restores.
func TestFormatOneStillRestores(t *testing.T) {
	m, _, _ := newTestMachine()
	_, _ = m.Apply(1, Encode(KindReserved, []byte("a")))
	_, _ = m.Apply(2, Encode(KindReserved+1, []byte("b")))
	doc, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	fresh, s, b := newTestMachine()
	if err := fresh.Restore(doc); err != nil {
		t.Fatal(err)
	}
	if strings.Join(s.items, ",") != "a" || b.v != "b" || fresh.Applied() != 2 {
		t.Fatalf("restored %v %q %d", s.items, b.v, fresh.Applied())
	}
}
