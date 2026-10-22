package consensus

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
)

// Encoding is how a snapshot section's records are written.
type Encoding byte

// Section encodings. A reader refuses one it does not know.
const (
	// EncodingBlob is one record holding a component's Snapshot bytes.
	EncodingBlob Encoding = 1
	// EncodingJSON is a stream of JSON records, one per entry.
	EncodingJSON Encoding = 2
	// EncodingCompact is a stream of binary records, one per entry.
	EncodingCompact Encoding = 3
)

func (e Encoding) known() bool { return e >= EncodingBlob && e <= EncodingCompact }

// KindMembership is the section that holds the machine's member table. The
// package reserves the kinds below KindReserved; 1 and 2 are taken.
const KindMembership Kind = 3

// SnapshotMagic opens both a snapshot file and a stored snapshot's data;
// the byte after it is the format.
var SnapshotMagic = []byte("HQRS")

// FileFormat is the snapshot file's format byte.
const FileFormat = 2

// MaxSnapshotRecord bounds one record: above MQTT's largest packet as base64
// in JSON, since one lease with its Will is one record.
const MaxSnapshotRecord = 512 << 20

// sectionEnd closes a section's records; a zero kind closes the sections.
const sectionEnd = 0xFFFFFFFF

// ErrSnapshotFormat is wrapped by every refusal of a snapshot file's content.
var ErrSnapshotFormat = errors.New("snapshot file")

// FileHeader is what a snapshot file says about itself: the log position it
// was taken at, the machine's applied index and the configuration state as
// bytes the shell encodes.
type FileHeader struct {
	Index     uint64
	Term      uint64
	Applied   uint64
	ConfState []byte
}

// FileWriter writes a snapshot file and keeps its digest.
type FileWriter struct {
	w       *bufio.Writer
	h       hash.Hash
	open    bool
	err     error
	written uint64
}

// NewFileWriter writes the header.
func NewFileWriter(w io.Writer, hd FileHeader) (*FileWriter, error) {
	fw := &FileWriter{w: bufio.NewWriterSize(w, 1<<20), h: sha256.New()}
	var b []byte
	b = append(b, SnapshotMagic...)
	b = append(b, FileFormat)
	b = binary.BigEndian.AppendUint64(b, hd.Index)
	b = binary.BigEndian.AppendUint64(b, hd.Term)
	b = binary.BigEndian.AppendUint64(b, hd.Applied)
	b = binary.BigEndian.AppendUint32(b, uint32(len(hd.ConfState)))
	b = append(b, hd.ConfState...)
	fw.put(b)
	return fw, fw.err
}

func (fw *FileWriter) put(b []byte) {
	if fw.err != nil {
		return
	}
	fw.h.Write(b)
	_, fw.err = fw.w.Write(b)
	fw.written += uint64(len(b))
}

// Section begins a section.
func (fw *FileWriter) Section(k Kind, e Encoding) error {
	if fw.open {
		return errors.New("snapshot file: section already open")
	}
	if k == 0 || !e.known() {
		return fmt.Errorf("snapshot file: section kind %d encoding %d", k, e)
	}
	fw.open = true
	fw.put([]byte{byte(k), byte(e)})
	return fw.err
}

// Record writes one record of the open section.
func (fw *FileWriter) Record(r []byte) error {
	if !fw.open {
		return errors.New("snapshot file: record outside a section")
	}
	if len(r) > MaxSnapshotRecord {
		return fmt.Errorf("snapshot file: record of %d bytes exceeds %d", len(r), MaxSnapshotRecord)
	}
	fw.put(binary.BigEndian.AppendUint32(nil, uint32(len(r))))
	fw.put(r)
	return fw.err
}

// EndSection closes the open section.
func (fw *FileWriter) EndSection() error {
	if !fw.open {
		return errors.New("snapshot file: no section open")
	}
	fw.open = false
	fw.put(binary.BigEndian.AppendUint32(nil, sectionEnd))
	return fw.err
}

// Close ends the sections, writes the digest and flushes. It returns the
// file's size and digest.
func (fw *FileWriter) Close() (size uint64, digest [32]byte, err error) {
	if fw.open {
		return 0, digest, errors.New("snapshot file: section left open")
	}
	fw.put([]byte{0})
	copy(digest[:], fw.h.Sum(nil))
	if fw.err == nil {
		_, fw.err = fw.w.Write(digest[:])
		fw.written += uint64(len(digest))
	}
	if fw.err == nil {
		fw.err = fw.w.Flush()
	}
	return fw.written, digest, fw.err
}

// FileReader reads a snapshot file, refusing anything it does not know, and
// checks the digest when the sections end.
type FileReader struct {
	r      *bufio.Reader
	h      hash.Hash
	kind   Kind
	enc    Encoding
	inSect bool
	done   bool
	digest [32]byte
}

// NewFileReader reads and checks the header.
func NewFileReader(r io.Reader) (*FileReader, FileHeader, error) {
	fr := &FileReader{r: bufio.NewReaderSize(r, 1<<20), h: sha256.New()}
	var hd FileHeader
	b, err := fr.read(len(SnapshotMagic) + 1 + 24 + 4)
	if err != nil {
		return nil, hd, err
	}
	if !bytes.Equal(b[:4], SnapshotMagic) {
		return nil, hd, fmt.Errorf("%w: not a HoardMQ snapshot", ErrSnapshotFormat)
	}
	if b[4] != FileFormat {
		return nil, hd, fmt.Errorf("%w: format %d not supported", ErrSnapshotFormat, b[4])
	}
	hd.Index = binary.BigEndian.Uint64(b[5:13])
	hd.Term = binary.BigEndian.Uint64(b[13:21])
	hd.Applied = binary.BigEndian.Uint64(b[21:29])
	n := binary.BigEndian.Uint32(b[29:33])
	if n > MaxSnapshotRecord {
		return nil, hd, fmt.Errorf("%w: configuration state of %d bytes", ErrSnapshotFormat, n)
	}
	if hd.ConfState, err = fr.read(int(n)); err != nil {
		return nil, hd, err
	}
	return fr, hd, nil
}

func (fr *FileReader) read(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(fr.r, b); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("%w: truncated", ErrSnapshotFormat)
		}
		return nil, err
	}
	fr.h.Write(b)
	return b, nil
}

// Digest is the file's digest, once the sections have been read to their
// end and it matched.
func (fr *FileReader) Digest() ([32]byte, bool) { return fr.digest, fr.done }

// NextSection reads the next section's kind and encoding; ok is false once
// the sections have ended and the digest matched. A section's records must
// have been read to its end first.
func (fr *FileReader) NextSection() (k Kind, e Encoding, ok bool, err error) {
	if fr.inSect {
		return 0, 0, false, errors.New("snapshot file: previous section not read to its end")
	}
	if fr.done {
		return 0, 0, false, nil
	}
	b, err := fr.read(1)
	if err != nil {
		return 0, 0, false, err
	}
	if b[0] == 0 {
		sum := fr.h.Sum(nil)
		var got [32]byte
		if _, err := io.ReadFull(fr.r, got[:]); err != nil {
			return 0, 0, false, fmt.Errorf("%w: truncated digest", ErrSnapshotFormat)
		}
		if !bytes.Equal(sum, got[:]) {
			return 0, 0, false, fmt.Errorf("%w: digest mismatch", ErrSnapshotFormat)
		}
		fr.done = true
		fr.digest = got
		return 0, 0, false, nil
	}
	eb, err := fr.read(1)
	if err != nil {
		return 0, 0, false, err
	}
	fr.kind, fr.enc, fr.inSect = Kind(b[0]), Encoding(eb[0]), true
	if !fr.enc.known() {
		return 0, 0, false, fmt.Errorf("%w: section kind %d has encoding %d, not supported", ErrSnapshotFormat, b[0], eb[0])
	}
	return fr.kind, fr.enc, true, nil
}

// Record reads the open section's next record; ok is false at its end.
func (fr *FileReader) Record() (rec []byte, ok bool, err error) {
	if !fr.inSect {
		return nil, false, errors.New("snapshot file: no section open")
	}
	b, err := fr.read(4)
	if err != nil {
		return nil, false, err
	}
	n := binary.BigEndian.Uint32(b)
	if n == sectionEnd {
		fr.inSect = false
		return nil, false, nil
	}
	if n > MaxSnapshotRecord {
		return nil, false, fmt.Errorf("%w: record of %d bytes in kind %d exceeds %d", ErrSnapshotFormat, n, fr.kind, MaxSnapshotRecord)
	}
	rec, err = fr.read(int(n))
	if err != nil {
		return nil, false, err
	}
	return rec, true, nil
}
