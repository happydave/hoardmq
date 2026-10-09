package sinkkafka

import (
	"errors"

	"github.com/happydave/hoardmq/ext"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The reasons a record is counted dropped.
const (
	reasonBufferFull = "buffer_full"
	reasonOverflow   = "overflow"
	reasonRejected   = "rejected"
	reasonShutdown   = "shutdown"
)

// waits reports whether a publish is held for room in the buffer before it
// is produced: under the refuse policy, a client's QoS 1 or 2 publish, which
// the admission check let in because the buffer had room then.
func waits(refuse bool, source ext.PublishSource, qos byte) bool {
	return refuse && source == ext.SourceClient && qos > 0
}

// refuses decides the admission check: a client's QoS 1 or 2 publish that a
// route matches is refused under the refuse policy while the buffer holds
// maxRecords records or maxBytes bytes.
func refuses(refuse bool, source ext.PublishSource, qos byte, matched bool, records, bytes, maxRecords, maxBytes int64) bool {
	return waits(refuse, source, qos) && matched && (records >= maxRecords || bytes >= maxBytes)
}

// outcome names what a record's produce result counts as: "" for produced,
// or the reason it is dropped. waited is true for a record the refuse
// policy held for room; stopped for one whose result came once the module was
// stopping.
func outcome(err error, waited, stopped bool) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, kgo.ErrMaxBuffered) && waited:
		return reasonOverflow
	case errors.Is(err, kgo.ErrMaxBuffered):
		return reasonBufferFull
	case stopped, errors.Is(err, kgo.ErrAborting), errors.Is(err, kgo.ErrClientClosed), errors.Is(err, errStopped):
		return reasonShutdown
	}
	return reasonRejected
}

// errStopped fails a record that reaches the module before it starts or
// after it stops.
var errStopped = errors.New("sink.kafka is not running")
