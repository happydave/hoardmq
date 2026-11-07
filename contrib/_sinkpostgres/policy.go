package sinkpostgres

import (
	"errors"

	"github.com/happydave/hoardmq/ext"
	"github.com/jackc/pgx/v5/pgconn"
)

// The reasons a row is counted dropped, sink.kafka's.
const (
	reasonBufferFull = "buffer_full"
	reasonOverflow   = "overflow"
	reasonRejected   = "rejected"
	reasonShutdown   = "shutdown"
)

// waits reports whether a publish is held for room in the buffer before its
// rows are taken: under the refuse policy, a client's QoS 1 or 2 publish,
// which the admission check let in because the buffer had room then.
func waits(refuse bool, source ext.PublishSource, qos byte) bool {
	return refuse && source == ext.SourceClient && qos > 0
}

// refuses decides the admission check: a client's QoS 1 or 2 publish that a
// route matches is refused under the refuse policy while the buffer holds
// maxRecords rows or maxBytes bytes.
func refuses(refuse bool, source ext.PublishSource, qos byte, matched bool, records, bytes, maxRecords, maxBytes int64) bool {
	return waits(refuse, source, qos) && matched && (records >= maxRecords || bytes >= maxBytes)
}

// transient reports whether a failed write is retried. A server error in
// SQLSTATE class 08 (connection), 40 (rollback, such as a serialization
// failure), 53 (insufficient resources) or 57 (operator intervention, such as
// a shutdown) is; any other server error refuses the batch for good. An error
// with no SQLSTATE did not reach the server's answer (a refused or broken
// connection, a timeout) and is retried.
func transient(err error) bool {
	var pg *pgconn.PgError
	if !errors.As(err, &pg) {
		return true
	}
	switch pg.Code[:2] {
	case "08", "40", "53", "57":
		return true
	}
	return false
}
