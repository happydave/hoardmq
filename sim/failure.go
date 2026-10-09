package sim

import (
	"errors"
	"io"
	"net"
	"sync/atomic"
	"syscall"
)

// The classes a failed connect is counted under on the client host. Two of
// them say the client host itself ran out of something — ports or file
// descriptors — and decide the client-limited verdict; the rest describe what
// the brokers and the network did.
const (
	failAddrNotAvailable  = "addr_not_available"
	failTooManyFiles      = "too_many_files"
	failRefused           = "refused"
	failDialTimeout       = "dial_timeout"
	failReset             = "reset"
	failHandshakeDeadline = "handshake_deadline"
	failEOF               = "eof"
	failOther             = "other"
)

var failureClasses = []string{failAddrNotAvailable, failTooManyFiles, failRefused, failDialTimeout, failReset, failHandshakeDeadline, failEOF, failOther}

// connectError marks which stage of a connect failed: the TCP dial, or the
// CONNECT/CONNACK exchange after it. A timeout means something different in
// each.
type connectError struct {
	stage string // "dial" or "handshake"
	err   error
}

func (e *connectError) Error() string { return e.stage + ": " + e.err.Error() }
func (e *connectError) Unwrap() error { return e.err }

// classifyFailure names the class of one failed connect.
func classifyFailure(err error) string {
	stage := "handshake"
	var ce *connectError
	if errors.As(err, &ce) {
		stage = ce.stage
	}
	var ne net.Error
	switch {
	case errors.Is(err, syscall.EADDRNOTAVAIL):
		return failAddrNotAvailable
	case errors.Is(err, syscall.EMFILE), errors.Is(err, syscall.ENFILE):
		return failTooManyFiles
	case errors.Is(err, syscall.ECONNREFUSED):
		return failRefused
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return failReset
	case errors.As(err, &ne) && ne.Timeout():
		if stage == "dial" {
			return failDialTimeout
		}
		return failHandshakeDeadline
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return failEOF
	default:
		return failOther
	}
}

// failureCounts counts failed connects by class. Clients add from their own
// goroutines; the sampler reads.
type failureCounts struct {
	counts [8]atomic.Int64 // indexed as failureClasses
}

func (f *failureCounts) add(err error) {
	class := classifyFailure(err)
	for i, c := range failureClasses {
		if c == class {
			f.counts[i].Add(1)
			return
		}
	}
}

// snapshot returns the cumulative count per class.
func (f *failureCounts) snapshot() map[string]int64 {
	out := make(map[string]int64, len(failureClasses))
	for i, c := range failureClasses {
		out[c] = f.counts[i].Load()
	}
	return out
}
