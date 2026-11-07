//go:build unix

package datadir

import (
	"errors"
	"os"
	"syscall"
)

func flock(f *os.File, exclusive bool) error {
	how := syscall.LOCK_SH
	if exclusive {
		how = syscall.LOCK_EX
	}
	return syscall.Flock(int(f.Fd()), how|syscall.LOCK_NB)
}

// classify names what a lock attempt's error means. Another holder is
// EWOULDBLOCK; a filesystem without locks is ENOLCK or EOPNOTSUPP.
func classify(err error) Outcome {
	switch {
	case err == nil:
		return Held
	case errors.Is(err, syscall.EWOULDBLOCK), errors.Is(err, syscall.EAGAIN):
		return HeldElsewhere
	case errors.Is(err, syscall.ENOLCK), errors.Is(err, syscall.ENOTSUP), errors.Is(err, syscall.EOPNOTSUPP):
		return Unsupported
	}
	return Failed
}
