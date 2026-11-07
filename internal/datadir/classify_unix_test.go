//go:build unix

package datadir

import (
	"fmt"
	"os"
	"syscall"
	"testing"
)

// Each error a lock attempt can return, bare and wrapped.
func TestClassify(t *testing.T) {
	for _, c := range []struct {
		err  error
		want Outcome
	}{
		{nil, Held},
		{syscall.EWOULDBLOCK, HeldElsewhere},
		{syscall.EAGAIN, HeldElsewhere},
		{syscall.ENOLCK, Unsupported},
		{syscall.ENOTSUP, Unsupported},
		{syscall.EOPNOTSUPP, Unsupported},
		{syscall.EBADF, Failed},
		{syscall.EACCES, Failed},
	} {
		for _, err := range []error{c.err, wrap(c.err)} {
			if got := classify(err); got != c.want {
				t.Errorf("classify(%v) = %v, want %v", err, got, c.want)
			}
		}
	}
}

func wrap(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("lock: %w", &os.PathError{Op: "flock", Path: "LOCK", Err: err})
}
