//go:build !unix

package datadir

import (
	"errors"
	"os"
)

var errNoFlock = errors.New("file locks are not supported on this platform")

func flock(*os.File, bool) error { return errNoFlock }

func classify(err error) Outcome {
	if err == nil {
		return Held
	}
	return Unsupported
}
