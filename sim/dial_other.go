//go:build !linux

package sim

import (
	"context"
	"syscall"
)

// sourceControl is nil where IP_BIND_ADDRESS_NO_PORT does not exist: a dial
// from a source address binds it with port 0, and the kernel chooses the port
// then.
var sourceControl func(context.Context, string, string, syscall.RawConn) error

// bindNoPort reports the option as unset where it does not exist.
func bindNoPort(syscall.RawConn) (bool, error) { return false, nil }
