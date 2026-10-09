//go:build linux

package sim

import (
	"context"
	"fmt"
	"syscall"
)

// ipBindAddressNoPort is IP_BIND_ADDRESS_NO_PORT from Linux's
// include/uapi/linux/in.h. Go's syscall package defines it for only some
// Linux architectures.
const ipBindAddressNoPort = 0x18

// sourceControl sets IP_BIND_ADDRESS_NO_PORT on a socket about to dial from
// a fixed source address, so the kernel chooses the local port at connect(),
// where it need only be unique for the four-tuple. Chosen at bind(), the port
// must be unique for the address across every destination: at a hundred
// thousand reconnecting clients over eight loopback addresses that search
// took most of the host's recorded CPU (WI 1476). A socket that refuses the
// option fails the dial rather than silently measuring the old mode.
func sourceControl(_ context.Context, _, _ string, c syscall.RawConn) error {
	var serr error
	if err := c.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, ipBindAddressNoPort, 1)
	}); err != nil {
		return fmt.Errorf("IP_BIND_ADDRESS_NO_PORT: %w", err)
	}
	if serr != nil {
		return fmt.Errorf("IP_BIND_ADDRESS_NO_PORT: %w", serr)
	}
	return nil
}

// bindNoPort reads IP_BIND_ADDRESS_NO_PORT back from a socket.
func bindNoPort(c syscall.RawConn) (bool, error) {
	var v int
	var gerr error
	if err := c.Control(func(fd uintptr) {
		v, gerr = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_IP, ipBindAddressNoPort)
	}); err != nil {
		return false, err
	}
	return v != 0, gerr
}
