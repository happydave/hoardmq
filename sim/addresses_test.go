package sim

import (
	"net"
	"strings"
	"testing"
)

// missingAddress returns the first of addrs that bind cannot bind, with its
// error, or "" when every one binds.
func missingAddress(addrs []string, bind func(addr string) error) (string, error) {
	for _, a := range addrs {
		if err := bind(a); err != nil {
			return a, err
		}
	}
	return "", nil
}

// bindLoopback binds and closes a TCP listener on addr's port 0.
func bindLoopback(addr string) error {
	l, err := net.Listen("tcp", net.JoinHostPort(addr, "0"))
	if err != nil {
		return err
	}
	return l.Close()
}

// needAddresses skips the test, saying why, unless every address binds here.
// Linux routes all of 127.0.0.0/8 to lo; other systems, macOS among them,
// have only 127.0.0.1 until an alias is added.
func needAddresses(t testing.TB, addrs ...string) {
	t.Helper()
	if a, err := missingAddress(addrs, bindLoopback); a != "" {
		t.Skipf("needs local address %s (%v): Linux routes 127.0.0.0/8 to lo; elsewhere add the alias (macOS: sudo ifconfig lo0 alias %s) or run on Linux", a, err, a)
	}
}

// TestMissingAddressIsTheFirstThatFails: the decision over bind results.
func TestMissingAddressIsTheFirstThatFails(t *testing.T) {
	failing := map[string]bool{"b": true, "c": true}
	bind := func(a string) error {
		if failing[a] {
			return net.UnknownNetworkError("no " + a)
		}
		return nil
	}
	if a, err := missingAddress([]string{"a", "b", "c"}, bind); a != "b" || err == nil {
		t.Fatalf("missing %q (%v), want b", a, err)
	}
	if a, _ := missingAddress([]string{"a"}, bind); a != "" {
		t.Fatalf("missing %q, want none", a)
	}
}

// TestNeedAddressesSkipsWhereAnAddressCannotBind: an address no host has
// (192.0.2.1, documentation only) skips the test with its reason, and
// 127.0.0.1 does not.
func TestNeedAddressesSkipsWhereAnAddressCannotBind(t *testing.T) {
	var skipped *testing.T
	t.Run("unbindable", func(t *testing.T) {
		skipped = t
		needAddresses(t, "127.0.0.1", "192.0.2.1")
		t.Fatal("not skipped for 192.0.2.1")
	})
	if !skipped.Skipped() {
		t.Fatal("the subtest was not skipped")
	}
	var ran *testing.T
	t.Run("bindable", func(t *testing.T) {
		ran = t
		needAddresses(t, "127.0.0.1")
	})
	if ran.Skipped() {
		t.Fatal("127.0.0.1 skipped")
	}
	if a, err := missingAddress([]string{"192.0.2.1"}, bindLoopback); a != "192.0.2.1" || err == nil || !strings.Contains(err.Error(), "192.0.2.1") {
		t.Fatalf("the real probe: %q (%v)", a, err)
	}
}
