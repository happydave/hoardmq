// Package gatechain holds chain.sh, the runner for a chain of gate or
// measurement steps that is started detached on a test host and can be
// stopped between steps by a file; sync.sh, which carries a work item's
// branch to the test host only while the work item holds a live claim there;
// and the tests of both. See the comment at the top of each script.
package gatechain
