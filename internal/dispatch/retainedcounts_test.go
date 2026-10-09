package dispatch

import (
	"testing"

	"github.com/happydave/hoardmq/internal/message"
)

// TestRetainedCountsByNamespace: each namespace's retained messages are
// counted apart, and a removal takes its namespace's count down.
func TestRetainedCountsByNamespace(t *testing.T) {
	r := New(RoundRobin)
	r.Retain(&message.Message{Namespace: "default", Topic: "a", Payload: []byte("1")})
	r.Retain(&message.Message{Namespace: "tenant-a", Topic: "a", Payload: []byte("1")})
	r.Retain(&message.Message{Namespace: "tenant-a", Topic: "b", Payload: []byte("1")})
	got := r.RetainedCounts()
	if len(got) != 2 || got["default"] != 1 || got["tenant-a"] != 2 {
		t.Fatalf("counts %v, want default 1, tenant-a 2", got)
	}
	r.Retain(&message.Message{Namespace: "default", Topic: "a"})
	if got := r.RetainedCounts(); got["default"] != 0 || got["tenant-a"] != 2 {
		t.Fatalf("after a removal %v", got)
	}
}
