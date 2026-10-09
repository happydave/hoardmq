package broker

import (
	"reflect"
	"testing"
)

// TestLiveAmong: a drain offers its leases only to members the lease table
// says are alive — not to itself, not to a node the leader has declared dead,
// and not to one with no alive record.
func TestLiveAmong(t *testing.T) {
	live := map[string]bool{"n1": true, "n2": true, "n3": false}
	got := liveAmong([]string{"n4", "n3", "n2", "n1"}, "n1", func(n string) bool { return live[n] })
	if want := []string{"n2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("liveAmong = %v, want %v", got, want)
	}
}
