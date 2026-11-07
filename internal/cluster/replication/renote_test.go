package replication

import (
	"testing"
	"time"
)

func TestRenoteAllowed(t *testing.T) {
	judged := retainedNote{Releaser: "n2", Version: 2, Own: true, HandOver: true, Since: time.Unix(10, 0)}
	other := judged
	other.Since = time.Unix(11, 0)
	for _, c := range []struct {
		name               string
		held               retainedNote
		noted              bool
		now, judgedVersion uint64
		stale              bool
		want               bool
	}{
		{"the judged note under the judged map", judged, true, 2, 2, false, true},
		{"a note made again since", other, true, 2, 2, false, false},
		{"no note", retainedNote{}, false, 2, 2, false, false},
		{"a newer map applied", judged, true, 3, 2, false, false},
		{"the stale judgement's defect", other, true, 3, 2, true, true},
	} {
		if got := renoteAllowed(c.held, c.noted, judged, c.now, c.judgedVersion, c.stale); got != c.want {
			t.Errorf("%s: renoteAllowed = %v, want %v", c.name, got, c.want)
		}
	}
}
