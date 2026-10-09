package trace

import (
	"strconv"
	"testing"
)

func TestRingIsBoundedAndOrdered(t *testing.T) {
	r := New(3)
	if got := r.Events(); len(got) != 0 {
		t.Fatalf("%v", got)
	}
	for i := 1; i <= 2; i++ {
		r.Record(Event{Type: strconv.Itoa(i)})
	}
	if got := r.Events(); len(got) != 2 || got[0].Type != "1" || got[1].Type != "2" {
		t.Fatalf("%v", got)
	}
	for i := 3; i <= 5; i++ {
		r.Record(Event{Type: strconv.Itoa(i)})
	}
	got := r.Events()
	if len(got) != 3 || got[0].Type != "3" || got[1].Type != "4" || got[2].Type != "5" {
		t.Fatalf("%v", got)
	}
	if r.Total() != 5 {
		t.Errorf("total %d", r.Total())
	}
	if New(0).ring == nil || len(New(0).ring) != 1 {
		t.Error("minimum size")
	}
}
