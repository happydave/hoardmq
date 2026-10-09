package sim

import (
	"strings"
	"testing"
	"time"
)

func TestAgreedLeader(t *testing.T) {
	for _, tc := range []struct {
		views  map[string]string
		leader string
		ok     bool
	}{
		{map[string]string{"n1": "n2", "n2": "n2", "n3": "n2"}, "n2", true},
		{map[string]string{"n1": "n2", "n2": "n3"}, "", false},
		{map[string]string{"n1": "n2", "n2": ""}, "", false},
		{map[string]string{}, "", false},
	} {
		if l, ok := agreedLeader(tc.views); l != tc.leader || ok != tc.ok {
			t.Errorf("%v: %q %v, want %q %v", tc.views, l, ok, tc.leader, tc.ok)
		}
	}
}

func TestStormTarget(t *testing.T) {
	names := []string{"n1", "n2", "n3"}
	for victim, want := range map[string]string{"n1": "n2", "n2": "n3", "n3": "n1", "n9": ""} {
		if got := stormTarget(names, victim); got != want {
			t.Errorf("victim %s: target %q, want %q", victim, got, want)
		}
	}
}

// TestChooseVictim: every policy against every leader position among three
// nodes. The before-* policies never crash the leader, and their storm lands
// on the leader or away from it as named.
func TestChooseVictim(t *testing.T) {
	names := []string{"n1", "n2", "n3"}
	counts := []int{33334, 33333, 33333}
	if v, err := chooseVictim("", names, counts, "n1"); err != nil || v != 0 {
		t.Fatalf("default: %d %v, want n1", v, err)
	}
	if v, err := chooseVictim(victimMostClients, names, []int{10, 12, 12}, ""); err != nil || v != 1 {
		t.Fatalf("most-clients picks the first of the largest: %d %v", v, err)
	}
	for _, leader := range names {
		for _, policy := range []string{victimBeforeLeader, victimBeforeFollower} {
			v, err := chooseVictim(policy, names, counts, leader)
			if err != nil {
				t.Fatalf("%s with leader %s: %v", policy, leader, err)
			}
			if names[v] == leader {
				t.Fatalf("%s with leader %s crashed the leader", policy, leader)
			}
			target := stormTarget(names, names[v])
			if onLeader := target == leader; onLeader != (policy == victimBeforeLeader) {
				t.Fatalf("%s with leader %s: victim %s sends its clients to %s", policy, leader, names[v], target)
			}
		}
	}
	if _, err := chooseVictim(victimBeforeLeader, names, counts, ""); err == nil || !strings.Contains(err.Error(), "agrees") {
		t.Fatalf("no leader: %v", err)
	}
	if _, err := chooseVictim("leader", names, counts, "n1"); err == nil || !strings.Contains(err.Error(), victimEnv) {
		t.Fatalf("unknown policy: %v", err)
	}
}

func TestParseProfileAt(t *testing.T) {
	got, err := parseProfileAt("30, 10,20")
	if err != nil || len(got) != 3 || got[0] != 10*time.Second || got[2] != 30*time.Second {
		t.Fatalf("parsed %v, %v", got, err)
	}
	if got, err := parseProfileAt(""); err != nil || got != nil {
		t.Fatalf("empty: %v %v", got, err)
	}
	for _, bad := range []string{"10,x", "0", "-5", "10,10"} {
		if _, err := parseProfileAt(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestOnlineSeries(t *testing.T) {
	from := time.Unix(1000, 0)
	s := &onlineSeries{from: from}
	s.observe(from.Add(500*time.Millisecond), 10)
	s.observe(from.Add(1100*time.Millisecond), 20)
	s.observe(from.Add(1900*time.Millisecond), 25)
	// A pass 2.5 s late fills the seconds it missed with what it saw.
	s.observe(from.Add(4200*time.Millisecond), 40)
	want := []int{20, 40, 40, 40}
	if len(s.samples) != len(want) {
		t.Fatalf("samples %v, want %v", s.samples, want)
	}
	for i := range want {
		if s.samples[i] != want[i] {
			t.Fatalf("samples %v, want %v", s.samples, want)
		}
	}
	for i := 0; i < 2*onlineSeriesMax; i++ {
		s.observe(from.Add(time.Duration(i)*time.Second), 1)
	}
	if len(s.samples) != onlineSeriesMax {
		t.Fatalf("series grew to %d, bound %d", len(s.samples), onlineSeriesMax)
	}
}
