package store

import (
	"encoding/json"
	"fmt"
)

// Version orders retained writes to one topic: the shard leader stamps each
// client write and clear with the shard map version it led the shard under
// (E) and a number that only rises on that node (N). Compared epoch first.
// The zero Version is an unversioned record, written before versions or
// under no map; it ranks below every stamped one and above an absent topic.
//
// A Version's numbers can exceed 2^53, so its JSON form ("rv": [E, N]) is
// decoded only into typed uint64; a float or JavaScript reader loses
// precision.
type Version struct {
	E, N uint64
}

// Zero reports whether v is the unversioned version.
func (v Version) Zero() bool { return v.E == 0 && v.N == 0 }

// Less reports whether v orders before w.
func (v Version) Less(w Version) bool {
	if v.E != w.E {
		return v.E < w.E
	}
	return v.N < w.N
}

// String is v as two decimal numbers, for logs and tools.
func (v Version) String() string { return fmt.Sprintf("%d.%d", v.E, v.N) }

// MarshalJSON writes v as [E, N].
func (v Version) MarshalJSON() ([]byte, error) { return json.Marshal([2]uint64{v.E, v.N}) }

// UnmarshalJSON reads [E, N].
func (v *Version) UnmarshalJSON(b []byte) error {
	var a [2]uint64
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	v.E, v.N = a[0], a[1]
	return nil
}

// MergeRetained decides whether a retained copy (a message or a tombstone) at
// version in replaces what a store holds for its topic: always into an absent
// topic; a higher version wins; an equal stamped version is the same write and
// changes nothing; two unversioned copies are decided by arrival, the later
// replacing, except on a hand-over or hand-on path (handOver), where the held
// one is kept. It is pure.
func MergeRetained(held Version, heldExists bool, in Version, handOver bool) bool {
	switch {
	case !heldExists:
		return true
	case held.Less(in):
		return true
	case in == held && in.Zero():
		return !handOver
	}
	return false
}

// RetainedVersion is the version a state holds a retained message at.
type RetainedVersion struct {
	Namespace string  `json:"ns"`
	Topic     string  `json:"topic"`
	RV        Version `json:"rv"`
}

// Tombstone is a retained topic a client cleared: its version, kept so that an
// older copy handed over later does not undo the clear. It is never served.
type Tombstone struct {
	Namespace string  `json:"ns"`
	Topic     string  `json:"topic"`
	RV        Version `json:"rv"`
}
