// Package records holds the values of the replicated records store: named
// collections of keyed JSON records that every node applies from the
// consensus log. A module reads and writes them through the broker's records
// service.
package records

import (
	"encoding/json"
	"time"
)

// Record is one stored record.
type Record struct {
	Value json.RawMessage `json:"v"`
	// Version is the log index of the record's last write; Created the
	// index of the write that created it.
	Version uint64    `json:"ver"`
	Created uint64    `json:"created"`
	By      string    `json:"by,omitempty"`
	At      time.Time `json:"at"`
}

// Entry is a record with its key.
type Entry struct {
	Key string
	Record
}

// Result is a write's outcome.
type Result struct {
	OK     bool   `json:"ok"`
	Reason string `json:"reason,omitempty"`
	// Index is the log index the write was applied at; Version the record's
	// version after it (unchanged on a refusal).
	Index   uint64 `json:"index"`
	Version uint64 `json:"version,omitempty"`
	// Previous is the record a delete removed or a put replaced.
	Previous *Record `json:"prev,omitempty"`
}
