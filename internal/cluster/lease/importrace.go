package lease

import (
	"sync"
	"sync/atomic"
	"time"
)

// A trace of adopted sessions imported after a takeover pass here answered the
// same key with no state. The pass marks the key's adoption; the import that
// follows reads the mark.

// ImportRace is one import of adopted state whose key a takeover pass here had
// already answered with no state.
type ImportRace struct {
	ClientID string `json:"client_id"`
	Path     string `json:"path"` // "worker" or "inline"
	Token    uint64 `json:"token"`
	// SeenBefore and SeenAfter are the highest grant seen for the client when
	// the import began and when it returned.
	SeenBefore uint64 `json:"seen_before"`
	SeenAfter  uint64 `json:"seen_after"`
	// AnsweredMs is when the empty answer was sent, from the fetch's lease
	// check; negative is before it.
	AnsweredMs float64 `json:"answered_after_check_ms"`
	GapMs      float64 `json:"check_to_import_ms"`
	ImportMs   float64 `json:"import_ms"`
}

// ReclaimedOnConfirm is one offline session a confirmation evicted.
type ReclaimedOnConfirm struct {
	ClientID  string `json:"client_id"`
	Held      uint64 `json:"held"`
	Confirmed uint64 `json:"confirmed"`
	From      string `json:"from"`
}

const importRaceSampleMax = 64

type importTrace struct {
	afterEmpty   atomic.Int64
	inlineAfter  atomic.Int64
	gapMaxNs     atomic.Int64
	importMaxNs  atomic.Int64
	mu           sync.Mutex
	races        []ImportRace
	racesDropped int64
	reclaimed    []ReclaimedOnConfirm
}

func maxNs(a *atomic.Int64, d time.Duration) {
	v := int64(d)
	for {
		cur := a.Load()
		if v <= cur || a.CompareAndSwap(cur, v) {
			return
		}
	}
}

// markAnsweredEmpty records that a pass answered the key with no state while
// its adoption was unfinished.
func (a *adoptionSet) markAnsweredEmpty(k key, at time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if ad := a.m[k]; ad != nil && ad.answeredEmpty.IsZero() {
		ad.answeredEmpty = at
	}
}

func (a *adoptionSet) answeredEmptyAt(ad *adoption) time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return ad.answeredEmpty
}

// noteImport records one import of adopted state: its timings, and whether a
// pass had answered the key empty.
func (s *Service) noteImport(k key, ad *adoption, path string, seenBefore uint64, checked, began, ended time.Time) {
	tr := &s.importTrace
	if path == "worker" {
		maxNs(&tr.gapMaxNs, began.Sub(checked))
		maxNs(&tr.importMaxNs, ended.Sub(began))
	}
	answered := s.adopting.answeredEmptyAt(ad)
	if answered.IsZero() {
		return
	}
	if path == "worker" {
		tr.afterEmpty.Add(1)
	} else {
		tr.inlineAfter.Add(1)
	}
	r := ImportRace{ClientID: k.id, Path: path, Token: ad.job.epoch, SeenBefore: seenBefore,
		SeenAfter: s.tokens.Seen(k.ns, k.id), AnsweredMs: msOf(answered.Sub(checked)),
		GapMs: msOf(began.Sub(checked)), ImportMs: msOf(ended.Sub(began))}
	tr.mu.Lock()
	if len(tr.races) < importRaceSampleMax {
		tr.races = append(tr.races, r)
	} else {
		tr.racesDropped++
	}
	tr.mu.Unlock()
}

func (s *Service) noteReclaimed(k key, held, confirmed uint64, from string) {
	tr := &s.importTrace
	tr.mu.Lock()
	if len(tr.reclaimed) < importRaceSampleMax {
		tr.reclaimed = append(tr.reclaimed, ReclaimedOnConfirm{ClientID: k.id, Held: held, Confirmed: confirmed, From: from})
	}
	tr.mu.Unlock()
}

func msOf(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// ImportTraceStats is the trace's reading.
type ImportTraceStats struct {
	AfterEmptyAnswer       int64                `json:"adopt_import_after_empty_answer"`
	InlineAfterEmptyAnswer int64                `json:"adopt_inline_after_empty_answer"`
	GapMaxMs               float64              `json:"adopt_import_gap_max_ms"`
	ImportMaxMs            float64              `json:"adopt_import_max_ms"`
	Races                  []ImportRace         `json:"import_race_sample,omitempty"`
	RacesDropped           int64                `json:"import_race_sample_dropped"`
	Reclaimed              []ReclaimedOnConfirm `json:"reclaimed_sample,omitempty"`
}

func (s *Service) importTraceStats() ImportTraceStats {
	tr := &s.importTrace
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return ImportTraceStats{AfterEmptyAnswer: tr.afterEmpty.Load(), InlineAfterEmptyAnswer: tr.inlineAfter.Load(),
		GapMaxMs:    float64(tr.gapMaxNs.Load()) / 1e6,
		ImportMaxMs: float64(tr.importMaxNs.Load()) / 1e6, Races: append([]ImportRace(nil), tr.races...),
		RacesDropped: tr.racesDropped, Reclaimed: append([]ReclaimedOnConfirm(nil), tr.reclaimed...)}
}
