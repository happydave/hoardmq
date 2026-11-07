package lease

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

// bareService is a Service for a test that calls its methods directly, with
// no consensus core, transport or session manager and nothing started: every
// set, queue and channel New makes is made, so a path that reads one does not
// meet a nil. A test sets what its path needs beyond that (s.o, a table).
func bareService(t testing.TB) *Service {
	t.Helper()
	s := &Service{log: slog.New(slog.NewTextHandler(io.Discard, nil)), table: NewTable(), tokens: NewTokens(),
		wake: make(chan struct{}, 1), renewWake: make(chan struct{}, 1),
		forwards: map[uint64]chan forwardResult{}, pulls: map[uint64]chan takeoverAck{}, timers: map[key]*adopted{},
		stop: make(chan struct{}), done: make(chan struct{})}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	t.Cleanup(s.cancel)
	s.servedEmpty = map[string]*atomic.Int64{}
	for _, r := range EmptyReasons {
		s.servedEmpty[r] = new(atomic.Int64)
	}
	s.takeovers = s.newQueue("takeover", 1, s.serveTakeover)
	s.lost = s.newQueue("lost-lease", 1, func(k key, _ []any) { s.lostLease(k) })
	s.adopts = s.newQueue("adopt", 1, s.serveAdopt)
	s.adopting = newAdoptionSet()
	s.confirmed = s.newQueue("confirmed", 1, s.serveConfirmed)
	s.sealChecks = s.newQueue("seal-check", 1, s.serveSealCheck)
	s.confirmOut.wake = make(chan struct{}, 1)
	s.confirmDone = make(chan struct{})
	s.transit = newTransitSet()
	s.connecting = newConnecting()
	s.transitDone = make(chan struct{})
	s.aliveMoved = make(chan struct{}, 1)
	s.fenceDone = make(chan struct{})
	s.abandoned = newAbandonedAcquires()
	s.firsts = newFirstAcquires()
	s.waitNotes = newWaitNotes()
	return s
}

// bareServiceUnset are the Service fields bareService leaves nil, each with
// why a path can meet it nil: a new field that holds a set is made there, or
// listed here with its reason.
var bareServiceUnset = map[string]string{
	"o":             "configuration: a test sets what its path reads",
	"renewCur":      "the renewal attempt in flight; nil between attempts",
	"firstTakeFrom": "made on first use (noteTakeFrom)",
	"rechecks":      "made on first use (armRecheck)",
	"race":          "its maps are made on the first lease it sees moved (claimRace.observe)",
}

// TestBareServiceMakesEverySet: every field of a Service that can be nil — a
// pointer, map, channel, function or interface, in the Service or in a struct
// it holds — is made by bareService, or listed in bareServiceUnset with its
// reason. A set added to Service without it fails here, not as a nil
// dereference in whichever test first reaches it.
func TestBareServiceMakesEverySet(t *testing.T) {
	var unset []string
	walkNilable(reflect.ValueOf(bareService(t)).Elem(), "", func(path string) {
		if _, listed := bareServiceUnset[path]; !listed {
			unset = append(unset, path)
		}
	})
	if len(unset) > 0 {
		t.Fatalf("bareService leaves %s nil: make it there as New does, or list it in bareServiceUnset with its reason", strings.Join(unset, ", "))
	}
	for name := range bareServiceUnset {
		if _, ok := reflect.TypeOf(Service{}).FieldByName(name); !ok {
			t.Errorf("bareServiceUnset lists %q, which Service no longer has", name)
		}
	}
}

// walkNilable calls nilAt with the path of every nil pointer, map, channel,
// function or interface field of v, and descends into struct fields outside
// the sync packages. A listed top-level field is not descended into.
func walkNilable(v reflect.Value, prefix string, nilAt func(string)) {
	for i := 0; i < v.NumField(); i++ {
		f, ft := v.Field(i), v.Type().Field(i)
		path := prefix + ft.Name
		if _, listed := bareServiceUnset[path]; listed && prefix == "" && f.Kind() == reflect.Struct {
			continue
		}
		switch f.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Chan, reflect.Func, reflect.Interface:
			if f.IsNil() {
				nilAt(path)
			}
		case reflect.Struct:
			if pkg := f.Type().PkgPath(); pkg == "sync" || pkg == "sync/atomic" || pkg == "time" {
				continue
			}
			walkNilable(f, path+".", nilAt)
		}
	}
}
