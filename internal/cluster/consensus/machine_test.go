package consensus

import "testing"

// recorder is a Recoverable component that records what it was told.
type recorder struct {
	applied    []byte
	recovering []bool
	recovered  int
}

func (r *recorder) Apply(_ uint64, p []byte) (any, error) {
	r.applied = append(r.applied, p...)
	return nil, nil
}
func (r *recorder) Snapshot() ([]byte, error) { return r.applied, nil }
func (r *recorder) Restore(d []byte) error    { r.applied = append([]byte(nil), d...); return nil }
func (r *recorder) SetRecovering(on bool)     { r.recovering = append(r.recovering, on) }
func (r *recorder) Recovered()                { r.recovered++ }

func TestRecoveryTellsComponentsOnceInKindOrder(t *testing.T) {
	m := NewMachine()
	a, b := &recorder{}, &recorder{}
	m.Register(KindReserved+1, a)
	m.BeginRecovery()
	// A component registered during recovery starts silent.
	m.Register(KindReserved+2, b)
	if !m.Recovering() || len(a.recovering) != 1 || !a.recovering[0] || len(b.recovering) != 1 || !b.recovering[0] {
		t.Fatalf("recovering not told: a %v b %v", a.recovering, b.recovering)
	}
	if _, err := m.Apply(5, append([]byte{byte(KindReserved + 1)}, 'x')); err != nil {
		t.Fatal(err)
	}
	m.BeginRecovery() // idempotent
	m.EndRecovery()
	m.EndRecovery() // idempotent
	if m.Recovering() || a.recovered != 1 || b.recovered != 1 {
		t.Fatalf("recovered once each: a %d b %d recovering %v", a.recovered, b.recovered, m.Recovering())
	}
	if len(a.recovering) != 2 || a.recovering[1] {
		t.Fatalf("a not told recovery ended: %v", a.recovering)
	}
	if string(a.applied) != "x" || m.Applied() != 5 {
		t.Fatalf("apply during recovery lost: %q applied %d", a.applied, m.Applied())
	}
}
