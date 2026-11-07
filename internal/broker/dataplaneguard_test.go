package broker

import (
	"testing"

	"github.com/happydave/hoardmq/internal/session"
)

// TestDataPlaneCarriesEveryOptionalMethod: the session manager's replication
// value in the broker has every optional method the manager asks for. One
// missing is not an error anywhere: the manager does without it, as it did
// without the retained stamp and a forward's source.
func TestDataPlaneCarriesEveryOptionalMethod(t *testing.T) {
	var r session.Replication = dataPlane{}
	if _, ok := r.(session.FullReplication); !ok {
		t.Fatal("dataPlane lacks an optional method of session.FullReplication")
	}
}
