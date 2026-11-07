package hoardmqcmd

import (
	"testing"

	"github.com/happydave/hoardmq/ext"
)

// TestEveryBinaryCarriesTheLinkedModules: a binary built from this entry
// point, the default one or hoardmq-build's, carries the Kafka forwarder, the
// webhooks and payload schema validation.
func TestEveryBinaryCarriesTheLinkedModules(t *testing.T) {
	for _, id := range []ext.ID{"sink.kafka", "auth.http", "events.http", "schema.json"} {
		if _, ok := ext.Lookup(id); !ok {
			t.Errorf("%s is not registered; the modules compiled in are %v", id, ext.Known())
		}
	}
}
