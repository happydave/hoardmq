package hoardmqcmd

import (
	"testing"

	"github.com/happydave/hoardmq/ext"
)

// TestEveryBinaryCarriesSinkKafka: a binary built from this entry point,
// the default one or hoardmq-build's, carries the Kafka forwarder.
func TestEveryBinaryCarriesSinkKafka(t *testing.T) {
	if _, ok := ext.Lookup("sink.kafka"); !ok {
		t.Fatalf("sink.kafka is not registered; the modules compiled in are %v", ext.Known())
	}
}
