package session

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// TestImportIsNotVisibleBeforeItIsBuilt: Import installs a session through
// Load, which adds it to the router. The router publishes it to every
// concurrent publisher, and delivery writes inflightByID and queuedIDs under
// the session's own mutex — while Load's population holds none. Adding to
// the router before the state was built raced, and the runtime caught it as
// "concurrent map read and map write" in allocateID during a whole-module
// run.
//
// The race is narrow, so this hammers it: many imports of a session that
// carries queued and in-flight entries, against a publisher hitting the same
// topic. Run with -race, where an unsynchronised access is reported whether
// or not it corrupts anything on the day.
func TestImportIsNotVisibleBeforeItIsBuilt(t *testing.T) {
	m, _ := newManager(t, nil)
	pub := newFake("pub", packet.V5, 1000)
	connectV5(m, pub, true, 0, nil)

	state := func(id string) *store.SessionState {
		ss := &store.SessionState{
			SessionMeta:   store.SessionMeta{Namespace: m.opts.Namespace, ClientID: id, ExpiryInterval: 600, Version: byte(packet.V5)},
			Subscriptions: []store.SubscriptionRecord{{Filter: "t/0", QoS: 1}},
		}
		for i := 1; i <= 24; i++ {
			ss.Entries = append(ss.Entries, store.Entry{
				Seq: uint64(i), QoS: 1, PacketID: uint16(i), State: store.StateQueued,
				Message: &message.Message{Namespace: m.opts.Namespace, Topic: "t/0", QoS: 1, Payload: []byte("x")},
			})
			ss.Entries = append(ss.Entries, store.Entry{
				Seq: uint64(100 + i), QoS: 1, PacketID: uint16(100 + i), State: store.StatePublishSent,
				Message: &message.Message{Namespace: m.opts.Namespace, Topic: "t/0", QoS: 1, Payload: []byte("y")},
			})
		}
		return ss
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			publish(m, pub, "t/0", 1, uint16(1+i%60000), "hello")
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.Import(state(fmt.Sprintf("sub%d", i%4)))
		}
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	time.Sleep(300 * time.Millisecond)
	close(stop)
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("import and publish did not finish")
	}
}
