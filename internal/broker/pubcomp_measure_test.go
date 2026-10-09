package broker

import (
	"encoding/json"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestMeasureInboundQoS2Exchange times inbound QoS 2 exchanges on a three-node
// cluster: PUBLISH to PUBREC, and PUBREL to PUBCOMP, for a durable publisher
// whose session replicates to its peers (WI 2031). It runs only when
// HOARDMQ_MEASURE_PUBCOMP names a file, to which it appends one JSON line.
func TestMeasureInboundQoS2Exchange(t *testing.T) {
	out := os.Getenv("HOARDMQ_MEASURE_PUBCOMP")
	if out == "" {
		t.Skip("a measurement: set HOARDMQ_MEASURE_PUBCOMP to a file to run it")
	}
	const warm, exchanges = 20, 200
	nodes := startTrio(t, nil)
	expiry := uint32(3600)
	pub, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "measure-pub", Properties: packet.Properties{SessionExpiryInterval: &expiry}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("precondition: CONNACK %v", ack.ReasonCode)
	}
	var publish, release, whole []time.Duration
	for i := 0; i < warm+exchanges; i++ {
		id := uint16(i%65535 + 1)
		t0 := time.Now()
		pub.send(&packet.Publish{Topic: "measure/q2", QoS: 2, PacketID: id, Payload: []byte("a payload of some tens of bytes")})
		rec, ok := pub.read().(*packet.Pubrec)
		if !ok || rec.PacketID != id || rec.ReasonCode.IsError() {
			t.Fatalf("precondition: exchange %d: no successful PUBREC (%+v); %d completed", i, rec, i)
		}
		t1 := time.Now()
		pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: id}})
		comp, ok := pub.read().(*packet.Pubcomp)
		if !ok || comp.PacketID != id || comp.ReasonCode != packet.Success {
			t.Fatalf("precondition: exchange %d: no successful PUBCOMP (%+v); %d completed", i, comp, i)
		}
		t2 := time.Now()
		if i >= warm {
			publish = append(publish, t1.Sub(t0))
			release = append(release, t2.Sub(t1))
			whole = append(whole, t2.Sub(t0))
		}
	}
	pct := func(d []time.Duration, p float64) float64 {
		s := slices.Clone(d)
		slices.Sort(s)
		return float64(s[int(p*float64(len(s)-1))].Microseconds()) / 1000
	}
	line := map[string]any{
		"exchanges":         len(whole),
		"publish_ms_median": pct(publish, 0.5), "publish_ms_p95": pct(publish, 0.95),
		"release_ms_median": pct(release, 0.5), "release_ms_p95": pct(release, 0.95),
		"whole_ms_median": pct(whole, 0.5), "whole_ms_p95": pct(whole, 0.95),
	}
	b, err := json.Marshal(line)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s", b)
	f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(append(b, '\n')); err != nil {
		t.Fatal(err)
	}
}
