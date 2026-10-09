package sinkkafka

import (
	"strconv"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/ext/mqtt"
	"github.com/twmb/franz-go/pkg/kgo"
)

// matches returns the routes that decide m's records: for each distinct
// Kafka topic among the routes m matches, the first such route in order.
func (s *settings) matches(m *mqtt.Message) []*route {
	var out []*route
	for i := range s.routes {
		r := &s.routes[i]
		if r.namespace != m.Namespace || !mqtt.MatchTopic(r.filter, m.Topic) {
			continue
		}
		seen := false
		for _, o := range out {
			if o.topic == r.topic {
				seen = true
				break
			}
		}
		if !seen {
			out = append(out, r)
		}
	}
	return out
}

// record builds the Kafka record route r makes of m.
func (s *settings) record(r *route, source ext.PublishSource, m *mqtt.Message) *kgo.Record {
	value := m.Payload
	if value == nil {
		value = []byte{}
	}
	rec := &kgo.Record{Topic: r.topic, Key: key(r, m), Value: value, Timestamp: m.Arrived}
	if s.metadata {
		h := []kgo.RecordHeader{
			{Key: "mqtt.topic", Value: []byte(m.Topic)},
			{Key: "mqtt.qos", Value: []byte(strconv.Itoa(int(m.QoS)))},
			{Key: "mqtt.source", Value: []byte(source.String())},
		}
		if m.Publisher.ClientID != "" {
			h = append(h, kgo.RecordHeader{Key: "mqtt.clientId", Value: []byte(m.Publisher.ClientID)})
		}
		if m.Retain {
			h = append(h, kgo.RecordHeader{Key: "mqtt.retain", Value: []byte("true")})
		}
		if m.ContentType != nil {
			h = append(h, kgo.RecordHeader{Key: "mqtt.contentType", Value: []byte(*m.ContentType)})
		}
		if m.PayloadFormat != nil {
			h = append(h, kgo.RecordHeader{Key: "mqtt.payloadFormat", Value: []byte(strconv.Itoa(int(*m.PayloadFormat)))})
		}
		if m.ResponseTopic != nil {
			h = append(h, kgo.RecordHeader{Key: "mqtt.responseTopic", Value: []byte(*m.ResponseTopic)})
		}
		if m.CorrelationData != nil {
			h = append(h, kgo.RecordHeader{Key: "mqtt.correlationData", Value: m.CorrelationData})
		}
		if m.Namespace != s.namespace {
			h = append(h, kgo.RecordHeader{Key: "mqtt.namespace", Value: []byte(m.Namespace)})
		}
		rec.Headers = h
	}
	if s.userProperties {
		for _, p := range m.UserProperties {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: p.Name, Value: []byte(p.Value)})
		}
	}
	return rec
}

// key is the record key route r takes from m, or nil for none.
func key(r *route, m *mqtt.Message) []byte {
	switch r.key {
	case keyTopic:
		return []byte(m.Topic)
	case keyClientID:
		if m.Publisher.ClientID == "" {
			return nil
		}
		return []byte(m.Publisher.ClientID)
	case keyUserProperty:
		for _, p := range m.UserProperties {
			if p.Name == r.keyProperty {
				return []byte(p.Value)
			}
		}
	}
	return nil
}
