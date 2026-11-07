package sinkkafka

import (
	"strconv"
	"strings"

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
			{Key: s.prefix + "topic", Value: []byte(m.Topic)},
			{Key: s.prefix + "qos", Value: []byte(strconv.Itoa(int(m.QoS)))},
			{Key: s.prefix + "source", Value: []byte(source.String())},
		}
		if m.Publisher.ClientID != "" {
			h = append(h, kgo.RecordHeader{Key: s.prefix + "clientId", Value: []byte(m.Publisher.ClientID)})
		}
		if m.Retain {
			h = append(h, kgo.RecordHeader{Key: s.prefix + "retain", Value: []byte("true")})
		}
		if m.ContentType != nil {
			h = append(h, kgo.RecordHeader{Key: s.prefix + "contentType", Value: []byte(*m.ContentType)})
		}
		if m.PayloadFormat != nil {
			h = append(h, kgo.RecordHeader{Key: s.prefix + "payloadFormat", Value: []byte(strconv.Itoa(int(*m.PayloadFormat)))})
		}
		if m.ResponseTopic != nil {
			h = append(h, kgo.RecordHeader{Key: s.prefix + "responseTopic", Value: []byte(*m.ResponseTopic)})
		}
		if m.CorrelationData != nil {
			h = append(h, kgo.RecordHeader{Key: s.prefix + "correlationData", Value: m.CorrelationData})
		}
		if m.Namespace != s.namespace {
			h = append(h, kgo.RecordHeader{Key: s.prefix + "namespace", Value: []byte(m.Namespace)})
		}
		rec.Headers = h
	}
	if s.userProperties {
		for _, p := range m.UserProperties {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: s.propertyPrefix + p.Name, Value: []byte(p.Value)})
		}
	}
	for _, h := range r.headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: h.name, Value: []byte(h.value)})
	}
	return rec
}

// segmentOf returns level n of topic, and false when the topic has fewer
// levels.
func segmentOf(topic string, n int) (string, bool) {
	for i := 0; ; i++ {
		level, rest, more := strings.Cut(topic, "/")
		if i == n {
			return level, true
		}
		if !more {
			return "", false
		}
		topic = rest
	}
}

// fellBack reports whether route r's segment key falls back for m: the
// topic has no level at the route's index.
func fellBack(r *route, m *mqtt.Message) bool {
	if r.key != keySegment {
		return false
	}
	_, ok := segmentOf(m.Topic, r.segment)
	return !ok
}

// key is the record key route r takes from m, or nil for none.
func key(r *route, m *mqtt.Message) []byte {
	return keyFrom(r.key, r, m)
}

func keyFrom(source string, r *route, m *mqtt.Message) []byte {
	switch source {
	case keySegment:
		if level, ok := segmentOf(m.Topic, r.segment); ok {
			return []byte(level)
		}
		return keyFrom(r.fallback, r, m)
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
