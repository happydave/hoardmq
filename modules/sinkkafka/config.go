package sinkkafka

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/happydave/hoardmq/ext/mqtt"
)

// config is modules.sink.kafka as written. Pointers tell an absent number
// from a zero one where zero is allowed.
type config struct {
	Brokers     []string      `json:"brokers"`
	ClientID    string        `json:"clientId"`
	TLS         *tlsConfig    `json:"tls"`
	SASL        *saslConfig   `json:"sasl"`
	Routes      []routeConfig `json:"routes"`
	Headers     headersConfig `json:"headers"`
	Acks        string        `json:"acks"`
	Compression string        `json:"compression"`
	LingerMs    *int          `json:"lingerMs"`
	Buffer      bufferConfig  `json:"buffer"`
	FlushMs     *int          `json:"flushTimeoutMs"`
}

type tlsConfig struct {
	CAFile     string `json:"caFile"`
	CertFile   string `json:"certFile"`
	KeyFile    string `json:"keyFile"`
	ServerName string `json:"serverName"`
}

type saslConfig struct {
	Mechanism    string `json:"mechanism"`
	Username     string `json:"username"`
	PasswordFile string `json:"passwordFile"`
}

type routeConfig struct {
	Filter      string            `json:"filter"`
	Topic       string            `json:"topic"`
	Key         keyConfig         `json:"key"`
	KeyProperty string            `json:"keyProperty"`
	Namespace   string            `json:"namespace"`
	Headers     map[string]string `json:"headers"`
}

// keyConfig is a route's key as written: a source's name, or an object
// naming a topic segment and what to use when the topic has none.
type keyConfig struct {
	Name     string
	Segment  *int
	Fallback string
	object   bool
}

// UnmarshalJSON reads either form, refusing unknown members of the object.
func (k *keyConfig) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	if len(b) > 0 && b[0] == '"' {
		return json.Unmarshal(b, &k.Name)
	}
	var o struct {
		Segment  *int   `json:"segment"`
		Fallback string `json:"fallback"`
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&o); err != nil {
		return fmt.Errorf("key: %w", err)
	}
	k.Segment, k.Fallback, k.object = o.Segment, o.Fallback, true
	return nil
}

type headersConfig struct {
	Metadata           *bool   `json:"metadata"`
	UserProperties     *bool   `json:"userProperties"`
	Prefix             *string `json:"prefix"`
	UserPropertyPrefix string  `json:"userPropertyPrefix"`
}

type bufferConfig struct {
	MaxRecords *int64 `json:"maxRecords"`
	MaxBytes   *int64 `json:"maxBytes"`
	WhenFull   string `json:"whenFull"`
	WaitMs     *int   `json:"waitMs"`
}

// The key sources a route may name.
const (
	keyTopic        = "topic"
	keyClientID     = "clientId"
	keyUserProperty = "userProperty"
	keyNone         = "none"
	// keySegment is the object form's source: a level of the MQTT topic.
	keySegment = "segment"
)

// The buffer policies.
const (
	whenFullRefuse = "refuse"
	whenFullDrop   = "drop"
)

// Defaults and bounds.
const (
	defaultMaxRecords = 100_000
	defaultMaxBytes   = 64 << 20
	minMaxBytes       = 1 << 20
	defaultLingerMs   = 5
	maxLingerMs       = 1000
	defaultWaitMs     = 1000
	maxWaitMs         = 10_000
	defaultFlushMs    = 5000
	maxFlushMs        = 60_000
	maxTopicLen       = 249
	maxSegment        = 127
	maxPrefixLen      = 64
	maxStaticHeaders  = 32
	maxHeaderNameLen  = 128
	maxHeaderValueLen = 1024
	defaultPrefix     = "mqtt."
)

// route is one validated route.
type route struct {
	filter      string
	topic       string
	key         string
	keyProperty string
	namespace   string
	// segment and fallback are the segment key's: the level's index, and the
	// source used when a topic has no such level.
	segment  int
	fallback string
	// headers are the route's static headers, in name order.
	headers []staticHeader
}

type staticHeader struct{ name, value string }

// settings is the configuration once validated, with every default applied
// and the files it names read.
type settings struct {
	brokers        []string
	clientID       string
	tls            *tls.Config
	saslMechanism  string
	saslUser       string
	saslPassword   string
	routes         []route
	metadata       bool
	userProperties bool
	prefix         string
	propertyPrefix string
	leaderAcks     bool
	compression    string
	linger         time.Duration
	maxRecords     int64
	maxBytes       int64
	refuse         bool
	wait           time.Duration
	flush          time.Duration
	// namespace is the node's default namespace: a route's when it names
	// none, and the one whose messages carry no mqtt.namespace header.
	namespace string
}

// decode parses the module's configuration, refusing unknown fields.
func decode(raw json.RawMessage) (config, error) {
	var c config
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return config{}, err
	}
	return c, nil
}

// validTopic reports whether name is a legal Kafka topic name: 1 to 249 of
// [A-Za-z0-9._-], and neither "." nor "..".
func validTopic(name string) bool {
	if name == "" || len(name) > maxTopicLen || name == "." || name == ".." {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
		default:
			return false
		}
	}
	return true
}

// validate checks c as a whole, reads the files it names, and returns the
// settings; every error names its field. node and namespace are the module
// context's.
func validate(c config, node, namespace string, readFile func(string) ([]byte, error)) (*settings, error) {
	var errs []error
	bad := func(field, format string, args ...any) {
		errs = append(errs, fmt.Errorf("%s: %s", field, fmt.Sprintf(format, args...)))
	}
	s := &settings{namespace: namespace, metadata: true, userProperties: true, prefix: defaultPrefix}
	if len(c.Brokers) == 0 {
		bad("brokers", "at least one broker is required")
	}
	for i, b := range c.Brokers {
		if host, port, err := net.SplitHostPort(b); err != nil || host == "" || port == "" {
			bad(fmt.Sprintf("brokers[%d]", i), "%q is not host:port", b)
		}
	}
	s.brokers = c.Brokers
	s.clientID = c.ClientID
	if s.clientID == "" {
		s.clientID = "hoardmq"
		if node != "" {
			s.clientID = "hoardmq-" + node
		}
	}
	if len(c.Routes) == 0 {
		bad("routes", "at least one route is required")
	}
	for i, r := range c.Routes {
		field := fmt.Sprintf("routes[%d]", i)
		rt := route{filter: r.Filter, topic: r.Topic, key: r.Key.Name, keyProperty: r.KeyProperty, namespace: r.Namespace}
		if msg := mqtt.ValidateTopicFilter(r.Filter); msg != "" {
			bad(field+".filter", "%s", msg)
		} else if mqtt.IsSharedFilter(r.Filter) {
			bad(field+".filter", "%q is a shared subscription filter, not a topic filter", r.Filter)
		}
		if !validTopic(r.Topic) {
			bad(field+".topic", "%q is not a valid Kafka topic name", r.Topic)
		}
		if r.Key.object {
			rt.key = keySegment
		} else if rt.key == "" {
			rt.key = keyTopic
		}
		switch rt.key {
		case keySegment:
			rt.segment, rt.fallback = segmentKey(r, field, bad)
		case keyTopic, keyClientID, keyNone:
			if r.KeyProperty != "" {
				bad(field+".keyProperty", "is only for key %q", keyUserProperty)
			}
		case keyUserProperty:
			if r.KeyProperty == "" {
				bad(field+".keyProperty", "is required with key %q", keyUserProperty)
			}
		default:
			bad(field+".key", "%q is not one of topic, clientId, userProperty, none", r.Key.Name)
		}
		if rt.namespace == "" {
			rt.namespace = namespace
		}
		rt.headers = staticHeaders(r.Headers, field, bad)
		s.routes = append(s.routes, rt)
	}
	if c.Headers.Metadata != nil {
		s.metadata = *c.Headers.Metadata
	}
	if c.Headers.UserProperties != nil {
		s.userProperties = *c.Headers.UserProperties
	}
	if c.Headers.Prefix != nil {
		s.prefix = *c.Headers.Prefix
	}
	if len(s.prefix) > maxPrefixLen {
		bad("headers.prefix", "is longer than %d bytes", maxPrefixLen)
	}
	s.propertyPrefix = c.Headers.UserPropertyPrefix
	if len(s.propertyPrefix) > maxPrefixLen {
		bad("headers.userPropertyPrefix", "is longer than %d bytes", maxPrefixLen)
	}
	switch c.Acks {
	case "", "all":
	case "leader":
		s.leaderAcks = true
	default:
		bad("acks", "%q is not one of all, leader", c.Acks)
	}
	s.compression = c.Compression
	switch c.Compression {
	case "":
		s.compression = "none"
	case "none", "gzip", "snappy", "lz4", "zstd":
	default:
		bad("compression", "%q is not one of none, gzip, snappy, lz4, zstd", c.Compression)
	}
	s.linger = defaultLingerMs * time.Millisecond
	if c.LingerMs != nil {
		if *c.LingerMs < 0 || *c.LingerMs > maxLingerMs {
			bad("lingerMs", "%d is outside 0–%d", *c.LingerMs, maxLingerMs)
		}
		s.linger = time.Duration(*c.LingerMs) * time.Millisecond
	}
	s.maxRecords = defaultMaxRecords
	if c.Buffer.MaxRecords != nil {
		if *c.Buffer.MaxRecords < 1 {
			bad("buffer.maxRecords", "must be at least 1")
		}
		s.maxRecords = *c.Buffer.MaxRecords
	}
	s.maxBytes = defaultMaxBytes
	if c.Buffer.MaxBytes != nil {
		if *c.Buffer.MaxBytes < minMaxBytes {
			bad("buffer.maxBytes", "must be at least %d", minMaxBytes)
		}
		s.maxBytes = *c.Buffer.MaxBytes
	}
	switch c.Buffer.WhenFull {
	case "", whenFullRefuse:
		s.refuse = true
	case whenFullDrop:
	default:
		bad("buffer.whenFull", "%q is not one of refuse, drop", c.Buffer.WhenFull)
	}
	s.wait = defaultWaitMs * time.Millisecond
	if c.Buffer.WaitMs != nil {
		if *c.Buffer.WaitMs < 0 || *c.Buffer.WaitMs > maxWaitMs {
			bad("buffer.waitMs", "%d is outside 0–%d", *c.Buffer.WaitMs, maxWaitMs)
		}
		s.wait = time.Duration(*c.Buffer.WaitMs) * time.Millisecond
	}
	s.flush = defaultFlushMs * time.Millisecond
	if c.FlushMs != nil {
		if *c.FlushMs < 0 || *c.FlushMs > maxFlushMs {
			bad("flushTimeoutMs", "%d is outside 0–%d", *c.FlushMs, maxFlushMs)
		}
		s.flush = time.Duration(*c.FlushMs) * time.Millisecond
	}
	if c.TLS != nil {
		cfg, err := tlsFrom(*c.TLS, readFile)
		if err != nil {
			errs = append(errs, err)
		}
		s.tls = cfg
	}
	if c.SASL != nil {
		switch c.SASL.Mechanism {
		case "plain", "scram-sha-256", "scram-sha-512":
		default:
			bad("sasl.mechanism", "%q is not one of plain, scram-sha-256, scram-sha-512", c.SASL.Mechanism)
		}
		if c.SASL.Username == "" {
			bad("sasl.username", "is required")
		}
		if c.SASL.PasswordFile == "" {
			bad("sasl.passwordFile", "is required")
		} else if b, err := readFile(c.SASL.PasswordFile); err != nil {
			bad("sasl.passwordFile", "%v", err)
		} else {
			s.saslPassword = strings.TrimSuffix(strings.TrimSuffix(string(b), "\n"), "\r")
		}
		s.saslMechanism, s.saslUser = c.SASL.Mechanism, c.SASL.Username
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return s, nil
}

// tlsFrom builds the client's TLS configuration from the files it names.
func tlsFrom(c tlsConfig, readFile func(string) ([]byte, error)) (*tls.Config, error) {
	cfg := &tls.Config{ServerName: c.ServerName, MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		pem, err := readFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("tls.caFile: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls.caFile: no PEM certificate in %s", c.CAFile)
		}
		cfg.RootCAs = pool
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return nil, errors.New("tls.certFile, tls.keyFile: give both or neither")
	}
	if c.CertFile != "" {
		certPEM, err := readFile(c.CertFile)
		if err != nil {
			return nil, fmt.Errorf("tls.certFile: %w", err)
		}
		keyPEM, err := readFile(c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls.keyFile: %w", err)
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, fmt.Errorf("tls.certFile, tls.keyFile: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// segmentKey validates a route's segment key: the index within bounds, the
// fallback one of topic, clientId or none, and, when the filter fixes its
// topics' level count, a level the topics have.
func segmentKey(r routeConfig, field string, bad func(field, format string, args ...any)) (int, string) {
	if r.KeyProperty != "" {
		bad(field+".keyProperty", "is only for key %q", keyUserProperty)
	}
	if r.Key.Segment == nil {
		bad(field+".key.segment", "is required in the object form")
		return 0, keyTopic
	}
	n := *r.Key.Segment
	if n < 0 || n > maxSegment {
		bad(field+".key.segment", "%d is outside 0–%d", n, maxSegment)
	}
	fallback := r.Key.Fallback
	switch fallback {
	case "":
		fallback = keyTopic
	case keyTopic, keyClientID, keyNone:
	default:
		bad(field+".key.fallback", "%q is not one of topic, clientId, none", r.Key.Fallback)
	}
	if levels := strings.Split(r.Filter, "/"); levels[len(levels)-1] != "#" && n >= len(levels) {
		bad(field+".key.segment", "%d is past the topic's last level: the filter %q has %d levels", n, r.Filter, len(levels))
	}
	return n, fallback
}

// staticHeaders validates a route's static headers and returns them in name
// order.
func staticHeaders(h map[string]string, field string, bad func(field, format string, args ...any)) []staticHeader {
	if len(h) > maxStaticHeaders {
		bad(field+".headers", "at most %d", maxStaticHeaders)
	}
	out := make([]staticHeader, 0, len(h))
	for name, value := range h {
		if name == "" || len(name) > maxHeaderNameLen {
			bad(field+".headers", "a name is 1 to %d bytes", maxHeaderNameLen)
		}
		if len(value) > maxHeaderValueLen {
			bad(field+".headers."+name, "is longer than %d bytes", maxHeaderValueLen)
		}
		out = append(out, staticHeader{name, value})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}
