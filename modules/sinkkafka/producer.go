package sinkkafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// newClient builds the franz-go client the settings describe. Its buffer is
// the forwarder's only one. Records are retried without bound and never time
// out (franz-go's defaults), so while Kafka is away they stay buffered until
// the buffer is full.
func newClient(s *settings) (producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.brokers...),
		kgo.ClientID(s.clientID),
		kgo.ProducerLinger(s.linger),
		kgo.MaxBufferedRecords(int(s.maxRecords)),
		kgo.MaxBufferedBytes(int(s.maxBytes)),
		kgo.ProducerBatchCompression(codec(s.compression)),
	}
	if s.leaderAcks {
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	} else {
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}
	if s.tls != nil {
		opts = append(opts, kgo.DialTLSConfig(s.tls))
	}
	switch s.saslMechanism {
	case "plain":
		opts = append(opts, kgo.SASL(plain.Auth{User: s.saslUser, Pass: s.saslPassword}.AsMechanism()))
	case "scram-sha-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: s.saslUser, Pass: s.saslPassword}.AsSha256Mechanism()))
	case "scram-sha-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: s.saslUser, Pass: s.saslPassword}.AsSha512Mechanism()))
	}
	return kgo.NewClient(opts...)
}

func codec(name string) kgo.CompressionCodec {
	switch name {
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	}
	return kgo.NoCompression()
}
