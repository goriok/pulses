package stream

import (
	"encoding/json"
	"goriok/pulses/internal/models"
	"goriok/pulses/internal/stream/aggregators"
	"goriok/pulses/internal/stream/aggregators/engines"
	"goriok/pulses/internal/stream/router"
	"goriok/pulses/internal/stream/sinks"

	"github.com/sirupsen/logrus"
)

// SourceConnector defines an input stream that can receive messages by topic.
type SourceConnector interface {
	Read(topic string, handler func(topic string, message []byte)) error
}

// SinkConnector defines an output stream that can publish messages to a topic.
type SinkConnector interface {
	Connect(topic string) error
	Write(topic string, message []byte) error
}

// Options defines the configuration for setting up the stream pipeline.
// It includes the source topic and the input/output stream interfaces.
type Options struct {
	SourceTopic     string
	SourceConnector SourceConnector
	SinkConnector   SinkConnector
}

// StartPipeline sets up and starts the stream processing pipeline.
//
// It connects to the given source topic via the SourceConnector, and for each incoming
// message:
//   - Deserializes it into a Pulse
//   - Routes it to a tenant-specific topic (raw fan-out)
//   - Sends it to an in-memory aggregator (TenantSKUAmount) for batching and flushing
//
// Aggregated results are emitted every 30 seconds to a sink via the Producer.
//
// # Parameters
//   - opts: configuration including source topic, SourceConnector, and producer.
//
// # Returns
//   - error if connection to the SourceConnector stream fails or processing panics.
func StartPipeline(opts *Options) error {
	sink := sinks.NewStreamSink(opts.SinkConnector)

	aggregator := engines.NewMemoryAggregator(
		aggregators.TenantSKUKey,
		aggregators.TenantSKUAmount,
		sink,
	)

	return opts.SourceConnector.Read(opts.SourceTopic, func(topic string, message []byte) {
		var pulse models.Pulse
		if err := json.Unmarshal(message, &pulse); err != nil {
			logrus.Errorf("stream: failed to unmarshal: %v", err)
			return
		}

		router.FanOutToTenant(opts.SinkConnector, &pulse, message)
		aggregator.Add(&pulse)
	})
}
