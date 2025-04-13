package stream

import (
	"encoding/json"
	"fmt"
	"goriok/pulses/internal/models"
	"goriok/pulses/internal/stream/aggregators"
	"goriok/pulses/internal/stream/aggregators/engines"
	"goriok/pulses/internal/stream/sinks"
	"time"

	"github.com/google/uuid"
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

type Pipeline struct{}

func NewPipeline() *Pipeline {
	return &Pipeline{}
}

func (p *Pipeline) Start(opts *Options) error {
	sourceConnector := opts.SourceConnector
	sinkConnector := opts.SinkConnector

	groupedSink := sinks.NewStreamSink(sinkConnector)

	aggregatedSink := sinks.NewStreamSink(sinkConnector)

	aggregator := engines.NewMemoryAggregator(
		aggregators.TenantSKUKey,
		aggregators.TenantSKUAmount,
		aggregators.TenantSKUInfo,
		aggregatedSink,
	)

	return sourceConnector.Read(opts.SourceTopic, func(topic string, message []byte) {
		var pulse models.Pulse
		if err := json.Unmarshal(message, &pulse); err != nil {
			logrus.Errorf("stream: failed to unmarshal: %v", err)
			return
		}

		groupedTopic := fmt.Sprintf("tenants.%s.grouped.pulses", pulse.TenantID)

		newMsg := map[string]any{
			"object_id":   uuid.New().String(),
			"tenant_id":   pulse.TenantID,
			"product_sku": pulse.ProductSKU,
			"use_unit":    pulse.UseUnity,
			"used_amount": pulse.UsedAmmount,
			"timestamp":   time.Now().Unix(),
		}

		newMsgData, err := json.Marshal(newMsg)
		if err != nil {
			logrus.Errorf("stream: failed to marshal grouped pulse: %v", err)
		}

		if err := groupedSink.Write(groupedTopic, newMsgData); err != nil {
			logrus.Errorf("stream: failed to sink raw grouped pulse: %v", err)
		}

		aggregator.Add(&pulse)
	})
}
