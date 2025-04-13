package ingestor

import (
	"encoding/json"
	"fmt"
	"goriok/pulses/internal/models"

	"github.com/sirupsen/logrus"
)

type Consumer interface {
	Connect(subject string, handler func(subject string, message []byte)) error
}

type Producer interface {
	Connect(subject string) error
	Publish(subject string, message []byte) error
}

type Options struct {
	PulsesSubject string
	Consumer      Consumer
	Producer      Producer
}

// GroupByTenant processes messages from a consumer and republishes them grouped by tenant.
// It connects to a consumer to receive messages on a specified subject, unmarshals the messages,
// and republishes them to a dynamically generated subject based on the tenant ID.
//
// Parameters:
//
//	opts - Options struct containing the producer, consumer, and subject configuration.
//
// Returns:
//
//	error - An error if the consumer connection or message processing fails.
func GroupByTenant(opts *Options) error {
	publisher := opts.Producer
	consumer := opts.Consumer

	err := consumer.Connect(opts.PulsesSubject, func(subject string, message []byte) {
		var pulse models.Pulse
		err := json.Unmarshal(message, &pulse)
		if err != nil {
			logrus.Errorf("ingestor: Failed to unmarshal message: %v", err)
		}

		byTenantSubject := fmt.Sprintf("pulses.tenant.%s", pulse.TenantID)
		err = publisher.Connect(byTenantSubject)
		if err != nil {
			logrus.Errorf("ingestor: Failed to connect to tenant pulses subject: %v", err)
			return
		}

		err = publisher.Publish(byTenantSubject, message)
		if err != nil {
			logrus.Errorf("ingestor: Failed to publish message to tenant pulses subject: %v", err)
			return
		}
	})
	return err
}
