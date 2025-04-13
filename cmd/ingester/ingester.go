package ingester

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

func Start(opts *Options) error {
	publisher := opts.Producer
	consumer := opts.Consumer

	err := consumer.Connect(opts.PulsesSubject, func(subject string, message []byte) {
		var pulse models.Pulse
		err := json.Unmarshal(message, &pulse)
		if err != nil {
			logrus.Errorf("ingestor: Failed to unmarshal message: %v", err)
		}

		tenantPulsesSubject := fmt.Sprintf("pulses.tenant.%s", pulse.TenantID)

		err = publisher.Connect(tenantPulsesSubject)
		if err != nil {
			logrus.Errorf("ingestor: Failed to connect to tenant pulses subject: %v", err)
			return
		}

		err = publisher.Publish(tenantPulsesSubject, message)
		if err != nil {
			logrus.Errorf("ingestor: Failed to publish message to tenant pulses subject: %v", err)
			return
		}
	})
	return err
}
