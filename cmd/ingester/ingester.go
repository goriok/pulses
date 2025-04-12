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

type Publisher interface {
	Connect(subject string)
	Publish(subject string, message []byte) error
}

type Options struct {
	PulsesSubject string
	Consumer      Consumer
	Publisher     Publisher
}

func Start(opts *Options) error {
	publisher := opts.Publisher
	consumer := opts.Consumer

	err := consumer.Connect(opts.PulsesSubject, func(subject string, message []byte) {
		var pulse *models.Pulse
		err := json.Unmarshal(message, pulse)
		if err != nil {
			logrus.Errorf("Failed to unmarshal message: %v", err)
		}
		tenantPulsesSubject := fmt.Sprintf("pulses.tenant.%s", pulse.TenantID)
		publisher.Connect(tenantPulsesSubject)
		publisher.Publish(tenantPulsesSubject, message)
	})
	return err
}
