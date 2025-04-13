package router

import (
	"fmt"
	"goriok/pulses/internal/models"

	"github.com/sirupsen/logrus"
)

type SinkConnector interface {
	Connect(topic string) error
	Write(topic string, message []byte) error
}

func FanOutToTenant(sinkConnector SinkConnector, pulse *models.Pulse, raw []byte) {
	topic := fmt.Sprintf("tenant.%s.pulses", pulse.TenantID)

	if err := sinkConnector.Connect(topic); err != nil {
		logrus.Errorf("grouper: failed to connect to %s: %v", topic, err)
		return
	}

	if err := sinkConnector.Write(topic, raw); err != nil {
		logrus.Errorf("grouper: failed to publish to %s: %v", topic, err)
	}
}
