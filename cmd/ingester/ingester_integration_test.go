package ingester

import (
	"encoding/json"
	"fmt"
	"goriok/pulses/cmd/stubs"
	"goriok/pulses/internal/fsbroker"
	"goriok/pulses/internal/models"
	"math/rand/v2"
	"testing"
)

const (
	brokerPort  = 9999
	testSubject = "test.pulses"
)

var brokerHost = fmt.Sprintf("localhost:%d", brokerPort)

func Test_integration_basic_no_error_publish_pulse(t *testing.T) {
	go stubs.BrokerStart(brokerPort)

	consumer := fsbroker.NewConsumer(brokerHost)
	consumer.Connect(testSubject, func(subject string, msg []byte) {
	})

	tenantId := "b9abc5d1-6fa0-4abb-b944-58dad567ff92"
	productSKU := "77f34178-f441-48f9-9ea8-523233e2c99b"
	useUnit := "GB"

	pulse := &models.Pulse{
		TenantID:    tenantId,
		ProductSKU:  productSKU,
		UsedAmmount: rand.Float64() * 100,
		UseUnity:    useUnit,
	}

	msg, err := json.Marshal(pulse)
	if err != nil {
		t.Fatalf("Failed to marshal message: %v", err)
	}

	producer := fsbroker.NewProducer(brokerHost)
	err = producer.Connect(testSubject)
	if err != nil {
		t.Fatalf("Failed to connect producer: %v", err)
	}

	producer.Publish(testSubject, msg)
	producer.Close()
}
