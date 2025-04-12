package ingester

import (
	"encoding/json"
	"fmt"
	"goriok/pulses/cmd/stubs"
	"goriok/pulses/internal/fsbroker"
	"goriok/pulses/internal/models"
	"math/rand/v2"
	"testing"
	"time"
)

const (
	brokerPort  = 9999
	testSubject = "test.pulses"
)

var brokerHost = fmt.Sprintf("localhost:%d", brokerPort)

func Test_integration_ingestor_receiving_message(t *testing.T) {
	done := make(chan bool)

	go stubs.BrokerStart(brokerPort)

	consumer := fsbroker.NewConsumer(brokerHost)

	go consumer.Connect(testSubject, func(subject string, msg []byte) {
		t.Logf("Received message on subject %s: %s", subject, msg)
		done <- true
	})
	time.Sleep(1 * time.Second)

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
	err = producer.Publish(testSubject, msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	select {
	case <-done:
		t.Log("Test passed: Message received")
	case <-time.After(3 * time.Second):
		t.Fatal("Test failed: Timeout waiting for message")
	}
}
