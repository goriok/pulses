package ingestor

import (
	"encoding/json"
	"fmt"
	"goriok/pulses/internal/fsbroker"
	"goriok/pulses/internal/models"
	"math/rand/v2"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

const (
	BROKER_PORT = 9999
)

var (
	broker     *fsbroker.Broker
	brokerOnce sync.Once
	brokerHost = fmt.Sprintf("localhost:%d", BROKER_PORT)
)

func TestMain(m *testing.M) {
	startBroker()
	m.Run()
}

func Test_integration_ingestor_receiving_message(t *testing.T) {
	msgChan := make(chan []byte, 1)

	tenantID := "b9abc5d1-6fa0-4abb-b944-58dad567ff92"
	productSKU := "77f34178-f441-48f9-9ea8-523233e2c99b"
	useUnit := "GB"

	testSubject := fmt.Sprintf("test.%s.cloud.pulses", uuid.New().String())
	testConsumer := fsbroker.NewConsumer(brokerHost)

	cleanedTenantID := regexp.MustCompile(`[^\w\n]`).ReplaceAllString(tenantID, "")

	pulsesTenantSubject := fmt.Sprintf("pulses.tenant.%s", cleanedTenantID)

	go testConsumer.Connect(pulsesTenantSubject, func(subject string, message []byte) {
		msgChan <- message
	})
	time.Sleep(1 * time.Second)

	pulse := &models.Pulse{
		TenantID:    tenantID,
		ProductSKU:  productSKU,
		UsedAmmount: rand.Float64() * 100,
		UseUnity:    useUnit,
	}

	pulses := []*models.Pulse{pulse}
	err := publish(testSubject, pulses)
	if err != nil {
		t.Fatalf("Test failed: Unable to publish message: %v", err)
	}

	select {
	case receivedMsg := <-msgChan:
		var receivedPulse models.Pulse
		err := json.Unmarshal(receivedMsg, &receivedPulse)
		if err != nil {
			t.Fatalf("Test failed: Unable to unmarshal received message: %v", err)
		}

		if receivedPulse.TenantID != tenantID || receivedPulse.ProductSKU != productSKU || receivedPulse.UseUnity != useUnit {
			t.Fatalf("Test failed: Received message does not match expected values. Got %+v", receivedPulse)
		}

		t.Logf("Test passed: Received expected message: %+v", receivedPulse)
	case <-time.After(3 * time.Second):
		t.Fatal("Test failed: Timeout waiting for message")
	}
}

func startBroker() {
	brokerOnce.Do(func() {
		broker = fsbroker.NewBroker(BROKER_PORT)
		go func() {
			err := broker.Start()
			if err != nil {
				logrus.Fatalf("Failed to start broker: %v", err)
			}
		}()
		time.Sleep(1 * time.Second)
	})
}

func publish(subject string, msgs []*models.Pulse) error {
	producer := fsbroker.NewProducer(brokerHost)
	err := producer.Connect(subject)
	if err != nil {
		return err
	}

	for _, msg := range msgs {
		msg, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		err = producer.Publish(subject, msg)
		if err != nil {
			return nil
		}
	}

	return nil
}
