package ingester

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
	brokerPort = 9999
)

var (
	broker     *fsbroker.Broker
	brokerOnce sync.Once
	brokerHost = fmt.Sprintf("localhost:%d", brokerPort)
)

func TestMain(m *testing.M) {
	startBroker()

	code := m.Run()
	logrus.Infof("TestMain: exiting with code %d", code)
}

func Test_integration_ingestor_receiving_message(t *testing.T) {
	msgChan := make(chan []byte, 1)

	tenantId := "b9abc5d1-6fa0-4abb-b944-58dad567ff92"
	productSKU := "77f34178-f441-48f9-9ea8-523233e2c99b"
	useUnit := "GB"

	pulse := &models.Pulse{
		TenantID:    tenantId,
		ProductSKU:  productSKU,
		UsedAmmount: rand.Float64() * 100,
		UseUnity:    useUnit,
	}

	testSubject := fmt.Sprintf("cloud.pulses.%s", uuid.New().String())
	msg, err := json.Marshal(pulse)
	if err != nil {
		t.Fatalf("Test failed: Unable to marshal pulse: %v", err)
	}

	err = publish(testSubject, [][]byte{msg})
	if err != nil {
		t.Fatalf("Test failed: Unable to publish message: %v", err)
	}

	testConsumer := fsbroker.NewConsumer(brokerHost)
	cleanedTenantID := regexp.MustCompile(`[^\w\n]`).ReplaceAllString(pulse.TenantID, "")
	pulsesTenantSubject := fmt.Sprintf("pulses.tenant.%s", cleanedTenantID)

	go testConsumer.Connect(pulsesTenantSubject, func(subject string, message []byte) {
		msgChan <- message
	})

	select {
	case receivedMsg := <-msgChan:
		var receivedPulse models.Pulse
		err := json.Unmarshal(receivedMsg, &receivedPulse)
		if err != nil {
			t.Fatalf("Test failed: Unable to unmarshal received message: %v", err)
		}

		if receivedPulse.TenantID != tenantId || receivedPulse.ProductSKU != productSKU || receivedPulse.UseUnity != useUnit {
			t.Fatalf("Test failed: Received message does not match expected values. Got %+v", receivedPulse)
		}

		t.Logf("Test passed: Received expected message: %+v", receivedPulse)
	case <-time.After(3 * time.Second):
		t.Fatal("Test failed: Timeout waiting for message")
	}
}

func startBroker() {
	brokerOnce.Do(func() {
		broker = fsbroker.NewBroker(brokerPort)
		go func() {
			err := broker.Start()
			if err != nil {
				logrus.Fatalf("Failed to start broker: %v", err)
			}
		}()
	})
}

func publish(subject string, msgs [][]byte) error {
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
