package ingester

import (
	"encoding/json"
	"fmt"
	"goriok/pulses/internal/fsbroker"
	"goriok/pulses/internal/models"
	"math/rand/v2"
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
	outboundMsgs := make(chan []byte, 1)

	tenantID := uuid.New().String()
	productSKU := uuid.New().String()
	useUnit := "GB"

	go outboundSubjectConsumer(tenantID, outboundMsgs)

	inboundSubject := testInboundSubject(tenantID)
	go startIngester(inboundSubject)

	time.Sleep(1 * time.Second)
	inboundMsgs := []*models.Pulse{{
		TenantID:    tenantID,
		ProductSKU:  productSKU,
		UsedAmmount: rand.Float64() * 100,
		UseUnity:    useUnit,
	}}
	err := publish(inboundSubject, inboundMsgs)
	if err != nil {
		t.Fatalf("Test failed: Unable to publish message: %v", err)
	}

	select {
	case outboundMessage := <-outboundMsgs:
		var pulse models.Pulse
		err := json.Unmarshal(outboundMessage, &pulse)
		if err != nil {
			t.Fatalf("Test failed: Unable to unmarshal received message: %v", err)
		}

		if pulse.TenantID != tenantID || pulse.ProductSKU != productSKU || pulse.UseUnity != useUnit {
			t.Fatalf("Test failed: Received message does not match expected values. Got %+v", pulse)
		}

		t.Logf("Test passed: Received expected message: %+v", pulse)
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

func outboundSubjectConsumer(tenantID string, msgChan chan []byte) {
	pulsesTenantSubject := fmt.Sprintf("pulses.tenant.%s", tenantID)
	testConsumer := fsbroker.NewConsumer(brokerHost)

	testConsumer.Connect(pulsesTenantSubject, func(subject string, message []byte) {
		msgChan <- message
	})
}

func startIngester(testSubject string) {
	consumer := fsbroker.NewConsumer(brokerHost)
	defer consumer.Close()

	producer := fsbroker.NewProducer(brokerHost)
	defer producer.Close()
	Start(&Options{
		PulsesSubject: testSubject,
		Consumer:      consumer,
		Producer:      producer,
	})
}

func testInboundSubject(id string) string {
	return fmt.Sprintf("test.%s.cloud.pulses", id)
}
