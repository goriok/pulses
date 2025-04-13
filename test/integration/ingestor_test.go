package integration_test

import (
	"encoding/json"
	"fmt"
	"goriok/pulses/internal/app/ingestor"
	"goriok/pulses/internal/broker/fsbroker"
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
	sinkChan := make(chan []byte, 1)
	tenantID := "b9abc5d1-6fa0-4abb-b944-58dad567ff92"
	productSKU := "77f34178-f441-48f9-9ea8-523233e2c99b"
	useUnit := "GB"

	sourceTopic := fmt.Sprintf("test.%s.source.pulses", uuid.New().String())
	ingestor := ingestor.New(ingestor.Config{
		BrokerPort:  BROKER_PORT,
		SourceTopic: sourceTopic,
		EnableStubs: false,
	})

	go ingestor.Start()

	go testSinkConsumer(tenantID, sinkChan)

	pulses := []*models.Pulse{{
		TenantID:    tenantID,
		ProductSKU:  productSKU,
		UsedAmmount: rand.Float64() * 100,
		UseUnity:    useUnit,
	}}
	err := publish(sourceTopic, pulses)
	if err != nil {
		t.Fatalf("Test failed: Unable to publish message: %v", err)
	}

	select {
	case receivedMsg := <-sinkChan:
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

func publish(topic string, msgs []*models.Pulse) error {
	producer := fsbroker.NewSinkConnector(brokerHost)
	err := producer.Connect(topic)
	if err != nil {
		return err
	}

	for _, msg := range msgs {
		msg, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		err = producer.Write(topic, msg)
		if err != nil {
			return nil
		}
	}

	return nil
}

func testSinkConsumer(tenant string, sinkChan chan []byte) {
	sinkTopic := fmt.Sprintf("tenant.%s.pulses", tenant)
	testOutboundConsumer := fsbroker.NewSourceConnector(brokerHost)
	testOutboundConsumer.Read(sinkTopic, func(topic string, message []byte) {
		sinkChan <- message
	})
}
