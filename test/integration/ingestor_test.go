package integration_test

import (
	"encoding/json"
	"fmt"
	"goriok/pulses/internal/app/ingestor"
	"goriok/pulses/internal/broker/fsbroker"
	"goriok/pulses/internal/models"
	"os"
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
	err := os.RemoveAll(".data")
	if err != nil {
		logrus.Fatalf("failed to clean .data folder: %v", err)
		return
	}
	logrus.Infof(".data folder cleaned successfully")

	startBroker()
	m.Run()
}

func Test_integration_ingestor_grouping(t *testing.T) {
	sinkChan := make(chan []byte, 3)
	tenantID := uuid.New().String()
	productSKU := uuid.New().String()
	useUnit := "kWh"
	sourceTopic := fmt.Sprintf("test.%s.source.pulses", uuid.New().String())

	ingestor := ingestor.New(ingestor.Config{
		BrokerPort:  BROKER_PORT,
		SourceTopic: sourceTopic,
		EnableStubs: false,
	})

	go ingestor.Start()

	sinkTopic := fmt.Sprintf("tenants.%s.grouped.pulses", tenantID)
	testSinkConnector := fsbroker.NewSourceConnector(brokerHost)
	go testSinkConnector.Read(sinkTopic, func(topic string, message []byte) {
		sinkChan <- message
	})

	pulses := []*models.Pulse{
		{TenantID: tenantID, ProductSKU: productSKU, UsedAmmount: 10.5, UseUnity: useUnit},
		{TenantID: tenantID, ProductSKU: productSKU, UsedAmmount: 20.0, UseUnity: useUnit},
		{TenantID: tenantID, ProductSKU: productSKU, UsedAmmount: 20.0, UseUnity: useUnit},
	}
	err := publish(sourceTopic, pulses)
	if err != nil {
		t.Fatalf("Test failed: Unable to publish message: %v", err)
	}

	// ✅ Wait for all 3 messages
	msgs := make([][]byte, 0, 3)
	timeout := time.After(5 * time.Second)

	for len(msgs) < 3 {
		select {
		case msg := <-sinkChan:
			msgs = append(msgs, msg)
		case <-timeout:
			t.Fatalf("Test failed: Timeout — only received %d messages", len(msgs))
		}
	}

	t.Logf("Test passed: received %d grouped messages", len(msgs))
}

func Test_integration_ingestor_aggregation_output(t *testing.T) {
	sinkChan := make(chan []byte, 1)
	tenantID := uuid.New().String()
	productSKU := uuid.New().String()
	useUnit := "kWh"
	sourceTopic := fmt.Sprintf("test.%s.source.pulses", uuid.New().String())

	ingestor := ingestor.New(ingestor.Config{
		BrokerPort:  BROKER_PORT,
		SourceTopic: sourceTopic,
		EnableStubs: false,
	})

	go ingestor.Start()

	sinkTopic := fmt.Sprintf("tenants.%s.aggregated.pulses.amount", tenantID)
	testOutboundConsumer := fsbroker.NewSourceConnector(brokerHost)
	go testOutboundConsumer.Read(sinkTopic, func(topic string, message []byte) {
		sinkChan <- message
	})

	pulses := []*models.Pulse{
		{TenantID: tenantID, ProductSKU: productSKU, UsedAmmount: 10.5, UseUnity: useUnit},
		{TenantID: tenantID, ProductSKU: productSKU, UsedAmmount: 20.0, UseUnity: useUnit},
	}
	err := publish(sourceTopic, pulses)
	if err != nil {
		t.Fatalf("Test failed: Unable to publish message: %v", err)
	}

	select {
	case aggregatedMsg := <-sinkChan:
		var data map[string]any
		err := json.Unmarshal(aggregatedMsg, &data)
		if err != nil {
			t.Fatalf("Test failed: Unable to unmarshal aggregated message: %v", err)
		}

		expectedTotal := 30.5
		got := data["total_amount"].(float64)
		if got != expectedTotal {
			t.Fatalf("Test failed: expected total %.1f, got %.1f", expectedTotal, got)
		}

		t.Logf("Test passed: Received expected aggregated message: %+v", data)
	case <-time.After(5 * time.Second): // must match flush timer + margin
		t.Fatal("Test failed: Timeout waiting for aggregated message")
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
		time.Sleep(time.Millisecond * 100)
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
