package stubs

import (
	"encoding/json"
	"fmt"
	"goriok/pulses/internal/fsbroker"
	"goriok/pulses/internal/models"
	"math/rand"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

func BrokerStart(port int) {
	broker := fsbroker.NewBroker(port)
	broker.Start()
}

func PulsesGenerator(brokerHost string, subject string) {
	producer := fsbroker.NewProducer(brokerHost)
	producer.Publish(subject, func(conn net.Conn, subject string) {
		fmt.Fprintf(conn, "producer_%s\n", subject)

		tenantId := uuid.New().String()
		productSKU := uuid.New().String()
		useUnit := fmt.Sprintf("unit_%d", rand.Intn(10))

		for {
			time.Sleep(500 * time.Millisecond)
			pulse := &models.Pulse{
				TenantID:    tenantId,
				ProductSKU:  productSKU,
				UsedAmmount: rand.Float64() * 100,
				UseUnity:    useUnit,
			}

			pulseJSON, err := json.Marshal(pulse)
			if err != nil {
				logrus.Errorf("Failed to marshal pulse: %v", err)
				continue
			}

			fmt.Fprintf(conn, "%s\n", pulseJSON)
			logrus.Debugf("PRODUCER-STUB: Sent message on subject %s: %s", subject, pulseJSON)
		}
	})
}
