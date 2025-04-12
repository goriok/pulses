package stubs

import (
	"encoding/json"
	"fmt"
	"goriok/pulses/internal/fsbroker"
	"goriok/pulses/internal/models"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

func BrokerStart(port int) {
	broker := fsbroker.NewBroker(port)
	broker.Start()
}

func PulsesGenerator(brokerHost string, subject string) error {
	producer := fsbroker.NewProducer(brokerHost)
	producer.Connect(subject)

	for {
		tenantId := uuid.New().String()
		productSKU := uuid.New().String()
		useUnit := fmt.Sprintf("unit_%d", rand.Intn(10))

		time.Sleep(500 * time.Millisecond)
		pulse := &models.Pulse{
			TenantID:    tenantId,
			ProductSKU:  productSKU,
			UsedAmmount: rand.Float64() * 100,
			UseUnity:    useUnit,
		}

		msg, err := json.Marshal(pulse)
		if err != nil {
			return err
		}
		producer.Publish(subject, msg)
		logrus.Debugf("PRODUCER-STUB: published message on subject %s: %s", subject, msg)
	}
}
