package stubs

import (
	"encoding/json"
	"fmt"
	"goriok/pulses/internal/fsbroker"
	"goriok/pulses/internal/models"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

const (
	DATA_DIR = ".data"
)

type SKU struct {
	Id      string
	UseUnit string
}

func ProduceRandomTenantPulses(brokerHost string, subject string, tenantsAmount int, skuAmount int) error {
	err := setupStub()
	if err != nil {
		return err
	}

	producer := fsbroker.NewProducer(brokerHost)
	producer.Connect(subject)
	defer producer.Close()

	tenants := generateRandomTenants(tenantsAmount)
	skus := generateRandomSKU(skuAmount)

	for {
		time.Sleep(300 * time.Millisecond)
		randomTenant := tenants[rand.Intn(len(tenants)-1)]
		randomSKU := skus[rand.Intn(len(skus)-1)]

		pulse := &models.Pulse{
			TenantID:    randomTenant,
			ProductSKU:  randomSKU.Id,
			UsedAmmount: rand.Float64() * 100,
			UseUnity:    randomSKU.UseUnit,
		}

		msg, err := json.Marshal(pulse)
		if err != nil {
			return err
		}
		producer.Publish(subject, msg)
	}
}

func setupStub() error {
	_, err := os.ReadDir(DATA_DIR)
	if err != nil && !os.IsNotExist(err) {
		logrus.Fatalf("failed to read .data folder: %v", err)
	}

	if os.IsNotExist(err) {
		err = os.MkdirAll(DATA_DIR, os.ModePerm)
		if err != nil {
			return err
		}
		logrus.Infof(".data folder created successfully")
	}

	return nil
}

func CleanSubjects() {
	err := os.RemoveAll(".data")
	if err != nil {
		logrus.Fatalf("failed to clean .data folder: %v", err)
	}
	logrus.Infof(".data folder cleaned successfully")
}

func generateRandomSKU(amount int) []*SKU {
	skus := make([]*SKU, 0, amount)

	for i := 1; i <= amount; i++ {
		skus = append(skus, &SKU{
			Id:      uuid.New().String(),
			UseUnit: fmt.Sprintf("some_unit_%d", i),
		})
	}
	return skus
}

func generateRandomTenants(amount int) []string {
	tenants := make([]string, 0, amount)

	for i := 1; i <= amount; i++ {
		tenants = append(tenants, uuid.New().String())
	}
	return tenants
}
