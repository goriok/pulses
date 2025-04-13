package main

import (
	"flag"
	"fmt"
	"goriok/pulses/cmd/ingester"
	"goriok/pulses/cmd/stubs"
	"goriok/pulses/internal/fsbroker"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	BROKER_PORT    = 9000
	PULSES_SUBJECT = "cloud.sku.pulses"
)

var (
	brokerHost = fmt.Sprintf("localhost:%d", BROKER_PORT)
	stub       = flag.Bool("stub", false, "Enable or disable stub (default: false)")
	tenants    = flag.Int("stub-tenants", 10, "Number of tenants (default: 10)")
	skus       = flag.Int("stub-skus", 50, "Number of sku (default: 50)")
	cleanStubs = flag.Bool("stub-clean", false, "Clean all subjects")
)

func main() {
	flag.Parse()

	broker := fsbroker.NewBroker(BROKER_PORT)
	go broker.Start()
	time.Sleep(1 * time.Second)

	defer broker.Stop()
	logrus.Infof("fsbroker started, port: %d", BROKER_PORT)

	if *stub {
		go stubsSetup()
	}

	consumer := fsbroker.NewConsumer(brokerHost)
	defer consumer.Close()

	producer := fsbroker.NewProducer(brokerHost)
	defer producer.Close()

	go ingester.Start(&ingester.Options{
		PulsesSubject: PULSES_SUBJECT,
		Consumer:      consumer,
		Producer:      producer,
	})
	logrus.Infof("ingester started, pulses subject: %s", PULSES_SUBJECT)

	select {}
}

func stubsSetup() {
	if *cleanStubs {
		stubs.CleanSubjects()
	}

	stubs.ProduceRandomTenantPulses(brokerHost, PULSES_SUBJECT, *tenants, *skus)
	logrus.Infof("stub pulses generator started, host: localhost, port: %d, tenants_amount: %d", BROKER_PORT, *tenants)
}
