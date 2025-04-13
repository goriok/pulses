package main

import (
	"flag"
	"fmt"
	"goriok/pulses/cmd/ingester"
	"goriok/pulses/cmd/stubs"
	"goriok/pulses/internal/fsbroker"

	"github.com/sirupsen/logrus"
)

const (
	brokerPort    = 9000
	pulsesSubject = "cloud.sku.pulses"
)

var (
	brokerHost = fmt.Sprintf("localhost:%d", brokerPort)
	stub       = flag.Bool("stub", false, "Enable or disable stub (default: false)")
	tenants    = flag.Int("stub-amount", 100, "Number of tenants (default: 100)")
)

func main() {
	flag.Parse()

	broker := fsbroker.NewBroker(brokerPort)
	go broker.Start()
	defer broker.Stop()
	logrus.Infof("fsbroker started, port: %d", brokerPort)

	if *stub {
		go stubs.PulsesGenerator(brokerHost, pulsesSubject, *tenants)
		logrus.Infof("stub pulses generator started, host: localhost, port: %d, tenants_amount: %d", brokerPort, *tenants)
	}

	consumer := fsbroker.NewConsumer(brokerHost)
	producer := fsbroker.NewProducer(brokerHost)
	go ingester.Start(&ingester.Options{
		PulsesSubject: pulsesSubject,
		Consumer:      consumer,
		Producer:      producer,
	})

	select {}
}
