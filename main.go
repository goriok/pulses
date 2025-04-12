package main

import (
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

func main() {
	go stubs.BrokerStart(brokerPort)
	logrus.Infof("stub broker started, port: %d", brokerPort)

	brokerHost := fmt.Sprintf("localhost:%d", brokerPort)
	go stubs.PulsesGenerator(brokerHost, pulsesSubject)
	logrus.Infof("stub pulses generator started, host: localhost, port: %d", brokerPort)

	consumer := fsbroker.NewConsumer(brokerHost)
	go ingester.Start(&ingester.Options{
		Subject:  pulsesSubject,
		Consumer: consumer,
	})

	select {}
}
