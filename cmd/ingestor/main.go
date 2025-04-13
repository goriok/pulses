// Package ingestor provides the main application entry point
// for coordinating the stream processing pipeline.
//
// It initializes connectors to a filesystem-backed message broker
// and delegates message routing and aggregation to internal stream logic.
package main

import (
	"flag"
	"fmt"
	"goriok/pulses/cmd/stubs"
	"goriok/pulses/internal/app/ingestor"
	"goriok/pulses/internal/broker/fsbroker"
	"log"
	"time"
)

// main configures the ingestor application using command-line flags,
// starts the local filesystem-backed broker, and launches the processing pipeline.
//
// Optionally, it runs stub data generators for simulating tenant pulse messages.
func main() {
	var cfg ingestor.Config

	flag.IntVar(&cfg.BrokerPort, "port", 9000, "Broker port")
	flag.StringVar(&cfg.SourceTopic, "source-topic", "source.pulses", "Source Topic")
	flag.BoolVar(&cfg.EnableStubs, "stub", false, "Enable stubs")
	flag.IntVar(&cfg.StubTenants, "stub-tenants", 10, "Number of tenants")
	flag.IntVar(&cfg.StubSKUs, "stub-skus", 50, "Number of SKUs")
	flag.BoolVar(&cfg.StubClean, "stub-clean", false, "Clean all topics")
	flag.Parse()

	broker := fsbroker.NewBroker(cfg.BrokerPort)
	go broker.Start()
	time.Sleep(1 * time.Second)

	app := ingestor.New(cfg)
	defer app.Stop()

	if cfg.EnableStubs {
		go func() {
			if cfg.StubClean {
				stubs.CleanTopics()
			}
			stubs.WriteRandomTenantPulses(
				"localhost:"+fmt.Sprint(cfg.BrokerPort),
				cfg.SourceTopic,
				cfg.StubTenants,
				cfg.StubSKUs,
			)
		}()
	}

	time.Sleep(1 * time.Second)
	if err := app.Start(); err != nil {
		log.Fatalf("app failed: %v", err)
	}
}
