package ingestor

import (
	"flag"
	"fmt"
	"goriok/pulses/cmd/stubs"
	"goriok/pulses/internal/app/ingestor"
	"log"
	"time"
)

func main() {
	var cfg ingestor.Config
	flag.IntVar(&cfg.BrokerPort, "port", 9000, "Broker port")
	flag.StringVar(&cfg.SourceTopic, "source-topic", "source.pulses", "Source Topic")
	flag.BoolVar(&cfg.EnableStubs, "stub", false, "Enable stubs")
	flag.IntVar(&cfg.StubTenants, "stub-tenants", 10, "Number of tenants")
	flag.IntVar(&cfg.StubSKUs, "stub-skus", 50, "Number of SKUs")
	flag.BoolVar(&cfg.StubClean, "stub-clean", false, "Clean all topics")
	flag.Parse()

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
	select {}
}
