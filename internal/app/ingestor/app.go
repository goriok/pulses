package ingestor

import (
	"fmt"
	"goriok/pulses/internal/broker/fsbroker"
	"goriok/pulses/internal/stream"

	"github.com/sirupsen/logrus"
)

type Config struct {
	BrokerPort  int
	SourceTopic string
	EnableStubs bool
	StubTenants int
	StubSKUs    int
	StubClean   bool
}

type App struct {
	cfg             Config
	broker          *fsbroker.Broker
	sinkConnector   *fsbroker.SinkConnector
	sourceConnector *fsbroker.SourceConnector
}

func New(cfg Config) *App {
	host := "localhost:" + fmt.Sprint(cfg.BrokerPort)

	return &App{
		cfg:             cfg,
		broker:          fsbroker.NewBroker(cfg.BrokerPort),
		sinkConnector:   fsbroker.NewSinkConnector(host),
		sourceConnector: fsbroker.NewSourceConnector(host),
	}
}

func (a *App) Start() error {
	go a.broker.Start()
	logrus.Infof("broker started on port %d", a.cfg.BrokerPort)

	err := stream.StartPipeline(&stream.Options{
		SourceTopic:     a.cfg.SourceTopic,
		SourceConnector: a.sourceConnector,
		SinkConnector:   a.sinkConnector,
	})
	if err != nil {
		return err
	}

	return nil
}

func (a *App) Stop() {
	a.sourceConnector.Close()
	a.sinkConnector.Close()
	a.broker.Stop()
}
