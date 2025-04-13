// Package ingestor provides the entry point for configuring and starting
// the stream processing pipeline.
//
// It wires together a source connector, a sink connector, and the pipeline
// that orchestrates message flow between them, using a filesystem-backed broker
// and Kappa-style architecture for processing.
package ingestor

import (
	"fmt"
	"goriok/pulses/internal/broker/fsbroker"
	"goriok/pulses/internal/stream"
)

type Pipeline interface {
	Start(opts *stream.Options) error
}

type SourceConnector interface {
	Read(topic string, handler func(topic string, message []byte)) error
	Close()
}

type SinkConnector interface {
	Connect(topic string) error
	Write(topic string, message []byte) error
	Close()
}

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
	sinkConnector   SinkConnector
	sourceConnector SourceConnector
	pipeline        Pipeline
}

func New(cfg Config) *App {
	host := "localhost:" + fmt.Sprint(cfg.BrokerPort)

	return &App{
		cfg:             cfg,
		sinkConnector:   fsbroker.NewSinkConnector(host),
		sourceConnector: fsbroker.NewSourceConnector(host),
		pipeline:        stream.NewPipeline(),
	}
}

func (a *App) Start() error {
	err := a.pipeline.Start(&stream.Options{
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
}
