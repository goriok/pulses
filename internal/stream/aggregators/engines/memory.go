// internal/stream/aggregators/engines/memory.go
package engines

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type AggregationEntry struct {
	Total     float64
	FirstSeen string
	LastSeen  string
	ObjectID  string
}

// Sink defines an output destination for aggregated results (e.g. a stream topic).
type Sink interface {
	Write(topic string, data []byte) error
}

// KeyFunc defines a function that generates a string key from a generic event.
type KeyFunc func(event any) string

// SinkTopicSuffixFunc defines a function that generates a topic suffix for the sink.
type SinkDataFunc func(key, window string, total float64) (map[string]any, string, error)

// AmountFunc defines a function that extracts the amount from a generic event.
type AmountFunc func(event any) float64

// MemoryAggregator aggregates generic events in memory by a dynamic key.
// It flushes the results every flushEvery arg seconds to the provided Sink.
type MemoryAggregator struct {
	keyFn      KeyFunc
	amountFn   AmountFunc
	buffer     map[string]*AggregationEntry
	flushEvery time.Duration
	sink       Sink
	mu         sync.Mutex
	sincDataFn SinkDataFunc
}

// NewMemoryAggregator creates a new in-memory aggregator
// that emits aggregates grouped by a caller-defined key every 30s.
func NewMemoryAggregator(keyFn KeyFunc, amountFn AmountFunc, sinkDataFn SinkDataFunc, sink Sink) *MemoryAggregator {
	a := &MemoryAggregator{
		keyFn:      keyFn,
		amountFn:   amountFn,
		buffer:     make(map[string]*AggregationEntry),
		flushEvery: 10 * time.Second,
		sink:       sink,
		sincDataFn: sinkDataFn,
	}
	go a.run()
	return a
}

// Add processes a new event and updates the in-memory buffer.
func (a *MemoryAggregator) Add(event any) {
	key := a.keyFn(event)
	amount := a.amountFn(event)

	a.mu.Lock()

	if _, ok := a.buffer[key]; !ok {
		a.buffer[key] = &AggregationEntry{}
	}
	entry := a.buffer[key]
	entry.Total += amount
	a.buffer[key] = entry
	a.mu.Unlock()
}

// run periodically flushes the buffer to the sink.
func (a *MemoryAggregator) run() {
	ticker := time.NewTicker(a.flushEvery)
	defer ticker.Stop()

	for range ticker.C {
		a.mu.Lock()
		bufferCopy := a.buffer
		a.buffer = make(map[string]*AggregationEntry)
		a.mu.Unlock()

		for key, entry := range bufferCopy {
			sinkData, topic, err := a.sincDataFn(key, a.flushEvery.String(), entry.Total)
			if err != nil {
				logrus.Errorf("aggregator.memory: failed to generate sink data: %v", err)
			}

			data, err := json.Marshal(sinkData)
			if err != nil {
				logrus.Errorf("aggregator.memory: failed to marshal sink data: %v", err)
			}

			if err := a.sink.Write(topic, data); err != nil {
				logrus.Errorf("aggregator.memory: failed to write: %v", err)
			}
		}
	}
}
