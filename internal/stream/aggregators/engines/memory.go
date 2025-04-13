// internal/stream/aggregators/engines/memory.go
package engines

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Sink defines an output destination for aggregated results (e.g. a stream topic).
type Sink interface {
	Write(topic string, data []byte) error
}

// KeyFunc defines a function that generates a string key from a generic event.
type KeyFunc func(event any) string

// AmountFunc defines a function that extracts the amount from a generic event.
type AmountFunc func(event any) float64

// MemoryAggregator aggregates generic events in memory by a dynamic key.
// It flushes the results every 30 seconds to the provided Sink.
type MemoryAggregator struct {
	keyFn      KeyFunc
	amountFn   AmountFunc
	buffer     map[string]float64
	flushEvery time.Duration
	sink       Sink
	mu         sync.Mutex
}

// NewMemoryAggregator creates a new in-memory aggregator
// that emits aggregates grouped by a caller-defined key every 30s.
func NewMemoryAggregator(keyFn KeyFunc, amountFn AmountFunc, sink Sink) *MemoryAggregator {
	a := &MemoryAggregator{
		keyFn:      keyFn,
		amountFn:   amountFn,
		buffer:     make(map[string]float64),
		flushEvery: 30 * time.Second,
		sink:       sink,
	}
	go a.run()
	return a
}

// Add processes a new event and updates the in-memory buffer.
func (a *MemoryAggregator) Add(event any) {
	key := a.keyFn(event)
	amount := a.amountFn(event)
	a.mu.Lock()
	a.buffer[key] += amount
	a.mu.Unlock()
}

// run periodically flushes the buffer to the sink.
func (a *MemoryAggregator) run() {
	ticker := time.NewTicker(a.flushEvery)
	defer ticker.Stop()

	for range ticker.C {
		a.mu.Lock()
		bufferCopy := a.buffer
		a.buffer = make(map[string]float64)
		a.mu.Unlock()

		timestamp := time.Now().Unix()
		for key, total := range bufferCopy {
			msg := map[string]interface{}{
				"key":          key,
				"total_amount": total,
				"window":       "30s",
				"timestamp":    timestamp,
			}

			topic := fmt.Sprintf("aggregated.%s", key)
			data, _ := json.Marshal(msg)
			if err := a.sink.Write(topic, data); err != nil {
				logrus.Errorf("aggregator.memory: failed to write: %v", err)
			}
		}
	}
}
