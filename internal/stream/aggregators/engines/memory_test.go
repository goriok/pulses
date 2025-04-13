package engines

import (
	"sync"

	"github.com/stretchr/testify/mock"
)

// --- Mocks ---

type MockSink struct {
	mock.Mock
	calledData sync.Map
}

func (m *MockSink) Write(topic string, data []byte) error {
	m.calledData.Store(topic, data)
	args := m.Called(topic, data)
	return args.Error(0)
}

// --- Dummy Functions ---

func testKeyFunc(event any) string {
	return event.(string)
}

func testAmountFunc(event any) float64 {
	return 1.0
}

func testSinkDataFunc(key, window string, total float64) (map[string]any, string, error) {
	return map[string]any{
		"key":   key,
		"total": total,
	}, "test.topic." + key, nil
}
