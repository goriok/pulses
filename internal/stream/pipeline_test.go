package stream

import (
	"encoding/json"
	"errors"
	"goriok/pulses/internal/models"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- Mocks ---

type MockSourceConnector struct {
	mock.Mock
}

func (m *MockSourceConnector) Read(topic string, handler func(string, []byte)) error {
	args := m.Called(topic, handler)
	if handler != nil {
		// Simulate sending a message
		pulse := &models.Pulse{
			TenantID:    "tenant123",
			ProductSKU:  "sku456",
			UseUnity:    "unit789",
			UsedAmmount: 42.0,
		}
		payload, _ := json.Marshal(pulse)
		handler(topic, payload)
	}
	return args.Error(0)
}

type MockSinkConnector struct {
	mock.Mock
}

func (m *MockSinkConnector) Connect(topic string) error {
	args := m.Called(topic)
	return args.Error(0)
}

func (m *MockSinkConnector) Write(topic string, message []byte) error {
	args := m.Called(topic, message)
	return args.Error(0)
}

// --- Test ---

func TestPipeline_Start_Success(t *testing.T) {
	source := new(MockSourceConnector)
	sink := new(MockSinkConnector)
	pipeline := NewPipeline()

	opts := &Options{
		SourceTopic:     "pulses.incoming",
		SourceConnector: source,
		SinkConnector:   sink,
	}

	expectedGroupedTopic := "tenants.tenant123.grouped.pulses"
	sink.On("Connect", expectedGroupedTopic).Return(nil)
	sink.On("Write", mock.MatchedBy(func(topic string) bool {
		return topic == expectedGroupedTopic
	}), mock.MatchedBy(func(data []byte) bool {
		var out map[string]any
		err := json.Unmarshal(data, &out)
		return err == nil &&
			out["tenant_id"] == "tenant123" &&
			out["product_sku"] == "sku456" &&
			out["use_unit"] == "unit789" &&
			out["used_amount"] == 42.0 &&
			out["object_id"] != "" &&
			out["timestamp"] != nil
	})).Return(nil)

	source.On("Read", "pulses.incoming", mock.Anything).Return(nil)

	err := pipeline.Start(opts)
	assert.NoError(t, err)

	source.AssertExpectations(t)
	sink.AssertExpectations(t)
}

// Optional: test marshal failure
func TestPipeline_Start_MarshalError(t *testing.T) {
	source := new(MockSourceConnector)
	sink := new(MockSinkConnector)
	pipeline := NewPipeline()

	opts := &Options{
		SourceTopic:     "pulses.incoming",
		SourceConnector: source,
		SinkConnector:   sink,
	}

	// Patch uuid.NewString to return a value that causes marshal failure (e.g., unsupported type)
	// but here we’ll simulate failure through sink returning an error instead

	sink.On("Connect", mock.Anything).Return(nil)
	sink.On("Write", mock.Anything, mock.Anything).Return(errors.New("sink error"))
	source.On("Read", "pulses.incoming", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(1).(func(string, []byte))
		p := models.Pulse{TenantID: "X", ProductSKU: "Y", UseUnity: "Z", UsedAmmount: 1}
		raw, _ := json.Marshal(p)
		handler("pulses.incoming", raw)
	})

	err := pipeline.Start(opts)
	assert.NoError(t, err)
}
