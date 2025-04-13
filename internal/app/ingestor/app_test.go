package ingestor

import (
	"errors"
	"goriok/pulses/internal/stream"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mocks
type MockPipeline struct {
	mock.Mock
}

func (m *MockPipeline) Start(opts *stream.Options) error {
	args := m.Called(opts)
	return args.Error(0)
}

type MockSourceConnector struct {
	mock.Mock
}

func (m *MockSourceConnector) Read(topic string, handler func(topic string, message []byte)) error {
	args := m.Called(topic, handler)
	return args.Error(0)
}

func (m *MockSourceConnector) Close() {
	m.Called()
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

func (m *MockSinkConnector) Close() {
	m.Called()
}

// Test App.Start when pipeline.Start returns nil (success)
func TestApp_Start_Success(t *testing.T) {
	mockPipeline := new(MockPipeline)
	mockSource := new(MockSourceConnector)
	mockSink := new(MockSinkConnector)

	cfg := Config{
		BrokerPort:  1234,
		SourceTopic: "test-topic",
	}

	app := &App{
		cfg:             cfg,
		pipeline:        mockPipeline,
		sourceConnector: mockSource,
		sinkConnector:   mockSink,
	}

	mockPipeline.On("Start", mock.MatchedBy(func(opts *stream.Options) bool {
		return opts.SourceTopic == "test-topic" &&
			opts.SourceConnector == mockSource &&
			opts.SinkConnector == mockSink
	})).Return(nil)

	err := app.Start()
	assert.NoError(t, err)
	mockPipeline.AssertExpectations(t)
}

// Test App.Start when pipeline.Start returns an error
func TestApp_Start_Failure(t *testing.T) {
	mockPipeline := new(MockPipeline)
	mockSource := new(MockSourceConnector)
	mockSink := new(MockSinkConnector)

	cfg := Config{
		BrokerPort:  1234,
		SourceTopic: "fail-topic",
	}

	app := &App{
		cfg:             cfg,
		pipeline:        mockPipeline,
		sourceConnector: mockSource,
		sinkConnector:   mockSink,
	}

	mockPipeline.On("Start", mock.Anything).Return(errors.New("start failed"))

	err := app.Start()
	assert.EqualError(t, err, "start failed")
	mockPipeline.AssertExpectations(t)
}

// Test App.Stop calls both Close methods
func TestApp_Stop(t *testing.T) {
	mockPipeline := new(MockPipeline)
	mockSource := new(MockSourceConnector)
	mockSink := new(MockSinkConnector)

	cfg := Config{
		BrokerPort:  1234,
		SourceTopic: "test-topic",
	}

	app := &App{
		cfg:             cfg,
		pipeline:        mockPipeline,
		sourceConnector: mockSource,
		sinkConnector:   mockSink,
	}

	mockSource.On("Close").Return()
	mockSink.On("Close").Return()

	app.Stop()

	mockSource.AssertCalled(t, "Close")
	mockSink.AssertCalled(t, "Close")
}
