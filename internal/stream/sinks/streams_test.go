package sinks

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- Mock SinkConnector ---

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

// --- Tests ---

func TestStreamSink_Write_Success(t *testing.T) {
	mockConnector := new(MockSinkConnector)
	sink := NewStreamSink(mockConnector)

	topic := "some.topic"
	data := []byte("hello world")

	mockConnector.On("Connect", topic).Return(nil)
	mockConnector.On("Write", topic, data).Return(nil)

	err := sink.Write(topic, data)

	assert.NoError(t, err)
	mockConnector.AssertExpectations(t)
}

func TestStreamSink_Write_ConnectError(t *testing.T) {
	mockConnector := new(MockSinkConnector)
	sink := NewStreamSink(mockConnector)

	topic := "some.topic"
	data := []byte("hello world")

	mockConnector.On("Connect", topic).Return(errors.New("connection failed"))

	err := sink.Write(topic, data)

	assert.EqualError(t, err, "connection failed")
	mockConnector.AssertCalled(t, "Connect", topic)
	mockConnector.AssertNotCalled(t, "Write", mock.Anything, mock.Anything)
}

func TestStreamSink_Write_WriteError(t *testing.T) {
	mockConnector := new(MockSinkConnector)
	sink := NewStreamSink(mockConnector)

	topic := "some.topic"
	data := []byte("test payload")

	mockConnector.On("Connect", topic).Return(nil)
	mockConnector.On("Write", topic, data).Return(errors.New("write failed"))

	err := sink.Write(topic, data)

	assert.EqualError(t, err, "write failed")
	mockConnector.AssertExpectations(t)
}
