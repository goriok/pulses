package sinks

type SinkConnector interface {
	Connect(topic string) error
	Write(topic string, message []byte) error
}

type StreamSink struct {
	SinkConnector SinkConnector
}

func NewStreamSink(p SinkConnector) *StreamSink {
	return &StreamSink{SinkConnector: p}
}

func (s *StreamSink) Write(topic string, data []byte) error {
	err := s.SinkConnector.Connect(topic)
	if err != nil {
		return err
	}
	return s.SinkConnector.Write(topic, data)
}
