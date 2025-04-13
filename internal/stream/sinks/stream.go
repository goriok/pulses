package sinks

type SinkConnector interface {
	Connect(topic string) error
	Write(topic string, message []byte) error
}

type Sink struct {
	SinkConnector SinkConnector
}

func NewStreamSink(p SinkConnector) *Sink {
	return &Sink{SinkConnector: p}
}

func (s *Sink) Write(topic string, data []byte) error {
	err := s.SinkConnector.Connect(topic)
	if err != nil {
		return err
	}
	return s.SinkConnector.Write(topic, data)
}
