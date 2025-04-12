package fsbroker

import (
	"net"

	"github.com/sirupsen/logrus"
)

type Producer struct {
	broker string
}

func NewProducer(broker string) *Producer {
	return &Producer{
		broker,
	}
}

func (p *Producer) Publish(subject string, gen func(conn net.Conn, subject string)) {
	conn, err := net.Dial("tcp", p.broker)
	if err != nil {
		logrus.Errorf("failed to connect to broker: %v", err)
		return
	}
	defer conn.Close()
	gen(conn, subject)
}
