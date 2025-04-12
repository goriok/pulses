package fsbroker

import (
	"fmt"
	"net"
)

type Producer struct {
	broker string
	conn   *net.Conn
}

func NewProducer(broker string) *Producer {
	return &Producer{
		broker,
		nil,
	}
}

func (p *Producer) Connect(subject string) error {
	conn, err := net.Dial("tcp", p.broker)
	if err != nil {
		return err
	}
	fmt.Fprintf(conn, "producer_%s\n", subject)
	return nil
}

func (p *Producer) Close() {
	if p.conn != nil {
		(*p.conn).Close()
	}
}

func (p *Producer) Publish(subject string, msg []byte) error {
	if p.conn == nil {
		return fmt.Errorf("producer not connected")
	}

	fmt.Fprintf(*p.conn, "%s\n", msg)
	return nil
}
