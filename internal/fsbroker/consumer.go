package fsbroker

import (
	"bufio"
	"fmt"
	"net"

	"github.com/sirupsen/logrus"
)

type Consumer struct {
	broker string
	conn   *net.Conn
}

func NewConsumer(broker string) *Consumer {
	return &Consumer{
		broker,
		nil,
	}
}

func (c *Consumer) Connect(subject string, handler func(subject string, msg []byte)) error {
	conn, err := net.Dial("tcp", c.broker)
	if err != nil {
		logrus.Errorf("consumer: error connecting to broker: %v", err)
		return err
	}
	c.conn = &conn
	defer conn.Close()

	fmt.Fprintf(conn, "consumer_%s\n", subject)
	logrus.Infof("consumer: connected to broker %s for subject %s", c.broker, subject)

	reader := bufio.NewReader(conn)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			logrus.Errorf("consumer: error reading message: %v", err)
			return err
		}
		handler(subject, []byte(message))
		logrus.Debugf("consumer: received message on subject %s: %s", subject, message)
	}
}

func (c *Consumer) Close() {
	(*c.conn).Close()
}
