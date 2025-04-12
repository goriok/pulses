package fsbroker

import (
	"bufio"
	"fmt"
	"net"

	"github.com/sirupsen/logrus"
)

type Consumer struct {
	broker string
}

func NewConsumer(broker string) *Consumer {
	return &Consumer{
		broker,
	}
}

func (c *Consumer) Connect(subject string, handler func(subject string, msg []byte)) error {
	conn, err := net.Dial("tcp", c.broker)
	if err != nil {
		logrus.Errorf("CONSUMER: Error connecting to broker: %v\n", err)
		return err
	}
	defer conn.Close()

	fmt.Fprintf(conn, "consumer_%s\n", subject)
	logrus.Infof("CONSUMER: Connected to broker %s as consumer for subject %s", c.broker, subject)

	reader := bufio.NewReader(conn)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			logrus.Errorf("CONSUMER: Error reading message: %v\n", err)
			return err
		}
		handler(subject, []byte(message))
		logrus.Debugf("CONSUMER: Received message on subject %s: %s", subject, message)
	}
}
