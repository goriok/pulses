package fsbroker

import (
	"bufio"
	"fmt"
	"net"

	"github.com/sirupsen/logrus"
)

type SourceConnector struct {
	broker string
	conn   *net.Conn
}

func NewSourceConnector(broker string) *SourceConnector {
	return &SourceConnector{
		broker,
		nil,
	}
}

// Read connects to the broker and subscribes to the given topic.
//
// It invokes the provided handler for every message received from the broker.
// This function blocks indefinitely unless an error occurs.
func (c *SourceConnector) Read(topic string, handler func(topic string, msg []byte)) error {
	conn, err := net.Dial("tcp", c.broker)
	if err != nil {
		logrus.Errorf("source-connector: error connecting to broker: %v", err)
		return err
	}
	c.conn = &conn
	defer conn.Close()

	fmt.Fprintf(conn, "source-connector_%s\n", topic)
	logrus.Infof("source-connector: connected to broker %s for topic %s", c.broker, topic)

	reader := bufio.NewReader(conn)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			logrus.Errorf("source-connector: error reading message: %v", err)
			return err
		}
		handler(topic, []byte(message))
		logrus.Debugf("source-connector: received message on topic %s: %s", topic, message)
	}
}

func (c *SourceConnector) Close() {
	(*c.conn).Close()
}
