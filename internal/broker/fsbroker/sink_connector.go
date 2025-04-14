package fsbroker

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// SinkConnector provides an interface for writing messages to a specific topic
// on a filesystem-backed broker via TCP. It includes connection caching and expiration.
type SinkConnector struct {
	broker     string
	mu         sync.Mutex
	cache      map[string]*net.Conn
	expiration map[string]time.Time
}

// SinkConnector manages outbound TCP connections to broker topics
// and publishes messages by writing to a topic-specific stream.
func NewSinkConnector(broker string) *SinkConnector {
	return &SinkConnector{
		broker,
		sync.Mutex{},
		make(map[string]*net.Conn),
		make(map[string]time.Time),
	}
}

// Connect establishes or reuses a TCP connection to the specified topic.
//
// Connections are cached per topic and automatically expire after 5 minutes.
// If a valid connection already exists, it is reused.
func (p *SinkConnector) Connect(topic string) error {
	if _, ok := p.cache[topic]; ok {
		if time.Now().Before(p.expiration[topic]) {
			return nil
		}
		p.mu.Lock()
		defer p.mu.Unlock()

		(*p.cache[topic]).Close()
		delete(p.cache, topic)
		delete(p.expiration, topic)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	conn, err := net.Dial("tcp", p.broker)
	if err != nil {
		return err
	}
	p.cache[topic] = &conn

	fmt.Fprintf(*p.cache[topic], "sink-connector_%s\n", topic)
	logrus.Infof("sink-connector: connected to broker %s for topic %s", p.broker, topic)

	p.expiration[topic] = time.Now().Add(5 * time.Minute)

	return nil
}

func (p *SinkConnector) Close() {
	for _, conn := range p.cache {
		if conn == nil {
			continue
		}

		(*conn).Close()
	}
}

func (p *SinkConnector) Write(topic string, msg []byte) error {
	if p.cache[topic] == nil {
		return fmt.Errorf("sink-connector: sink-connector not connected")
	}

	msgCleaned := strings.TrimSuffix(string(msg), "\n")
	fmt.Fprintf(*p.cache[topic], "%s\n", msgCleaned)
	return nil
}
