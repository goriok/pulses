package fsbroker

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type Producer struct {
	broker     string
	conn       *net.Conn
	mu         sync.Mutex
	cache      map[string]string
	expiration map[string]time.Time
}

func NewProducer(broker string) *Producer {
	return &Producer{
		broker,
		nil,
		sync.Mutex{},
		make(map[string]string),
		make(map[string]time.Time),
	}
}

func (p *Producer) Connect(subject string) error {
	if _, ok := p.cache[subject]; ok {
		if time.Now().Before(p.expiration[subject]) {
			return nil
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		delete(p.cache, subject)
		delete(p.expiration, subject)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	conn, err := net.Dial("tcp", p.broker)
	if err != nil {
		return err
	}

	p.conn = &conn
	fmt.Fprintf(*(p.conn), "producer_%s\n", subject)
	logrus.Infof("Connected to broker %s as producer for subject %s", p.broker, subject)

	p.cache[subject] = subject
	p.expiration[subject] = time.Now().Add(5 * time.Minute)

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

	msgCleaned := strings.TrimSuffix(string(msg), "\n")

	fmt.Fprintf(*p.conn, "%s\n", msgCleaned)
	logrus.Debugf("Published message to subject %s: %s", subject, msg)
	return nil
}
