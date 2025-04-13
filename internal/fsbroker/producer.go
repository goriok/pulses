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
	mu         sync.Mutex
	cache      map[string]*net.Conn
	expiration map[string]time.Time
}

func NewProducer(broker string) *Producer {
	return &Producer{
		broker,
		sync.Mutex{},
		make(map[string]*net.Conn),
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

		(*p.cache[subject]).Close()
		delete(p.cache, subject)
		delete(p.expiration, subject)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	conn, err := net.Dial("tcp", p.broker)
	if err != nil {
		return err
	}
	p.cache[subject] = &conn

	fmt.Fprintf(*p.cache[subject], "producer_%s\n", subject)
	logrus.Infof("producer: connected to broker %s for subject %s", p.broker, subject)

	p.expiration[subject] = time.Now().Add(5 * time.Minute)

	return nil
}

func (p *Producer) Close() {
	for _, conn := range p.cache {
		if conn == nil {
			continue
		}

		(*conn).Close()
	}
}

func (p *Producer) Publish(subject string, msg []byte) error {
	if p.cache[subject] == nil {
		return fmt.Errorf("producer: producer not connected")
	}

	msgCleaned := strings.TrimSuffix(string(msg), "\n")
	fmt.Fprintf(*p.cache[subject], "%s\n", msgCleaned)
	return nil
}
