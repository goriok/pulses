package fsbroker

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

const (
	dataDir = ".data"
)

type Broker struct {
	consumers  map[string][]net.Conn
	newMessage chan struct {
		Subject string
		Message string
	}
	mu       sync.Mutex
	host     string
	listener *net.Listener
}

func NewBroker(port int) *Broker {
	host := fmt.Sprintf("localhost:%d", port)

	return &Broker{
		consumers: make(map[string][]net.Conn),
		newMessage: make(chan struct {
			Subject string
			Message string
		}),
		host: host,
	}
}

func (b *Broker) On() bool {
	return b.listener != nil
}

func (b *Broker) Start() error {
	listener, err := net.Listen("tcp", b.host)
	if err != nil {
		logrus.Errorf("Broker: Error starting server => %v\n", err)
		return err
	}

	b.listener = &listener
	defer listener.Close()

	logrus.Infof("Broker: Listening on %s", b.host)

	go b.broadcastMessages()

	for {
		conn, err := listener.Accept()
		if err != nil {
			logrus.Errorf("Broker: Error accepting connection => %v", err)
			continue
		}

		go b.handleConnection(conn)
	}
}

func (b *Broker) Stop() {
	if b.listener != nil {
		(*b.listener).Close()
	}
}

func (b *Broker) Host() string {
	return b.host
}

func (b *Broker) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	greeting, _ := reader.ReadString('\n')
	greeting = greeting[:len(greeting)-1]

	data := strings.Split(greeting, "_")
	if data[0] == "producer" {
		logrus.Debugf("Broker: Producer connected on subject %s", data[1])
		b.handleProducer(reader, data[1])
	} else if data[0] == "consumer" {
		logrus.Debugf("Broker: Consumer connected on subject %s", data[1])
		b.handleConsumer(conn, data[1])
	} else {
		logrus.Errorf("Broker: Unknown client type: %s", data[0])
	}
}

func (b *Broker) handleProducer(reader *bufio.Reader, subject string) {
	data := fmt.Sprintf("%s/%s", dataDir, subject)
	logrus.Info(data)

	file, err := os.OpenFile(data, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logrus.Fatalf("Broker: invalid subject: %v", err)
		return
	}
	defer file.Close()

	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			logrus.Errorf("Broker: Error reading message: %v", err)
			return
		}

		_, err = file.WriteString(message)
		if err != nil {
			logrus.Errorf("Broker: Error writing message: %v", err)
			return
		}
		logrus.Infof("Broker: Message written to file: %s", message)

		b.newMessage <- struct {
			Subject string
			Message string
		}{Subject: subject, Message: message}
	}
}

func (b *Broker) handleConsumer(conn net.Conn, subject string) {
	b.mu.Lock()
	b.consumers[subject] = append(b.consumers[subject], conn)
	b.mu.Unlock()

	data := fmt.Sprintf("%s/%s", dataDir, subject)
	logrus.Info(data)

	file, err := os.OpenFile(data, os.O_APPEND|os.O_CREATE|os.O_RDONLY, 0777)
	if err != nil {
		logrus.Fatalf("Broker: invalid subject: %v", err)
	}

	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		_, err := fmt.Fprintf(conn, "%s\n", scanner.Text())
		if err != nil {
			logrus.Errorf("Broker: Error writing message to consumer: %v", err)
			return
		}
	}

	select {}
}

func (b *Broker) broadcastMessages() {
	for msg := range b.newMessage {
		b.mu.Lock()
		for _, consumer := range b.consumers[msg.Subject] {
			_, err := fmt.Fprintf(consumer, "%s", msg.Message)
			if err != nil {
				logrus.Errorf("Broker: Error writing message to consumer: %v", err)
			}
		}
		b.mu.Unlock()
	}
}
