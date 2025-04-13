// Package fsbroker implements a lightweight, filesystem-backed message broker
// for local development and testing. It simulates publish/subscribe behavior
// using TCP connections and disk-based topic storage in the `.data` directory.
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
	DATA_DIR = ".data"
)

// Broker is a local TCP server that simulates a pub/sub broker.
// It stores messages per topic as files in the .data directory,
// and manages connected source and sink connectors.
type Broker struct {
	sourceConnectors map[string][]*net.Conn
	newMessage       chan struct {
		Topic   string
		Message string
	}
	mu       sync.Mutex
	host     string
	listener *net.Listener
}

func NewBroker(port int) *Broker {
	host := fmt.Sprintf("localhost:%d", port)

	return &Broker{
		sourceConnectors: make(map[string][]*net.Conn),
		newMessage: make(chan struct {
			Topic   string
			Message string
		}),
		host: host,
	}
}

func (b *Broker) On() bool {
	return b.listener != nil
}

// Start begins accepting TCP connections from clients.
// It listens for both sink and source connectors and handles broadcasting.
func (b *Broker) Start() error {
	listener, err := net.Listen("tcp", b.host)
	if err != nil {
		logrus.Errorf("broker: Error starting server => %v\n", err)
		return err
	}

	b.listener = &listener
	defer listener.Close()

	logrus.Infof("broker: listening on %s", b.host)

	go b.broadcastMessages()

	for {
		conn, err := listener.Accept()
		if err != nil {
			logrus.Errorf("broker: Error accepting connection => %v", err)
			continue
		}

		go b.handleConnection(&conn)
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

// handleConnection receives the initial greeting from a connector to determine
// whether it is a sink or source, and delegates to the appropriate handler.
func (b *Broker) handleConnection(conn *net.Conn) {
	defer (*conn).Close()

	reader := bufio.NewReader(*conn)
	greeting, _ := reader.ReadString('\n')
	greeting = greeting[:len(greeting)-1]

	data := strings.Split(greeting, "_")
	if data[0] == "sink-connector" {
		logrus.Debugf("broker: sink-connector connected on topic %s", data[1])
		b.handleSinkConnector(reader, data[1])
	} else if data[0] == "source-connector" {
		logrus.Debugf("broker: source-connector connected on topic %s", data[1])
		b.handleSourceConnector(conn, data[1])
	} else {
		logrus.Errorf("broker: Unknown client type: %s", data[0])
	}
}

// handleSinkConnector reads messages from a sink connector and appends them to disk.
// It then broadcasts the message to all connected source connectors for that topic.
func (b *Broker) handleSinkConnector(reader *bufio.Reader, topic string) {
	data := fmt.Sprintf("%s/%s", DATA_DIR, topic)
	if err := ensureDataDirExists(); err != nil {
		logrus.Fatalf("broker: failed to ensure .data exists: %v", err)
		return
	}

	file, err := os.OpenFile(data, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logrus.Fatalf("broker: invalid topic: %v", err)
		return
	}
	defer file.Close()

	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			logrus.Errorf("broker: Error reading message: %v", err)
			return
		}

		_, err = file.WriteString(message)
		if err != nil {
			logrus.Errorf("broker: Error writing message: %v", err)
			return
		}

		logrus.Infof("broker: [APPEND] %s <= %s", topic, message)
		b.newMessage <- struct {
			Topic   string
			Message string
		}{Topic: topic, Message: message}
	}
}

// handleSourceConnector registers a source connector to a topic and replays existing messages.
// It will remain open indefinitely, receiving messages via broadcast.
func (b *Broker) handleSourceConnector(conn *net.Conn, topic string) {
	b.mu.Lock()
	b.sourceConnectors[topic] = append(b.sourceConnectors[topic], conn)
	b.mu.Unlock()

	if err := ensureDataDirExists(); err != nil {
		logrus.Fatalf("broker: failed to ensure .data exists: %v", err)
		return
	}

	data := fmt.Sprintf("%s/%s", DATA_DIR, topic)
	file, err := os.OpenFile(data, os.O_APPEND|os.O_CREATE|os.O_RDONLY, 0777)
	if err != nil {
		logrus.Fatalf("broker: invalid topic: %v", err)
	}

	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		_, err := fmt.Fprintf(*conn, "%s\n", scanner.Text())
		if err != nil {
			logrus.Errorf("broker: error writing message to source-connector: %v", err)
			return
		}
	}

	select {}
}

// broadcastMessages delivers new messages to all source connectors subscribed to the topic.
func (b *Broker) broadcastMessages() {
	for msg := range b.newMessage {
		b.mu.Lock()
		for _, conn := range b.sourceConnectors[msg.Topic] {
			_, err := fmt.Fprintf(*conn, "%s", msg.Message)
			if err != nil {
				logrus.Errorf("broker: Error writing message to consumer: %v", err)
			}
		}
		b.mu.Unlock()
	}
}

func ensureDataDirExists() error {
	return os.MkdirAll(DATA_DIR, os.ModePerm)
}
