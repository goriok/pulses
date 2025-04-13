package fsbroker

import (
	"bufio"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

// startTestTCPServer starts a TCP server and returns:
// - address to connect to
// - a channel receiving incoming lines
// - a cleanup function
func startTestTCPServer(t *testing.T) (addr string, received chan string, cleanup func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0") // listen on random available port
	assert.NoError(t, err)

	received = make(chan string, 10)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					received <- scanner.Text()
				}
			}(conn)
		}
	}()

	return ln.Addr().String(), received, func() {
		ln.Close()
		close(received)
	}
}

func TestSinkConnector_ConnectAndWrite(t *testing.T) {
	addr, received, cleanup := startTestTCPServer(t)
	defer cleanup()

	sink := NewSinkConnector(addr)
	topic := "test.topic"

	err := sink.Connect(topic)
	assert.NoError(t, err)

	msg := "hello test"
	err = sink.Write(topic, []byte(msg))
	assert.NoError(t, err)

	// Expect topic connection message
	line1 := <-received
	assert.Equal(t, "sink-connector_test.topic", line1)

	// Expect actual message
	line2 := <-received
	assert.Equal(t, msg, line2)
}
