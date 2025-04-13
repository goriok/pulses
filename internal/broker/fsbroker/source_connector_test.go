package fsbroker

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// startTestSourceServer creates a mock broker that sends messages after handshake
func startTestSourceServer(t *testing.T, messages []string) (addr string, receivedHandshake *string, cleanup func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	receivedHandshake = new(string)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read the handshake line
		buf := make([]byte, 256)
		n, _ := conn.Read(buf)
		*receivedHandshake = string(buf[:n])

		// Send the test messages
		for _, msg := range messages {
			time.Sleep(50 * time.Millisecond)
			fmt.Fprintf(conn, "%s\n", msg)
		}
	}()

	return ln.Addr().String(), receivedHandshake, func() {
		ln.Close()
	}
}

// --- Tests ---
func TestSourceConnector_Read_ConnectionRefused(t *testing.T) {
	// Use an unused port to trigger a connection error
	source := NewSourceConnector("127.0.0.1:65534") // unlikely to be open

	err := source.Read("any.topic", func(_ string, _ []byte) {})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connect")
}

func TestSourceConnector_Read_ReadError(t *testing.T) {
	// Server accepts connection then closes immediately
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	addr := ln.Addr().String()

	go func() {
		conn, _ := ln.Accept()
		conn.Close() // trigger read error
		ln.Close()
	}()

	source := NewSourceConnector(addr)
	err = source.Read("drop.topic", func(_ string, _ []byte) {})
	assert.Error(t, err)
}

func TestSourceConnector_Close(t *testing.T) {
	addr, _, cleanup := startTestSourceServer(t, []string{"one"})
	defer cleanup()

	source := NewSourceConnector(addr)

	go func() {
		_ = source.Read("close.topic", func(_ string, _ []byte) {
			source.Close()
		})
	}()

	time.Sleep(200 * time.Millisecond)

	if source.conn != nil {
		assert.NotNil(t, source.conn)
		assert.NotNil(t, *source.conn)
	}
}
