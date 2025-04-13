package fsbroker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBroker_HostAndOn(t *testing.T) {
	b := NewBroker(12345)
	assert.False(t, b.On())
	assert.Contains(t, b.Host(), "localhost:12345")
}
