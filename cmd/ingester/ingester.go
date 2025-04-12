package ingester

import (
	"time"

	"github.com/sirupsen/logrus"
)

type Consumer interface {
	Connect(subject string, handler func(subject string, message []byte))
}

type Options struct {
	Consumer Consumer
	Subject  string
}

func Start(opts *Options) {
	messageChan := make(chan []byte, 100)
	done := make(chan struct{})

	go func() {
		messageCount := 0
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case message := <-messageChan:
				messageCount++
				logrus.Infof("INGESTER: Received message => %s", message)
			case <-ticker.C:
				logrus.Infof("AGGREGATION: Received %d messages in the last 30 seconds on subject %s", messageCount, opts.Subject)
				messageCount = 0
			case <-done:
				logrus.Info("Shutting down aggregation goroutine")
				return
			}
		}
	}()

	opts.Consumer.Connect(opts.Subject, func(subject string, message []byte) {
		select {
		case messageChan <- message:
		default:
			logrus.Warn("Message channel is full, dropping message")
		}
	})

	defer close(done)
}
