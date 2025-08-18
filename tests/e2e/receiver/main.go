package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/sushant-115/gojodb/config/certs"
	"github.com/sushant-115/gojodb/core/replication/events"
	"go.uber.org/zap"
)

const (
	testAddress = "localhost:6001"
)

func main() {
	// Logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	_, serverCert := certs.LoadCerts("/Users/sushant/go/src/gojodb/config/certs")

	// Receiver config
	cfg := events.ReceiverConfig{
		Addr:    testAddress,
		URLPath: "/events",
		TLS:     serverCert,
	}

	receiver, err := events.NewEventReceiver(cfg, nil, events.ReceiverHooks{
		OnAccepted: func() {
			fmt.Println("Received a new connection")
		},
		OnDropped: func(reason string) {
			fmt.Println("Dropped the packets due to ", reason)
		},
		OnStreamStart: func(remote string) {
			fmt.Println("Started the stream from: ", remote)
		},
		OnStreamClose: func(remote string, err error) {
			fmt.Println("Stream closed from: ", remote, " Due to: ", err)
		},
	})
	if err != nil {
		log.Fatal("❌ Failed to create event receiver:", err)
	}

	// Start receiver (blocking)
	go func() {
		log.Println("🚀 RECEIVER: Starting server...")
		if err := receiver.Start(); err != nil {
			if err.Error() != "http: Server closed" {
				log.Fatalf("❌ RECEIVER: Failed to start event receiver: %v", err)
			}
		}
	}()

	// Run until manually stopped
	// (or simulate a test timeout)
	time.Sleep(30 * time.Second)

	log.Println("🛑 RECEIVER: Shutting down...")
	if err := receiver.Close(context.Background()); err != nil {
		log.Fatalf("❌ RECEIVER: Failed to close: %v", err)
	}
	log.Println("✅ RECEIVER: Shutdown complete.")
}
