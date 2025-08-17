package main

import (
	"fmt"
	"log"
	"time"

	"github.com/sushant-115/gojodb/config/certs"
	"github.com/sushant-115/gojodb/core/replication/events"
	"go.uber.org/zap"
)

const (
	testAddress = "localhost:6001"
	testURL     = "https://localhost:6001/events"
)

func main() {
	// Logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	_, clientCert := certs.LoadCerts("/Users/sushant/go/src/gojodb/config/certs")
	// Sender config
	cfg := events.Config{
		Addr:    testAddress,
		URLPath: "/events",
		TLS:     clientCert,
	}

	sender, err := events.NewEventSender(cfg)
	if err != nil {
		log.Fatal("❌ Failed to create sender:", err)
	}
	defer sender.Close()

	log.Println("🚀 SENDER: Connecting to receiver...")

	// Send events
	for i := 1; i <= 5; i++ {
		message := fmt.Sprintf("event-message-%d", i)
		log.Printf("➡️ SENDER: Sending event: '%s'\n", message)
		if ok := sender.TrySend([]byte(message)); !ok {
			sender.Send([]byte(message))
			log.Printf("❌ SENDER: TrySend failed, used Send fallback")
		}
		time.Sleep(500 * time.Millisecond)
	}

	log.Println("✅ SENDER: All events sent.")
}
