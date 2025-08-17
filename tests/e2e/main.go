package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sushant-115/gojodb/core/replication/events"
	"github.com/sushant-115/gojodb/core/security/encryption/internaltls"
	"go.uber.org/zap"
)

const (
	// The address for the receiver to listen on.
	testAddress = "localhost:6001"
	// The full URL for the sender to connect to.
	testURL = "https://localhost:6001/events"
)

// main function orchestrates the test.
func main() {
	// Use a WaitGroup to synchronize the sender and receiver goroutines.
	var wg sync.WaitGroup
	wg.Add(2)

	// A channel to signal that the receiver is ready to accept connections.
	receiverReady := make(chan bool)

	// --- 1. Start the EventReceiver in a goroutine ---
	go func() {
		defer wg.Done()

		// Basic zap logger for the receiver.
		logger, _ := zap.NewDevelopment()
		defer logger.Sync()

		// Create a new EventReceiver.
		// The event handler function simply logs the received data.
		cfg := events.ReceiverConfig{
			Addr:    testAddress,
			URLPath: "/events",
			TLS:     internaltls.GetTestServerCert(),
		}
		eventsReceiver, err := events.NewEventReceiver(cfg, nil, events.ReceiverHooks{
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
			log.Fatal("Failed to create event receiver")
		}

		// Start the receiver. This is a blocking call, so it runs in a goroutine.
		go func() {
			log.Println("🚀 RECEIVER: Starting server...")
			// Signal that the receiver is about to start.
			close(receiverReady)
			if err := eventsReceiver.Start(); err != nil {
				// We expect an error when we close the server, so we check for it.
				if err.Error() != "http: Server closed" {
					log.Fatalf("❌ RECEIVER: Failed to start event receiver: %v", err)
				}
			}
		}()

		// Wait for a moment to let the sender finish its work.
		time.Sleep(5 * time.Second)

		// Gracefully shut down the receiver.
		log.Println("🛑 RECEIVER: Shutting down...")
		if err := eventsReceiver.Close(context.Background()); err != nil {
			log.Fatalf("❌ RECEIVER: Failed to close event receiver: %v", err)
		}
		log.Println("✅ RECEIVER: Shutdown complete.")
	}()

	// --- 2. Wait for the receiver to be ready ---
	<-receiverReady
	// Add a small delay to ensure the server is fully up.
	time.Sleep(500 * time.Millisecond)

	// --- 3. Start the EventSender in a goroutine ---
	go func() {
		defer wg.Done()

		// Basic zap logger for the sender.
		logger, _ := zap.NewDevelopment()
		defer logger.Sync()

		// Configure the client's TLS settings.
		// InsecureSkipVerify is set to true because we're using a self-signed test certificate.

		// Create a new EventSender.
		cfg := events.Config{
			Addr:    testAddress,
			URLPath: "/events",
			TLS:     internaltls.GetTestClientCert(),
		}
		eventSender, err := events.NewEventSender(cfg)
		if err != nil {
			log.Fatal("Failed to create eventSender for ", testAddress)
		}

		// Establish the connection to the receiver.
		log.Println("🚀 SENDER: Connecting to receiver...")
		// if err := eventSender.Send([]byte{}); err != nil {
		// 	log.Fatalf("❌ SENDER: Failed to connect: %v", err)
		// }
		defer eventSender.Close()
		log.Println("✅ SENDER: Connection established.")

		// Send a series of test messages.
		for i := 1; i <= 5; i++ {
			message := fmt.Sprintf("event-message-%d", i)
			log.Printf("➡️ SENDER: Sending event: '%s'\n", message)
			if err := eventSender.TrySend([]byte(message)); !err {
				eventSender.Send([]byte(message))
				log.Printf("❌ SENDER: Failed to send event: %v", err)
			}
			// Wait a bit between messages to simulate a real-world scenario.
			time.Sleep(500 * time.Millisecond)
		}
		log.Println("✅ SENDER: All events sent.")
	}()

	// --- 4. Wait for both goroutines to complete ---
	wg.Wait()
	log.Println("🎉 Test finished.")
}
