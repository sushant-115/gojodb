package events

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/quic-go/quic-go/http3"
)

// EventReceiver handles receiving and processing length-prefixed events over HTTP/3.
type EventReceiver struct {
	addr     string
	server   *http3.Server
	eventsCh chan []byte
	pool     *sync.Pool
	wg       sync.WaitGroup
	stopChan chan struct{}
}

// NewEventReceiver creates and configures a new EventReceiver.
// bufferSize specifies the capacity of the output events channel.
func NewEventReceiver(addr string, bufferSize int, tlsConf *tls.Config) *EventReceiver {
	r := &EventReceiver{
		addr:     addr,
		eventsCh: make(chan []byte, bufferSize),
		pool: &sync.Pool{
			New: func() interface{} {
				// Pre-allocate a reasonably sized buffer.
				return make([]byte, 4096)
			},
		},
		stopChan: make(chan struct{}),
	}

	// Set up the HTTP handler.
	handler := http.NewServeMux()
	handler.HandleFunc("/events", r.eventHandler)

	r.server = &http3.Server{
		Addr:      addr,
		TLSConfig: tlsConf,
		Handler:   handler,
	}

	return r
}

// Start begins listening for incoming connections in a separate goroutine.
func (r *EventReceiver) Start() error {
	// Since HTTP/3 runs on UDP, we create a UDP packet connection.
	// This is a synchronous call that will fail immediately if the address is in use.
	conn, err := net.ListenPacket("udp", r.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on UDP address %s: %w", r.addr, err)
	}

	fmt.Println("Event receiver listening on", conn.LocalAddr().String())

	// Start serving on that connection in a background goroutine.
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		// The error from Serve will only be returned on shutdown or a critical error.
		// It should be logged here rather than returned.
		if err := r.server.Serve(conn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("Event receiver server error: %v\n", err)
		}
	}()

	go func() {
		<-r.stopChan
		conn.Close()
	}()

	return nil // Successfully started listening.
}

// Close gracefully shuts down the server and closes the events channel.
func (r *EventReceiver) Close() error {
	// Close the server. This stops it from accepting new connections.
	err := r.server.Close()
	// Wait for the ListenAndServe goroutine to exit.
	r.wg.Wait()
	// Close the channel to signal to consumers that no more events will be sent.
	close(r.eventsCh)
	return err
}

// Events returns a read-only channel for consuming received events.
func (r *EventReceiver) Events() <-chan []byte {
	return r.eventsCh
}

// eventHandler is the HTTP handler for incoming event streams.
// It runs in a new goroutine for each incoming connection.
func (r *EventReceiver) eventHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	fmt.Println("server: received new stream")
	defer fmt.Println("server: stream finished")

	body := req.Body
	defer body.Close()

	var lenBuf [4]byte

	for {
		// 1. Read the 4-byte length prefix.
		_, err := io.ReadFull(body, lenBuf[:])
		if err != nil {
			if err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
				fmt.Printf("server read len error: %v\n", err)
			}
			break // End of stream or error.
		}

		payloadLen := binary.BigEndian.Uint32(lenBuf[:])
		if payloadLen == 0 {
			continue
		}

		// 2. Read the full payload.
		// Get a buffer from the pool.
		payloadBuf := r.pool.Get().([]byte)
		if uint32(cap(payloadBuf)) < payloadLen {
			// If the buffer is too small, allocate a new one.
			payloadBuf = make([]byte, payloadLen)
		} else {
			// Otherwise, just slice it to the required length.
			payloadBuf = payloadBuf[:payloadLen]
		}

		_, err = io.ReadFull(body, payloadBuf)
		if err != nil {
			fmt.Printf("server read payload error: %v\n", err)
			r.pool.Put(payloadBuf) // Return buffer to the pool on error.
			break
		}

		// 3. Send the event for processing.
		// We send a copy so the consumer doesn't have to worry about the buffer being reused.
		eventCopy := make([]byte, payloadLen)
		copy(eventCopy, payloadBuf)

		select {
		case r.eventsCh <- eventCopy:
			// Event successfully sent.
		default:
			fmt.Println("Warning: Event receiver channel is full. Dropping event.")
		}

		// Return the buffer to the pool for reuse.
		r.pool.Put(payloadBuf)
	}

	w.WriteHeader(http.StatusOK)
}
