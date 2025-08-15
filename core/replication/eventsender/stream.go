package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// EventSender sends events from many goroutines over a single HTTP/3 stream.
type EventSender struct {
	addr string
	url  string

	// config
	maxBatchBytes    int           // coalesce up to this many bytes before writing
	maxBatchMessages int           // or this many messages
	flushInterval    time.Duration // or every this duration

	// internal
	eventsCh chan []byte
	pool     *sync.Pool
	quit     chan struct{}
	closed   int32
	wg       sync.WaitGroup

	// --- HTTP/3 Connection Management ---
	connMu       sync.RWMutex
	conn         io.WriteCloser     // The writer for the HTTP/3 request body stream (an io.PipeWriter)
	cancelReq    context.CancelFunc // Function to cancel the current HTTP/3 request
	roundTripper *http3.Transport
	h3Client     *http.Client
}

// NewEventSender returns a configured EventSender for HTTP/3.
// bufferSize is the number of messages that can be queued before Send blocks.
// clientTLSConf is the TLS configuration the client will use to connect.
func NewEventSender(addr string, bufferSize int, clientTLSConf *tls.Config) *EventSender {
	// Initialize an HTTP/3 round-tripper (transport)
	rt := &http3.Transport{
		TLSClientConfig: clientTLSConf,
		QUICConfig: &quic.Config{
			MaxIdleTimeout:  time.Minute,
			KeepAlivePeriod: 15 * time.Second,
		},
	}

	s := &EventSender{
		addr:             addr,
		url:              fmt.Sprintf("https://%s/events", addr),
		maxBatchBytes:    64 * 1024,             // 64KB per network write
		maxBatchMessages: 256,                   // max messages to group
		flushInterval:    50 * time.Millisecond, // flush at least every 50ms
		eventsCh:         make(chan []byte, bufferSize),
		pool: &sync.Pool{
			New: func() interface{} { return make([]byte, 0, 1024) },
		},
		quit:         make(chan struct{}),
		roundTripper: rt,
		h3Client:     &http.Client{Transport: rt},
	}

	s.wg.Add(1)
	go s.writerLoop()
	return s
}

// Send blocks until data is queued (or returns error if sender closed).
func (s *EventSender) Send(data []byte) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return errors.New("sender closed")
	}
	// Make a copy into a pooled buffer to avoid data races.
	buf := s.pool.Get().([]byte)[:0]
	buf = append(buf, data...)
	select {
	case s.eventsCh <- buf:
		return nil
	case <-s.quit:
		s.pool.Put(buf[:0]) // Return buffer to pool
		return errors.New("sender closed")
	}
}

// TrySend attempts to enqueue without blocking; returns true if accepted.
func (s *EventSender) TrySend(data []byte) bool {
	if atomic.LoadInt32(&s.closed) == 1 {
		return false
	}
	buf := s.pool.Get().([]byte)[:0]
	buf = append(buf, data...)
	select {
	case s.eventsCh <- buf:
		return true
	default:
		// Queue full, drop and return buffer to pool
		s.pool.Put(buf[:0])
		return false
	}
}

// Close gracefully shuts down the writerLoop, flushing any queued events.
func (s *EventSender) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return errors.New("already closed")
	}
	close(s.quit)
	s.wg.Wait()

	// Clean up the active connection
	s.connMu.Lock()
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	if s.cancelReq != nil {
		s.cancelReq()
		s.cancelReq = nil
	}
	s.connMu.Unlock()

	// Close the underlying QUIC connection
	s.roundTripper.Close()
	return nil
}

// writerLoop is the single writer goroutine.
// It batches messages, frames them, and writes them to the HTTP/3 stream.
func (s *EventSender) writerLoop() {
	defer s.wg.Done()

	var batch bytes.Buffer
	var messagesInBatch int
	flushTimer := time.NewTimer(s.flushInterval)
	defer flushTimer.Stop()

	// Helper to write the current batch to the stream.
	writeBatch := func(b []byte) error {
		if len(b) == 0 {
			return nil
		}
		// Ensure we have a connection (an active POST request stream).
		if err := s.ensureConn(); err != nil {
			return err
		}

		s.connMu.RLock()
		conn := s.conn
		s.connMu.RUnlock()
		if conn == nil {
			return errors.New("no connection")
		}

		// Write to the pipe. This will block if the pipe buffer is full (network backpressure).
		// It will return an error if the reading side (the HTTP client) closes the pipe due to a network error.
		_, err := conn.Write(b)
		if err != nil {
			// The stream is broken. Close and nil out the connection so ensureConn re-establishes it.
			s.connMu.Lock()
			if s.conn != nil {
				_ = s.conn.Close() // Close the pipe writer
				s.conn = nil
			}
			if s.cancelReq != nil {
				s.cancelReq() // Cancel the context of the http request
				s.cancelReq = nil
			}
			s.connMu.Unlock()
		}
		return err
	}

	// Flushes the current batch.
	flushNow := func() {
		if batch.Len() > 0 {
			if err := writeBatch(batch.Bytes()); err != nil {
				fmt.Printf("write error, will retry: %v\n", err)
				// Note: In a real-world scenario, you might want to handle this error
				// more gracefully, e.g., by not clearing the batch and retrying.
				// For this example, we'll drop the batch on persistent failure.
			}
			batch.Reset()
			messagesInBatch = 0
		}
		if !flushTimer.Stop() {
			select {
			case <-flushTimer.C:
			default:
			}
		}
		flushTimer.Reset(s.flushInterval)
	}

	for {
		select {
		case <-s.quit:
			// Drain the channel and attempt a final flush.
			for {
				select {
				case msg := <-s.eventsCh:
					s.frameAppend(&batch, msg)
					messagesInBatch++
					s.pool.Put(msg[:0])
				default:
					flushNow()
					return
				}
			}
		case msg := <-s.eventsCh:
			s.frameAppend(&batch, msg)
			messagesInBatch++
			s.pool.Put(msg[:0])

			if batch.Len() >= s.maxBatchBytes || messagesInBatch >= s.maxBatchMessages {
				flushNow()
			}
		case <-flushTimer.C:
			flushNow()
		}
	}
}

// frameAppend appends a length-prefixed message to the buffer.
func (s *EventSender) frameAppend(buf *bytes.Buffer, msg []byte) {
	var lenBytes [4]byte
	binary.BigEndian.PutUint32(lenBytes[:], uint32(len(msg)))
	buf.Write(lenBytes[:])
	buf.Write(msg)
}

// ensureConn establishes an HTTP/3 stream via a POST request with a piped body.
// This is the HTTP/3 equivalent of the original `net.Dial`.
func (s *EventSender) ensureConn() error {
	s.connMu.RLock()
	if s.conn != nil {
		s.connMu.RUnlock()
		return nil
	}
	s.connMu.RUnlock()

	s.connMu.Lock()
	defer s.connMu.Unlock()
	// Double-check after acquiring write lock.
	if s.conn != nil {
		return nil
	}

	// Use an io.Pipe to connect our writerLoop to the HTTP request body.
	// We write to `pipeWriter`, and the `h3Client` reads from `pipeReader`.
	pipeReader, pipeWriter := io.Pipe()

	// Create a new cancellable request.
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, pipeReader)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to create h3 request: %w", err)
	}

	// Start the request in a background goroutine.
	// `h3Client.Do` will block until response headers are received.
	go func() {
		resp, err := s.h3Client.Do(req)
		if err != nil {
			// If the request fails (e.g., network error, server down),
			// close the pipe with an error to unblock our writerLoop.
			_ = pipeWriter.CloseWithError(err)
			fmt.Printf("client request failed: %v\n", err)
			return
		}
		defer resp.Body.Close()

		// The request was successful. We can optionally read the response body.
		// For now, we just check the status code.
		if resp.StatusCode >= 300 {
			err := fmt.Errorf("server returned non-2xx status: %s", resp.Status)
			_ = pipeWriter.CloseWithError(err)
			fmt.Printf("server returned error status: %s\n", resp.Status)
		} else {
			// If the server closes the connection gracefully, we might not get an error
			// on the writer, so we close it here to signal completion.
			_ = pipeWriter.Close()
		}
	}()

	s.conn = pipeWriter
	s.cancelReq = cancel
	fmt.Println("established new http/3 stream")
	return nil
}

//////////////////////////////
// Example Usage & Server
//////////////////////////////

// generateTLSConfig sets up a dummy TLS config for testing.
// It returns a server config and a certificate pool for the client.
func generateTLSConfig() (*tls.Config, *x509.CertPool) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"Test Co"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	leafCert, err := x509.ParseCertificate(certDER)
	if err != nil {
		panic(err)
	}
	serverTLSConf := &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{certDER}, PrivateKey: key, Leaf: leafCert}},
		NextProtos:   []string{"h3"},
	}
	certPool := x509.NewCertPool()
	certPool.AddCert(leafCert)
	return serverTLSConf, certPool
}

// runDummyH3Server is an HTTP/3 server that accepts framed messages and counts them.
func runDummyH3Server(addr string, tlsConf *tls.Config) {
	handler := http.NewServeMux()
	handler.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		fmt.Println("server: received new stream")
		var total int
		var lenBuf [4]byte
		body := r.Body
		defer body.Close()

		for {
			_, err := io.ReadFull(body, lenBuf[:])
			if err != nil {
				if err != io.EOF {
					fmt.Printf("server read len error: %v\n", err)
				}
				break
			}

			payloadLen := binary.BigEndian.Uint32(lenBuf[:])
			if payloadLen == 0 {
				continue
			}

			payload := make([]byte, payloadLen)
			_, err = io.ReadFull(body, payload)
			if err != nil {
				fmt.Printf("server read payload error: %v\n", err)
				break
			}
			total++
			fmt.Println("Payload: ", string(payload))
			if strings.Contains(string(payload), "producer=0") {
				time.Sleep(100 * time.Millisecond)
			}
			if total%20000 == 0 {
				fmt.Printf("server: received total %d messages on this stream\n", total)
			}
		}
		fmt.Printf("server: stream finished, received total %d messages\n", total)
		w.WriteHeader(http.StatusOK)
	})

	server := &http3.Server{
		Addr:      addr,
		TLSConfig: tlsConf,
		Handler:   handler,
	}

	fmt.Println("dummy HTTP/3 server listening on", addr)
	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}

func main() {
	// --- SETUP INSTRUCTIONS ---
	// The "undefined: http3.Transport" error occurs when your Go environment
	// cannot find the necessary library or is using an outdated version.
	// To fix this, please manage the dependencies using Go Modules.
	//
	// In your terminal, in the same directory as this file:
	//
	// 1. If you don't have a go.mod file, create one:
	//    go mod init <your_module_name>
	//    (e.g., go mod init myh3sender)
	//
	// 2. Tidy the dependencies. This command analyzes the imports and automatically
	//    fetches the correct versions of the libraries (like quic-go).
	//    go mod tidy
	//
	// 3. Run the program:
	//    go run .
	//
	// This process ensures your project has a `go.mod` and `go.sum` file that
	// lock the dependency versions, making the build reproducible and error-free.
	// --------------------------

	// 1. Generate TLS configs for server and client
	serverTLSConf, clientCertPool := generateTLSConfig()
	addr := "localhost:9090"

	// 2. Start the HTTP/3 server in a goroutine
	go runDummyH3Server(addr, serverTLSConf)
	time.Sleep(100 * time.Millisecond) // Give server a moment to start

	// 3. Create the client-side TLS config that trusts our self-signed cert
	clientTLSConf := &tls.Config{
		RootCAs:    clientCertPool,
		NextProtos: []string{"h3"},
	}

	// 4. Create and run the EventSender
	sender := NewEventSender(addr, 4096, clientTLSConf)
	defer sender.Close()

	// 5. Simulate many producers sending events
	var wg sync.WaitGroup
	producers := 20
	msgsPerProducer := 100

	start := time.Now()
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < msgsPerProducer; j++ {
				payload := []byte(fmt.Sprintf("producer=%d seq=%d time=%d", id, j, time.Now().UnixNano()))
				// Use TrySend for non-blocking, fall back to blocking Send to apply backpressure.
				if !sender.TrySend(payload) {
					_ = sender.Send(payload)
				}
			}
		}(i)
	}
	wg.Wait()

	// Allow some time for the final batch to be flushed before closing.
	time.Sleep(5000 * time.Millisecond)
	fmt.Printf("done sending %d messages in %v\n", producers*msgsPerProducer, time.Since(start))
}
