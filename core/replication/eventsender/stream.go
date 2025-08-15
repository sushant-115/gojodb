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

const numConnections = 2

// EventSender sends events from many goroutines over multiple concurrent HTTP/3 streams.
type EventSender struct {
	addr string
	url  string

	// config
	maxBatchBytes    int           // coalesce up to this many bytes before writing
	maxBatchMessages int           // or this many messages
	flushInterval    time.Duration // or every this duration

	// internal
	eventsCh chan []byte
	batchChs [numConnections]chan []byte // Channels to send batches to connection managers
	pool     *sync.Pool
	quit     chan struct{}
	closed   int32
	wg       sync.WaitGroup

	// --- HTTP/3 Connection Management ---
	roundTripper *http3.Transport
	h3Client     *http.Client
}

// connectionState holds the active components for a single HTTP/3 stream.
type connectionState struct {
	writer    io.WriteCloser
	cancelReq context.CancelFunc
}

// NewEventSender returns a configured EventSender for HTTP/3.
// It creates two concurrent connections for high availability.
func NewEventSender(addr string, bufferSize int, clientTLSConf *tls.Config) *EventSender {
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
		maxBatchBytes:    64 * 1024,
		maxBatchMessages: 256,
		flushInterval:    50 * time.Millisecond,
		eventsCh:         make(chan []byte, bufferSize),
		pool: &sync.Pool{
			New: func() interface{} { return make([]byte, 0, 1024) },
		},
		quit:         make(chan struct{}),
		roundTripper: rt,
		h3Client:     &http.Client{Transport: rt},
	}

	// Initialize a channel for each connection manager.
	// A small buffer of 1 allows the batching loop to not block if a manager is busy.
	for i := 0; i < numConnections; i++ {
		s.batchChs[i] = make(chan []byte, 1)
	}

	// Launch the main batching loop.
	s.wg.Add(1)
	go s.batchingLoop()

	// Launch a dedicated manager for each connection.
	for i := 0; i < numConnections; i++ {
		s.wg.Add(1)
		go s.connectionManager(i, s.batchChs[i])
	}

	return s
}

// Send blocks until data is queued (or returns error if sender closed).
func (s *EventSender) Send(data []byte) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return errors.New("sender closed")
	}
	buf := s.pool.Get().([]byte)[:0]
	buf = append(buf, data...)
	select {
	case s.eventsCh <- buf:
		return nil
	case <-s.quit:
		s.pool.Put(buf[:0])
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
		s.pool.Put(buf[:0])
		return false
	}
}

// Close gracefully shuts down all loops and connections.
func (s *EventSender) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return errors.New("already closed")
	}
	close(s.quit)
	s.wg.Wait()
	s.roundTripper.Close()
	return nil
}

// batchingLoop is the single goroutine that collects events and forms batches.
// It then dispatches the batch to any available connection manager.
func (s *EventSender) batchingLoop() {
	defer s.wg.Done()
	// When this loop exits, close the batch channels to signal the connection managers to exit.
	defer func() {
		for i := 0; i < numConnections; i++ {
			close(s.batchChs[i])
		}
	}()

	var batch bytes.Buffer
	var messagesInBatch int
	flushTimer := time.NewTimer(s.flushInterval)
	defer flushTimer.Stop()

	dispatchBatch := func() {
		if batch.Len() == 0 {
			return
		}

		// We must copy the bytes because the two channel sends in the select
		// statement below can happen concurrently. If we sent a pointer to the
		// same underlying byte slice, we could have a data race.
		payload := make([]byte, batch.Len())
		copy(payload, batch.Bytes())

		// This select statement is the core of the high-availability logic.
		// It will send the batch to the FIRST available connection manager.
		// If both are available, one is chosen at random (load balancing).
		// If one is blocked, the other is chosen instantly (failover).
		select {
		case s.batchChs[0] <- payload:
		case s.batchChs[1] <- payload:
		case <-s.quit:
			// If shutting down, don't block trying to send a final batch.
		}

		batch.Reset()
		messagesInBatch = 0
	}

	resetTimer := func() {
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
			// Drain eventsCh and attempt one final dispatch.
			for len(s.eventsCh) > 0 {
				msg := <-s.eventsCh
				s.frameAppend(&batch, msg)
				messagesInBatch++
				s.pool.Put(msg[:0])
			}
			dispatchBatch()
			return

		case msg := <-s.eventsCh:
			s.frameAppend(&batch, msg)
			messagesInBatch++
			s.pool.Put(msg[:0])
			if batch.Len() >= s.maxBatchBytes || messagesInBatch >= s.maxBatchMessages {
				dispatchBatch()
				resetTimer()
			}

		case <-flushTimer.C:
			dispatchBatch()
			resetTimer()
		}
	}
}

// connectionManager runs in its own goroutine, managing a single HTTP/3 stream.
func (s *EventSender) connectionManager(id int, batchCh <-chan []byte) {
	defer s.wg.Done()
	var connState *connectionState

	// Ensure connection is torn down on exit.
	defer func() {
		if connState != nil {
			_ = connState.writer.Close()
			connState.cancelReq()
		}
	}()

	for batch := range batchCh {
		// Establish connection if we don't have one.
		if connState == nil {
			var err error
			connState, err = s.establishConnection(id)
			if err != nil {
				fmt.Printf("[conn %d] failed to establish connection: %v. Dropping batch.\n", id, err)
				// Sleep briefly to prevent a tight loop of failures.
				time.Sleep(250 * time.Millisecond)
				continue
			}
		}

		// Write the batch.
		_, err := connState.writer.Write(batch)
		if err != nil {
			fmt.Printf("[conn %d] write error: %v. Tearing down connection.\n", id, err)
			// Error on write means the stream is broken. Close it and nil it out
			// so it gets re-established on the next batch.
			_ = connState.writer.Close()
			connState.cancelReq()
			connState = nil
		}
	}
}

// establishConnection creates a new HTTP/3 stream and returns its state.
func (s *EventSender) establishConnection(id int) (*connectionState, error) {
	pipeReader, pipeWriter := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, pipeReader)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create h3 request: %w", err)
	}

	go func() {
		resp, err := s.h3Client.Do(req)
		if err != nil {
			_ = pipeWriter.CloseWithError(fmt.Errorf("client request failed: %w", err))
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 300 {
			err := fmt.Errorf("server returned non-2xx status: %s", resp.Status)
			_ = pipeWriter.CloseWithError(err)
		} else {
			// Read the body to completion to allow the server to close gracefully.
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = pipeWriter.Close()
		}
	}()

	fmt.Printf("[conn %d] established new http/3 stream\n", id)
	return &connectionState{writer: pipeWriter, cancelReq: cancel}, nil
}

// frameAppend appends a length-prefixed message to the buffer.
func (s *EventSender) frameAppend(buf *bytes.Buffer, msg []byte) {
	var lenBytes [4]byte
	binary.BigEndian.PutUint32(lenBytes[:], uint32(len(msg)))
	buf.Write(lenBytes[:])
	buf.Write(msg)
}

// --- Example Usage & Server ---

func main() {
	serverTLSConf, clientCertPool := generateTLSConfig()
	addr := "localhost:9090"

	go runDummyH3Server(addr, serverTLSConf)
	time.Sleep(100 * time.Millisecond)

	clientTLSConf := &tls.Config{
		RootCAs:    clientCertPool,
		NextProtos: []string{"h3"},
	}

	sender := NewEventSender(addr, 4096, clientTLSConf)
	defer sender.Close()

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
				if !sender.TrySend(payload) {
					_ = sender.Send(payload)
				}
			}
		}(i)
	}
	wg.Wait()

	time.Sleep(12000 * time.Millisecond)
	fmt.Printf("done sending %d messages in %v\n", producers*msgsPerProducer, time.Since(start))
}

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
				if err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
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
			fmt.Println("Payload: ", string(payload))
			if strings.Contains(string(payload), "producer=0") {
				time.Sleep(100 * time.Millisecond)
			}
			total++
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
