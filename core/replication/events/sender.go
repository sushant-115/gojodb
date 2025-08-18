// Package events provides a production‑ready HTTP/3 (QUIC) batching sender
// with multi-connection streaming, bounded backpressure, retries with
// exponential backoff, and graceful shutdown.
package events

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/logging"
	"github.com/quic-go/quic-go/qlog"
)

// Logger is a tiny interface to allow plugging your own logger (zap, slog, logrus).
// The standard library's *log.Logger satisfies it via LoggerFunc below.
type Logger interface {
	Printf(format string, args ...any)
}

// LoggerFunc adapts a function to the Logger interface.
type LoggerFunc func(format string, args ...any)

func (f LoggerFunc) Printf(format string, args ...any) { f(format, args...) }

// Metrics hooks let you observe behavior without importing a metrics library here.
// Provide nil to skip.
type Metrics struct {
	OnBatchEnqueued   func(bytes int, msgs int)
	OnBatchDispatched func(connID int, bytes int, msgs int)
	OnBatchRetried    func(connID int, attempt int)
	OnBatchDropped    func(connID int, reason string)
	OnConnEstablished func(connID int)
	OnConnFailed      func(connID int, err error)
	OnConnTornDown    func(connID int)
}

// Config controls EventSender behavior.
type Config struct {
	// Remote endpoint.
	Addr    string      // host:port
	URLPath string      // e.g. "/events"
	TLS     *tls.Config // TLS config (SNI, RootCAs, etc.)

	// Concurrency & buffering.
	NumConnections   int           // number of concurrent streaming POSTs
	QueueCapacity    int           // capacity of the ingress queue (messages)
	MaxBatchBytes    int           // max bytes per batch
	MaxBatchMessages int           // max messages per batch
	FlushInterval    time.Duration // max time to wait before flushing a partial batch

	// Retry policy for establishing connections and writing batches.
	MaxWriteRetries   int           // total attempts = 1 + MaxWriteRetries
	InitialBackoff    time.Duration // base backoff (e.g., 100ms)
	MaxBackoff        time.Duration // cap for backoff
	BackoffJitterFrac float64       // e.g., 0.2 => ±20% jitter

	// QUIC/HTTP3 low-level knobs (optional).
	QUIC *quic.Config

	// Logging & metrics (optional).
	Logger  Logger
	Metrics *Metrics
}

// setDefaults applies sensible defaults when fields are zero.
func (c *Config) setDefaults() {
	if c.URLPath == "" {
		c.URLPath = "/events"
	}
	if c.NumConnections <= 0 {
		c.NumConnections = 4
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = 4096
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = 64 * 1024
	}
	if c.MaxBatchMessages <= 0 {
		c.MaxBatchMessages = 256
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 50 * time.Millisecond
	}
	if c.MaxWriteRetries < 0 {
		c.MaxWriteRetries = 0
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = 100 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 5 * time.Second
	}
	if c.BackoffJitterFrac <= 0 {
		c.BackoffJitterFrac = 0.2
	}
	if c.Logger == nil {
		c.Logger = LoggerFunc(func(f string, a ...any) { log.Printf(f, a...) })
	}
}

// EventSender sends events over HTTP/3 using concurrent long‑lived requests.
type EventSender struct {
	cfg        Config
	url        string
	pool       *sync.Pool
	quit       chan struct{}
	closed     int32
	wg         sync.WaitGroup
	client     *http.Client
	rt         *http3.Transport
	eventsCh   chan []byte   // ingress of individual messages (owned by batchingLoop)
	connInputs []chan []byte // one batch channel per connectionManager
	randSrc    *rand.Rand
}

// newQUICConfig creates a new quic.Config with qlog tracing enabled.
// This implementation uses the modern API for the quic-go library.
func newQUICConfig() *quic.Config {
	// This function defines how to create a log file (an io.WriteCloser)
	// for each new QUIC connection.
	newQlogWriter := func(p logging.Perspective, connID []byte) io.WriteCloser {
		// Create a unique file name for each connection using its ID.
		// This prevents connections from overwriting each other's logs.
		filename := fmt.Sprintf("/tmp/connection_%x.qlog", connID)

		// Differentiate between client and server logs if needed.
		if p == logging.PerspectiveServer {
			filename = fmt.Sprintf("/tmp/server_connection_%x.qlog", connID)
		}

		// Create the file.
		f, err := os.Create(filename)
		if err != nil {
			// If file creation fails, log the error and return nil.
			// Returning nil will disable qlog for this specific connection.
			fmt.Printf("Error creating qlog file %s: %v\n", filename, err)
			return nil
		}

		fmt.Printf("Created qlog file: %s\n", filename)
		// The returned *os.File implements the io.WriteCloser interface.
		return f
	}

	// The quic.Config.Tracer field now expects a function that returns a new
	// connection tracer for each new connection. This is a factory pattern.
	return &quic.Config{
		EnableDatagrams: true,
		Tracer: func(ctx context.Context, p logging.Perspective, connID quic.ConnectionID) *logging.ConnectionTracer {
			// This function is called by quic-go for each new connection.
			// We return a new tracer that uses our file-creating function.
			return qlog.NewConnectionTracer(newQlogWriter(p, connID.Bytes()), p, connID)
		},
	}
}

// New creates a production‑ready EventSender.
func NewEventSender(cfg Config) (*EventSender, error) {
	cfg.setDefaults()
	if cfg.Addr == "" {
		return nil, errors.New("Config.Addr is required")
	}
	cfg.QUIC = newQUICConfig()
	rt := &http3.Transport{TLSClientConfig: cfg.TLS, QUICConfig: cfg.QUIC}
	client := &http.Client{Transport: rt}

	s := &EventSender{
		cfg:      cfg,
		url:      fmt.Sprintf("https://%s%s", cfg.Addr, cfg.URLPath),
		pool:     &sync.Pool{New: func() any { return make([]byte, 0, 2048) }},
		quit:     make(chan struct{}),
		client:   client,
		rt:       rt,
		eventsCh: make(chan []byte, cfg.QueueCapacity),
		randSrc:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	// Per‑connection batch channels (buffer=1 to decouple a bit).
	s.connInputs = make([]chan []byte, cfg.NumConnections)
	for i := 0; i < cfg.NumConnections; i++ {
		s.connInputs[i] = make(chan []byte, 1)
	}

	// Start batching loop and connection managers.
	s.wg.Add(1)
	go s.batchingLoop()

	for i := 0; i < cfg.NumConnections; i++ {
		s.wg.Add(1)
		go s.connectionManager(i, s.connInputs[i])
	}
	return s, nil
}

// Send blocks until the message is enqueued or the sender is closed.
func (s *EventSender) Send(msg []byte) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return errors.New("sender closed")
	}
	buf := s.pool.Get().([]byte)[:0]
	buf = append(buf, msg...)
	select {
	case s.eventsCh <- buf:
		return nil
	case <-s.quit:
		s.pool.Put(buf[:0])
		return errors.New("sender closed")
	}
}

// TrySend attempts to enqueue without blocking.
func (s *EventSender) TrySend(msg []byte) bool {
	if atomic.LoadInt32(&s.closed) == 1 {
		return false
	}
	buf := s.pool.Get().([]byte)[:0]
	buf = append(buf, msg...)
	select {
	case s.eventsCh <- buf:
		return true
	default:
		s.pool.Put(buf[:0])
		return false
	}
}

// Close gracefully drains and stops all goroutines.
func (s *EventSender) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return errors.New("already closed")
	}
	close(s.quit)
	s.wg.Wait()
	return s.rt.Close()
}

// ----------------- internal -----------------

type connectionState struct {
	writer    io.WriteCloser
	cancelReq context.CancelFunc
}

func (s *EventSender) batchingLoop() {
	defer s.wg.Done()
	defer func() { // close per‑conn inputs so managers exit
		for _, ch := range s.connInputs {
			close(ch)
		}
	}()

	var batch bytes.Buffer
	msgs := 0
	flushTimer := time.NewTimer(s.cfg.FlushInterval)
	defer flushTimer.Stop()

	dispatch := func() {
		if msgs == 0 {
			return
		}
		payload := make([]byte, batch.Len())
		copy(payload, batch.Bytes())
		// try to hand off to any connection non‑blocking first (randomized start for fairness)
		start := s.randSrc.Intn(len(s.connInputs))
		for i := 0; i < len(s.connInputs); i++ {
			idx := (start + i) % len(s.connInputs)
			select {
			case s.connInputs[idx] <- payload:
				if s.cfg.Metrics != nil && s.cfg.Metrics.OnBatchDispatched != nil {
					s.cfg.Metrics.OnBatchDispatched(idx, len(payload), msgs)
				}
				batch.Reset()
				msgs = 0
				return
			default:
			}
		}
		// If none accepted immediately, block on any one becoming available or quit.
		select {
		case s.connInputs[start] <- payload: // pick deterministic one to avoid reflect.Select overhead
			if s.cfg.Metrics != nil && s.cfg.Metrics.OnBatchDispatched != nil {
				s.cfg.Metrics.OnBatchDispatched(start, len(payload), msgs)
			}
			batch.Reset()
			msgs = 0
		case <-s.quit:
			return
		}
	}

	resetTimer := func() {
		if !flushTimer.Stop() {
			select {
			case <-flushTimer.C:
			default:
			}
		}
		flushTimer.Reset(s.cfg.FlushInterval)
	}

	for {
		select {
		case <-s.quit:
			// drain
			for {
				select {
				case m := <-s.eventsCh:
					s.frameAppend(&batch, m)
					msgs++
					s.pool.Put(m[:0])
					if s.cfg.Metrics != nil && s.cfg.Metrics.OnBatchEnqueued != nil {
						s.cfg.Metrics.OnBatchEnqueued(len(m), 1)
					}
				default:
					dispatch()
					return
				}
			}

		case m := <-s.eventsCh:
			s.frameAppend(&batch, m)
			msgs++
			s.pool.Put(m[:0])
			if s.cfg.Metrics != nil && s.cfg.Metrics.OnBatchEnqueued != nil {
				s.cfg.Metrics.OnBatchEnqueued(len(m), 1)
			}
			if batch.Len() >= s.cfg.MaxBatchBytes || msgs >= s.cfg.MaxBatchMessages {
				dispatch()
				resetTimer()
			}

		case <-flushTimer.C:
			dispatch()
			resetTimer()
		}
	}
}

func (s *EventSender) connectionManager(id int, in <-chan []byte) {
	defer s.wg.Done()
	var st *connectionState
	defer func() {
		if st != nil {
			_ = st.writer.Close()
			st.cancelReq()
			if s.cfg.Metrics != nil && s.cfg.Metrics.OnConnTornDown != nil {
				s.cfg.Metrics.OnConnTornDown(id)
			}
		}
	}()

	for payload := range in { // receive batches
		// lazy connect or reconnect if needed
		if st == nil {
			var err error
			st, err = s.establishConnection(id)
			if err != nil {
				s.noteConnFailed(id, err)
				// retry sending this payload with backoff and a fresh connection each time
				if !s.retrySend(id, nil, payload) {
					s.drop(id, payload, "establish failed")
				}
				continue
			}
		}
		// log.Println("Payload: ", payload)
		if _, err := st.writer.Write(payload); err != nil {
			s.cfg.Logger.Printf("[conn %d] write error: %v — reconnecting", id, err)
			_ = st.writer.Close()
			st.cancelReq()
			st = nil
			if !s.retrySend(id, nil, payload) {
				s.drop(id, payload, "write failed")
			}
			continue
		}
	}
}

// retrySend attempts to (re)establish a connection and write the payload
// using exponential backoff. On success, returns true.
func (s *EventSender) retrySend(id int, st *connectionState, payload []byte) bool {
	backoff := s.cfg.InitialBackoff
	attempts := 0
	for {
		if attempts > s.cfg.MaxWriteRetries {
			return false
		}
		attempts++

		if st == nil {
			var err error
			st, err = s.establishConnection(id)
			if err != nil {
				s.noteConnFailed(id, err)
				if !s.sleepBackoff(backoff) {
					return false
				}
				backoff = nextBackoff(backoff, s.cfg.MaxBackoff, s.cfg.BackoffJitterFrac, s.randSrc)
				continue
			}
		}

		if _, err := st.writer.Write(payload); err == nil {
			return true
		}
		// write failed; tear down and try again
		_ = st.writer.Close()
		st.cancelReq()
		st = nil
		if s.cfg.Metrics != nil && s.cfg.Metrics.OnBatchRetried != nil {
			s.cfg.Metrics.OnBatchRetried(id, attempts)
		}
		if !s.sleepBackoff(backoff) {
			return false
		}
		backoff = nextBackoff(backoff, s.cfg.MaxBackoff, s.cfg.BackoffJitterFrac, s.randSrc)
	}
}

func (s *EventSender) sleepBackoff(d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-s.quit:
		return false
	}
}

func nextBackoff(cur, max time.Duration, jitterFrac float64, r *rand.Rand) time.Duration {
	next := time.Duration(float64(cur) * 2)
	if next > max {
		next = max
	}
	if jitterFrac > 0 && r != nil {
		j := 1 + (r.Float64()*2-1)*jitterFrac // 1±frac
		next = time.Duration(math.Max(0, float64(next)*j))
	}
	return next
}

func (s *EventSender) drop(connID int, payload []byte, reason string) {
	if s.cfg.Metrics != nil && s.cfg.Metrics.OnBatchDropped != nil {
		s.cfg.Metrics.OnBatchDropped(connID, reason)
	}
	s.cfg.Logger.Printf("[conn %d] dropping batch (%d bytes): %s", connID, len(payload), reason)
}

// establishConnection opens a streaming HTTP/3 POST using io.Pipe for body.
func (s *EventSender) establishConnection(id int) (*connectionState, error) {
	pr, pw := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, pr)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("new request: %w", err)
	}

	// Fire the request in a goroutine and watch its response lifecycle.
	go func() {
		resp, err := s.client.Do(req)
		if err != nil {
			_ = pw.CloseWithError(fmt.Errorf("client request failed: %w", err))
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 300 {
			_ = pw.CloseWithError(fmt.Errorf("server returned non-2xx: %s", resp.Status))
			return
		}
		// Drain so server can close cleanly when we finish.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = pw.Close()
	}()

	if s.cfg.Metrics != nil && s.cfg.Metrics.OnConnEstablished != nil {
		s.cfg.Metrics.OnConnEstablished(id)
	}
	s.cfg.Logger.Printf("[conn %d] established new HTTP/3 stream to %s", id, s.url)
	return &connectionState{writer: pw, cancelReq: cancel}, nil
}

func (s *EventSender) noteConnFailed(id int, err error) {
	if s.cfg.Metrics != nil && s.cfg.Metrics.OnConnFailed != nil {
		s.cfg.Metrics.OnConnFailed(id, err)
	}
	s.cfg.Logger.Printf("[conn %d] establish failed: %v", id, err)
}

// frameAppend writes a 4‑byte big‑endian length prefix followed by msg bytes into buf.
func (s *EventSender) frameAppend(buf *bytes.Buffer, msg []byte) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(msg)))
	buf.Write(n[:])
	buf.Write(msg)
}
