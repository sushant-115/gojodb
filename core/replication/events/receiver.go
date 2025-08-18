package events

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

type ReceiverLogger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
	Debugf(format string, args ...any)
}

// NopLogger is used if you don't supply a logger.
type NopLogger struct{}

func (NopLogger) Infof(string, ...any)  {}
func (NopLogger) Warnf(string, ...any)  {}
func (NopLogger) Errorf(string, ...any) {}
func (NopLogger) Debugf(string, ...any) {}

// Hooks let you wire metrics/alerts without coupling.
type ReceiverHooks struct {
	OnAccepted    func()                         // called when an event is accepted into the channel
	OnDropped     func(reason string)            // called when an event is dropped (queue full, too large, etc.)
	OnStreamStart func(remote string)            // new incoming POST /events
	OnStreamClose func(remote string, err error) // stream ended (err may be nil)
	OnServerStart func(addr string)
	OnServerClose func()
	OnError       func(stage string, err error)
}

// Backpressure policy for the events channel.
type BackpressurePolicy int

const (
	// BlockSender blocks the handler until a slot frees up (risk: head-of-line blocking).
	BlockSender BackpressurePolicy = iota
	// DropIfFull drops the event immediately when channel is full (lossy, but protects latency).
	DropIfFull
)

type ReceiverConfig struct {
	Addr    string       // e.g., ":8444" or "0.0.0.0:8444"
	URLPath string       // e.g., "/events"
	TLS     *tls.Config  // required for HTTP/3
	QUIC    *quic.Config // optional; timeouts, keepalive, etc.

	QueueCapacity int // capacity of r.eventsCh

	// Limits
	MaxEventBytes   int   // maximum single event size (length-prefix) e.g., 1<<20 (1MiB)
	MaxStreamBytes  int64 // cap total bytes we will accept from a single stream; 0 = unlimited
	MaxConcurrency  int   // max concurrent request handlers; 0/neg = unlimited
	Backpressure    BackpressurePolicy
	ResponseOnStart bool // if true, write 200 OK immediately (best-effort acknowledge). Note: client still must finish sending.
	RequireMethod   bool // if true, enforce POST only (otherwise allow any method)
}

type EventReceiver struct {
	cfg     ReceiverConfig
	log     ReceiverLogger
	hooks   ReceiverHooks
	server  *http3.Server
	ln      net.PacketConn
	events  chan []byte
	pool    *sync.Pool
	wg      sync.WaitGroup
	closed  int32
	sem     chan struct{} // concurrency limiter
	started int32
}

// NewEventReceiver builds a receiver. Pass nil logger to use NopLogger.
func NewEventReceiver(cfg ReceiverConfig, logger ReceiverLogger, hooks ReceiverHooks) (*EventReceiver, error) {
	if cfg.Addr == "" {
		return nil, errors.New("Config.Addr is required")
	}
	if cfg.URLPath == "" {
		cfg.URLPath = "/events"
	}
	if cfg.TLS == nil {
		return nil, errors.New("Config.TLS is required for HTTP/3")
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 4096
	}
	if cfg.MaxEventBytes <= 0 {
		cfg.MaxEventBytes = 1 << 20 // 1 MiB
	}

	cfg.ResponseOnStart = true

	if logger == nil {
		logger = NopLogger{}
	}

	r := &EventReceiver{
		cfg:    cfg,
		log:    logger,
		hooks:  hooks,
		events: make(chan []byte, cfg.QueueCapacity),
		pool: &sync.Pool{
			New: func() any {
				// a modest default; will grow on demand
				b := make([]byte, 4096)
				return &b
			},
		},
	}
	if cfg.MaxConcurrency > 0 {
		r.sem = make(chan struct{}, cfg.MaxConcurrency)
	}

	mux := http.NewServeMux()
	mux.HandleFunc(cfg.URLPath, r.eventHandler)

	r.server = &http3.Server{
		Addr:       cfg.Addr,
		TLSConfig:  cfg.TLS,
		Handler:    mux,
		QUICConfig: cfg.QUIC,
	}

	return r, nil
}

// Start begins listening on UDP and serving HTTP/3.
func (r *EventReceiver) Start() error {
	if !atomic.CompareAndSwapInt32(&r.started, 0, 1) {
		return errors.New("EventReceiver already started")
	}

	conn, err := net.ListenPacket("udp", r.cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen UDP %s: %w", r.cfg.Addr, err)
	}
	r.ln = conn

	if r.hooks.OnServerStart != nil {
		r.hooks.OnServerStart(conn.LocalAddr().String())
	}
	r.log.Infof("EventReceiver: listening (HTTP/3) on %s path=%s", conn.LocalAddr().String(), r.cfg.URLPath)

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := r.server.Serve(conn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			r.log.Errorf("EventReceiver serve error: %v", err)
			if r.hooks.OnError != nil {
				r.hooks.OnError("serve", err)
			}
		}
	}()

	return nil
}

// Close stops the server and closes the events channel after all handlers exit.
func (r *EventReceiver) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&r.closed, 0, 1) {
		return nil
	}

	// Stop accepting new connections.
	_ = r.server.Close()
	// Close UDP socket so Serve returns.
	if r.ln != nil {
		_ = r.ln.Close()
	}

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		r.log.Warnf("EventReceiver: Close timed out: %v", ctx.Err())
	case <-done:
	}

	// No more producers; close channel to signal consumers.
	close(r.events)

	if r.hooks.OnServerClose != nil {
		r.hooks.OnServerClose()
	}
	r.log.Infof("EventReceiver: closed")
	return nil
}

// Events returns the consumer channel (read-only).
func (r *EventReceiver) Events() <-chan []byte {
	return r.events
}

func (r *EventReceiver) acquire() func() {
	if r.sem == nil {
		return func() {}
	}
	r.sem <- struct{}{}
	return func() { <-r.sem }
}

// eventHandler processes a length-prefixed stream: [4B big-endian len][payload]...
func (r *EventReceiver) eventHandler(w http.ResponseWriter, req *http.Request) {
	if r.cfg.RequireMethod && req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	release := r.acquire()
	defer release()

	remote := req.RemoteAddr
	if req.Body == nil {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	if r.hooks.OnStreamStart != nil {
		r.hooks.OnStreamStart(remote)
	}
	defer func(start time.Time) {
		if r.hooks.OnStreamClose != nil {
			r.hooks.OnStreamClose(remote, nil)
		}
		r.log.Debugf("EventReceiver: stream from %s finished in %s", remote, time.Since(start))
	}(time.Now())

	ctx := req.Context()
	body := req.Body
	//defer body.Close()

	// Optionally acknowledge early (best effort).
	if r.cfg.ResponseOnStart {
		w.WriteHeader(http.StatusOK)
		// For HTTP/3, headers are sent when handler returns or when you write.
		// Writing an empty body nudges headers out.
		_, _ = w.Write(nil)
	}

	var lenBuf [4]byte
	var streamBytes int64

	readFull := func(dst []byte) error {
		n, err := io.ReadFull(body, dst)
		fmt.Println("Received bytes count: ", n, err)
		return err
	}

	for {
		// Allow cancel on client disconnect.
		select {
		case <-ctx.Done():
			r.log.Debugf("EventReceiver: client cancelled from %s: %v", remote, ctx.Err())
			return
		default:
		}

		if r.cfg.MaxStreamBytes > 0 && streamBytes >= r.cfg.MaxStreamBytes {
			r.drop("stream_bytes_cap", fmt.Errorf("stream exceeded cap %d", r.cfg.MaxStreamBytes))
			return
		}

		if err := readFull(lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				fmt.Println("End of stream")
				// normal close or truncated final frame; treat as end of stream
				return
			}
			r.err("read_len", err)
			// For well-formedness errors, respond 400 if we didn't already.
			if !r.cfg.ResponseOnStart {
				http.Error(w, "bad stream", http.StatusBadRequest)
			}
			return
		}
		streamBytes += 4

		n := binary.BigEndian.Uint32(lenBuf[:])
		if n == 0 {
			// skip zero-length frames
			continue
		}
		if int(n) <= 0 || int(n) > r.cfg.MaxEventBytes {
			r.drop("event_too_large", fmt.Errorf("event size %d > max %d", n, r.cfg.MaxEventBytes))
			if !r.cfg.ResponseOnStart {
				http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
			}
			return
		}

		// Rent buffer
		bufPtr := r.pool.Get().(*[]byte)
		b := *bufPtr
		if cap(b) < int(n) {
			b = make([]byte, int(n))
		} else {
			b = b[:int(n)]
		}

		// Read payload
		if err := readFull(b); err != nil {
			// return buffer before exiting
			r.pool.Put(&b)
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// truncated final frame; end stream
				return
			}
			r.err("read_payload", err)
			if !r.cfg.ResponseOnStart {
				http.Error(w, "bad stream", http.StatusBadRequest)
			}
			return
		}
		streamBytes += int64(n)

		// Handoff: copy into stable slice owned by receiver (so pool reuse is safe)
		ev := make([]byte, int(n))
		copy(ev, b)
		r.pool.Put(&b)
		if string(ev) == "Test" {
			fmt.Println("Received test message: ", string(ev))
			continue
		}
		// Backpressure policy
		switch r.cfg.Backpressure {
		case BlockSender:
			select {
			case r.events <- ev:
				if r.hooks.OnAccepted != nil {
					r.hooks.OnAccepted()
				}
			case <-ctx.Done():
				// client left while we were blocking
				r.drop("client_cancelled_blocking", ctx.Err())
				return
			}
		case DropIfFull:
			select {
			case r.events <- ev:
				if r.hooks.OnAccepted != nil {
					r.hooks.OnAccepted()
				}
			default:
				r.drop("queue_full", nil)
				// drop ev; GC will collect
			}
		}
	}
}

// helper: uniform error logging + hook
func (r *EventReceiver) err(stage string, err error) {
	r.log.Errorf("EventReceiver: %s: %v", stage, err)
	if r.hooks.OnError != nil {
		r.hooks.OnError(stage, err)
	}
}

func (r *EventReceiver) drop(reason string, err error) {
	if err != nil {
		r.log.Warnf("EventReceiver: drop (%s): %v", reason, err)
	} else {
		r.log.Warnf("EventReceiver: drop (%s)", reason)
	}
	if r.hooks.OnDropped != nil {
		r.hooks.OnDropped(reason)
	}
}
