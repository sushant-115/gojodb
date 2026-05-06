// Package lru provides a production‑grade, concurrency‑safe cache with LRU eviction
// and optional per-entry TTL, on-evict callbacks, metrics, and single-flight loading.
//
// Usage:
//
//	c := lru.New[string, []byte](1024,
//	    lru.WithDefaultTTL[string, []byte](0), // no default TTL
//	    lru.WithOnEvict[string, []byte](func(k string, v []byte, reason lru.EvictReason) {
//	        fmt.Printf("evicted %q reason=%s\n", k, reason)
//	    }),
//	    lru.WithJanitor[string, []byte](time.Minute), // optional background expiry sweeper
//	)
//
//	c.Set("k", []byte("v"))
//	if v, ok := c.Get("k"); ok { _ = v }
package lru

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// EvictReason explains why an entry was removed.
//go:generate stringer -type=EvictReason -trimprefix=Evict
// (stringer is optional; without it, fmt will print numeric constants.)

type EvictReason int

const (
	EvictLRU EvictReason = iota + 1
	EvictCapacity
	EvictExpired
	EvictDeleted
)

// entry is a node in the intrusive doubly linked list (LRU order).
// Newest (MRU) is at head; oldest (LRU) is at tail.

type entry[K comparable, V any] struct {
	key        K
	value      V
	prev, next *entry[K, V]
	expiresAt  time.Time // zero => no TTL
}

// Metrics are safe for concurrent use. Use Load() to read.

type Metrics struct {
	Hits        atomic.Int64
	Misses      atomic.Int64
	Sets        atomic.Int64
	Evictions   atomic.Int64
	Expirations atomic.Int64
}

func (m *Metrics) Snapshot() (hits, misses, sets, evictions, expirations int64) {
	return m.Hits.Load(), m.Misses.Load(), m.Sets.Load(), m.Evictions.Load(), m.Expirations.Load()
}

// Options

type options[K comparable, V any] struct {
	defaultTTL time.Duration
	onEvict    func(K, V, EvictReason)
	janitorInt time.Duration
}

type Option[K comparable, V any] func(*options[K, V])

func WithDefaultTTL[K comparable, V any](d time.Duration) Option[K, V] {
	return func(o *options[K, V]) { o.defaultTTL = d }
}

func WithOnEvict[K comparable, V any](cb func(K, V, EvictReason)) Option[K, V] {
	return func(o *options[K, V]) { o.onEvict = cb }
}

// WithJanitor starts a background goroutine that periodically scans for expired entries.
// Set interval to 0 to disable.
func WithJanitor[K comparable, V any](interval time.Duration) Option[K, V] {
	return func(o *options[K, V]) { o.janitorInt = interval }
}

// Cache is a concurrency‑safe LRU cache with optional TTL.
// It uses a global RWMutex for simplicity and predictable performance. For very high write
// contention scenarios, you can shard at a higher level by running multiple Cache instances.

type Cache[K comparable, V any] struct {
	mu      sync.RWMutex
	cap     int
	items   map[K]*entry[K, V]
	head    *entry[K, V]
	tail    *entry[K, V]
	opts    options[K, V]
	metrics Metrics

	// single-flight loader state
	loadMu   sync.Mutex
	inflight map[K]*flight[V]

	// janitor
	stopJanitor chan struct{}
}

type flight[V any] struct {
	ch   chan result[V]
	once sync.Once
}

type result[V any] struct {
	v   V
	err error
}

var ErrNotFound = errors.New("lru: not found")

// New creates a new Cache with the given capacity (must be > 0).
func New[K comparable, V any](capacity int, opts ...Option[K, V]) *Cache[K, V] {
	if capacity <= 0 {
		panic("lru: capacity must be > 0")
	}
	c := &Cache[K, V]{
		cap:         capacity,
		items:       make(map[K]*entry[K, V], capacity),
		inflight:    make(map[K]*flight[V]),
		stopJanitor: make(chan struct{}),
	}
	for _, o := range opts {
		o(&c.opts)
	}
	if c.opts.janitorInt > 0 {
		go c.janitor(c.opts.janitorInt)
	}
	return c
}

// Close stops any background goroutines (janitor). Cache remains usable.
func (c *Cache[K, V]) Close() { close(c.stopJanitor) }

// Len returns the number of items in the cache.
func (c *Cache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Capacity returns the configured capacity.
func (c *Cache[K, V]) Capacity() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cap
}

// SetCapacity changes capacity and evicts LRU entries if needed.
func (c *Cache[K, V]) SetCapacity(n int) {
	if n <= 0 {
		panic("lru: capacity must be > 0")
	}
	c.mu.Lock()
	c.cap = n
	for len(c.items) > c.cap {
		c.evictLRULocked()
	}
	c.mu.Unlock()
}

// Set inserts or updates a value with the cache's default TTL (if any).
func (c *Cache[K, V]) Set(key K, val V) { c.SetWithTTL(key, val, c.opts.defaultTTL) }

// SetWithTTL inserts or updates a value with a per-item TTL (0 => no TTL).
func (c *Cache[K, V]) SetWithTTL(key K, val V, ttl time.Duration) {
	c.metrics.Sets.Add(1)
	exp := time.Time{}
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}

	c.mu.Lock()
	if e, ok := c.items[key]; ok {
		// update in place and move to head
		e.value = val
		e.expiresAt = exp
		c.moveToHeadLocked(e)
	} else {
		e := &entry[K, V]{key: key, value: val, expiresAt: exp}
		c.items[key] = e
		c.insertAtHeadLocked(e)
		if len(c.items) > c.cap {
			c.evictLRULocked()
		}
	}
	c.mu.Unlock()
}

// Get returns the value and true if present and not expired. It updates LRU order.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock() // we need write lock to potentially move node
	if e, ok := c.items[key]; ok {
		if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
			c.metrics.Expirations.Add(1)
			c.removeLocked(e, EvictExpired)
			var zero V
			c.metrics.Misses.Add(1)
			c.mu.Unlock()
			return zero, false
		}
		c.moveToHeadLocked(e)
		c.metrics.Hits.Add(1)
		v := e.value
		c.mu.Unlock()
		return v, true
	}
	c.metrics.Misses.Add(1)
	var zero V
	c.mu.Unlock()
	return zero, false
}

// Peek returns the value without updating LRU order.
func (c *Cache[K, V]) Peek(key K) (V, bool) {
	c.mu.RLock()
	if e, ok := c.items[key]; ok {
		if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
			c.mu.RUnlock()
			c.mu.Lock()
			// re-check under write lock
			if e2, ok2 := c.items[key]; ok2 && e2 == e && !e2.expiresAt.IsZero() && time.Now().After(e2.expiresAt) {
				c.metrics.Expirations.Add(1)
				c.removeLocked(e2, EvictExpired)
			}
			c.mu.Unlock()
			var zero V
			return zero, false
		}
		v := e.value
		c.mu.RUnlock()
		return v, true
	}
	c.mu.RUnlock()
	var zero V
	return zero, false
}

// Delete removes a key if present.
func (c *Cache[K, V]) Delete(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[key]; ok {
		c.removeLocked(e, EvictDeleted)
		return true
	}
	return false
}

// GetOrLoad returns an existing value if present, otherwise calls loader once (single‑flight)
// to obtain the value, stores it with the provided ttl, and returns it.
// Concurrent callers for the same key will wait for the same loader.
func (c *Cache[K, V]) GetOrLoad(ctx context.Context, key K, ttl time.Duration, loader func(context.Context, K) (V, error)) (V, error) {
	if v, ok := c.Get(key); ok {
		return v, nil
	}

	// single-flight
	f := c.getFlight(key)
	f.once.Do(func() {
		go func() {
			var res result[V]
			v, err := loader(ctx, key)
			res.v, res.err = v, err
			if err == nil {
				c.SetWithTTL(key, v, ttl)
			}
			f.ch <- res
			close(f.ch)
			c.clearFlight(key)
		}()
	})
	res, ok := <-f.ch
	if !ok { // raced, but channel closed; try cache again
		if v, ok2 := c.Get(key); ok2 {
			return v, nil
		}
		var zero V
		return zero, ErrNotFound
	}
	if res.err != nil {
		var zero V
		return zero, res.err
	}
	return res.v, nil
}

// Purge removes all entries.
func (c *Cache[K, V]) Purge() {
	c.mu.Lock()
	for e := c.head; e != nil; e = e.next {
		if c.opts.onEvict != nil {
			c.opts.onEvict(e.key, e.value, EvictDeleted)
		}
	}
	c.items = make(map[K]*entry[K, V], c.cap)
	c.head, c.tail = nil, nil
	c.mu.Unlock()
}

// Internal helpers (require c.mu held appropriately)

func (c *Cache[K, V]) insertAtHeadLocked(e *entry[K, V]) {
	e.prev = nil
	e.next = c.head
	if c.head != nil {
		c.head.prev = e
	}
	c.head = e
	if c.tail == nil {
		c.tail = e
	}
}

func (c *Cache[K, V]) moveToHeadLocked(e *entry[K, V]) {
	if c.head == e {
		return
	}
	// unlink
	if e.prev != nil {
		e.prev.next = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	}
	if c.tail == e {
		c.tail = e.prev
	}
	// relink at head
	e.prev = nil
	e.next = c.head
	if c.head != nil {
		c.head.prev = e
	}
	c.head = e
	if c.tail == nil {
		c.tail = e
	}
}

func (c *Cache[K, V]) removeLocked(e *entry[K, V], reason EvictReason) {
	delete(c.items, e.key)
	if e.prev != nil {
		e.prev.next = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	}
	if c.head == e {
		c.head = e.next
	}
	if c.tail == e {
		c.tail = e.prev
	}
	if reason == EvictExpired || reason == EvictLRU || reason == EvictCapacity {
		c.metrics.Evictions.Add(1)
	}
	if reason == EvictExpired {
		c.metrics.Expirations.Add(1)
	}
	if cb := c.opts.onEvict; cb != nil {
		cb(e.key, e.value, reason)
	}
}

func (c *Cache[K, V]) evictLRULocked() {
	if c.tail != nil {
		c.removeLocked(c.tail, EvictLRU)
	}
}

// single-flight helpers
func (c *Cache[K, V]) getFlight(key K) *flight[V] {
	c.loadMu.Lock()
	defer c.loadMu.Unlock()
	if f, ok := c.inflight[key]; ok {
		return f
	}
	f := &flight[V]{ch: make(chan result[V], 1)}
	c.inflight[key] = f
	return f
}

func (c *Cache[K, V]) clearFlight(key K) {
	c.loadMu.Lock()
	delete(c.inflight, key)
	c.loadMu.Unlock()
}

// janitor sweeps expired entries periodically. It uses best‑effort scanning.
func (c *Cache[K, V]) janitor(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.stopJanitor:
			return
		case <-t.C:
			now := time.Now()
			c.mu.Lock()
			for e := c.tail; e != nil; e = e.prev { // older entries more likely expired
				if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
					c.removeLocked(e, EvictExpired)
				}
			}
			c.mu.Unlock()
		}
	}
}

// DebugString returns a snapshot of keys from MRU->LRU (for testing/diagnostics only).
func (c *Cache[K, V]) DebugString() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := "["
	for e := c.head; e != nil; e = e.next {
		out += fmt.Sprintf("%v", e.key)
		if e.next != nil {
			out += " "
		}
	}
	out += "]"
	return out
}
