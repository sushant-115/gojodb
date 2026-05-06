// Package admission_control provides a per-tenant token-bucket rate limiter that
// limits the number of requests allowed per second.  Tenants that exceed their
// configured burst are rejected immediately (no blocking / queuing).
package admission_control

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ErrRateLimitExceeded is returned when a tenant has no tokens available.
var ErrRateLimitExceeded = fmt.Errorf("rate limit exceeded")

// TenantConfig holds the token-bucket parameters for one tenant.
type TenantConfig struct {
	Rate  float64
	Burst int
}

type bucket struct {
	mu       sync.Mutex
	tokens   float64
	rate     float64
	burst    float64
	lastFill time.Time
}

func (b *bucket) take(n float64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.lastFill).Seconds()
	b.tokens += elapsed * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.lastFill = now
	if b.tokens < n {
		return ErrRateLimitExceeded
	}
	b.tokens -= n
	return nil
}

// Controller manages per-tenant token buckets.
type Controller struct {
	mu       sync.RWMutex
	buckets  map[string]*bucket
	defaults TenantConfig
}

// NewController creates an admission controller with the given default tenant config.
func NewController(defaults TenantConfig) *Controller {
	return &Controller{
		buckets:  make(map[string]*bucket),
		defaults: defaults,
	}
}

// LoadConfig sets or updates the rate-limit parameters for a tenant.
func (c *Controller) LoadConfig(tenantID string, cfg TenantConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok := c.buckets[tenantID]; ok {
		b.mu.Lock()
		b.rate = cfg.Rate
		b.burst = float64(cfg.Burst)
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.mu.Unlock()
		return
	}
	c.buckets[tenantID] = &bucket{
		tokens:   float64(cfg.Burst),
		rate:     cfg.Rate,
		burst:    float64(cfg.Burst),
		lastFill: time.Now(),
	}
}

// RemoveConfig removes the bucket for a tenant.
func (c *Controller) RemoveConfig(tenantID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.buckets, tenantID)
}

// Allow checks whether one token can be consumed for tenantID.
func (c *Controller) Allow(tenantID string) error {
	return c.AllowN(tenantID, 1)
}

// AllowN checks whether n tokens can be consumed for tenantID.
func (c *Controller) AllowN(tenantID string, n int) error {
	b := c.getOrCreate(tenantID)
	return b.take(float64(n))
}

// AllowCtx is like Allow but also honours context cancellation.
func (c *Controller) AllowCtx(ctx context.Context, tenantID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return c.Allow(tenantID)
}

func (c *Controller) getOrCreate(tenantID string) *bucket {
	c.mu.RLock()
	b, ok := c.buckets[tenantID]
	c.mu.RUnlock()
	if ok {
		return b
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok = c.buckets[tenantID]; ok {
		return b
	}
	b = &bucket{
		tokens:   float64(c.defaults.Burst),
		rate:     c.defaults.Rate,
		burst:    float64(c.defaults.Burst),
		lastFill: time.Now(),
	}
	c.buckets[tenantID] = b
	return b
}

// Stats returns the current token count for a tenant.
func (c *Controller) Stats(tenantID string) (tokens float64, burst float64) {
	b := c.getOrCreate(tenantID)
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tokens, b.burst
}

