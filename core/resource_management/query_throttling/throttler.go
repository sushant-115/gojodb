// Package query_throttling enforces per-query and per-tenant timeouts, and
// records slow queries for later analysis.
package query_throttling

import (
	"context"
	"fmt"
	"sync"
	"time"
)

var ErrQueryTimeout = fmt.Errorf("query timeout exceeded")

type SlowQueryEntry struct {
	TenantID  string
	Query     string
	StartedAt time.Time
	Duration  time.Duration
}

type Config struct {
	DefaultTimeout     time.Duration
	SlowQueryThreshold time.Duration
	MaxSlowQueryLog    int
}

type Throttler struct {
	cfg            Config
	tenantTimeouts map[string]time.Duration
	mu             sync.RWMutex
	slowLog        []SlowQueryEntry
	slowLogPos     int
}

func NewThrottler(cfg Config) *Throttler {
	if cfg.MaxSlowQueryLog <= 0 {
		cfg.MaxSlowQueryLog = 1000
	}
	return &Throttler{
		cfg:            cfg,
		tenantTimeouts: make(map[string]time.Duration),
		slowLog:        make([]SlowQueryEntry, cfg.MaxSlowQueryLog),
	}
}

func (t *Throttler) SetTenantTimeout(tenantID string, d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tenantTimeouts[tenantID] = d
}

func (t *Throttler) RemoveTenantTimeout(tenantID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.tenantTimeouts, tenantID)
}

func (t *Throttler) WithQueryTimeout(ctx context.Context, tenantID string) (context.Context, context.CancelFunc) {
	to := t.timeout(tenantID)
	if to <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, to)
}

func (t *Throttler) Track(ctx context.Context, tenantID, query string, fn func(context.Context) error) error {
	ctx, cancel := t.WithQueryTimeout(ctx, tenantID)
	defer cancel()
	start := time.Now()
	err := fn(ctx)
	dur := time.Since(start)
	if t.cfg.SlowQueryThreshold > 0 && dur >= t.cfg.SlowQueryThreshold {
		t.recordSlow(tenantID, query, start, dur)
	}
	return err
}

func (t *Throttler) SlowQueryLog() []SlowQueryEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]SlowQueryEntry, 0, t.cfg.MaxSlowQueryLog)
	for i := 0; i < t.cfg.MaxSlowQueryLog; i++ {
		idx := (t.slowLogPos + i) % t.cfg.MaxSlowQueryLog
		if t.slowLog[idx].TenantID != "" {
			out = append(out, t.slowLog[idx])
		}
	}
	return out
}

func (t *Throttler) timeout(tenantID string) time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if d, ok := t.tenantTimeouts[tenantID]; ok {
		return d
	}
	return t.cfg.DefaultTimeout
}

func (t *Throttler) recordSlow(tenantID, query string, start time.Time, dur time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.slowLog[t.slowLogPos%t.cfg.MaxSlowQueryLog] = SlowQueryEntry{
		TenantID:  tenantID,
		Query:     query,
		StartedAt: start,
		Duration:  dur,
	}
	t.slowLogPos++
}
