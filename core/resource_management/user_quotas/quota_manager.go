// Package user_quotas tracks per-namespace storage consumption and request
// rates and enforces hard limits.
package user_quotas

import (
	"fmt"
	"sync"
	"time"
)

var ErrQuotaExceeded = fmt.Errorf("quota exceeded")

type Quota struct {
	MaxStorageBytes      int64
	MaxRequestsPerSecond float64
	MaxKeysCount         int64
}

type Usage struct {
	StorageBytes int64
	KeysCount    int64
	requests     float64
	windowStart  time.Time
}

type namespaceEntry struct {
	mu    sync.Mutex
	Quota Quota
	Usage Usage
}

type Manager struct {
	mu         sync.RWMutex
	namespaces map[string]*namespaceEntry
}

func NewManager() *Manager {
	return &Manager{namespaces: make(map[string]*namespaceEntry)}
}

func (m *Manager) SetQuota(namespace string, q Quota) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.namespaces[namespace]; ok {
		e.mu.Lock()
		e.Quota = q
		e.mu.Unlock()
		return
	}
	m.namespaces[namespace] = &namespaceEntry{Quota: q, Usage: Usage{windowStart: time.Now()}}
}

func (m *Manager) RemoveQuota(namespace string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.namespaces, namespace)
}

func (m *Manager) CheckAndRecordWrite(namespace string, valueBytes int64) error {
	e := m.getOrCreate(namespace)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Quota.MaxStorageBytes > 0 && e.Usage.StorageBytes+valueBytes > e.Quota.MaxStorageBytes {
		return fmt.Errorf("%w: storage bytes for %s", ErrQuotaExceeded, namespace)
	}
	if e.Quota.MaxKeysCount > 0 && e.Usage.KeysCount+1 > e.Quota.MaxKeysCount {
		return fmt.Errorf("%w: key count for %s", ErrQuotaExceeded, namespace)
	}
	e.Usage.StorageBytes += valueBytes
	e.Usage.KeysCount++
	return nil
}

func (m *Manager) RecordDelete(namespace string, freedBytes int64) {
	e := m.getOrCreate(namespace)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Usage.StorageBytes -= freedBytes
	if e.Usage.StorageBytes < 0 {
		e.Usage.StorageBytes = 0
	}
	e.Usage.KeysCount--
	if e.Usage.KeysCount < 0 {
		e.Usage.KeysCount = 0
	}
}

func (m *Manager) CheckRequest(namespace string) error {
	if namespace == "" {
		return nil
	}
	e := m.getOrCreate(namespace)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Quota.MaxRequestsPerSecond <= 0 {
		return nil
	}
	now := time.Now()
	elapsed := now.Sub(e.Usage.windowStart).Seconds()
	if elapsed >= 1.0 {
		e.Usage.requests = 0
		e.Usage.windowStart = now
	}
	if e.Usage.requests+1 > e.Quota.MaxRequestsPerSecond {
		return fmt.Errorf("%w: request rate for %s", ErrQuotaExceeded, namespace)
	}
	e.Usage.requests++
	return nil
}

func (m *Manager) GetUsage(namespace string) Usage {
	e := m.getOrCreate(namespace)
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.Usage
}

func (m *Manager) GetQuota(namespace string) Quota {
	e := m.getOrCreate(namespace)
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.Quota
}

func (m *Manager) getOrCreate(namespace string) *namespaceEntry {
	m.mu.RLock()
	e, ok := m.namespaces[namespace]
	m.mu.RUnlock()
	if ok {
		return e
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok = m.namespaces[namespace]; ok {
		return e
	}
	e = &namespaceEntry{Usage: Usage{windowStart: time.Now()}}
	m.namespaces[namespace] = e
	return e
}
