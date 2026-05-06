// Package node_discovery tracks live storage nodes by recording periodic
// heartbeats and expiring silent nodes.
package node_discovery

import (
	"sync"
	"time"
)

type NodeInfo struct {
	NodeID        string
	GRPCAddr      string
	ReplAddr      string
	HeartbeatAddr string
	LastSeen      time.Time
	Extra         map[string]string
}

type Registry struct {
	mu       sync.RWMutex
	nodes    map[string]*NodeInfo
	ttl      time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
	OnExpiry func(nodeID string)
}

func NewRegistry(ttl time.Duration) *Registry {
	r := &Registry{
		nodes:  make(map[string]*NodeInfo),
		ttl:    ttl,
		stopCh: make(chan struct{}),
	}
	r.wg.Add(1)
	go r.reaper()
	return r
}

func (r *Registry) Heartbeat(info NodeInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	info.LastSeen = time.Now()
	r.nodes[info.NodeID] = &info
}

func (r *Registry) Remove(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodes, nodeID)
}

func (r *Registry) Get(nodeID string) *NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := r.nodes[nodeID]
	if n == nil {
		return nil
	}
	cp := *n
	return &cp
}

func (r *Registry) LiveNodes() []NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]NodeInfo, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, *n)
	}
	return out
}

func (r *Registry) LiveNodeIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.nodes))
	for id := range r.nodes {
		ids = append(ids, id)
	}
	return ids
}

func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

func (r *Registry) Stop() {
	close(r.stopCh)
	r.wg.Wait()
}

func (r *Registry) reaper() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.expire()
		}
	}
}

func (r *Registry) expire() {
	deadline := time.Now().Add(-r.ttl)
	r.mu.Lock()
	var expired []string
	for id, n := range r.nodes {
		if n.LastSeen.Before(deadline) {
			expired = append(expired, id)
			delete(r.nodes, id)
		}
	}
	r.mu.Unlock()
	if r.OnExpiry != nil {
		for _, id := range expired {
			r.OnExpiry(id)
		}
	}
}
