// Package leader_election provides utilities for determining and monitoring
// Raft leadership state.
package leader_election

import (
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type State int

const (
	Follower  State = iota
	Candidate State = iota
	Leader    State = iota
)

type LeadershipChange struct {
	IsLeader bool
	LeaderID string
	At       time.Time
}

type Monitor struct {
	r           *raft.Raft
	mu          sync.RWMutex
	subscribers map[int]chan LeadershipChange
	nextID      int
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

func NewMonitor(r *raft.Raft) *Monitor {
	m := &Monitor{
		r:           r,
		subscribers: make(map[int]chan LeadershipChange),
		stopCh:      make(chan struct{}),
	}
	m.wg.Add(1)
	go m.watch()
	return m
}

func (m *Monitor) Subscribe(ch chan LeadershipChange) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.nextID
	m.nextID++
	m.subscribers[id] = ch
	return id
}

func (m *Monitor) Unsubscribe(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.subscribers, id)
}

func (m *Monitor) IsLeader() bool {
	return m.r.State() == raft.Leader
}

func (m *Monitor) LeaderID() string {
	_, id := m.r.LeaderWithID()
	return string(id)
}

func (m *Monitor) Stop() {
	close(m.stopCh)
	m.wg.Wait()
}

func (m *Monitor) watch() {
	defer m.wg.Done()
	obsCh := make(chan raft.Observation, 16)
	obs := raft.NewObserver(obsCh, true, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	m.r.RegisterObserver(obs)
	defer m.r.DeregisterObserver(obs)
	for {
		select {
		case <-m.stopCh:
			return
		case o := <-obsCh:
			if lo, ok := o.Data.(raft.LeaderObservation); ok {
				change := LeadershipChange{
					IsLeader: m.r.State() == raft.Leader,
					LeaderID: string(lo.LeaderID),
					At:       time.Now(),
				}
				m.broadcast(change)
			}
		}
	}
}

func (m *Monitor) broadcast(change LeadershipChange) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, ch := range m.subscribers {
		select {
		case ch <- change:
		default:
		}
	}
}
