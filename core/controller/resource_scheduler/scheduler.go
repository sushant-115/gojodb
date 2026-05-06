// Package resource_scheduler assigns shards to storage nodes and rebalances
// the cluster when the node topology changes.
package resource_scheduler

import (
	"fmt"
	"sort"
	"sync"
)

type ShardAssignment struct {
	ShardID      string
	StartSlot    int
	EndSlot      int
	PrimaryNode  string
	ReplicaNodes []string
}

type NodeLoad struct {
	NodeID     string
	ShardCount int
}

type Scheduler struct {
	mu          sync.RWMutex
	assignments map[string]*ShardAssignment
	nodeLoads   map[string]int
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		assignments: make(map[string]*ShardAssignment),
		nodeLoads:   make(map[string]int),
	}
}

func (s *Scheduler) AddShard(a ShardAssignment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.assignments[a.ShardID]; ok {
		s.nodeLoads[old.PrimaryNode]--
	}
	s.assignments[a.ShardID] = &a
	s.nodeLoads[a.PrimaryNode]++
}

func (s *Scheduler) RemoveShard(shardID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.assignments[shardID]; ok {
		s.nodeLoads[a.PrimaryNode]--
		delete(s.assignments, shardID)
	}
}

func (s *Scheduler) Assignment(shardID string) (*ShardAssignment, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.assignments[shardID]
	if !ok {
		return nil, false
	}
	cp := *a
	return &cp, true
}

func (s *Scheduler) AllAssignments() []ShardAssignment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ShardAssignment, 0, len(s.assignments))
	for _, a := range s.assignments {
		out = append(out, *a)
	}
	return out
}

func (s *Scheduler) RegisterNode(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodeLoads[nodeID]; !ok {
		s.nodeLoads[nodeID] = 0
	}
}

func (s *Scheduler) DeregisterNode(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.nodeLoads, nodeID)
}

func (s *Scheduler) LeastLoadedNode() (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.nodeLoads) == 0 {
		return "", fmt.Errorf("resource_scheduler: no nodes registered")
	}
	best := ""
	bestLoad := int(^uint(0) >> 1)
	for id, load := range s.nodeLoads {
		if load < bestLoad {
			bestLoad = load
			best = id
		}
	}
	return best, nil
}

func (s *Scheduler) Rebalance() [][3]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var moves [][3]string
	for {
		maxN, minN := s.maxMinNodes()
		if maxN == "" || minN == "" || s.nodeLoads[maxN]-s.nodeLoads[minN] <= 1 {
			break
		}
		shardID := s.shardForNode(maxN)
		if shardID == "" {
			break
		}
		a := s.assignments[shardID]
		from := a.PrimaryNode
		a.PrimaryNode = minN
		s.nodeLoads[from]--
		s.nodeLoads[minN]++
		moves = append(moves, [3]string{shardID, from, minN})
	}
	return moves
}

func (s *Scheduler) NodeLoads() []NodeLoad {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]NodeLoad, 0, len(s.nodeLoads))
	for id, cnt := range s.nodeLoads {
		out = append(out, NodeLoad{NodeID: id, ShardCount: cnt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

func (s *Scheduler) maxMinNodes() (maxNode, minNode string) {
	for id, load := range s.nodeLoads {
		if maxNode == "" || load > s.nodeLoads[maxNode] {
			maxNode = id
		}
		if minNode == "" || load < s.nodeLoads[minNode] {
			minNode = id
		}
	}
	return
}

func (s *Scheduler) shardForNode(nodeID string) string {
	for id, a := range s.assignments {
		if a.PrimaryNode == nodeID {
			return id
		}
	}
	return ""
}
