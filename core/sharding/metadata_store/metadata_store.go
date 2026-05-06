// Package metadata_store provides a higher-level view of shard-to-node mapping
// persisted via the Raft FSM.  It wraps the FSM's SlotRangeInfo structures and
// provides helpers for key routing and membership queries.
package metadata_store

import (
	"fmt"
	"hash/fnv"
	"sync"

	fsm "github.com/sushant-115/gojodb/core/replication/raft_consensus"
)

const DefaultSlotCount = 16384

type NodeID = string
type SlotID = uint16

type Store struct {
	mu            sync.RWMutex
	fsmHandle     *fsm.FSM
	slotToPrimary map[SlotID]NodeID
}

func NewStore(f *fsm.FSM) *Store {
	return &Store{
		fsmHandle:     f,
		slotToPrimary: make(map[SlotID]NodeID),
	}
}

func (s *Store) Refresh() {
	shardMap := s.fsmHandle.GetShardMap()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slotToPrimary = make(map[SlotID]NodeID, DefaultSlotCount)
	for _, info := range shardMap {
		start, end, err := parseRange(info.SlotRange)
		if err != nil {
			continue
		}
		for slot := start; slot <= end; slot++ {
			s.slotToPrimary[SlotID(slot)] = info.PrimaryNodeID
		}
	}
}

func (s *Store) PrimaryForKey(key string) (NodeID, error) {
	slot := HashSlot(key)
	s.mu.RLock()
	node, ok := s.slotToPrimary[slot]
	s.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("metadata_store: no assignment for slot %d (key %q)", slot, key)
	}
	return node, nil
}

func (s *Store) ReplicasForKey(key string) ([]NodeID, error) {
	slot := HashSlot(key)
	shardMap := s.fsmHandle.GetShardMap()
	for _, info := range shardMap {
		start, end, err := parseRange(info.SlotRange)
		if err != nil {
			continue
		}
		if uint16(start) <= slot && slot <= uint16(end) {
			return info.ReplicaNodeIDs, nil
		}
	}
	return nil, fmt.Errorf("metadata_store: no slot info for key %q", key)
}

func (s *Store) ShardMap() map[string]*fsm.SlotRangeInfo {
	return s.fsmHandle.GetShardMap()
}

func HashSlot(key string) SlotID {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return SlotID(h.Sum32() % DefaultSlotCount)
}

func parseRange(rangeStr string) (int, int, error) {
	var start, end int
	n, err := fmt.Sscanf(rangeStr, "%d-%d", &start, &end)
	if err != nil || n < 2 {
		n2, err2 := fmt.Sscanf(rangeStr, "%d", &start)
		if err2 != nil || n2 < 1 {
			return 0, 0, fmt.Errorf("invalid slot range %q", rangeStr)
		}
		end = start
	}
	if start < 0 || end >= DefaultSlotCount || start > end {
		return 0, 0, fmt.Errorf("slot range %q out of bounds", rangeStr)
	}
	return start, end, nil
}
