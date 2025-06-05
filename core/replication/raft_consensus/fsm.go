package fsm

import (
	"encoding/json"
	"fmt"
	"hash/crc32" // For CRC32 hashing
	"io"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// LogCommand defines the structure of commands applied to the FSM via Raft.
// This is what gets replicated.
type LogCommand struct {
	Op    string `json:"op"`    // Operation type (e.g., "set_metadata", "add_node", "assign_slot_range")
	Key   string `json:"key"`   // Key for the operation (e.g., metadata key, node ID, slot range string "start-end")
	Value string `json:"value"` // Value for the operation (e.g., metadata value, node address, JSON-marshaled SlotRangeInfo)
}

// Operation types for the FSM
const (
	OpSetMetadata       = "set_metadata"
	OpDeleteMetadata    = "delete_metadata"
	OpAddStorageNode    = "add_storage_node"
	OpRemoveStorageNode = "remove_node"
	OpUpdateNodeStatus  = "update_node_status"

	// --- Hash Sharding Operations ---
	OpAssignSlotRange = "assign_slot_range" // Assigns a range of slots to a storage node
	OpSplitSlotRange  = "split_slot_range"  // Splits an existing slot range into new ones
	OpRemoveSlotRange = "remove_slot_range" // Removes a slot range (e.g., after merge or rebalance)

	// Replication Operations ---
	OpSetPrimaryReplica = "set_primary_replica" // Assigns primary and replica(s) for a slot range
	OpPromoteReplica    = "promote_replica"     // Promotes a replica to primary
	OpRemoveReplica     = "remove_replica"      // Removes a replica from a slot range
)

// TotalHashSlots defines the fixed number of hash slots for sharding.
const TotalHashSlots = 1024 // A common number in distributed systems (e.g., Redis uses 16384)

// SlotRangeInfo defines the metadata for a contiguous range of hash slots.
// It is designed to be JSON serializable for storage in Raft logs.
type SlotRangeInfo struct {
	RangeID        string    `json:"range_id"`         // Unique ID for this slot range (e.g., "0-1023", "512-767")
	StartSlot      int       `json:"start_slot"`       // Inclusive start slot
	EndSlot        int       `json:"end_slot"`         // Inclusive end slot
	AssignedNodeID string    `json:"assigned_node_id"` // ID of the Storage Node currently hosting this slot range
	Status         string    `json:"status"`           // e.g., "active", "migrating_in", "migrating_out", "offline"
	LastUpdated    time.Time `json:"last_updated"`     // Timestamp of last update
	// Replication Fields
	PrimaryNodeID  string   `json:"primary_node_id"`  // ID of the Storage Node acting as primary for this slot range
	ReplicaNodeIDs []string `json:"replica_node_ids"` // IDs of Storage Nodes acting as replicas for this slot range
}

// GojoDBFSM implements the raft.FSM interface.
// It holds the replicated state of the Control Plane.
type GojoDBFSM struct {
	mu               sync.RWMutex
	metadata         map[string]string        // Replicated general metadata (e.g., table schema, config)
	storageNodes     map[string]string        // Replicated active Storage Node IDs -> Address (or more complex status)
	slotAssignments  map[string]SlotRangeInfo // Replicated slot range ID -> SlotRangeInfo mapping
	lastAppliedIndex uint64                   // The last Raft log index applied to this FSM
}

// NewGojoDBFSM creates a new instance of GojoDBFSM.
func NewGojoDBFSM() *GojoDBFSM {
	return &GojoDBFSM{
		metadata:        make(map[string]string),
		storageNodes:    make(map[string]string),
		slotAssignments: make(map[string]SlotRangeInfo), // Initialize slot assignments map
	}
}

// Apply applies a Raft log entry to the FSM.
// This method is called by Raft on the leader and followers to update the state machine.
func (f *GojoDBFSM) Apply(logEntry *raft.Log) interface{} {
	var cmd LogCommand
	if err := json.Unmarshal(logEntry.Data, &cmd); err != nil {
		log.Printf("ERROR: Failed to unmarshal Raft log entry: %v", err)
		return nil // Should not happen with valid commands
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.lastAppliedIndex = logEntry.Index // Update the last applied index

	switch cmd.Op {
	case OpSetMetadata:
		f.metadata[cmd.Key] = cmd.Value
		// log.Printf("DEBUG: FSM applied: Set metadata '%s' = '%s' (Index: %d)", cmd.Key, cmd.Value, logEntry.Index)
		return nil
	case OpDeleteMetadata:
		delete(f.metadata, cmd.Key)
		// log.Printf("DEBUG: FSM applied: Deleted metadata '%s' (Index: %d)", cmd.Key, logEntry.Index)
		return nil
	case OpAddStorageNode:
		f.storageNodes[cmd.Key] = cmd.Value // Key is NodeID, Value is Address
		// log.Printf("DEBUG: FSM applied: Added Storage Node '%s' at '%s' (Index: %d)", cmd.Key, cmd.Value, logEntry.Index)
		return nil
	case OpRemoveStorageNode:
		delete(f.storageNodes, cmd.Key)
		// log.Printf("DEBUG: FSM applied: Removed Storage Node '%s' (Index: %d)", cmd.Key, logEntry.Index)
		return nil
	case OpUpdateNodeStatus:
		f.storageNodes[cmd.Key] = cmd.Value // Update status/address
		// log.Printf("DEBUG: FSM applied: Updated Storage Node '%s' status to '%s' (Index: %d)", cmd.Key, cmd.Value, logEntry.Index)
		return nil

	// --- Hash Sharding Operations Apply Logic ---
	case OpAssignSlotRange:
		var slotRangeInfo SlotRangeInfo
		if err := json.Unmarshal([]byte(cmd.Value), &slotRangeInfo); err != nil {
			log.Printf("ERROR: FSM failed to unmarshal SlotRangeInfo for OpAssignSlotRange: %v", err)
			return fmt.Errorf("invalid SlotRangeInfo format for OpAssignSlotRange: %w", err)
		}
		// Use RangeID as the map key
		f.slotAssignments[slotRangeInfo.RangeID] = slotRangeInfo
		// log.Printf("DEBUG: FSM applied: Assigned Slot Range '%s' (%d-%d) to Node '%s' (Index: %d)",
		// slotRangeInfo.RangeID, slotRangeInfo.StartSlot, slotRangeInfo.EndSlot, slotRangeInfo.AssignedNodeID, logEntry.Index)
		return nil
	case OpSplitSlotRange:
		// cmd.Key: original RangeID to split
		// cmd.Value: JSON array of new SlotRangeInfo structs for the split parts
		var newSlotRanges []SlotRangeInfo
		if err := json.Unmarshal([]byte(cmd.Value), &newSlotRanges); err != nil {
			log.Printf("ERROR: FSM failed to unmarshal new SlotRangeInfo array for OpSplitSlotRange: %v", err)
			return fmt.Errorf("invalid new slot ranges format for OpSplitSlotRange: %w", err)
		}

		// Remove the original slot range
		delete(f.slotAssignments, cmd.Key)
		// log.Printf("DEBUG: FSM applied: Removed original slot range '%s' for split (Index: %d)", cmd.Key, logEntry.Index)

		// Add the new split slot ranges
		for _, newRange := range newSlotRanges {
			f.slotAssignments[newRange.RangeID] = newRange
			// log.Printf("DEBUG: FSM applied: Added new split slot range '%s' (%d-%d) to Node '%s' (Index: %d)",
			// newRange.RangeID, newRange.StartSlot, newRange.EndSlot, newRange.AssignedNodeID, logEntry.Index)
		}
		return nil
	case OpRemoveSlotRange:
		delete(f.slotAssignments, cmd.Key)
		// log.Printf("DEBUG: FSM applied: Removed Slot Range '%s' (Index: %d)", cmd.Key, logEntry.Index)
		return nil
		// --- End Hash Sharding Operations Apply Logic ---

		// --- NEW: Replication Operations Apply Logic ---
	case OpSetPrimaryReplica:
		var slotInfo SlotRangeInfo // Expecting a full SlotRangeInfo with Primary/Replicas
		if err := json.Unmarshal([]byte(cmd.Value), &slotInfo); err != nil {
			log.Printf("ERROR: FSM failed to unmarshal SlotRangeInfo for OpSetPrimaryReplica: %v", err)
			return fmt.Errorf("invalid SlotRangeInfo format for OpSetPrimaryReplica: %w", err)
		}
		if existing, ok := f.slotAssignments[slotInfo.RangeID]; ok {
			existing.PrimaryNodeID = slotInfo.PrimaryNodeID
			existing.ReplicaNodeIDs = slotInfo.ReplicaNodeIDs
			existing.Status = slotInfo.Status // Update status if provided (e.g., "active")
			existing.LastUpdated = time.Now()
			f.slotAssignments[slotInfo.RangeID] = existing
			// log.Printf("DEBUG: FSM applied: Set Primary '%s' and Replicas %v for slot range '%s' (Index: %d)",
			// slotInfo.PrimaryNodeID, slotInfo.ReplicaNodeIDs, slotInfo.RangeID, logEntry.Index)
		} else {
			log.Printf("WARNING: FSM tried to set primary/replica for non-existent slot range '%s' (Index: %d)", slotInfo.RangeID, logEntry.Index)
			return fmt.Errorf("slot range %s not found for primary/replica assignment", slotInfo.RangeID)
		}
		return nil
	case OpPromoteReplica:
		// cmd.Key: RangeID
		// cmd.Value: New PrimaryNodeID
		newPrimaryNodeID := cmd.Value
		if existing, ok := f.slotAssignments[cmd.Key]; ok {
			// Remove old primary if it exists in replicas
			newReplicas := []string{}
			if existing.PrimaryNodeID != "" {
				newReplicas = append(newReplicas, existing.PrimaryNodeID)
			}
			// Add existing replicas, excluding the new primary
			for _, replicaID := range existing.ReplicaNodeIDs {
				if replicaID != newPrimaryNodeID {
					newReplicas = append(newReplicas, replicaID)
				}
			}

			existing.PrimaryNodeID = newPrimaryNodeID
			existing.ReplicaNodeIDs = newReplicas
			existing.Status = "active" // Or "recovering"
			existing.LastUpdated = time.Now()
			f.slotAssignments[cmd.Key] = existing
			// log.Printf("DEBUG: FSM applied: Promoted Node '%s' to Primary for slot range '%s' (Index: %d)",
			// newPrimaryNodeID, cmd.Key, logEntry.Index)
		} else {
			log.Printf("WARNING: FSM tried to promote replica for non-existent slot range '%s' (Index: %d)", cmd.Key, logEntry.Index)
			return fmt.Errorf("slot range %s not found for replica promotion", cmd.Key)
		}
		return nil
	case OpRemoveReplica:
		// cmd.Key: RangeID
		// cmd.Value: ReplicaNodeID to remove
		replicaToRemoveID := cmd.Value
		if existing, ok := f.slotAssignments[cmd.Key]; ok {
			newReplicas := []string{}
			for _, replicaID := range existing.ReplicaNodeIDs {
				if replicaID != replicaToRemoveID {
					newReplicas = append(newReplicas, replicaID)
				}
			}
			existing.ReplicaNodeIDs = newReplicas
			existing.LastUpdated = time.Now()
			f.slotAssignments[cmd.Key] = existing
			// log.Printf("DEBUG: FSM applied: Removed Replica '%s' from slot range '%s' (Index: %d)",
			// replicaToRemoveID, cmd.Key, logEntry.Index)
		} else {
			log.Printf("WARNING: FSM tried to remove replica for non-existent slot range '%s' (Index: %d)", cmd.Key, logEntry.Index)
			return fmt.Errorf("slot range %s not found for replica removal", cmd.Key)
		}
		return nil
		// --- END NEW ---

	default:
		log.Printf("WARNING: Unknown FSM command operation: %s (Index: %d)", cmd.Op, logEntry.Index)
		return fmt.Errorf("unknown FSM command operation: %s", cmd.Op)
	}
}

// Snapshot returns a snapshot of the FSM's state.
// This is used by Raft to truncate the log and recover faster.
func (f *GojoDBFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Create a deep copy of the current state
	metadataCopy := make(map[string]string)
	for k, v := range f.metadata {
		metadataCopy[k] = v
	}
	storageNodesCopy := make(map[string]string)
	for k, v := range f.storageNodes {
		storageNodesCopy[k] = v
	}
	slotAssignmentsCopy := make(map[string]SlotRangeInfo) // Copy slot assignments
	for k, v := range f.slotAssignments {
		slotAssignmentsCopy[k] = v
	}

	log.Printf("DEBUG: FSM Snapshot created at index %d", f.lastAppliedIndex)
	return &gojoDBFSMSnapshot{
		metadata:        metadataCopy,
		storageNodes:    storageNodesCopy,
		slotAssignments: slotAssignmentsCopy, // Include slot assignments in snapshot
	}, nil
}

// Restore restores the FSM's state from a snapshot.
// This is used by Raft when a node joins a cluster or recovers from a crash.
func (f *GojoDBFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snapshotData struct {
		Metadata        map[string]string        `json:"metadata"`
		StorageNodes    map[string]string        `json:"storage_nodes"`
		SlotAssignments map[string]SlotRangeInfo `json:"slot_assignments"` // Include slot assignments in snapshot data
	}

	if err := json.NewDecoder(rc).Decode(&snapshotData); err != nil {
		return fmt.Errorf("failed to decode FSM snapshot: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.metadata = snapshotData.Metadata
	f.storageNodes = snapshotData.StorageNodes
	f.slotAssignments = snapshotData.SlotAssignments // Restore slot assignments
	// Note: lastAppliedIndex is typically restored by Raft itself, not the FSM.
	// The FSM's Apply method will continue from the next log entry.

	log.Println("INFO: FSM state restored from snapshot.")
	return nil
}

// --- FSM Query Methods (Read-only access to the state) ---

// GetMetadata retrieves a metadata value.
func (f *GojoDBFSM) GetMetadata(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	val, ok := f.metadata[key]
	return val, ok
}

// GetStorageNodes returns a copy of the current storage node map.
func (f *GojoDBFSM) GetStorageNodes() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	copyMap := make(map[string]string)
	for k, v := range f.storageNodes {
		copyMap[k] = v
	}
	return copyMap
}

// GetStorageNodeCount returns the number of active storage nodes.
func (f *GojoDBFSM) GetStorageNodeCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.storageNodes)
}

// GetSlotRangeInfo retrieves information about a specific slot range.
func (f *GojoDBFSM) GetSlotRangeInfo(rangeID string) (SlotRangeInfo, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	info, ok := f.slotAssignments[rangeID]
	return info, ok
}

// GetSlotForHashKey hashes a key to a slot.
func GetSlotForHashKey(key string) int {
	// Using CRC32 IEEE polynomial for hashing, similar to Redis Cluster
	// The result is then modulo TotalHashSlots to get the slot number.
	checksum := crc32.ChecksumIEEE([]byte(key))
	return int(checksum % TotalHashSlots)
}

// GetNodeForHashKey determines which Storage Node a given key's slot is assigned to.
// Returns the AssignedNodeID and true if found, otherwise empty string and false.
func (f *GojoDBFSM) GetNodeForHashKey(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	targetSlot := GetSlotForHashKey(key)

	// Iterate through slot assignments to find the one that covers the target slot
	for _, assignment := range f.slotAssignments {
		if targetSlot >= assignment.StartSlot && targetSlot <= assignment.EndSlot {
			return assignment.AssignedNodeID, true
		}
	}
	return "", false // No matching slot range found
}

// GetAllSlotAssignments returns a copy of the entire slot assignments map.
func (f *GojoDBFSM) GetAllSlotAssignments() map[string]SlotRangeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	copyMap := make(map[string]SlotRangeInfo)
	for k, v := range f.slotAssignments {
		copyMap[k] = v
	}
	return copyMap
}

// --- FSMSnapshot Implementation ---

// gojoDBFSMSnapshot implements the raft.FSMSnapshot interface.
type gojoDBFSMSnapshot struct {
	metadata        map[string]string
	storageNodes    map[string]string
	slotAssignments map[string]SlotRangeInfo // Include slot assignments in snapshot struct
}

// Persist writes the snapshot to the given sink.
func (s *gojoDBFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

	snapshotData := struct {
		Metadata        map[string]string        `json:"metadata"`
		StorageNodes    map[string]string        `json:"storage_nodes"`
		SlotAssignments map[string]SlotRangeInfo `json:"slot_assignments"` // Include slot assignments in snapshot data
	}{
		Metadata:        s.metadata,
		StorageNodes:    s.storageNodes,
		SlotAssignments: s.slotAssignments, // Populate slot assignments
	}

	bytes, err := json.Marshal(snapshotData)
	if err != nil {
		return fmt.Errorf("failed to marshal FSM snapshot: %w", err)
	}

	if _, err := sink.Write(bytes); err != nil {
		return fmt.Errorf("failed to write FSM snapshot to sink: %w", err)
	}

	log.Println("DEBUG: FSM Snapshot persisted.")
	return nil
}

// Release is called when the snapshot is no longer needed.
func (s *gojoDBFSMSnapshot) Release() {
	log.Println("DEBUG: FSM Snapshot released.")
	// No-op for in-memory snapshot data after it's persisted.
}
