package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/hashicorp/raft"
)

// LogCommand defines the structure of commands applied to the FSM via Raft.
// This is what gets replicated.
type LogCommand struct {
	Op    string `json:"op"`    // Operation type (e.g., "set_metadata", "add_node", "remove_node")
	Key   string `json:"key"`   // Key for the operation
	Value string `json:"value"` // Value for the operation (e.g., metadata value, node address)
	// Add other fields as needed for specific command types
}

// Operation types for the FSM
const (
	OpSetMetadata       = "set_metadata"
	OpDeleteMetadata    = "delete_metadata"
	OpAddStorageNode    = "add_storage_node"
	OpRemoveStorageNode = "remove_storage_node"
	OpUpdateNodeStatus  = "update_node_status" // For more granular node status
)

// GojoDBFSM implements the raft.FSM interface.
// It holds the replicated state of the Control Plane.
type GojoDBFSM struct {
	mu               sync.RWMutex
	metadata         map[string]string // Replicated metadata (e.g., table schema, config)
	storageNodes     map[string]string // Replicated active Storage Node IDs -> Address (or more complex status)
	lastAppliedIndex uint64            // The last Raft log index applied to this FSM
}

// NewGojoDBFSM creates a new instance of GojoDBFSM.
func NewGojoDBFSM() *GojoDBFSM {
	return &GojoDBFSM{
		metadata:     make(map[string]string),
		storageNodes: make(map[string]string),
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
		log.Printf("DEBUG: FSM applied: Set metadata '%s' = '%s' (Index: %d)", cmd.Key, cmd.Value, logEntry.Index)
		return nil
	case OpDeleteMetadata:
		delete(f.metadata, cmd.Key)
		log.Printf("DEBUG: FSM applied: Deleted metadata '%s' (Index: %d)", cmd.Key, logEntry.Index)
		return nil
	case OpAddStorageNode:
		f.storageNodes[cmd.Key] = cmd.Value // Key is NodeID, Value is Address
		log.Printf("DEBUG: FSM applied: Added Storage Node '%s' at '%s' (Index: %d)", cmd.Key, cmd.Value, logEntry.Index)
		return nil
	case OpRemoveStorageNode:
		delete(f.storageNodes, cmd.Key)
		log.Printf("DEBUG: FSM applied: Removed Storage Node '%s' (Index: %d)", cmd.Key, logEntry.Index)
		return nil
	case OpUpdateNodeStatus:
		// This could be a more complex update, e.g., for health status
		f.storageNodes[cmd.Key] = cmd.Value // Update status/address
		log.Printf("DEBUG: FSM applied: Updated Storage Node '%s' status to '%s' (Index: %d)", cmd.Key, cmd.Value, logEntry.Index)
		return nil
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

	log.Printf("DEBUG: FSM Snapshot created at index %d", f.lastAppliedIndex)
	return &gojoDBFSMSnapshot{
		metadata:     metadataCopy,
		storageNodes: storageNodesCopy,
	}, nil
}

// Restore restores the FSM's state from a snapshot.
// This is used by Raft when a node joins a cluster or recovers from a crash.
func (f *GojoDBFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snapshotData struct {
		Metadata     map[string]string `json:"metadata"`
		StorageNodes map[string]string `json:"storage_nodes"`
	}

	if err := json.NewDecoder(rc).Decode(&snapshotData); err != nil {
		return fmt.Errorf("failed to decode FSM snapshot: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.metadata = snapshotData.Metadata
	f.storageNodes = snapshotData.StorageNodes
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

// --- FSMSnapshot Implementation ---

// gojoDBFSMSnapshot implements the raft.FSMSnapshot interface.
type gojoDBFSMSnapshot struct {
	metadata     map[string]string
	storageNodes map[string]string
}

// Persist writes the snapshot to the given sink.
func (s *gojoDBFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

	snapshotData := struct {
		Metadata     map[string]string `json:"metadata"`
		StorageNodes map[string]string `json:"storage_nodes"`
	}{
		Metadata:     s.metadata,
		StorageNodes: s.storageNodes,
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
