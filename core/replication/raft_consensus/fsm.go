package fsm

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sushant-115/gojodb/core/indexing"
	logreplication "github.com/sushant-115/gojodb/core/replication/log_replication"
	"github.com/sushant-115/gojodb/core/storage_engine/tiered_storage" // For TieredDataMetadata
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	"go.uber.org/zap"
)

// CommandType defines the type of command to be applied to the FSM.
type CommandType uint8

const (
	CommandNoOp                  CommandType = iota // Ensure NoOp is 0 for safety
	CommandRegisterNode                             // Storage node registers with controller/FSM
	CommandDeregisterNode                           //Removes a node from the cluster
	CommandUpdateNodeStatus                         // Storage node sends heartbeat/status
	CommandAssignSlot                               // Assign a shard/slot to a node (making it primary)
	CommandAddReplicaToSlot                         // Add a replica to a shard/slot
	CommandRemoveReplicaFromSlot                    // Remove a replica
	CommandSetSlotPrimary                           // Explicitly set a new primary for a slot (for failover)

	// Tiered Storage Commands
	CommandUpdateTieringMetadata // Create or update metadata for a tiered data unit
	CommandDeleteTieringMetadata // Delete metadata for a tiered data unit
	CommandUpdateDLMPolicy       // Add or update a DLM policy
	CommandDeleteDLMPolicy       // Delete a DLM policy

	// Replica Onboarding Commands (tracking states from Blueprint Table 2)
	CommandInitiateReplicaOnboarding    // Starts the process
	CommandUpdateReplicaOnboardingState // Source/Target nodes report progress

	// Shard Rebalancing Commands (tracking states from Blueprint Table 3)
	CommandInitiateShardMigration    // Starts shard migration
	CommandUpdateShardMigrationState // Source/Target/Controller updates migration progress
	CommandCommitShardMigration      // Atomically changes shard ownership in SlotAssignments

	// TODO: Add other command types as needed (e.g., for cluster config changes)
)

var TotalHashSlots uint32 = 8

// var sample string = `
// {
// 	"type":1,
// 	"node_id":"node_1",
// 	"node_addr":"127.0.0.1:8080",
// 	"slot_range":"0-511"
// }
// `

// Command represents a command to be applied to the FSM.
type Command struct {
	Type           CommandType       `json:"type"`
	NodeID         string            `json:"node_id,omitempty"`
	NodeAddr       string            `json:"node_addr,omitempty"` // For registration, replication endpoint
	SlotID         uint64            `json:"slot_id,omitempty"`
	SlotRange      string            `json:"slot_range"`
	TargetNodeID   string            `json:"target_node_id,omitempty"`  // For replica add/remove, migration target
	ShardID        string            `json:"shard_id,omitempty"`        // For more granular shard ID if different from SlotID
	Replicas       map[string]string `json:"replicas,omitempty"`        // map[NodeID]NodeAddr for a slot's replicas
	PrimaryNodeID  string            `json:"primary_node_id,omitempty"` // For setting primary
	ReplicaNodeIDs []string          `json:"replica_node_ids,omitempty"`

	// Tiered Storage Fields
	TieringMetadata *tiered_storage.TieredDataMetadata `json:"tiering_metadata,omitempty"`
	DLMPolicy       *tiered_storage.DLMPolicy          `json:"dlm_policy,omitempty"`
	PolicyID        string                             `json:"policy_id,omitempty"` // For deleting DLM policy

	// Replica Onboarding & Shard Migration Fields
	OperationID      string                           `json:"operation_id,omitempty"` // Unique ID for onboarding or migration op
	OnboardingState  *ReplicaOnboardingState          `json:"onboarding_state,omitempty"`
	MigrationState   *ShardMigrationState             `json:"migration_state,omitempty"`
	SnapshotManifest *logreplication.SnapshotManifest `json:"snapshot_manifest,omitempty"` // For certain onboarding states

	// Generic payload for flexible commands
	Payload json.RawMessage `json:"payload,omitempty"`
}

// StorageNodeInfo holds information about a registered storage node.
type StorageNodeInfo struct {
	NodeID            string            `json:"node_id"`
	RaftAddr          string            `json:"raft_addr"`        // Raft address of the node
	APIAddr           string            `json:"api_addr"`         // gRPC/HTTP API address
	ReplicationAddr   string            `json:"replication_addr"` // Address for replication data exchange
	Status            string            `json:"status"`           // e.g., "healthy", "unhealthy", "decommissioning"
	LastHeartbeat     time.Time         `json:"last_heartbeat"`
	ShardsOwned       map[uint64]bool   `json:"shards_owned"`       // Slots for which this node is primary
	ShardsReplicating map[uint64]string `json:"shards_replicating"` // Slots this node replicates, value is primaryNodeID
	Capacity          NodeCapacityInfo  `json:"capacity"`           // Disk space, CPU, etc.
	RegisteredAt      time.Time         `json:"registered_at"`
	GrpcAddr          string            `json:"grpc_addr"`
}
type NodeCapacityInfo struct { /* ... */
}

type SlotRangeInfo struct {
	SlotRange      string            `json:"slot_range"`
	PrimaryNodeID  string            `json:"primary_node_id"`
	ReplicaNodeIDs []string          `json:"replica_node_ids"`
	ReplicaNodes   map[string]string `json:"replica_nodes"` // map[ReplicaNodeID]ReplicaNodeAddr
}

// SlotAssignment defines the primary and replicas for a given slot.
type SlotAssignment struct {
	SlotID        uint64            `json:"slot_id"`
	PrimaryNodeID string            `json:"primary_node_id"`
	ReplicaNodes  map[string]string `json:"replica_nodes"` // map[ReplicaNodeID]ReplicaNodeAddr
	Version       uint64            `json:"version"`       // For optimistic locking or version tracking
}

// ReplicaOnboardingState tracks the progress of a new replica joining a shard. (Ref: Blueprint Table 2)
type ReplicaOnboardingState struct {
	OperationID       string `json:"operation_id"` // Links to the command that initiated it
	SlotID            uint64
	ShardID           string    `json:"shard_id"`
	TargetNodeID      string    `json:"target_node_id"` // The new replica node
	SourceNodeID      string    `json:"source_node_id"` // Node providing the snapshot
	CurrentStage      string    `json:"current_stage"`  // e.g., "SNAPSHOT_TRANSFER_REQUESTED", "LOG_REPLAY_IN_PROGRESS"
	SnapshotLSN       wal.LSN   `json:"snapshot_lsn"`
	CurrentAppliedLSN wal.LSN   `json:"current_applied_lsn"` // Reported by target node during WAL replay
	StatusMessage     string    `json:"status_message"`
	StartedAt         time.Time `json:"started_at"`
	LastUpdatedAt     time.Time `json:"last_updated_at"`
}

// ShardMigrationState tracks the progress of a shard rebalancing (migration) operation. (Ref: Blueprint Table 3)
type ShardMigrationState struct {
	OperationID         string `json:"operation_id"`
	ShardID             string `json:"shard_id"`
	SlotID              uint64
	SourceNodeID        string    `json:"source_node_id"`
	TargetNodeID        string    `json:"target_node_id"`
	CurrentPhase        string    `json:"current_phase"` // e.g., "PREPARING", "DATA_COPYING", "FINAL_SYNC"
	StatusMessage       string    `json:"status_message"`
	SnapshotLSN         wal.LSN   `json:"snapshot_lsn_on_target"`  // LSN of snapshot applied on target
	TargetCaughtUpToLSN wal.LSN   `json:"target_caught_up_to_lsn"` // LSN target has replayed up to
	WritePauseInitiated bool      `json:"write_pause_initiated"`   // If source has paused writes for cutover
	StartedAt           time.Time `json:"started_at"`
	LastUpdatedAt       time.Time `json:"last_updated_at"`
}

// FSM (Finite State Machine) stores the cluster's metadata.
type FSM struct {
	mu sync.RWMutex // Protects all state within the FSM

	// Cluster State
	storageNodes    map[string]*StorageNodeInfo                   // Key: NodeID
	slotAssignments map[uint64]*SlotAssignment                    // Key: SlotID (or ShardID if 1:1)
	slotRangeInfo   map[string]*SlotRangeInfo                     // Key: SlotRange(0-511, 512-1024)
	dlmPolicies     map[string]*tiered_storage.DLMPolicy          // Key: PolicyID
	tieringMetadata map[string]*tiered_storage.TieredDataMetadata // Key: LogicalUnitID (e.g., PageID, WALSegmentName)

	// Ongoing Operations Tracking
	activeReplicaOnboardings map[string]*ReplicaOnboardingState // Key: OperationID
	activeShardMigrations    map[string]*ShardMigrationState    // Key: OperationID

	// Local (non-Raft replicated) state/dependencies for this FSM instance
	logger               *zap.Logger
	localNodeID          string                                                            // ID of the node this FSM instance is running on
	localNodeReplAddr    string                                                            // Replication address of the local node
	indexReplicationMgrs map[indexing.IndexType]logreplication.ReplicationManagerInterface // For triggering replication changes on this node

	// DB instances (if FSM needs direct access for some reason, typically not for pure state)
	// dbInstance            *btree.BTree[string, string]
	// logManager            *wal.LogManager
	// invertedIndexInstance *inverted_index.InvertedIndex
	// spatialIdx            *spatial.IndexManager
	shutdownCh chan struct{} // Added shutdown channel
}

func NewFSM(
	// db *btree.BTree[string, string], lm *wal.LogManager, ii *inverted_index.InvertedIndex, si *spatial.IndexManager, // Pass if needed
	localNodeID string,
	localNodeReplAddr string,
	replMgrs map[indexing.IndexType]logreplication.ReplicationManagerInterface,
	logger *zap.Logger,
) *FSM {
	return &FSM{
		storageNodes:             make(map[string]*StorageNodeInfo),
		slotAssignments:          make(map[uint64]*SlotAssignment),
		dlmPolicies:              make(map[string]*tiered_storage.DLMPolicy),
		tieringMetadata:          make(map[string]*tiered_storage.TieredDataMetadata),
		activeReplicaOnboardings: make(map[string]*ReplicaOnboardingState),
		activeShardMigrations:    make(map[string]*ShardMigrationState),
		logger:                   logger.Named("fsm"),
		localNodeID:              localNodeID,
		localNodeReplAddr:        localNodeReplAddr,
		indexReplicationMgrs:     replMgrs,
		// dbInstance:            db,
		// logManager:            lm,
		// invertedIndexInstance: ii,
		// spatialIdx:            si,
		shutdownCh: make(chan struct{}),
	}
}

// Apply applies a command to the FSM. This is the core of Raft consensus.
// It MUST be deterministic.
func (f *FSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error("Failed to unmarshal FSM command", zap.Error(err), zap.ByteString("data", log.Data))
		// This is a critical error, as it means a Raft log entry cannot be processed.
		// Depending on policy, might panic or return an error that Raft handles.
		// Returning an error here might cause Raft to retry or halt.
		return fmt.Errorf("unmarshal command failed: %w", err)
	}

	//f.logger.Debug("Applying FSM command", zap.Uint8("type", uint8(cmd.Type)), zap.Any("command", cmd)) // Be careful logging full command if it contains sensitive data

	switch cmd.Type {
	case CommandRegisterNode:
		return f.applyRegisterNode(cmd)
	case CommandDeregisterNode:
		return f.applyDeregisterNode(cmd)
	case CommandUpdateNodeStatus:
		return f.applyUpdateNodeStatus(cmd)
	case CommandAssignSlot: // Make a node primary for a slot, also informs replicas
		return f.applyAssignSlot(cmd)
	case CommandAddReplicaToSlot: // Typically part of AssignSlot or an explicit update
		return f.applyAddReplicaToSlot(cmd)
	case CommandRemoveReplicaFromSlot:
		return f.applyRemoveReplicaFromSlot(cmd)
	case CommandSetSlotPrimary: // For failover / planned primary change
		return f.applySetSlotPrimary(cmd)

	// Tiered Storage
	case CommandUpdateTieringMetadata:
		if cmd.TieringMetadata == nil {
			return fmt.Errorf("UpdateTieringMetadata command missing TieringMetadata")
		}
		f.tieringMetadata[cmd.TieringMetadata.LogicalUnitID] = cmd.TieringMetadata
		f.logger.Info("Applied UpdateTieringMetadata", zap.String("unitID", cmd.TieringMetadata.LogicalUnitID))
		return nil
	case CommandDeleteTieringMetadata:
		if cmd.TieringMetadata == nil || cmd.TieringMetadata.LogicalUnitID == "" {
			return fmt.Errorf("DeleteTieringMetadata command missing LogicalUnitID")
		}
		delete(f.tieringMetadata, cmd.TieringMetadata.LogicalUnitID)
		f.logger.Info("Applied DeleteTieringMetadata", zap.String("unitID", cmd.TieringMetadata.LogicalUnitID))
		return nil
	case CommandUpdateDLMPolicy:
		if cmd.DLMPolicy == nil {
			return fmt.Errorf("UpdateDLMPolicy command missing DLMPolicy")
		}
		f.dlmPolicies[cmd.DLMPolicy.PolicyID] = cmd.DLMPolicy
		f.logger.Info("Applied UpdateDLMPolicy", zap.String("policyID", cmd.DLMPolicy.PolicyID))
		return nil
	case CommandDeleteDLMPolicy:
		if cmd.PolicyID == "" {
			return fmt.Errorf("DeleteDLMPolicy command missing PolicyID")
		}
		delete(f.dlmPolicies, cmd.PolicyID)
		f.logger.Info("Applied DeleteDLMPolicy", zap.String("policyID", cmd.PolicyID))
		return nil

	// Replica Onboarding
	case CommandInitiateReplicaOnboarding:
		if cmd.OnboardingState == nil || cmd.OnboardingState.OperationID == "" {
			return fmt.Errorf("InitiateReplicaOnboarding missing OnboardingState or OperationID")
		}
		cmd.OnboardingState.LastUpdatedAt = time.Now()
		f.activeReplicaOnboardings[cmd.OnboardingState.OperationID] = cmd.OnboardingState
		f.logger.Info("Applied InitiateReplicaOnboarding", zap.String("opID", cmd.OnboardingState.OperationID), zap.String("shard", cmd.OnboardingState.ShardID))
		// Controller logic (outside FSM Apply) would then RPC the source node to start snapshot.
		return nil
	case CommandUpdateReplicaOnboardingState:
		if cmd.OnboardingState == nil || cmd.OnboardingState.OperationID == "" {
			return fmt.Errorf("UpdateReplicaOnboardingState missing OnboardingState or OperationID")
		}
		existing, ok := f.activeReplicaOnboardings[cmd.OnboardingState.OperationID]
		if !ok {
			return fmt.Errorf("cannot update non-existent replica onboarding operation %s", cmd.OnboardingState.OperationID)
		}
		// Merge updates carefully
		existing.CurrentStage = cmd.OnboardingState.CurrentStage
		existing.StatusMessage = cmd.OnboardingState.StatusMessage
		if cmd.OnboardingState.SnapshotLSN > 0 {
			existing.SnapshotLSN = cmd.OnboardingState.SnapshotLSN
		}
		if cmd.OnboardingState.CurrentAppliedLSN > 0 {
			existing.CurrentAppliedLSN = cmd.OnboardingState.CurrentAppliedLSN
		}
		existing.LastUpdatedAt = time.Now()

		f.logger.Info("Applied UpdateReplicaOnboardingState", zap.String("opID", existing.OperationID), zap.String("stage", existing.CurrentStage))
		// If stage is REPLICA_SYNCED or REPLICA_ACTIVE, controller logic might clean up this entry.
		if existing.CurrentStage == "REPLICA_ACTIVE" || strings.HasPrefix(existing.CurrentStage, "REPLICA_FAILED") {
			// delete(f.activeReplicaOnboardings, existing.OperationID) // Or mark as completed/failed
			f.logger.Info("Replica onboarding operation concluded", zap.String("opID", existing.OperationID), zap.String("final_stage", existing.CurrentStage))
		}
		return nil

	// Shard Migration
	case CommandInitiateShardMigration:
		if cmd.MigrationState == nil || cmd.MigrationState.OperationID == "" {
			return fmt.Errorf("InitiateShardMigration missing MigrationState or OperationID")
		}
		cmd.MigrationState.LastUpdatedAt = time.Now()
		f.activeShardMigrations[cmd.MigrationState.OperationID] = cmd.MigrationState
		f.logger.Info("Applied InitiateShardMigration", zap.String("opID", cmd.MigrationState.OperationID), zap.String("shard", cmd.MigrationState.ShardID))
		// Controller logic (outside FSM Apply) would then RPC source/target to begin.
		for index, replMgr := range f.indexReplicationMgrs {
			_, err := replMgr.PrepareSnapshot(cmd.ShardID)
			if err != nil {
				f.logger.Error("Error while creating snapshot", zap.String("opID", cmd.MigrationState.OperationID), zap.String("shard", cmd.MigrationState.ShardID), zap.String("indexType", string(index)))
			}
			// replMgr.GetSnapshotDataStream(cmd.ShardID, manifest.SnapshotID, manifest.)
		}
		return nil
	case CommandUpdateShardMigrationState:
		if cmd.MigrationState == nil || cmd.MigrationState.OperationID == "" {
			return fmt.Errorf("UpdateShardMigrationState missing MigrationState or OperationID")
		}
		existing, ok := f.activeShardMigrations[cmd.MigrationState.OperationID]
		if !ok {
			return fmt.Errorf("cannot update non-existent shard migration operation %s", cmd.MigrationState.OperationID)
		}
		// Merge updates
		existing.CurrentPhase = cmd.MigrationState.CurrentPhase
		existing.StatusMessage = cmd.MigrationState.StatusMessage
		if cmd.MigrationState.SnapshotLSN > 0 {
			existing.SnapshotLSN = cmd.MigrationState.SnapshotLSN
		}
		if cmd.MigrationState.TargetCaughtUpToLSN > 0 {
			existing.TargetCaughtUpToLSN = cmd.MigrationState.TargetCaughtUpToLSN
		}
		if cmd.MigrationState.WritePauseInitiated {
			existing.WritePauseInitiated = true
		}
		existing.LastUpdatedAt = time.Now()

		f.logger.Info("Applied UpdateShardMigrationState", zap.String("opID", existing.OperationID), zap.String("phase", existing.CurrentPhase))
		return nil
	case CommandCommitShardMigration: // This is the atomic handoff
		// Payload for CommitShardMigration should contain ShardID, NewPrimaryNodeID (TargetNodeID)
		// And optionally, the OperationID to finalize the migration tracking state.
		if cmd.ShardID == "" || cmd.TargetNodeID == "" { // TargetNodeID is the new primary
			return fmt.Errorf("CommitShardMigration missing ShardID or TargetNodeID (new primary)")
		}
		slotID, err := f.getSlotIDFromShardID(cmd.ShardID) // Helper to map shard to slot if necessary
		if err != nil {
			return fmt.Errorf("could not map shardID %s to slotID for commit: %w", cmd.ShardID, err)
		}

		assignment, ok := f.slotAssignments[slotID]
		if !ok {
			return fmt.Errorf("slot %d (for shard %s) not found for committing migration", slotID, cmd.ShardID)
		}
		oldPrimary := assignment.PrimaryNodeID
		assignment.PrimaryNodeID = cmd.TargetNodeID // New primary
		// Replicas might also need updating if the target was a new node not previously a replica.
		// For a simple move, replicas might remain the same or target becomes primary and old primary might become replica or be removed.
		// This command should ideally specify the full new replica set for the slot.
		// For now, just update primary.
		if cmd.Replicas != nil { // If new replica set is provided
			assignment.ReplicaNodes = cmd.Replicas
		} else {
			// If target was a replica, remove it from replica list
			delete(assignment.ReplicaNodes, cmd.TargetNodeID)
			// What happens to the old primary? Does it become a replica? Is it decommissioned from this slot?
			// This needs to be part of the command's payload or a defined rebalancing strategy.
			// For now, assume it's just removed or becomes a replica if specified in cmd.Replicas
		}

		assignment.Version++
		f.logger.Info("Applied CommitShardMigration: atomically changed primary",
			zap.String("shardID", cmd.ShardID),
			zap.Uint64("slotID", slotID),
			zap.String("oldPrimary", oldPrimary),
			zap.String("newPrimary", cmd.TargetNodeID))

		// Update and potentially remove the active migration tracking state
		if cmd.OperationID != "" {
			if migState, exists := f.activeShardMigrations[cmd.OperationID]; exists {
				migState.CurrentPhase = "COMPLETED_HANDOFF"
				migState.LastUpdatedAt = time.Now()
				// delete(f.activeShardMigrations, cmd.OperationID) // Or keep for history with "COMPLETED" status
			}
		}
		// Trigger actions on the local node if its role for this slot changed.
		f.handleSlotAssignmentChange(slotID, assignment, oldPrimary)
		return nil

	case CommandNoOp:
		f.logger.Debug("Applied NoOp command")
		return nil
	default:
		f.logger.Error("Unknown FSM command type", zap.Uint8("type", uint8(cmd.Type)))
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

func (f *FSM) GetNodeStatus(nodeID string) (string, bool) {
	node, found := f.storageNodes[nodeID]
	if found {
		return node.Status, found
	}
	return "", false
}

func (f *FSM) GetNodeStatuses() map[string]string {
	nodeStatuses := make(map[string]string)
	for node, nodeInfo := range f.storageNodes {
		nodeStatuses[node] = nodeInfo.Status
	}
	return nodeStatuses
}

func (f *FSM) GetNodeAddresses() map[string]string {
	nodeAddresses := make(map[string]string)
	for node, nodeInfo := range f.storageNodes {
		nodeAddresses[node] = nodeInfo.RaftAddr
	}
	return nodeAddresses
}

func (f *FSM) GetNodeGrpcAddresses() map[string]string {
	nodeAddresses := make(map[string]string)
	for node, nodeInfo := range f.storageNodes {
		nodeAddresses[node] = nodeInfo.GrpcAddr
	}
	return nodeAddresses
}

func (f *FSM) GetSlotAssignments() map[uint64]SlotAssignment {
	slotAssignments := make(map[uint64]SlotAssignment)
	for slot, slotInfo := range f.slotAssignments {
		slotAssignments[slot] = *slotInfo
	}
	return slotAssignments
}

func (f *FSM) GetReplicaOnboardingStates() map[string]ReplicaOnboardingState {
	replicaOnboardingState := make(map[string]ReplicaOnboardingState)
	for s, info := range f.activeReplicaOnboardings {
		replicaOnboardingState[s] = *info
	}
	return replicaOnboardingState
}

func (f *FSM) GetShardMigrationStates() map[string]ShardMigrationState {
	shardMigrationState := make(map[string]ShardMigrationState)
	for s, info := range f.activeShardMigrations {
		shardMigrationState[s] = *info
	}
	return shardMigrationState
}

func (f *FSM) applyRegisterNode(cmd Command) error {
	if cmd.NodeID == "" || cmd.NodeAddr == "" { // NodeAddr here is Raft Addr
		return fmt.Errorf("RegisterNode command missing NodeID or RaftAddr")
	}
	if _, exists := f.storageNodes[cmd.NodeID]; exists {
		//f.logger.Warn("Node already registered, updating info", zap.String("nodeID", cmd.NodeID))
		// Update existing node info (e.g. if address changed or for re-registration)
		f.storageNodes[cmd.NodeID].RaftAddr = cmd.NodeAddr
		f.storageNodes[cmd.NodeID].APIAddr = cmd.PayloadGet("api_addr")                 // Assuming API addr is in payload
		f.storageNodes[cmd.NodeID].ReplicationAddr = cmd.PayloadGet("replication_addr") // Assuming Replication addr is in payload
		f.storageNodes[cmd.NodeID].GrpcAddr = cmd.PayloadGet("grpc_addr")               // Assuming Replication addr is in payload
		f.storageNodes[cmd.NodeID].LastHeartbeat = time.Now()
		f.storageNodes[cmd.NodeID].Status = "healthy" // Or from payload
		return nil
	}
	f.storageNodes[cmd.NodeID] = &StorageNodeInfo{
		NodeID:            cmd.NodeID,
		RaftAddr:          cmd.NodeAddr,
		APIAddr:           cmd.PayloadGet("api_addr"),
		ReplicationAddr:   cmd.PayloadGet("replication_addr"),
		Status:            "healthy",
		LastHeartbeat:     time.Now(),
		RegisteredAt:      time.Now(),
		ShardsOwned:       make(map[uint64]bool),
		ShardsReplicating: make(map[uint64]string),
	}
	f.logger.Info("Registered new storage node", zap.String("nodeID", cmd.NodeID), zap.String("raftAddr", cmd.NodeAddr))
	return nil
}

func (f *FSM) applyDeregisterNode(cmd Command) error {
	if cmd.NodeID == "" { // NodeAddr here is Raft Addr
		return fmt.Errorf("RegisterNode command missing NodeID or RaftAddr")
	}
	if _, exists := f.storageNodes[cmd.NodeID]; exists {
		f.logger.Warn("Deregistering the node from cluster", zap.String("nodeID", cmd.NodeID))
		// Update existing node info (e.g. if address changed or for re-registration)
		delete(f.storageNodes, cmd.NodeID)
		return nil
	}

	f.logger.Info("Deregistered storage node", zap.String("nodeID", cmd.NodeID), zap.String("raftAddr", cmd.NodeAddr))
	return nil
}

func (f *FSM) applyUpdateNodeStatus(cmd Command) error {
	if cmd.NodeID == "" {
		return fmt.Errorf("UpdateNodeStatus command missing NodeID")
	}
	node, ok := f.storageNodes[cmd.NodeID]
	if !ok {
		return fmt.Errorf("cannot update status for unknown node %s", cmd.NodeID)
	}
	node.LastHeartbeat = time.Now()
	newStatus := cmd.PayloadGet("status")
	if newStatus != "" {
		node.Status = newStatus
	}
	// Update capacity, etc. from payload if provided
	//f.logger.Debug("Updated node status", zap.String("nodeID", cmd.NodeID), zap.String("status", node.Status))
	return nil
}

func (f *FSM) applyAssignSlot(cmd Command) error {

	if cmd.SlotID > 1023 || cmd.PrimaryNodeID == "" { // NodeID is the new primary
		return fmt.Errorf("AssignSlot command missing SlotID or NodeID (primary)")
	}
	if _, nodeExists := f.storageNodes[cmd.PrimaryNodeID]; !nodeExists {
		return fmt.Errorf("cannot assign slot to unknown node %s", cmd.PrimaryNodeID)
	}

	var oldPrimaryNodeID string
	assignment, exists := f.slotAssignments[cmd.SlotID]
	if exists {
		oldPrimaryNodeID = assignment.PrimaryNodeID
		if oldPrimaryNodeID == cmd.PrimaryNodeID && mapContainKeys(assignment.ReplicaNodes, cmd.ReplicaNodeIDs) {
			f.logger.Info("Slot assignment unchanged", zap.Uint64("slotID", cmd.SlotID))
			assignment.Version++ // Still bump version for idempotency if needed, or just return
			return nil
		}
		// If primary changes, update old primary's owned shards
		if oldPrimaryNodeID != "" && oldPrimaryNodeID != cmd.PrimaryNodeID {
			if oldNode, ok := f.storageNodes[oldPrimaryNodeID]; ok {
				delete(oldNode.ShardsOwned, cmd.SlotID)
			}
		}
	} else {
		assignment = &SlotAssignment{SlotID: cmd.SlotID}
		f.slotAssignments[cmd.SlotID] = assignment
	}
	replicas := make(map[string]string)
	for _, r := range cmd.ReplicaNodeIDs {
		info, found := f.storageNodes[r]
		if !found {
			return fmt.Errorf("cannot assign slot to unknown replica node %s", r)
		}
		replicas[r] = info.RaftAddr
	}
	assignment.PrimaryNodeID = cmd.PrimaryNodeID
	assignment.ReplicaNodes = replicas // cmd.Replicas should be map[ReplicaNodeID]ReplicaNodeAddr
	if assignment.ReplicaNodes == nil {
		assignment.ReplicaNodes = make(map[string]string)
	}
	assignment.Version++

	// Update new primary's owned shards
	f.storageNodes[cmd.PrimaryNodeID].ShardsOwned[cmd.SlotID] = true
	// Ensure new primary is not also listed as its own replica for this slot
	delete(f.storageNodes[cmd.PrimaryNodeID].ShardsReplicating, cmd.SlotID)

	f.logger.Info("Assigned slot to node (primary)",
		zap.Uint64("slotID", cmd.SlotID),
		zap.String("primaryNodeID", cmd.PrimaryNodeID),
		zap.Any("replicas", cmd.Replicas),
		zap.String("oldPrimary", oldPrimaryNodeID))

	// This function will notify local replication managers if this FSM instance's node
	// had its role changed for this slot.

	f.handleSlotAssignmentChange(cmd.SlotID, assignment, oldPrimaryNodeID)
	return nil
}

func (f *FSM) applySetSlotPrimary(cmd Command) error {
	if cmd.SlotID == 0 || cmd.PrimaryNodeID == "" {
		return fmt.Errorf("SetSlotPrimary command missing SlotID or PrimaryNodeID")
	}
	assignment, ok := f.slotAssignments[cmd.SlotID]
	if !ok {
		return fmt.Errorf("slot %d not found for setting new primary", cmd.SlotID)
	}
	if _, nodeExists := f.storageNodes[cmd.PrimaryNodeID]; !nodeExists {
		return fmt.Errorf("cannot set primary for slot %d to unknown node %s", cmd.SlotID, cmd.PrimaryNodeID)
	}

	oldPrimaryNodeID := assignment.PrimaryNodeID
	if oldPrimaryNodeID == cmd.PrimaryNodeID {
		f.logger.Info("Primary for slot already set to this node", zap.Uint64("slotID", cmd.SlotID), zap.String("nodeID", cmd.PrimaryNodeID))
		return nil // No change
	}

	assignment.PrimaryNodeID = cmd.PrimaryNodeID
	assignment.Version++

	// Update shard ownership maps
	if oldNode, exists := f.storageNodes[oldPrimaryNodeID]; exists {
		delete(oldNode.ShardsOwned, cmd.SlotID)
	}
	if newNode, exists := f.storageNodes[cmd.PrimaryNodeID]; exists {
		newNode.ShardsOwned[cmd.SlotID] = true
		// If the new primary was previously a replica for this slot, remove it from replicating list
		delete(newNode.ShardsReplicating, cmd.SlotID)
	}

	// Ensure new primary is not in its own replica list for this slot
	if assignment.ReplicaNodes != nil {
		delete(assignment.ReplicaNodes, cmd.PrimaryNodeID)
	}

	f.logger.Info("Set new primary for slot",
		zap.Uint64("slotID", cmd.SlotID),
		zap.String("oldPrimary", oldPrimaryNodeID),
		zap.String("newPrimary", cmd.PrimaryNodeID))

	f.handleSlotAssignmentChange(cmd.SlotID, assignment, oldPrimaryNodeID)
	return nil
}

func (f *FSM) applyAddReplicaToSlot(cmd Command) error {
	// Similar to AssignSlot but focuses on just adding/updating replicas for an existing primary.
	// Or, this logic can be part of ApplyAssignSlot if it always provides the full replica set.
	// This is crucial for shard rebalancing (e.g. new node added to cluster, starts replicating).
	if cmd.SlotID == 0 || cmd.TargetNodeID == "" { // TargetNodeID is the replica to add
		return fmt.Errorf("AddReplicaToSlot command missing SlotID or TargetNodeID (replica)")
	}
	assignment, ok := f.slotAssignments[cmd.SlotID]
	if !ok {
		return fmt.Errorf("cannot add replica to non-existent slot %d", cmd.SlotID)
	}
	if assignment.PrimaryNodeID == "" {
		return fmt.Errorf("cannot add replica to slot %d with no primary assigned", cmd.SlotID)
	}
	if assignment.PrimaryNodeID == cmd.TargetNodeID {
		return fmt.Errorf("cannot add primary node %s as its own replica for slot %d", cmd.TargetNodeID, cmd.SlotID)
	}

	replicaAddr := cmd.PayloadGet("replica_addr") // Address of the replica node for replication
	if replicaAddr == "" {
		// Try to get from registered nodes if not provided
		if nodeInfo, exists := f.storageNodes[cmd.TargetNodeID]; exists {
			replicaAddr = nodeInfo.ReplicationAddr
		} else {
			return fmt.Errorf("replica address not provided and target node %s not registered", cmd.TargetNodeID)
		}
	}

	if assignment.ReplicaNodes == nil {
		assignment.ReplicaNodes = make(map[string]string)
	}
	assignment.ReplicaNodes[cmd.TargetNodeID] = replicaAddr
	assignment.Version++

	if replicaNode, ok := f.storageNodes[cmd.TargetNodeID]; ok {
		replicaNode.ShardsReplicating[cmd.SlotID] = assignment.PrimaryNodeID
		delete(replicaNode.ShardsOwned, cmd.SlotID) // Ensure it's not marked as owner
	}

	f.logger.Info("Added/Updated replica for slot",
		zap.Uint64("slotID", cmd.SlotID),
		zap.String("primary", assignment.PrimaryNodeID),
		zap.String("replicaNodeID", cmd.TargetNodeID),
		zap.String("replicaAddr", replicaAddr))

	// Inform the primary node about the new/updated replica.
	// Inform the replica node that it should start replicating.
	// This happens in handleSlotAssignmentChange if PrimaryNodeID is f.localNodeID OR TargetNodeID is f.localNodeID
	f.handleSlotAssignmentChange(cmd.SlotID, assignment, assignment.PrimaryNodeID) // Pass current primary as "old" because primary itself didn't change
	return nil
}

func (f *FSM) applyRemoveReplicaFromSlot(cmd Command) error {
	if cmd.SlotID == 0 || cmd.TargetNodeID == "" { // TargetNodeID is the replica to remove
		return fmt.Errorf("RemoveReplicaFromSlot command missing SlotID or TargetNodeID (replica)")
	}
	assignment, ok := f.slotAssignments[cmd.SlotID]
	if !ok {
		f.logger.Warn("Attempted to remove replica from non-existent slot", zap.Uint64("slotID", cmd.SlotID))
		return nil // Or error, depending on strictness
	}
	if assignment.ReplicaNodes == nil {
		f.logger.Warn("Attempted to remove replica from slot with no replicas", zap.Uint64("slotID", cmd.SlotID))
		return nil
	}

	removedAddr, replicaExisted := assignment.ReplicaNodes[cmd.TargetNodeID]
	if !replicaExisted {
		f.logger.Warn("Attempted to remove non-existent replica from slot", zap.Uint64("slotID", cmd.SlotID), zap.String("replicaNodeID", cmd.TargetNodeID))
		return nil
	}

	delete(assignment.ReplicaNodes, cmd.TargetNodeID)
	assignment.Version++

	if replicaNode, ok := f.storageNodes[cmd.TargetNodeID]; ok {
		delete(replicaNode.ShardsReplicating, cmd.SlotID)
	}

	f.logger.Info("Removed replica from slot",
		zap.Uint64("slotID", cmd.SlotID),
		zap.String("primary", assignment.PrimaryNodeID),
		zap.String("removedReplicaNodeID", cmd.TargetNodeID),
		zap.String("removedReplicaAddr", removedAddr))

	f.handleSlotAssignmentChange(cmd.SlotID, assignment, assignment.PrimaryNodeID)
	return nil
}

// getSlotIDFromShardID is a helper. In many designs, SlotID and ShardID might be synonymous
// or have a fixed mapping. If ShardID is more granular (e.g. ranges within a slot), this needs defining.
func (f *FSM) getSlotIDFromShardID(shardID string) (uint64, error) {
	// For now, assume shardID is parseable to uint64 as SlotID.
	// This needs to match your system's sharding strategy.
	sID, err := strconv.ParseUint(shardID, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid shardID format '%s', cannot parse to uint64 slotID: %w", shardID, err)
	}
	return sID, nil
}

// handleSlotAssignmentChange is called after an FSM Apply modifies slotAssignments.
// It checks if the local node's role for the given slot has changed and, if so,
// instructs the local ReplicationManagers.
func (f *FSM) handleSlotAssignmentChange(slotID uint64, newAssignment *SlotAssignment, oldPrimaryNodeID string) {
	f.logger.Info("Handling slot assignment")
	if newAssignment == nil {
		f.logger.Warn("handleSlotAssignmentChange called with nil newAssignment", zap.Uint64("slotID", slotID))
		return
	}
	// --- Check if this node's role as PRIMARY changed ---
	isNowPrimary := newAssignment.PrimaryNodeID == f.localNodeID
	wasPrimary := oldPrimaryNodeID == f.localNodeID
	if isNowPrimary && !wasPrimary {
		// This node just became primary for slotID
		f.logger.Info("FSM: This node BECAME PRIMARY for slot", zap.Uint64("slotID", slotID), zap.Any("replicas", newAssignment.ReplicaNodes))
		for _, repMgr := range f.indexReplicationMgrs {
			// Pass replica addresses (NodeID -> Addr from f.storageNodes)
			replicaAddrMap := make(map[string]string)
			for repNodeID := range newAssignment.ReplicaNodes { // newAssignment.ReplicaNodes should store addresses directly
				if nodeInfo, ok := f.storageNodes[repNodeID]; ok {
					replicaAddrMap[repNodeID] = nodeInfo.ReplicationAddr // Ensure StorageNodeInfo has ReplicationAddr
				} else {
					f.logger.Warn("Replica node info not found in FSM for primary assignment", zap.String("replicaNodeID", repNodeID), zap.Uint64("slotID", slotID))
				}
			}
			if err := repMgr.BecomePrimaryForSlot(slotID, replicaAddrMap); err != nil {
				f.logger.Error("Error calling BecomePrimaryForSlot on local manager", zap.Error(err), zap.String("indexType", string(repMgr.GetIndexType())))
			}
		}
	} else if !isNowPrimary && wasPrimary {

		// This node CEASED to be primary for slotID
		f.logger.Info("This node CEASED TO BE PRIMARY for slot", zap.Uint64("slotID", slotID))
		for _, repMgr := range f.indexReplicationMgrs {
			if err := repMgr.CeasePrimaryForSlot(slotID); err != nil {
				f.logger.Error("Error calling CeasePrimaryForSlot on local manager", zap.Error(err), zap.String("indexType", string(repMgr.GetIndexType())))
			}
		}
	} else if isNowPrimary && wasPrimary {

		// Still primary, but replica set might have changed.
		f.logger.Info("FSM: This node REMAINS PRIMARY for slot, replica set may have changed", zap.Uint64("slotID", slotID), zap.Any("newReplicas", newAssignment.ReplicaNodes))
		// The BecomePrimaryForSlot on replication managers should be idempotent or handle updates to the replica set.
		// It might need to start streaming to new replicas and stop streaming to removed ones.
		// For simplicity, re-calling BecomePrimaryForSlot might refresh its state.
		for _, repMgr := range f.indexReplicationMgrs {
			replicaAddrMap := make(map[string]string)
			for repNodeID := range newAssignment.ReplicaNodes {
				if nodeInfo, ok := f.storageNodes[repNodeID]; ok {
					replicaAddrMap[repNodeID] = nodeInfo.ReplicationAddr
				}
			}

			if err := repMgr.BecomePrimaryForSlot(slotID, replicaAddrMap); err != nil { // This should reconcile replicas
				f.logger.Error("Error calling BecomePrimaryForSlot (for update) on local manager", zap.Error(err), zap.String("indexType", string(repMgr.GetIndexType())))
			}
		}
	}

	// --- Check if this node's role as REPLICA changed ---
	_, isNowReplica := newAssignment.ReplicaNodes[f.localNodeID]

	// Determine if this node WAS a replica for this slot under the OLD primary.
	// This requires knowing the slot's state BEFORE this current change.
	// For simplicity, assume `oldAssignment` could be passed or reconstructed if complex state transitions needed.
	// Here, we primarily care if we are *now* a replica and from whom.
	wasReplica := false
	if oldPrimaryNodeID != "" { // If there was an old primary for this slot
		// Check if we were a replica to that old primary OR to the current primary if it hasn't changed.
		// This logic gets complex if oldAssignment isn't available.
		// A simpler check: if we were in f.storageNodes[f.localNodeID].ShardsReplicating for this slot.
		_, wasReplica = f.storageNodes[f.localNodeID].ShardsReplicating[slotID]
	}

	if isNowReplica {
		primaryForThisSlot := newAssignment.PrimaryNodeID
		if primaryInfo, ok := f.storageNodes[primaryForThisSlot]; ok {
			f.logger.Info("FSM: This node IS/REMAINS A REPLICA for slot", zap.Uint64("slotID", slotID), zap.String("primary", primaryForThisSlot))
			for _, repMgr := range f.indexReplicationMgrs {
				f.logger.Info("Called Become primary 813")
				if err := repMgr.BecomeReplicaForSlot(slotID, primaryForThisSlot, primaryInfo.ReplicationAddr); err != nil {
					f.logger.Error("Error calling BecomeReplicaForSlot on local manager", zap.Error(err), zap.String("indexType", string(repMgr.GetIndexType())))
				}
			}
		} else {
			f.logger.Error("Primary node info not found for replica assignment", zap.String("primaryNodeID", primaryForThisSlot), zap.Uint64("slotID", slotID))
		}
	} else if !isNowReplica && wasReplica {
		// This node CEASED to be a replica for slotID
		f.logger.Info("FSM: This node CEASED TO BE REPLICA for slot", zap.Uint64("slotID", slotID))
		for _, repMgr := range f.indexReplicationMgrs {
			if err := repMgr.CeaseReplicaForSlot(slotID); err != nil {
				f.logger.Error("Error calling CeaseReplicaForSlot on local manager", zap.Error(err), zap.String("indexType", string(repMgr.GetIndexType())))
			}
		}
	}
}

// Snapshot creates a snapshot of the FSM state.
// Raft calls this to compact its log.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	f.logger.Info("Creating FSM snapshot...")
	// Serialize the entire FSM state. Using JSON for simplicity here.
	// For performance with large state, a more efficient binary format might be needed.
	state := map[string]interface{}{
		"storageNodes":             f.storageNodes,
		"slotAssignments":          f.slotAssignments,
		"dlmPolicies":              f.dlmPolicies,
		"tieringMetadata":          f.tieringMetadata,
		"activeReplicaOnboardings": f.activeReplicaOnboardings,
		"activeShardMigrations":    f.activeShardMigrations,
	}
	data, err := json.Marshal(state)
	if err != nil {
		f.logger.Error("Failed to marshal FSM state for snapshot", zap.Error(err))
		return nil, fmt.Errorf("marshal FSM state: %w", err)
	}
	f.logger.Info("FSM snapshot created successfully", zap.Int("size_bytes", len(data)))
	return &fsmSnapshot{data: data, logger: f.logger.Named("snapshot")}, nil
}

// Restore restores the FSM state from a snapshot.
// Raft calls this when a node is starting up and needs to catch up.
func (f *FSM) Restore(snapshotReader io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer snapshotReader.Close()

	f.logger.Info("Restoring FSM from snapshot...")
	data, err := io.ReadAll(snapshotReader)
	if err != nil {
		f.logger.Error("Failed to read FSM snapshot data", zap.Error(err))
		return fmt.Errorf("read snapshot: %w", err)
	}

	var state map[string]json.RawMessage // Use RawMessage for deferred parsing
	if err := json.Unmarshal(data, &state); err != nil {
		f.logger.Error("Failed to unmarshal FSM snapshot state (outer map)", zap.Error(err))
		return fmt.Errorf("unmarshal snapshot state: %w", err)
	}

	// Explicitly unmarshal each part to handle potential nil maps gracefully.
	if raw, ok := state["storageNodes"]; ok {
		if err := json.Unmarshal(raw, &f.storageNodes); err != nil {
			return fmt.Errorf("unmarshal storageNodes: %w", err)
		}
	} else {
		f.storageNodes = make(map[string]*StorageNodeInfo)
	}

	if raw, ok := state["slotAssignments"]; ok {
		if err := json.Unmarshal(raw, &f.slotAssignments); err != nil {
			return fmt.Errorf("unmarshal slotAssignments: %w", err)
		}
	} else {
		f.slotAssignments = make(map[uint64]*SlotAssignment)
	}

	if raw, ok := state["dlmPolicies"]; ok {
		if err := json.Unmarshal(raw, &f.dlmPolicies); err != nil {
			return fmt.Errorf("unmarshal dlmPolicies: %w", err)
		}
	} else {
		f.dlmPolicies = make(map[string]*tiered_storage.DLMPolicy)
	}

	if raw, ok := state["tieringMetadata"]; ok {
		if err := json.Unmarshal(raw, &f.tieringMetadata); err != nil {
			return fmt.Errorf("unmarshal tieringMetadata: %w", err)
		}
	} else {
		f.tieringMetadata = make(map[string]*tiered_storage.TieredDataMetadata)
	}

	if raw, ok := state["activeReplicaOnboardings"]; ok {
		if err := json.Unmarshal(raw, &f.activeReplicaOnboardings); err != nil {
			return fmt.Errorf("unmarshal activeReplicaOnboardings: %w", err)
		}
	} else {
		f.activeReplicaOnboardings = make(map[string]*ReplicaOnboardingState)
	}

	if raw, ok := state["activeShardMigrations"]; ok {
		if err := json.Unmarshal(raw, &f.activeShardMigrations); err != nil {
			return fmt.Errorf("unmarshal activeShardMigrations: %w", err)
		}
	} else {
		f.activeShardMigrations = make(map[string]*ShardMigrationState)
	}

	f.logger.Info("FSM restored from snapshot successfully.", zap.Int("size_bytes", len(data)))
	// After restoring, this node needs to evaluate its roles for all slots based on the new state.
	f.reEvaluateLocalNodeRolesAfterRestore()
	return nil
}

// reEvaluateLocalNodeRolesAfterRestore is called after an FSM restore.
// It iterates through all slot assignments and calls the appropriate methods on local replication managers
// to ensure this node's replication state matches the restored FSM state.
func (f *FSM) reEvaluateLocalNodeRolesAfterRestore() {
	f.logger.Info("Re-evaluating local node roles after FSM restore...")
	for slotID, assignment := range f.slotAssignments {
		// For each slot, determine if this local node is primary or replica.
		// The `oldPrimaryNodeID` is tricky here. For a fresh restore, we might assume no prior state or use a placeholder.
		// The goal is to ensure the replication managers are correctly started/stopped/configured.
		// Passing assignment.PrimaryNodeID as oldPrimary effectively triggers a "state reconciliation" call.
		f.handleSlotAssignmentChange(slotID, assignment, assignment.PrimaryNodeID)
	}
	f.logger.Info("Local node roles re-evaluation complete.")
}

// ShutdownChan returns a channel that is closed when the FSM is shutting down.
func (f *FSM) ShutdownChan() <-chan struct{} {
	return f.shutdownCh
}

// Close is called when the Raft node is shutting down.
// It signals any FSM background goroutines to stop.
func (f *FSM) Close() error {
	f.logger.Info("FSM Close called, signaling shutdown.")
	close(f.shutdownCh)
	// Perform any other cleanup if necessary
	return nil
}

// Helper for command payload
func (c Command) PayloadGet(key string) string {
	if len(c.Payload) == 0 {
		return ""
	}
	var m map[string]interface{}
	if err := json.Unmarshal(c.Payload, &m); err != nil {
		return ""
	}
	if val, ok := m[key].(string); ok {
		return val
	}
	return ""
}

// mapsAreEqual checks if two map[string]string are equal.
func mapContainKeys(m1 map[string]string, keys []string) bool {
	if len(m1) != len(keys) {
		return false
	}
	for _, v := range keys {
		if _, ok := m1[v]; !ok {
			return false
		}
	}
	return true
}

// --- FSMSnapshot ---
type fsmSnapshot struct {
	data   []byte
	logger *zap.Logger
}

func GetSlotForHashKey(key string) uint32 {
	// Using CRC32 IEEE polynomial for hashing, similar to Redis Cluster
	// The result is then modulo TotalHashSlots to get the slot number.
	checksum := crc32.ChecksumIEEE([]byte(key))
	return uint32(checksum % TotalHashSlots)
}

// Persist saves the FSM snapshot to a sink.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	s.logger.Debug("Persisting FSM snapshot", zap.Int("size", len(s.data)))
	if _, err := sink.Write(s.data); err != nil {
		s.logger.Error("Failed to write FSM snapshot to sink", zap.Error(err))
		sink.Cancel() // Signal an error to Raft
		return fmt.Errorf("sink write: %w", err)
	}
	return sink.Close() // Close successfully
}

// Release is called when Raft is done with the snapshot.
func (s *fsmSnapshot) Release() {
	s.logger.Debug("Releasing FSM snapshot resources.")
	s.data = nil // Allow GC
}

func Keys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (f *FSM) GetShardMap() map[string]*SlotRangeInfo {
	return f.slotRangeInfo
}

// Status returns a comprehensive overview of the cluster's current state.
func (f *FSM) Status() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	status := make(map[string]string)
	nodeDetails := ""
	for _, nodeInfo := range f.storageNodes {
		nodeDetails += nodeInfo.NodeID + " " + nodeInfo.APIAddr + "\n"
	}
	status["storage_nodes"] = nodeDetails
	slotDetails := ""
	for rangeID, slot := range f.slotRangeInfo {
		slotDetails += rangeID + ": " + "Primary (" + slot.PrimaryNodeID + ") Replicas: (" + strings.Join(Keys(slot.ReplicaNodes), ",") + ")"
	}
	status["slot_assignments"] = slotDetails
	b, _ := json.Marshal(status)
	return string(b)
}
