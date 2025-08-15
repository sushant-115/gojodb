package gojodbcontroller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	fsm "github.com/sushant-115/gojodb/core/replication/raft_consensus"
	"google.golang.org/grpc"
)

func (c *Controller) initiateMigration(sourceNodeID, targetNodeID string, shardIDs []string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get a consistent view of the state
	nodes, assignments := c.fsm.GetNodeAddresses(), c.fsm.GetShardMigrationStates()

	if _, ok := nodes[sourceNodeID]; !ok {
		return nil, fmt.Errorf("source node %s not found", sourceNodeID)
	}
	targetNodeInfo, ok := nodes[targetNodeID]
	if !ok {
		return nil, fmt.Errorf("target node %s not found", targetNodeID)
	}

	migrationIDs := make(map[string]string)

	for _, shardID := range shardIDs {
		if assignments[shardID].SourceNodeID != sourceNodeID {
			log.Printf("Cannot migrate shard %d: not owned by source node %s", shardID, sourceNodeID)
			continue
		}

		migrationID := uuid.New().String()
		migrationState := &fsm.ShardMigrationState{
			OperationID:  migrationID,
			ShardID:      shardID,
			SourceNodeID: sourceNodeID,
			TargetNodeID: targetNodeID,
			CurrentPhase: "PREPARING",
		}

		// cmdData, err := json.Marshal(migrationState)
		// if err != nil {
		// 	log.Printf("Failed to marshal migration state: %v", err)
		// 	continue
		// }

		cmd := &fsm.Command{Type: fsm.CommandInitiateShardMigration, MigrationState: migrationState}
		cmdBytes, err := json.Marshal(cmd)
		if err != nil {
			log.Printf("Failed to marshal raft command: %v", err)
			continue
		}

		applyFuture := c.raft.Apply(cmdBytes, 500*time.Millisecond)
		if err := applyFuture.Error(); err != nil {
			log.Printf("Failed to apply initiate migration command: %v", err)
			continue
		}

		migrationIDs[shardID] = migrationID
		log.Printf("Initiated migration %s for shard %d from %s to %s", migrationID, shardID, sourceNodeID, targetNodeID)

		// Asynchronously tell the target node to start onboarding
		go c.triggerReplicaOnboarding(targetNodeInfo, sourceNodeID, shardID)
	}
	return migrationIDs, nil
}

func (c *Controller) triggerReplicaOnboarding(targetNodeAddr, sourceNodeID string, shardID string) {
	// In a real system, you'd get the source node's gRPC address from the FSM.
	// We'll make a simplifying assumption about its address format for now.
	sourceNodeAddr := sourceNodeID + ":50051" // This is a placeholder convention.

	conn, err := grpc.Dial(targetNodeAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		log.Printf("Failed to connect to target node %s for onboarding: %v", targetNodeAddr, err)
		// Here we should probably apply a Raft command to mark the migration as FAILED.
		return
	}
	defer conn.Close()
	client := pb.NewGojoDBClient(conn)

	req := &pb.InitiateReplicaOnboardingRequest{
		ShardId:           shardID,
		SourceNodeId:      sourceNodeID,
		SourceNodeAddress: sourceNodeAddr,
	}

	_, err = client.InitiateReplicaOnboarding(context.Background(), req)
	if err != nil {
		log.Printf("Failed to initiate replica onboarding on %s for shard %d: %v", targetNodeAddr, shardID, err)
	} else {
		log.Printf("Successfully triggered onboarding on %s for shard %d", targetNodeAddr, shardID)
	}
}

func (c *Controller) handleCommitShardMigration(w http.ResponseWriter, r *http.Request) {
	if c.raft.State() != raft.Leader {
		http.Error(w, "not the leader", http.StatusServiceUnavailable)
		return
	}

	var req pb.CommitShardMigrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// It's good practice to check if the migration is in a committable state.
	// This requires reading the FSM state, which is fine for a leader.
	// For simplicity, we proceed directly to applying the command.

	cmdData, _ := json.Marshal(fsm.ShardMigrationState{OperationID: req.MigrationId, ShardID: req.ShardId})
	cmd := &fsm.Command{Type: fsm.CommandCommitShardMigration, Data: cmdData}
	cmdBytes, _ := json.Marshal(cmd)

	applyFuture := c.raft.Apply(cmdBytes, 500*time.Millisecond)
	if err := applyFuture.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Committed migration %s for shard %d", req.MigrationId, req.ShardId)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&pb.CommitShardMigrationResponse{Success: true, Message: "Migration committed"})
}

func (c *Controller) handleUpdateShardMigrationState(w http.ResponseWriter, r *http.Request) {
	// This is called by the target node. It must be forwarded to the leader.
	if c.raft.State() != raft.Leader {
		http.Error(w, "not the leader", http.StatusServiceUnavailable)
		return
	}

	var req pb.UpdateShardMigrationStateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cmdData, _ := json.Marshal(fsm.ShardMigrationState{
		MigrationID:         req.MigrationId,
		Phase:               req.Status,
		TargetCaughtUpToLSN: req.TargetCaughtUpToLsn,
		ErrorMessage:        req.ErrorMessage,
	})
	cmd := &fsm.Command{Type: fsm.CommandUpdateShardMigration, Data: cmdData}
	cmdBytes, _ := json.Marshal(cmd)

	applyFuture := c.raft.Apply(cmdBytes, 500*time.Millisecond)
	if err := applyFuture.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&pb.UpdateShardMigrationStateResponse{Success: true})
}

// --- Automated Rebalancing ---

func (c *Controller) startRebalancingLoop() {
	ticker := time.NewTicker(1 * time.Minute) // Check for rebalancing every minute
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if c.raft.State() == raft.Leader {
				c.checkForRebalancing()
			}
		case <-c.shutdownCh:
			return
		}
	}
}

func (c *Controller) checkForRebalancing() {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodes, assignments := c.fsm.GetState()
	// Create a temporary copy of active migrations to check against
	activeMigrations := make(map[string]*fsm.ShardMigrationState)
	for id, state := range c.fsm.activeShardMigrations {
		activeMigrations[id] = state
	}

	shardCounts := make(map[string]int)
	for _, nodeId := range assignments {
		shardCounts[nodeId]++
	}
	for nodeId := range nodes {
		if _, ok := shardCounts[nodeId]; !ok {
			shardCounts[nodeId] = 0
		}
	}

	// Simple rebalancing logic: find an empty node and a loaded node
	var targetNodeId string
	for nodeId, count := range shardCounts {
		isTarget := false
		for _, mig := range activeMigrations {
			if mig.TargetNodeID == nodeId {
				isTarget = true
				break
			}
		}
		if count == 0 && !isTarget {
			targetNodeId = nodeId
			break
		}
	}

	if targetNodeId == "" {
		return // No empty, non-migrating-to node found
	}

	// Find the most loaded node
	var sourceNodeId string
	maxShards := -1
	for nodeId, count := range shardCounts {
		if count > maxShards {
			maxShards = count
			sourceNodeId = nodeId
		}
	}

	// Only rebalance if the difference is significant (e.g., > 1)
	if maxShards <= shardCounts[targetNodeId]+1 {
		return
	}

	// Find a shard to move from the source node
	var shardToMove int32 = -1
	for shardId, nodeId := range assignments {
		isMigrating := false
		for _, mig := range activeMigrations {
			if mig.ShardID == shardId {
				isMigrating = true
				break
			}
		}
		if nodeId == sourceNodeId && !isMigrating {
			shardToMove = shardId
			break
		}
	}

	if shardToMove != -1 {
		log.Printf("AUTOMATED REBALANCING: Identified need to move a shard from %s to %s.", sourceNodeId, targetNodeId)
		// Drop the lock before calling initiateMigration which takes its own lock
		c.mu.Unlock()
		c.initiateMigration(sourceNodeId, targetNodeId, []int32{shardToMove})
		c.mu.Lock() // Re-acquire lock before loop ends
	}
}
