// Package coordinator orchestrates shard splits, merges and migrations by
// issuing Raft-committed commands through the FSM.
package coordinator

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	fsm "github.com/sushant-115/gojodb/core/replication/raft_consensus"
	"github.com/sushant-115/gojodb/core/sharding/metadata_store"
)

type MigrationState int

const (
	MigrationPending    MigrationState = iota
	MigrationInProgress MigrationState = iota
	MigrationDone       MigrationState = iota
)

type Migration struct {
	ID        string
	SlotRange string
	FromNode  string
	ToNode    string
	State     MigrationState
	StartedAt time.Time
}

type Coordinator struct {
	raftNode *raft.Raft
	fsmRef   *fsm.FSM
	meta     *metadata_store.Store
	timeout  time.Duration
}

func NewCoordinator(r *raft.Raft, f *fsm.FSM, meta *metadata_store.Store) *Coordinator {
	return &Coordinator{
		raftNode: r,
		fsmRef:   f,
		meta:     meta,
		timeout:  5 * time.Second,
	}
}

func (c *Coordinator) AssignSlotRange(startSlot, endSlot int, primaryNode string, replicaNodes []string) error {
	slotRange := fmt.Sprintf("%d-%d", startSlot, endSlot)
	cmd := fsm.Command{
		Type:           fsm.CommandAssignSlot,
		SlotRange:      slotRange,
		PrimaryNodeID:  primaryNode,
		ReplicaNodeIDs: replicaNodes,
	}
	if err := c.apply(cmd); err != nil {
		return fmt.Errorf("coordinator: assign slot range %s: %w", slotRange, err)
	}
	c.meta.Refresh()
	return nil
}

func (c *Coordinator) BeginMigration(migrationID, slotRange, fromNode, toNode string) error {
	cmd := fsm.Command{
		Type:         fsm.CommandInitiateShardMigration,
		ShardID:      migrationID,
		SlotRange:    slotRange,
		NodeID:       fromNode,
		TargetNodeID: toNode,
	}
	if err := c.apply(cmd); err != nil {
		return fmt.Errorf("coordinator: begin migration %s: %w", migrationID, err)
	}
	return nil
}

func (c *Coordinator) AdvanceMigration(migrationID string, newState MigrationState) error {
	cmd := fsm.Command{
		Type:    fsm.CommandUpdateShardMigrationState,
		ShardID: migrationID,
		NodeID:  fmt.Sprintf("state:%d", int(newState)),
	}
	if err := c.apply(cmd); err != nil {
		return fmt.Errorf("coordinator: advance migration %s: %w", migrationID, err)
	}
	return nil
}

func (c *Coordinator) CompleteMigration(migrationID, slotRange, newPrimary string, replicas []string) error {
	cmd := fsm.Command{
		Type:           fsm.CommandCommitShardMigration,
		ShardID:        migrationID,
		SlotRange:      slotRange,
		PrimaryNodeID:  newPrimary,
		ReplicaNodeIDs: replicas,
	}
	if err := c.apply(cmd); err != nil {
		return fmt.Errorf("coordinator: complete migration %s: %w", migrationID, err)
	}
	c.meta.Refresh()
	return nil
}

func (c *Coordinator) PrimaryForKey(key string) (string, error) {
	return c.meta.PrimaryForKey(key)
}

func (c *Coordinator) SlotRangesForNode(nodeID string) []string {
	shardMap := c.fsmRef.GetShardMap()
	var ranges []string
	for _, info := range shardMap {
		if strings.EqualFold(info.PrimaryNodeID, nodeID) {
			ranges = append(ranges, info.SlotRange)
		}
	}
	return ranges
}

func (c *Coordinator) apply(cmd fsm.Command) error {
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := c.raftNode.Apply(b, c.timeout)
	return f.Error()
}
