package logreplication

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/sushant-115/gojodb/core/indexing/btree"
	flushmanager "github.com/sushant-115/gojodb/core/write_engine/flush_manager"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	"go.uber.org/zap"
)

// BTreeReplicationManager handles replication specifically for B-tree indexes.
type BTreeReplicationManager struct {
	BaseReplicationManager
	DbInstance *btree.BTree[string, string] // The actual B-tree instance
}

// NewBTreeReplicationManager creates a new BTreeReplicationManager.
func NewBTreeReplicationManager(nodeID string, db *btree.BTree[string, string], lm *wal.LogManager, logger *zap.Logger, nodeDataDir string) *BTreeReplicationManager {
	return &BTreeReplicationManager{
		BaseReplicationManager: NewBaseReplicationManager(nodeID, BTreeIndexType, lm, logger, nodeDataDir),
		DbInstance:             db,
	}
}

// Start initiates the B-tree replication manager.
func (brm *BTreeReplicationManager) Start() error {
	brm.Logger.Info("BTreeReplicationManager starting")
	return brm.StartBase()
}

// Stop gracefully shuts down the B-tree replication manager.
func (brm *BTreeReplicationManager) Stop() error {
	brm.Logger.Info("BTreeReplicationManager stopping")
	brm.StopBase() // Signal base manager to stop its activities
	return nil
}

// ApplyLogRecord applies a B-tree specific log record.
func (brm *BTreeReplicationManager) ApplyLogRecord(lr wal.LogRecord) error {
	brm.Logger.Debug("Applying B-tree log record", zap.Uint64("lsn", uint64(lr.LSN)), zap.Any("type", lr.Type))

	if brm.DbInstance == nil {
		return fmt.Errorf("BTree instance is nil in BTreeReplicationManager")
	}
	brm.mu.Lock()
	defer brm.mu.Unlock()

	// Apply B-tree log record (simplified logic from your original code)
	switch lr.Type {
	case wal.LogRecordTypeNewPage, wal.LogRecordTypeInsertKey, wal.LogTypeRTreeInsert, wal.LogTypeRTreeNewRoot:
		if lr.PageID.GetID() >= brm.DbInstance.GetNumPages() {
			emptyPage := make([]byte, brm.DbInstance.GetPageSize())
			if writeErr := brm.DbInstance.WritePage(lr.PageID, emptyPage); writeErr != nil {
				return fmt.Errorf("failed to allocate/write empty page %d on disk: %w", lr.PageID, writeErr)
			}
			if lr.PageID >= pagemanager.PageID(brm.DbInstance.GetNumPages()) {
				brm.DbInstance.SetNumPages(uint64(lr.PageID) + 1)
			}
		}
		page, fetchErr := brm.DbInstance.FetchPage(lr.PageID)
		if fetchErr != nil {
			return fmt.Errorf("failed to fetch page %d for NEW_PAGE: %w", lr.PageID, fetchErr)
		}
		page.Lock()
		page.SetData(lr.Data)
		page.SetDirty(true)
		page.Unlock()
		if errUnpin := brm.DbInstance.UnpinPage(page.GetPageID(), true); errUnpin != nil {
			log.Printf("WARNING: Failed to unpin page %d after NEW_PAGE application: %v", page.GetPageID(), errUnpin)
		}
		if errFlush := brm.DbInstance.FlushPage(page.GetPageID()); errFlush != nil {
			return fmt.Errorf("failed to flush page %d after NEW_PAGE: %w", page.GetPageID(), errFlush)
		}
	case wal.LogRecordTypeUpdate, wal.LogTypeRTreeUpdate:
		page, fetchErr := brm.DbInstance.FetchPage(lr.PageID)
		if fetchErr != nil {
			return fmt.Errorf("failed to fetch page %d for UPDATE: %w", lr.PageID, fetchErr)
		}
		page.Lock()
		page.SetData(lr.Data)
		page.SetDirty(true)
		page.Unlock()
		if errUnpin := brm.DbInstance.UnpinPage(page.GetPageID(), true); errUnpin != nil {
			log.Printf("WARNING: Failed to unpin page %d after UPDATE application: %v", page.GetPageID(), errUnpin)
		}
		if errFlush := brm.DbInstance.FlushPage(page.GetPageID()); errFlush != nil {
			return fmt.Errorf("failed to flush page %d after UPDATE: %w", page.GetPageID(), errFlush)
		}
	case wal.LogRecordTypeRootChange, wal.LogTypeRTreeSplit:
		newRootPageID := pagemanager.PageID(binary.LittleEndian.Uint64(lr.Data))
		brm.DbInstance.SetRootPageID(newRootPageID, lr.TxnID)
		if err := brm.DbInstance.GetDiskManager().UpdateHeaderField(func(h *flushmanager.DBFileHeader) {
			h.RootPageID = newRootPageID
		}); err != nil {
			return fmt.Errorf("failed to update disk header with new root page ID %d: %w", newRootPageID, err)
		}
	default:
		log.Printf("INFO: ReplicationManager: Applying B-Tree log record type %v (LSN %d) - specific logic depends on btree implementation details for these types.", lr.Type, lr.LSN)
		// Add handling for InsertKey, DeleteKey, NodeSplit, NodeMerge, etc., if needed at this level.
		// Often, these logical operations are covered by the physical page updates (NewPage, Update).
	}
	return nil
}

// HandleInboundStream processes an incoming replication stream from a primary for B-tree.
func (brm *BTreeReplicationManager) HandleInboundStream(conn net.Conn) error {
	brm.Logger.Info("Handling inbound B-tree replication stream", zap.String("from", conn.RemoteAddr().String()))

	// This connection is already demultiplexed by the server to be for BTreeIndexType.
	// No need to send/receive handshake here if the server already did.

	// The BaseReplicationManager's receiveAndApplyLogs can be used if it's generic enough.
	// We pass brm.ApplyLogRecord as the function to apply decoded logs.
	brm.wg.Add(1)
	go brm.receiveAndApplyLogs(conn, brm.stopChan, &brm.wg, brm.ApplyLogRecord) // stopChan from BaseReplicationManager

	// Note: The ReplicaConnectionInfo for this primary connection should be managed
	// by BecomeReplicaForSlot method when the FSM dictates this node is a replica.
	// HandleInboundStream is typically called by the server's main listener.
	// There needs to be coordination if the connection set up by BecomeReplicaForSlot
	// is different from the one handled here.
	// For now, assume this is a passive acceptance of a primary's stream.
	return nil
}

// BecomePrimaryForSlot: This node is now primary for the given slot for B-tree.
func (brm *BTreeReplicationManager) BecomePrimaryForSlot(slotID uint64, replicas map[string]string) error {
	brm.mu.Lock()
	defer brm.mu.Unlock()
	brm.Logger.Info("Becoming primary for B-tree slot", zap.Uint64("slotID", slotID), zap.Any("replicas", replicas))

	if _, exists := brm.PrimarySlotReplicas[slotID]; !exists {
		brm.PrimarySlotReplicas[slotID] = make(map[string]*ReplicaConnectionInfo)
	}

	for replicaNodeID, replicaAddress := range replicas {
		if _, exists := brm.PrimarySlotReplicas[slotID][replicaNodeID]; exists && brm.PrimarySlotReplicas[slotID][replicaNodeID].IsActive {
			brm.Logger.Info("Already primary and streaming to replica for B-tree slot", zap.Uint64("slotID", slotID), zap.String("replicaNodeID", replicaNodeID))
			continue
		}

		brm.Logger.Info("Attempting to connect to B-tree replica", zap.String("replicaNodeID", replicaNodeID), zap.String("address", replicaAddress))
		conn, err := net.DialTimeout("tcp", replicaAddress, 5*time.Second) // TODO: Make timeout configurable
		if err != nil {
			brm.Logger.Error("Failed to connect to B-tree replica", zap.Error(err), zap.String("replicaNodeID", replicaNodeID))
			continue // Try next replica
		}

		// Send handshake to specify IndexType
		if err := brm.sendHandshake(conn); err != nil {
			brm.Logger.Error("Failed to send handshake to B-tree replica", zap.Error(err), zap.String("replicaNodeID", replicaNodeID))
			conn.Close()
			continue
		}

		brm.Logger.Info("Successfully connected to B-tree replica and sent handshake", zap.String("replicaNodeID", replicaNodeID), zap.String("address", replicaAddress))

		connInfo := &ReplicaConnectionInfo{
			NodeID:     replicaNodeID,
			Address:    replicaAddress,
			Conn:       conn,
			StopChan:   make(chan struct{}),
			IsActive:   true,
			LastAckLSN: 0, // TODO: This should be fetched, e.g., from persisted state or an initial sync with replica
		}
		brm.PrimarySlotReplicas[slotID][replicaNodeID] = connInfo

		brm.wg.Add(1)
		go brm.streamLogs(conn, connInfo.LastAckLSN, connInfo.StopChan, &brm.wg, replicaNodeID)
	}
	return nil
}

// CeasePrimaryForSlot: This node is no longer primary for the B-tree slot.
func (brm *BTreeReplicationManager) CeasePrimaryForSlot(slotID uint64) error {
	brm.mu.Lock()
	defer brm.mu.Unlock()
	brm.Logger.Info("Ceasing to be primary for B-tree slot", zap.Uint64("slotID", slotID))

	if replicaConns, exists := brm.PrimarySlotReplicas[slotID]; exists {
		for replicaNodeID, connInfo := range replicaConns {
			if connInfo.IsActive {
				brm.Logger.Info("Stopping log stream to B-tree replica", zap.String("replicaNodeID", replicaNodeID), zap.Uint64("slotID", slotID))
				close(connInfo.StopChan) // Signal streamLogs goroutine to stop
				// connInfo.Conn.Close() // streamLogs will close it
				connInfo.IsActive = false
			}
		}
		delete(brm.PrimarySlotReplicas, slotID)
	}
	return nil
}

// BecomeReplicaForSlot: This node is now a replica for the B-tree slot.
func (brm *BTreeReplicationManager) BecomeReplicaForSlot(slotID uint64, primaryNodeID string, primaryAddress string) error {
	brm.mu.Lock()
	defer brm.mu.Unlock()
	brm.Logger.Info("Becoming replica for B-tree slot", zap.Uint64("slotID", slotID), zap.String("primaryNodeID", primaryNodeID), zap.String("primaryAddress", primaryAddress))

	if connInfo, exists := brm.ReplicaSlotPrimaries[slotID]; exists && connInfo.IsActive {
		if connInfo.NodeID == primaryNodeID && connInfo.Address == primaryAddress {
			brm.Logger.Info("Already connected to this B-tree primary for slot", zap.Uint64("slotID", slotID))
			return nil
		}
		// Stop existing connection if primary changed
		brm.Logger.Info("Primary changed for B-tree slot, stopping old connection", zap.Uint64("slotID", slotID))
		close(connInfo.StopChan)
		connInfo.IsActive = false
	}

	// This method sets up the expectation. The actual connection might be initiated
	// by the primary, and `HandleInboundStream` would be called.
	// Or, if replicas actively connect:
	// conn, err := net.DialTimeout("tcp", primaryAddress, 5*time.Second)
	// if err != nil {
	//     brm.Logger.Error("Failed to connect to B-tree primary", zap.Error(err), zap.String("primaryAddress", primaryAddress))
	//     return err
	// }
	// if err := brm.sendHandshake(conn); err != nil { // Replica sends handshake too, to identify itself
	//     conn.Close()
	//     return err
	// }
	// connInfo := &ReplicaConnectionInfo{
	//     NodeID: primaryNodeID,
	//     Address: primaryAddress,
	//     Conn: conn,
	//     StopChan: make(chan struct{}),
	//     IsActive: true,
	// }
	// brm.ReplicaSlotPrimaries[slotID] = connInfo
	// brm.wg.Add(1)
	// go brm.receiveAndApplyLogs(conn, connInfo.StopChan, &brm.wg, brm.ApplyLogRecord)

	// For now, we assume the primary initiates the connection. This method prepares the state.
	// The HandleInboundStream will be invoked by the server when the primary connects.
	// We store the expected primary's info.
	placeholderConnInfo := &ReplicaConnectionInfo{
		NodeID:   primaryNodeID,
		Address:  primaryAddress,
		StopChan: make(chan struct{}), // This will be used if HandleInboundStream sets up the real connInfo
		IsActive: false,               // Will become true when primary connects and HandleInboundStream is called
	}
	brm.ReplicaSlotPrimaries[slotID] = placeholderConnInfo
	brm.Logger.Info("Configured as replica for B-tree slot, waiting for primary to connect.", zap.Uint64("slotID", slotID), zap.String("primaryNodeID", primaryNodeID))

	return nil
}

// CeaseReplicaForSlot: This node is no longer a replica for the B-tree slot.
func (brm *BTreeReplicationManager) CeaseReplicaForSlot(slotID uint64) error {
	brm.mu.Lock()
	defer brm.mu.Unlock()
	brm.Logger.Info("Ceasing to be replica for B-tree slot", zap.Uint64("slotID", slotID))

	if connInfo, exists := brm.ReplicaSlotPrimaries[slotID]; exists {
		if connInfo.IsActive {
			brm.Logger.Info("Stopping connection from B-tree primary for slot", zap.Uint64("slotID", slotID))
			close(connInfo.StopChan) // Signal receiveAndApplyLogs goroutine to stop
			// connInfo.Conn.Close() // receiveAndApplyLogs will close it
			connInfo.IsActive = false
		}
		delete(brm.ReplicaSlotPrimaries, slotID)
	}
	return nil
}
