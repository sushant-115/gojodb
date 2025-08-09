package logreplication

import (
	"fmt"
	"net"

	"github.com/sushant-115/gojodb/core/indexing/spatial"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	"github.com/sushant-115/gojodb/pkg/connection"
	"go.uber.org/zap"
)

// SpatialReplicationManager handles replication for Spatial Indexes.
type SpatialReplicationManager struct {
	BaseReplicationManager
	SpatialIndex    *spatial.SpatialIndexManager // The actual Spatial Index manager instance
	replicaConnPool *connection.ConnectionPoolManager
}

// NewSpatialReplicationManager creates a new SpatialReplicationManager.
func NewSpatialReplicationManager(nodeID string, sim *spatial.SpatialIndexManager, lm *wal.LogManager, logger *zap.Logger, nodeDataDir string, replicaConnPool *connection.ConnectionPoolManager) *SpatialReplicationManager {
	// NOTE: spatial.IndexManager does not currently have GetLogManager() or an obvious LogManager field.
	// We are passing the 'lm' parameter, assuming it's the correct one or spatial.IndexManager will be adapted.
	return &SpatialReplicationManager{
		BaseReplicationManager: NewBaseReplicationManager(nodeID, SpatialIndexType, lm, logger, nodeDataDir),
		SpatialIndex:           sim,
		replicaConnPool:        replicaConnPool,
	}
}

// Start initiates the Spatial Index replication manager.
func (srm *SpatialReplicationManager) Start() error {
	srm.Logger.Info("SpatialReplicationManager starting")
	return srm.StartBase()
}

// Stop gracefully shuts down the Spatial Index replication manager.
func (srm *SpatialReplicationManager) Stop() error {
	srm.Logger.Info("SpatialReplicationManager stopping")
	srm.StopBase()
	return nil
}

// ApplyLogRecord applies a Spatial Index specific log record.
func (srm *SpatialReplicationManager) ApplyLogRecord(lr wal.LogRecord) error {
	srm.Logger.Debug("Applying Spatial Index log record", zap.Uint64("lsn", uint64(lr.LSN)), zap.Any("type", lr.Type))
	if srm.SpatialIndex == nil {
		return fmt.Errorf("SpatialIndex instance is nil in SpatialReplicationManager")
	}

	// TODO: Implement spatial index log application.
	// The spatial.IndexManager needs a method like ApplyReplicationLog(lr wal.LogRecord)
	// or similar mechanism to apply changes from a log record.
	// Example:
	// if lr.LogType != wal.LogTypeSpatialIndex { // Assuming a LogTypeSpatialIndex exists
	//     srm.Logger.Warn("Received log record not for Spatial Index", zap.Any("log_record_type", lr.LogType))
	//	   return fmt.Errorf("log record type %s not applicable for spatial index", lr.LogType)
	// }
	// err := srm.SpatialIndex.ApplyReplicationLog(lr) // Hypothetical method
	// if err != nil {
	//    srm.Logger.Error("Failed to apply Spatial Index log record", zap.Error(err), zap.Uint64("lsn", uint64(lr.LSN)))
	//	  return err
	// }
	srm.Logger.Warn("ApplyLogRecord for Spatial Index is a placeholder and needs actual implementation.", zap.Any("logRecord", lr))
	return fmt.Errorf("ApplyLogRecord for Spatial Index not yet implemented")
}

// HandleInboundStream processes an incoming replication stream from a primary for Spatial Index.
func (srm *SpatialReplicationManager) HandleInboundStream(conn net.Conn) error {
	srm.Logger.Info("Handling inbound Spatial Index replication stream", zap.String("from", conn.RemoteAddr().String()))
	srm.wg.Add(1)
	go srm.receiveAndApplyLogs(conn, srm.stopChan, &srm.wg, srm.ApplyLogRecord)
	return nil
}

// BecomePrimaryForSlot: This node is now primary for the given slot for Spatial Index.
func (srm *SpatialReplicationManager) BecomePrimaryForSlot(slotID uint64, replicas map[string]string) error {
	srm.mu.Lock()
	defer srm.mu.Unlock()
	srm.Logger.Info("Becoming primary for Spatial Index slot", zap.Uint64("slotID", slotID), zap.Any("replicas", replicas))

	if _, exists := srm.PrimarySlotReplicas[slotID]; !exists {
		srm.PrimarySlotReplicas[slotID] = make(map[string]*ReplicaConnectionInfo)
	}

	for replicaNodeID, replicaAddress := range replicas {
		if _, exists := srm.PrimarySlotReplicas[slotID][replicaNodeID]; exists && srm.PrimarySlotReplicas[slotID][replicaNodeID].IsActive {
			continue // Already streaming
		}

		// conn, err := net.DialTimeout("tcp", replicaAddress, 5*time.Second)
		conn, err := srm.replicaConnPool.Get(replicaAddress)
		if err != nil {
			srm.Logger.Error("Failed to connect to Spatial Index replica", zap.Error(err), zap.String("replicaNodeID", replicaNodeID))
			continue
		}
		if err := srm.sendHandshake(conn); err != nil {
			srm.Logger.Error("Failed to send handshake to Spatial replica", zap.Error(err), zap.String("replicaNodeID", replicaNodeID))
			conn.Close()
			continue
		}
		srm.Logger.Info("Successfully connected to Spatial Index replica", zap.String("replicaNodeID", replicaNodeID), zap.String("address", replicaAddress))

		connInfo := &ReplicaConnectionInfo{
			NodeID:     replicaNodeID,
			Address:    replicaAddress,
			Conn:       conn,
			StopChan:   make(chan struct{}),
			IsActive:   true,
			LastAckLSN: 0, // TODO: Fetch initial LSN
		}
		srm.PrimarySlotReplicas[slotID][replicaNodeID] = connInfo

		srm.wg.Add(1)
		go srm.streamLogs(conn, connInfo.LastAckLSN, connInfo.StopChan, &srm.wg, replicaNodeID)
	}
	return nil
}

// CeasePrimaryForSlot: This node is no longer primary for the Spatial Index slot.
func (srm *SpatialReplicationManager) CeasePrimaryForSlot(slotID uint64) error {
	srm.mu.Lock()
	defer srm.mu.Unlock()
	srm.Logger.Info("Ceasing to be primary for Spatial Index slot", zap.Uint64("slotID", slotID))

	if replicaConns, exists := srm.PrimarySlotReplicas[slotID]; exists {
		for _, connInfo := range replicaConns {
			if connInfo.IsActive {
				close(connInfo.StopChan)
				connInfo.IsActive = false
			}
		}
		delete(srm.PrimarySlotReplicas, slotID)
	}
	return nil
}

// BecomeReplicaForSlot: This node is now a replica for the Spatial Index slot.
func (srm *SpatialReplicationManager) BecomeReplicaForSlot(slotID uint64, primaryNodeID string, primaryAddress string) error {
	srm.mu.Lock()
	defer srm.mu.Unlock()
	srm.Logger.Info("Becoming replica for Spatial Index slot", zap.Uint64("slotID", slotID), zap.String("primaryNodeID", primaryNodeID), zap.String("primaryAddress", primaryAddress))

	if connInfo, exists := srm.ReplicaSlotPrimaries[slotID]; exists && connInfo.IsActive {
		if connInfo.NodeID == primaryNodeID && connInfo.Address == primaryAddress {
			return nil // Already connected
		}
		close(connInfo.StopChan)
		connInfo.IsActive = false
	}

	placeholderConnInfo := &ReplicaConnectionInfo{
		NodeID:   primaryNodeID,
		Address:  primaryAddress,
		StopChan: make(chan struct{}),
		IsActive: false,
	}
	srm.ReplicaSlotPrimaries[slotID] = placeholderConnInfo
	srm.Logger.Info("Configured as replica for Spatial Index slot, waiting for primary.", zap.Uint64("slotID", slotID))
	return nil
}

// CeaseReplicaForSlot: This node is no longer a replica for the Spatial Index slot.
func (srm *SpatialReplicationManager) CeaseReplicaForSlot(slotID uint64) error {
	srm.mu.Lock()
	defer srm.mu.Unlock()
	srm.Logger.Info("Ceasing to be replica for Spatial Index slot", zap.Uint64("slotID", slotID))

	if connInfo, exists := srm.ReplicaSlotPrimaries[slotID]; exists {
		if connInfo.IsActive {
			close(connInfo.StopChan)
			connInfo.IsActive = false
		}
		delete(srm.ReplicaSlotPrimaries, slotID)
	}
	return nil
}
