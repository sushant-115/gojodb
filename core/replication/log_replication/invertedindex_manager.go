package logreplication

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"log"
	"net"

	"github.com/sushant-115/gojodb/config/certs"
	"github.com/sushant-115/gojodb/core/indexing"
	"github.com/sushant-115/gojodb/core/indexing/inverted_index"
	"github.com/sushant-115/gojodb/core/replication/events"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	"go.uber.org/zap"
)

// InvertedIndexReplicationManager handles replication for Inverted Indexes.
type InvertedIndexReplicationManager struct {
	BaseReplicationManager
	InvertedIndex *inverted_index.InvertedIndex
}

// NewInvertedIndexReplicationManager creates a new InvertedIndexReplicationManager.
func NewInvertedIndexReplicationManager(nodeID string, ii *inverted_index.InvertedIndex, lm *wal.LogManager, logger *zap.Logger, nodeDataDir string, clientCert *tls.Config) *InvertedIndexReplicationManager {
	return &InvertedIndexReplicationManager{
		BaseReplicationManager: NewBaseReplicationManager(nodeID, indexing.InvertedIndexType, lm, logger, nodeDataDir, clientCert),
		InvertedIndex:          ii,
	}
}

// Start initiates the Inverted Index replication manager.
func (iirm *InvertedIndexReplicationManager) Start() error {
	iirm.Logger.Info("InvertedIndexReplicationManager starting")
	return iirm.StartBase()
}

// Stop gracefully shuts down the Inverted Index replication manager.
func (iirm *InvertedIndexReplicationManager) Stop() error {
	iirm.Logger.Info("InvertedIndexReplicationManager stopping")
	iirm.StopBase()
	return nil
}

// ApplyLogRecord applies an Inverted Index specific log record.
func (iirm *InvertedIndexReplicationManager) ApplyLogRecord(lr wal.LogRecord) error {
	iirm.Logger.Debug("Applying Inverted Index log record", zap.Uint64("lsn", uint64(lr.LSN)), zap.Any("type", lr.Type), zap.String("log_type", string(lr.Type)))
	if iirm.InvertedIndex == nil {
		return fmt.Errorf("InvertedIndex instance is nil in InvertedIndexReplicationManager")
	}

	// This is likely an Inverted Index term dictionary update
	parts := bytes.SplitN(lr.Data, []byte{0}, 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid Inverted Index log record format for LSN %d", lr.LSN)
	}
	term := string(parts[0])
	textValue := string(parts[1]) // Assuming the second part is the text to be indexed for this key (term)

	// For inverted index, the 'key' is the documentID/original key, and 'value' is the text.
	// The log record's NewData was: term (docKey) + 0 + textValue
	if err := iirm.InvertedIndex.Insert(textValue, term); err != nil { // Corrected order
		return fmt.Errorf("failed to apply Inverted Index update for term '%s' (text: '%s', LSN %d): %w", term, textValue, lr.LSN, err)
	}
	log.Printf("INFO: ReplicationManager: Replica applied Inverted Index update for term '%s' (LSN %d).", term, lr.LSN)

	return nil
}

// HandleInboundStream processes an incoming replication stream from a primary for Inverted Index.
func (iirm *InvertedIndexReplicationManager) HandleInboundStream(conn net.Conn) error {
	iirm.Logger.Info("Handling inbound Inverted Index replication stream", zap.String("from", conn.RemoteAddr().String()))
	iirm.wg.Add(1)
	go iirm.receiveAndApplyLogs(conn, iirm.stopChan, &iirm.wg, iirm.ApplyLogRecord)
	return nil
}

// BecomePrimaryForSlot: This node is now primary for the given slot for Inverted Index.
func (iirm *InvertedIndexReplicationManager) BecomePrimaryForSlot(slotID uint64, replicas map[string]string) error {
	iirm.mu.Lock()
	defer iirm.mu.Unlock()
	iirm.Logger.Info("Becoming primary for Inverted Index slot", zap.Uint64("slotID", slotID), zap.Any("replicas", replicas))

	if _, exists := iirm.PrimarySlotReplicas[slotID]; !exists {
		iirm.PrimarySlotReplicas[slotID] = make(map[string]*ReplicaConnectionInfo)
	}

	for replicaNodeID, replicaAddress := range replicas {
		if _, exists := iirm.PrimarySlotReplicas[slotID][replicaNodeID]; exists && iirm.PrimarySlotReplicas[slotID][replicaNodeID].IsActive {
			iirm.Logger.Info("Already primary and streaming to replica for Inverted Index slot", zap.Uint64("slotID", slotID), zap.String("replicaNodeID", replicaNodeID))
			continue
		}
		_, cert := certs.LoadCerts("/tmp")
		cfg := events.Config{
			Addr:    replicaAddress,
			URLPath: "/events",
			TLS:     cert,
		}
		eventSender, err := events.NewEventSender(cfg)
		if err != nil {
			iirm.Logger.Error("Failed to create eventSender for ", zap.String("replicaAddr", replicaAddress))
		}

		iirm.Logger.Info("Successfully connected to Inverted Index replica", zap.String("replicaNodeID", replicaNodeID), zap.String("address", replicaAddress))

		connInfo := &ReplicaConnectionInfo{
			NodeID:      replicaNodeID,
			Address:     replicaAddress,
			EventSender: eventSender,
			StopChan:    make(chan struct{}),
			IsActive:    true,
			LastAckLSN:  0, // TODO: Fetch initial LSN
		}
		iirm.PrimarySlotReplicas[slotID][replicaNodeID] = connInfo

		iirm.wg.Add(1)
		go iirm.streamLogs(connInfo, &iirm.wg, replicaNodeID)
	}
	return nil
}

// CeasePrimaryForSlot: This node is no longer primary for the Inverted Index slot.
func (iirm *InvertedIndexReplicationManager) CeasePrimaryForSlot(slotID uint64) error {
	iirm.mu.Lock()
	defer iirm.mu.Unlock()
	iirm.Logger.Info("Ceasing to be primary for Inverted Index slot", zap.Uint64("slotID", slotID))

	if replicaConns, exists := iirm.PrimarySlotReplicas[slotID]; exists {
		for replicaNodeID, connInfo := range replicaConns {
			if connInfo.IsActive {
				iirm.Logger.Info("Stopping log stream to Inverted Index replica", zap.String("replicaNodeID", replicaNodeID), zap.Uint64("slotID", slotID))
				close(connInfo.StopChan)
				connInfo.IsActive = false
			}
		}
		delete(iirm.PrimarySlotReplicas, slotID)
	}
	return nil
}

// BecomeReplicaForSlot: This node is now a replica for the Inverted Index slot.
func (iirm *InvertedIndexReplicationManager) BecomeReplicaForSlot(slotID uint64, primaryNodeID string, primaryAddress string) error {
	iirm.mu.Lock()
	defer iirm.mu.Unlock()
	iirm.Logger.Info("Becoming replica for Inverted Index slot", zap.Uint64("slotID", slotID), zap.String("primaryNodeID", primaryNodeID), zap.String("primaryAddress", primaryAddress))

	if connInfo, exists := iirm.ReplicaSlotPrimaries[slotID]; exists && connInfo.IsActive {
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
	iirm.ReplicaSlotPrimaries[slotID] = placeholderConnInfo
	iirm.Logger.Info("Configured as replica for Inverted Index slot, waiting for primary.", zap.Uint64("slotID", slotID))
	return nil
}

// CeaseReplicaForSlot: This node is no longer a replica for the Inverted Index slot.
func (iirm *InvertedIndexReplicationManager) CeaseReplicaForSlot(slotID uint64) error {
	iirm.mu.Lock()
	defer iirm.mu.Unlock()
	iirm.Logger.Info("Ceasing to be replica for Inverted Index slot", zap.Uint64("slotID", slotID))

	if connInfo, exists := iirm.ReplicaSlotPrimaries[slotID]; exists {
		if connInfo.IsActive {
			close(connInfo.StopChan)
			connInfo.IsActive = false
		}
		delete(iirm.ReplicaSlotPrimaries, slotID)
	}
	return nil
}
