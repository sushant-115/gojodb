package logreplication

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	btree_core "github.com/sushant-115/gojodb/core/indexing/btree"
	"github.com/sushant-115/gojodb/core/indexing/inverted_index"
	"github.com/sushant-115/gojodb/core/indexing/spatial"
	fsm "github.com/sushant-115/gojodb/core/replication/raft_consensus"
	flushmanager "github.com/sushant-115/gojodb/core/write_engine/flush_manager"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
)

const (
	LogTypeBTree          = 1
	LogTypeInvertedIndex  = 2
	LogTypeSpatial        = 3
	MaxRetryAttempts      = 5
	InitialRetryDelay     = 1 * time.Second
	MaxRetryDelay         = 30 * time.Second
	ReplicaReconnectCheck = 1 * time.Minute // How often to check if a suspended replica is back online
	DefaultConnTimeout    = 5 * time.Second // Default timeout for establishing connections
)

var (
	replicationStreamInterval = 5 * time.Second // How often primary checks for new logs to send
	storageNodeDialTimeout    = 2 * time.Second // Timeout for connecting to a storage node
)

// ReplicaInfo holds state specific to each replica
type ReplicaInfo struct {
	Addr                  string
	ID                    string
	Conn                  net.Conn
	LastAckLSNBTree       wal.LSN
	LastAckLSNInvertedIdx wal.LSN
	FailedAttempts        int
	Suspended             bool
	LastAttemptTime       time.Time
	mu                    sync.Mutex    // Protects Conn and other mutable fields specific to this replica
	stopStreaming         chan struct{} // To stop the streaming goroutine for this replica
}

// ReplicationManager manages sending and receiving log streams for replication.
type ReplicationManager struct {
	nodeID                             string
	logManager                         *wal.LogManager // B-tree's log manager
	dbInstance                         *btree_core.BTree[string, string]
	dbLock                             *sync.RWMutex
	invertedIndex                      *inverted_index.InvertedIndex
	spatialIdx                         *spatial.SpatialIndexManager
	replicationBTreeListenPort         string
	replicationInvertedIndexListenPort string

	// State for Primary role (sending logs)
	primaryForSlots        map[string]fsm.SlotRangeInfo
	myAssignedSlots        map[string]fsm.SlotRangeInfo
	replicas               map[string]*ReplicaInfo // ReplicaNodeAddr -> *ReplicaInfo
	primaryMu              sync.Mutex              // Protects primary role state (primaryForSlots, replicas map itself)
	storageNodeAddressesMu sync.RWMutex
	storageNodeAddresses   map[string]string

	// State for Replica role (receiving logs)
	bTreeReplicationListener       net.Listener
	invertedIdxReplicationListener net.Listener
	stopChan                       chan struct{}
	wg                             sync.WaitGroup
}

// NewReplicationManager creates and initializes a ReplicationManager.
func NewReplicationManager(nodeID string, lm *wal.LogManager, db *btree_core.BTree[string, string], lock *sync.RWMutex, ii *inverted_index.InvertedIndex, spatialIdx *spatial.SpatialIndexManager) *ReplicationManager {
	return &ReplicationManager{
		nodeID:                             nodeID,
		logManager:                         lm,
		dbInstance:                         db,
		dbLock:                             lock,
		invertedIndex:                      ii,
		spatialIdx:                         spatialIdx,
		primaryForSlots:                    make(map[string]fsm.SlotRangeInfo),
		replicas:                           make(map[string]*ReplicaInfo),
		stopChan:                           make(chan struct{}),
		storageNodeAddresses:               make(map[string]string),
		replicationBTreeListenPort:         os.Getenv("BTREE_REPLICATION_LISTEN_PORT"),
		replicationInvertedIndexListenPort: os.Getenv("INVERTED_IDX_REPLICATION_LISTEN_PORT"),
	}
}

// Start initiates the replication manager's background goroutines.
func (rm *ReplicationManager) Start() {
	rm.wg.Add(1)
	go rm.startReplicationListener()

	rm.wg.Add(1)
	go rm.manageOutboundReplication()
}

// Stop gracefully shuts down the replication manager.
func (rm *ReplicationManager) Stop() {
	close(rm.stopChan)
	rm.wg.Wait()

	rm.primaryMu.Lock()
	for _, replica := range rm.replicas {
		replica.mu.Lock()
		if replica.Conn != nil {
			replica.Conn.Close()
		}
		if replica.stopStreaming != nil {
			close(replica.stopStreaming)
		}
		replica.mu.Unlock()
	}
	rm.replicas = make(map[string]*ReplicaInfo) // Clear the map
	rm.primaryMu.Unlock()

	if rm.bTreeReplicationListener != nil {
		rm.bTreeReplicationListener.Close()
	}
	if rm.invertedIdxReplicationListener != nil {
		rm.invertedIdxReplicationListener.Close()
	}
	log.Printf("INFO: ReplicationManager for %s stopped.", rm.nodeID)
}

// startReplicationListener starts TCP listeners for incoming replication streams.
func (rm *ReplicationManager) startReplicationListener() {
	defer rm.wg.Done()
	rm.wg.Add(2) // For the two listeners
	go rm.startReplicationListenerInternal(rm.replicationBTreeListenPort, LogTypeBTree)
	go rm.startReplicationListenerInternal(rm.replicationInvertedIndexListenPort, LogTypeInvertedIndex)
}

func (rm *ReplicationManager) startReplicationListenerInternal(port string, logType int) {
	defer rm.wg.Done()
	addr := fmt.Sprintf(":%s", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("FATAL: ReplicationManager: Failed to start replication listener on %s: %v", addr, err)
	}

	if logType == LogTypeBTree {
		rm.bTreeReplicationListener = listener
	} else {
		rm.invertedIdxReplicationListener = listener
	}
	log.Printf("INFO: ReplicationManager: Listening for replication streams (Type: %d) on TCP %s", logType, listener.Addr().String())

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-rm.stopChan:
				log.Printf("INFO: Replication listener (Type: %d) stopping.", logType)
				return
			default:
				log.Printf("ERROR: ReplicationManager: Error accepting replication connection (Type: %d): %v", logType, err)
				continue
			}
		}
		rm.wg.Add(1)
		go rm.handleInboundReplicationStream(conn, logType)
	}
}

// handleInboundReplicationStream processes a single incoming replication stream.
func (rm *ReplicationManager) handleInboundReplicationStream(conn net.Conn, logType int) {
	defer rm.wg.Done()
	defer conn.Close()
	log.Printf("INFO: ReplicationManager: Accepted inbound replication stream (Type: %d) from %s", logType, conn.RemoteAddr().String())

	reader := bufio.NewReader(conn)
	for {
		select {
		case <-rm.stopChan:
			log.Printf("INFO: Replication inbound stream handler (Type: %d) for %s stopping.", logType, conn.RemoteAddr().String())
			return
		default:
			var lr wal.LogRecord
			err := rm.readLogRecordFromStream(reader, &lr) // Use a helper to read the log record
			if err == io.EOF {
				log.Printf("INFO: ReplicationManager: Primary %s closed replication stream (Type: %d).", conn.RemoteAddr().String(), logType)
				return
			}
			if err != nil {
				log.Printf("ERROR: ReplicationManager: Failed to read log record from stream %s (Type: %d): %v", conn.RemoteAddr().String(), logType, err)
				return
			}

			log.Printf("DEBUG: ReplicationManager: Received log record LSN %d (Type: %v, Txn: %d, Page: %d, LogType: %d) from Primary (%s)",
				lr.LSN, lr.Type, lr.TxnID, lr.PageID, logType, conn.RemoteAddr().String())

			if err := rm.applyLogRecord(lr, logType); err != nil {
				log.Printf("ERROR: ReplicationManager: Failed to apply log record LSN %d (Type: %d) from %s: %v", lr.LSN, logType, conn.RemoteAddr().String(), err)
				// Decide on error handling: stop processing, request resend, etc.
				// For now, we continue, but this could lead to inconsistencies.
				// A more robust system might mark the replica as needing recovery.
			}
		}
	}
}

// readLogRecordFromStream reads and deserializes a single log record from the stream.
// This handles the length prefix for the log record.
func (rm *ReplicationManager) readLogRecordFromStream(reader *bufio.Reader, lr *wal.LogRecord) error {
	var recordLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &recordLen); err != nil {
		if err == io.EOF {
			return io.EOF
		}
		return fmt.Errorf("failed to read log record length: %w", err)
	}

	if recordLen == 0 {
		return io.EOF // Or a more specific error for zero-length record if unexpected
	}

	recordBytes := make([]byte, recordLen)
	if _, err := io.ReadFull(reader, recordBytes); err != nil {
		return fmt.Errorf("failed to read log record data (expected %d bytes): %w", recordLen, err)
	}

	return lr.Deserialize(recordBytes)
}

// applyLogRecord applies a received log record to the local state.
func (rm *ReplicationManager) applyLogRecord(lr wal.LogRecord, logType int) error {
	// Differentiate between B-tree and Inverted Index log records
	if logType == LogTypeInvertedIndex {
		// This is likely an Inverted Index term dictionary update
		parts := bytes.SplitN(lr.NewData, []byte{0}, 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid Inverted Index log record format for LSN %d", lr.LSN)
		}
		term := string(parts[0])
		textValue := string(parts[1]) // Assuming the second part is the text to be indexed for this key (term)

		// For inverted index, the 'key' is the documentID/original key, and 'value' is the text.
		// The log record's NewData was: term (docKey) + 0 + textValue
		if err := rm.invertedIndex.Insert(textValue, term); err != nil { // Corrected order
			return fmt.Errorf("failed to apply Inverted Index update for term '%s' (text: '%s', LSN %d): %w", term, textValue, lr.LSN, err)
		}
		log.Printf("INFO: ReplicationManager: Replica applied Inverted Index update for term '%s' (LSN %d).", term, lr.LSN)
	} else { // B-tree log record
		rm.dbLock.Lock()
		defer rm.dbLock.Unlock()

		// Apply B-tree log record (simplified logic from your original code)
		switch lr.Type {
		case wal.LogRecordTypeNewPage, wal.LogRecordTypeInsertKey, wal.LogTypeRTreeInsert, wal.LogTypeRTreeNewRoot:
			if lr.PageID.GetID() >= rm.dbInstance.GetNumPages() {
				emptyPage := make([]byte, rm.dbInstance.GetPageSize())
				if writeErr := rm.dbInstance.WritePage(lr.PageID, emptyPage); writeErr != nil {
					return fmt.Errorf("failed to allocate/write empty page %d on disk: %w", lr.PageID, writeErr)
				}
				if lr.PageID >= pagemanager.PageID(rm.dbInstance.GetNumPages()) {
					rm.dbInstance.SetNumPages(uint64(lr.PageID) + 1)
				}
			}
			page, fetchErr := rm.dbInstance.FetchPage(lr.PageID)
			if fetchErr != nil {
				return fmt.Errorf("failed to fetch page %d for NEW_PAGE: %w", lr.PageID, fetchErr)
			}
			page.Lock()
			page.SetData(lr.NewData)
			page.SetDirty(true)
			page.Unlock()
			if errUnpin := rm.dbInstance.UnpinPage(page.GetPageID(), true); errUnpin != nil {
				log.Printf("WARNING: Failed to unpin page %d after NEW_PAGE application: %v", page.GetPageID(), errUnpin)
			}
			if errFlush := rm.dbInstance.FlushPage(page.GetPageID()); errFlush != nil {
				return fmt.Errorf("failed to flush page %d after NEW_PAGE: %w", page.GetPageID(), errFlush)
			}
		case wal.LogRecordTypeUpdate, wal.LogTypeRTreeUpdate:
			page, fetchErr := rm.dbInstance.FetchPage(lr.PageID)
			if fetchErr != nil {
				return fmt.Errorf("failed to fetch page %d for UPDATE: %w", lr.PageID, fetchErr)
			}
			page.Lock()
			page.SetData(lr.NewData)
			page.SetDirty(true)
			page.Unlock()
			if errUnpin := rm.dbInstance.UnpinPage(page.GetPageID(), true); errUnpin != nil {
				log.Printf("WARNING: Failed to unpin page %d after UPDATE application: %v", page.GetPageID(), errUnpin)
			}
			if errFlush := rm.dbInstance.FlushPage(page.GetPageID()); errFlush != nil {
				return fmt.Errorf("failed to flush page %d after UPDATE: %w", page.GetPageID(), errFlush)
			}
		case wal.LogRecordTypeRootChange, wal.LogTypeRTreeSplit:
			newRootPageID := pagemanager.PageID(binary.LittleEndian.Uint64(lr.NewData))
			rm.dbInstance.SetRootPageID(newRootPageID, lr.TxnID)
			if err := rm.dbInstance.GetDiskManager().UpdateHeaderField(func(h *flushmanager.DBFileHeader) {
				h.RootPageID = newRootPageID
			}); err != nil {
				return fmt.Errorf("failed to update disk header with new root page ID %d: %w", newRootPageID, err)
			}
		default:
			log.Printf("INFO: ReplicationManager: Applying B-Tree log record type %v (LSN %d) - specific logic depends on btree implementation details for these types.", lr.Type, lr.LSN)
			// Add handling for InsertKey, DeleteKey, NodeSplit, NodeMerge, etc., if needed at this level.
			// Often, these logical operations are covered by the physical page updates (NewPage, Update).
		}
	}
	return nil
}

func getReplicaPort(replicaAddr string) (string, string, error) {
	conn, err := net.DialTimeout("tcp", replicaAddr, DefaultConnTimeout)
	if err != nil {
		return "", "", fmt.Errorf("failed to connect to replica %s to get port: %w", replicaAddr, err)
	}
	defer conn.Close()

	if _, err = fmt.Fprint(conn, "SHOW CONFIG REPLICA_PORT\n"); err != nil {
		return "", "", fmt.Errorf("failed to send command to replica %s: %w", replicaAddr, err)
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", "", fmt.Errorf("failed to read response from replica %s: %w", replicaAddr, err)
	}

	parts := strings.Fields(line) // Use Fields to handle multiple spaces
	if len(parts) < 3 || strings.TrimSpace(parts[0]) != "OK" {
		return "", "", fmt.Errorf("invalid response from replica %s: %s", replicaAddr, line)
	}

	// parts[1] should be like "host:bTreePort", parts[2] like "host:idxPort"
	bTreeHostPort := strings.Split(strings.TrimSpace(parts[1]), ":")
	idxHostPort := strings.Split(strings.TrimSpace(parts[2]), ":")

	if len(bTreeHostPort) != 2 || len(idxHostPort) != 2 {
		return "", "", fmt.Errorf("invalid port format in response from replica %s: %s", replicaAddr, line)
	}
	return bTreeHostPort[1], idxHostPort[1], nil
}

func (rm *ReplicationManager) SetStorageNodeAddress(nodeID, address string) {
	rm.storageNodeAddressesMu.Lock()
	defer rm.storageNodeAddressesMu.Unlock()
	rm.storageNodeAddresses[nodeID] = address
}

func (rm *ReplicationManager) SetAssignedNodeAddress(myAssignedSlots map[string]fsm.SlotRangeInfo) {
	rm.primaryMu.Lock()
	defer rm.primaryMu.Unlock()
	rm.myAssignedSlots = myAssignedSlots
}

func (rm *ReplicationManager) getPrimarySlots() map[string]fsm.SlotRangeInfo {
	currentPrimarySlots := make(map[string]fsm.SlotRangeInfo)
	rm.primaryMu.Lock() // Lock for reading rm.myAssignedSlots
	for rangeID, slotInfo := range rm.myAssignedSlots {
		if slotInfo.PrimaryNodeID == rm.nodeID {
			currentPrimarySlots[rangeID] = slotInfo
		}
	}
	rm.primaryForSlots = currentPrimarySlots
	rm.primaryMu.Unlock()
	return currentPrimarySlots
}

// manageOutboundReplication identifies primary roles and streams logs to replicas.
func (rm *ReplicationManager) manageOutboundReplication() {
	defer rm.wg.Done()
	ticker := time.NewTicker(replicationStreamInterval)
	defer ticker.Stop()

	reconnectTicker := time.NewTicker(ReplicaReconnectCheck)
	defer reconnectTicker.Stop()

	for {
		select {
		case <-rm.stopChan:
			log.Println("INFO: Outbound replication manager stopping.")
			return
		case <-ticker.C:
			currentPrimarySlots := rm.getPrimarySlots()
			rm.initConnectionsAndStreamLogs(currentPrimarySlots)
		case <-reconnectTicker.C:
			rm.attemptReconnectSuspendedReplicas()
		}
	}
}

func (rm *ReplicationManager) initConnectionsAndStreamLogs(currentPrimarySlots map[string]fsm.SlotRangeInfo) {
	rm.primaryMu.Lock() // Lock to access rm.replicas
	defer rm.primaryMu.Unlock()

	for rangeID, slotInfo := range currentPrimarySlots {
		for _, replicaID := range slotInfo.ReplicaNodeIDs {
			rm.storageNodeAddressesMu.RLock()
			replicaAddr, ok := rm.storageNodeAddresses[replicaID]
			rm.storageNodeAddressesMu.RUnlock()

			if !ok || replicaAddr == "" {
				log.Printf("WARNING: ReplicationManager: Replica %s address not found for slot %s. Cannot stream logs.", replicaID, rangeID)
				continue
			}

			if _, exists := rm.replicas[replicaAddr]; !exists {
				rm.replicas[replicaAddr] = &ReplicaInfo{
					Addr:            replicaAddr,
					ID:              replicaID,
					stopStreaming:   make(chan struct{}),
					LastAttemptTime: time.Now().Add(-ReplicaReconnectCheck), // Allow immediate connect attempt
				}
			}
			replica := rm.replicas[replicaAddr]

			replica.mu.Lock()
			if replica.Conn == nil && !replica.Suspended {
				if time.Since(replica.LastAttemptTime) < InitialRetryDelay*time.Duration(1<<uint(replica.FailedAttempts)) {
					// Still in backoff period
					replica.mu.Unlock()
					continue
				}
				replica.LastAttemptTime = time.Now()

				bTreePort, idxPort, err := getReplicaPort(replicaAddr)
				if err != nil {
					log.Printf("ERROR: Failed to get replica ports for %s: %v", replicaAddr, err)
					replica.mu.Unlock()
					continue
				}

				// Connect and stream for BTree
				bTreeTargetAddr := fmt.Sprintf("%s:%s", strings.Split(replicaAddr, ":")[0], bTreePort)
				connBTree, err := net.DialTimeout("tcp", bTreeTargetAddr, DefaultConnTimeout)
				if err != nil {
					log.Printf("ERROR: ReplicationManager: Failed to connect to BTree replica %s at %s: %v", replicaID, bTreeTargetAddr, err)
					replica.FailedAttempts++
					if replica.FailedAttempts >= MaxRetryAttempts {
						log.Printf("INFO: ReplicationManager: Suspending BTree replication to %s after %d failed attempts.", replicaAddr, MaxRetryAttempts)
						replica.Suspended = true // Simplified: suspend entire replica if one stream fails connect
					}
				} else {
					replica.Conn = connBTree // For simplicity, store one connection; real system might need separate
					replica.FailedAttempts = 0
					log.Printf("INFO: ReplicationManager: Established BTree replication stream to %s.", replicaAddr)
					rm.wg.Add(1)
					go rm.streamLogsToSpecificReplica(replica, LogTypeBTree)
				}

				// Connect and stream for Inverted Index
				idxTargetAddr := fmt.Sprintf("%s:%s", strings.Split(replicaAddr, ":")[0], idxPort)
				connIdx, err := net.DialTimeout("tcp", idxTargetAddr, DefaultConnTimeout)
				if err != nil {
					log.Printf("ERROR: ReplicationManager: Failed to connect to Inverted Index replica %s at %s: %v", replicaID, idxTargetAddr, err)
					// Handle failure similar to BTree stream if needed
					replica.FailedAttempts++
					if replica.FailedAttempts >= MaxRetryAttempts && replica.Conn == nil { // Only suspend if BTree also failed
						log.Printf("INFO: ReplicationManager: Suspending Inverted Index replication to %s after %d failed attempts (BTree also failed).", replicaAddr, MaxRetryAttempts)
						replica.Suspended = true
					}
				} else {
					// For inverted index, we would need another conn field in ReplicaInfo or a map of conns
					// This example will just start a new stream goroutine for idx logs on a new connection.
					// Ideally, ReplicaInfo would manage multiple connections or types.
					log.Printf("INFO: ReplicationManager: Established Inverted Index replication stream to %s.", replicaAddr)
					// Store this conn if ReplicaInfo is extended for it.
					// For now, pass it directly to a new goroutine.
					rm.wg.Add(1)
					go func(idxConn net.Conn) { // Pass the connection to the goroutine
						defer idxConn.Close()
						// Create a temporary ReplicaInfo-like structure or pass parameters
						tempReplicaInfoForIdx := &ReplicaInfo{
							Addr:                  replicaAddr,
							ID:                    replicaID,
							Conn:                  idxConn,
							LastAckLSNInvertedIdx: replica.LastAckLSNInvertedIdx, // Use the replica's actual LSN state
							stopStreaming:         replica.stopStreaming,         // Share the stop signal
						}
						rm.streamLogsToSpecificReplica(tempReplicaInfoForIdx, LogTypeInvertedIndex)
					}(connIdx) // Pass the newly created connIdx
					// replica.FailedAttempts = 0 // Reset if BTree connection also succeeded
				}
			}
			replica.mu.Unlock()
		}
	}
}

func (rm *ReplicationManager) attemptReconnectSuspendedReplicas() {
	rm.primaryMu.Lock()
	defer rm.primaryMu.Unlock()

	for addr, replica := range rm.replicas {
		replica.mu.Lock()
		if replica.Suspended {
			log.Printf("INFO: Attempting to reconnect to suspended replica %s", addr)
			replica.Suspended = false                                        // Allow new connection attempts
			replica.FailedAttempts = 0                                       // Reset failure count
			replica.LastAttemptTime = time.Now().Add(-ReplicaReconnectCheck) // Allow immediate attempt
			// The main loop in manageOutboundReplication will pick it up
		}
		replica.mu.Unlock()
	}
}

// streamLogsToSpecificReplica sends logs of a specific type to a given replica.
func (rm *ReplicationManager) streamLogsToSpecificReplica(replica *ReplicaInfo, logType int) {
	defer rm.wg.Done()
	if logType == LogTypeBTree {
		defer func() {
			replica.mu.Lock()
			if replica.Conn != nil {
				replica.Conn.Close()
				replica.Conn = nil
			}
			replica.mu.Unlock()
		}()
	}
	// If it's LogTypeInvertedIndex, the passed replica.Conn is specific to that stream and
	// will be closed by the defer in the calling goroutine in initConnectionsAndStreamLogs.

	var logMan *wal.LogManager
	var lastAckLSN *wal.LSN
	// var lsnKeySuffix string

	if logType == LogTypeBTree {
		logMan = rm.logManager
		lastAckLSN = &replica.LastAckLSNBTree
		// lsnKeySuffix = "_btree"
	} else if logType == LogTypeInvertedIndex {
		logMan = rm.invertedIndex.GetLogManager()
		lastAckLSN = &replica.LastAckLSNInvertedIdx
		// lsnKeySuffix = "_inverted_index"
	} else {
		log.Printf("ERROR: Unknown log type %d for streaming to replica %s", logType, replica.ID)
		return
	}

	log.Printf("DEBUG: ReplicationManager: Starting log stream (Type: %d) to replica %s from LSN %d.", logType, replica.ID, *lastAckLSN)
	logStream, err := logMan.StartLogStream(*lastAckLSN)
	if err != nil {
		log.Printf("ERROR: ReplicationManager: Failed to start log stream (Type: %d) for replica %s: %v", logType, replica.ID, err)
		rm.handleStreamFailure(replica)
		return
	}

	for {
		select {
		case lr, ok := <-logStream:
			if !ok {
				log.Printf("INFO: ReplicationManager: Log stream (Type: %d) for replica %s closed by source.", logType, replica.ID)
				// Stream ended, but connection might still be good for future logs if primary keeps it open
				// Or, primary might close it if no more logs.
				return
			}

			serializedRecord, err := lr.Serialize()
			if err != nil {
				log.Printf("ERROR: ReplicationManager: Failed to serialize log record LSN %d (Type: %d) for replica %s: %v", lr.LSN, logType, replica.ID, err)
				rm.handleStreamFailure(replica)
				return
			}

			// Prepend length of the record
			recordLen := uint32(len(serializedRecord))
			lenBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(lenBytes, recordLen)

			replica.mu.Lock() // Lock before accessing replica.Conn
			conn := replica.Conn
			if conn == nil { // Connection might have been closed by another part
				replica.mu.Unlock()
				log.Printf("ERROR: ReplicationManager: Connection to replica %s (Type: %d) is nil before sending LSN %d.", replica.ID, logType, lr.LSN)
				rm.handleStreamFailure(replica)
				return
			}

			_, err = conn.Write(lenBytes)
			if err == nil {
				_, err = conn.Write(serializedRecord)
			}
			replica.mu.Unlock() // Unlock after using conn

			if err != nil {
				log.Printf("ERROR: ReplicationManager: Failed to send log record LSN %d (Type: %d) to replica %s: %v", lr.LSN, logType, replica.ID, err)
				rm.handleStreamFailure(replica)
				return
			}

			// Update Last Acknowledged LSN (assuming send implies acknowledgment for this example)
			// In a real system, you'd wait for an ACK from the replica.
			replica.mu.Lock()
			*lastAckLSN = lr.LSN + wal.LSN(lr.Size()) // LSN of the *next* record to send
			// Also update the global map in ReplicationManager if needed, though replica.LastAckLSN should be authoritative for this stream
			rm.primaryMu.Lock()
			rm.replicas[replica.Addr].setLastAckLSN(logType, *lastAckLSN)
			rm.primaryMu.Unlock()
			replica.mu.Unlock()

			log.Printf("DEBUG: ReplicationManager: Sent log record LSN %d (Type: %d) to replica %s. Next LSN to send: %d", lr.LSN, logType, replica.ID, *lastAckLSN)
			replica.mu.Lock()
			replica.FailedAttempts = 0 // Reset on successful send
			replica.mu.Unlock()

		case <-replica.stopStreaming:
			log.Printf("INFO: Log streaming (Type: %d) to replica %s stopping due to signal.", logType, replica.ID)
			return
		case <-rm.stopChan: // Global stop signal for the manager
			log.Printf("INFO: Log streaming (Type: %d) to replica %s stopping due to manager shutdown.", logType, replica.ID)
			return
		}
	}
}

func (r *ReplicaInfo) setLastAckLSN(logType int, lsn wal.LSN) {
	if logType == LogTypeBTree {
		r.LastAckLSNBTree = lsn
	} else if logType == LogTypeInvertedIndex {
		r.LastAckLSNInvertedIdx = lsn
	}
}

func (rm *ReplicationManager) handleStreamFailure(replica *ReplicaInfo) {
	replica.mu.Lock()
	defer replica.mu.Unlock()

	if replica.Conn != nil {
		replica.Conn.Close()
		replica.Conn = nil
	}
	replica.FailedAttempts++
	log.Printf("INFO: Replication stream failure for %s. Attempt %d.", replica.Addr, replica.FailedAttempts)

	if replica.FailedAttempts >= MaxRetryAttempts {
		log.Printf("INFO: Suspending replication to %s after %d failed attempts.", replica.Addr, MaxRetryAttempts)
		replica.Suspended = true
		// No longer close replica.stopStreaming here, let manageOutboundReplication handle it
		// if the replica info is removed entirely.
	}
	// The manageOutboundReplication loop will attempt to reconnect based on Suspended and LastAttemptTime.
}

// Helper to calculate exponential backoff delay
func calculateBackoff(attempts int) time.Duration {
	if attempts <= 0 {
		return InitialRetryDelay
	}
	delay := InitialRetryDelay * time.Duration(1<<(uint(attempts-1))) // 2^(attempts-1)
	if delay > MaxRetryDelay {
		return MaxRetryDelay
	}
	return delay
}
