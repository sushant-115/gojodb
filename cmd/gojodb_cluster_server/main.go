package main

import (
	"bufio"
	"encoding/binary" // Added for PageID serialization/deserialization
	"encoding/json"   // Needed for (de)serializing TransactionOperations
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sushant-115/gojodb/cmd/gojodb_controller/fsm"      // FSM package for sharding info
	btree_core "github.com/sushant-115/gojodb/core/indexing/btree" // B-tree core package
	"github.com/sushant-115/gojodb/core/indexing/inverted_index"   // NEW: Inverted Index package
)

const (
	// Database configuration
	dbFilePath     = "data/gojodb_shard.db" // Each storage node will have its own DB file
	logDir         = "data/logs"
	archiveDir     = "data/archives"
	logBufferSize  = 4096
	logSegmentSize = 16 * 1024
	bTreeDegree    = 3
	bufferPoolSize = 10
	dbPageSize     = 4096

	// Storage Node specific configuration
	storageNodeListenHost = "localhost" // Listen address for client requests
	storageNodeListenPort = "9000"      // Default listen port for Storage Node

	// Controller interaction configuration
	controllerHeartbeatTargetPort = 8086             // Default UDP port on Controller for heartbeats
	controllerHTTPAddresses       = "localhost:8080" // Comma-separated list of Controller HTTP addresses
	heartbeatInterval             = 5 * time.Second  // How often to send heartbeats
	shardMapFetchInterval         = 10 * time.Second // How often to fetch shard map
	replicationStreamInterval     = 10 * time.Second // How often primary checks for new logs to send
	storageNodeDialTimeout        = 2 * time.Second  // Timeout for connecting to a storage node

	// GOJODB.TEXT() wrapper prefix
	gojoDBTextPrefix = "GOJODB.TEXT("
	gojoDBTextSuffix = ")"
)

// Global database instance and its lock for simple concurrency control
var (
	dbInstance            *btree_core.BTree[string, string] // Using string keys now
	dbLock                sync.RWMutex                      // Global lock for the entire B-Tree operations
	logManager            *btree_core.LogManager
	invertedIndexInstance *inverted_index.InvertedIndex // NEW: Global inverted index instance

	// Storage Node identity
	myStorageNodeID   string
	myStorageNodeAddr string // e.g., "localhost:9000"

	// Sharding awareness
	myAssignedSlotsMu sync.RWMutex
	myAssignedSlots   map[string]fsm.SlotRangeInfo // Cache of slot ranges assigned to THIS node
	// Replication Manager instance
	replicationManager *ReplicationManager
)

// Request represents a parsed client request for single operations.
type Request struct {
	Command  string
	Key      string // Key is now string
	Value    string // Only for PUT
	TxnID    uint64 // NEW: Transaction ID for 2PC operations (0 for auto-commit)
	StartKey string // For range query
	EndKey   string // For range query
	Query    string // NEW: For text search queries
}

// Response represents a server's reply to a client request.
type Response struct {
	Status  string // OK, ERROR, NOT_FOUND, REDIRECT, VOTE_COMMIT, VOTE_ABORT, COMMITTED, ABORTED
	Message string // Details or value for GET, or target node for REDIRECT
}

type Entry struct {
	Key   string
	Value string
}

type TransactionOperations struct {
	Operations []btree_core.TransactionOperation `json:"operations"`
}

// ReplicationManager manages sending and receiving log streams for replication.
type ReplicationManager struct {
	nodeID                string
	logManager            *btree_core.LogManager
	dbInstance            *btree_core.BTree[string, string]
	dbLock                *sync.RWMutex
	replicationListenPort string

	// State for Primary role (sending logs)
	primaryForSlots        map[string]fsm.SlotRangeInfo // SlotRangeID -> SlotRangeInfo (where this node is primary)
	replicaClients         map[string]net.Conn          // ReplicaNodeAddr -> TCP connection to replica
	lastSentLSN            map[string]btree_core.LSN    // ReplicaNodeAddr -> Last LSN sent to this replica
	primaryMu              sync.Mutex                   // Protects primary role state
	storageNodeAddressesMu sync.RWMutex
	storageNodeAddresses   map[string]string

	// State for Replica role (receiving logs)
	replicationListener net.Listener // TCP listener for incoming replication streams
	stopChan            chan struct{}
	wg                  sync.WaitGroup
}

// NewReplicationManager creates and initializes a ReplicationManager.
func NewReplicationManager(nodeID string, lm *btree_core.LogManager, db *btree_core.BTree[string, string], lock *sync.RWMutex) *ReplicationManager {
	return &ReplicationManager{
		nodeID:                nodeID,
		logManager:            lm,
		dbInstance:            db,
		dbLock:                lock,
		primaryForSlots:       make(map[string]fsm.SlotRangeInfo),
		replicaClients:        make(map[string]net.Conn),
		lastSentLSN:           make(map[string]btree_core.LSN),
		stopChan:              make(chan struct{}),
		storageNodeAddresses:  make(map[string]string),
		replicationListenPort: os.Getenv("REPLICATION_LISTEN_PORT"),
	}
}

// Start initiates the replication manager's background goroutines.
func (rm *ReplicationManager) Start() {
	// Start listener for incoming replication streams (for Replica role)
	rm.wg.Add(1)
	go rm.startReplicationListener()

	// Start goroutine to manage outbound replication streams (for Primary role)
	rm.wg.Add(1)
	go rm.manageOutboundReplication()
}

// Stop gracefully shuts down the replication manager.
func (rm *ReplicationManager) Stop() {
	close(rm.stopChan)
	rm.wg.Wait() // Wait for all goroutines to finish

	// Close all active replica client connections
	rm.primaryMu.Lock()
	for addr, conn := range rm.replicaClients {
		conn.Close()
		delete(rm.replicaClients, addr)
	}
	rm.primaryMu.Unlock()

	if rm.replicationListener != nil {
		rm.replicationListener.Close()
	}
	log.Printf("INFO: ReplicationManager for %s stopped.", rm.nodeID)
}

// startReplicationListener starts a TCP listener for incoming replication streams (for Replica role).
func (rm *ReplicationManager) startReplicationListener() {
	defer rm.wg.Done()
	log.Println("DEBUG: Listening logs from: ", rm.replicaClients)
	addr := fmt.Sprintf(":%s", rm.replicationListenPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("FATAL: ReplicationManager: Failed to start replication listener on %s: %v", addr, err)
	}
	rm.replicationListener = listener
	log.Printf("INFO: ReplicationManager: Listening for replication streams on TCP %s", listener.Addr().String())

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-rm.stopChan:
				log.Println("INFO: Replication listener stopping.")
				return
			default:
				log.Printf("ERROR: ReplicationManager: Error accepting replication connection: %v", err)
				continue
			}
		}
		rm.wg.Add(1)
		go rm.handleInboundReplicationStream(conn)
	}
}

// handleInboundReplicationStream processes a single incoming replication stream (for Replica role).
// It reads log records from the primary and applies them locally.
func (rm *ReplicationManager) handleInboundReplicationStream(conn net.Conn) {
	defer rm.wg.Done()
	defer conn.Close()
	log.Printf("INFO: ReplicationManager: Accepted inbound replication stream from %s", conn.RemoteAddr().String())

	reader := bufio.NewReader(conn)
	for {
		select {
		case <-rm.stopChan:
			log.Println("INFO: Replication inbound stream handler stopping.")
			return
		default:
			// Read and deserialize log record
			var lr btree_core.LogRecord
			err := rm.logManager.ReadLogRecord(reader, &lr)
			if err == io.EOF {
				log.Printf("INFO: ReplicationManager: Primary %s closed replication stream.", conn.RemoteAddr().String())
				return
			}
			if err != nil {
				log.Printf("ERROR: ReplicationManager: Failed to read log record from stream %s: %v", conn.RemoteAddr().String(), err)
				return
			}

			log.Printf("DEBUG: ReplicationManager: Received log record LSN %d (Type: %v, Txn: %d, Page: %d) from Primary.",
				lr.LSN, lr.Type, lr.TxnID, lr.PageID)
			// var od any
			// err = json.Unmarshal(lr.OldData, &od)
			// if err != nil {
			// 	log.Println("Unamrshal failed")
			// }
			// var nd any
			// err = json.Unmarshal(lr.OldData, &nd)
			// if err != nil {
			// 	log.Println("Unamrshal failed")
			// }

			// log.Println("Debug LogRecord: ", od, nd)
			// Acquire global DB write lock for overall consistency, though page latches are more fine-grained.
			// This lock will prevent concurrent client requests from interfering with replication application.
			rm.dbLock.Lock()

			switch lr.Type {
			case btree_core.LogRecordTypeNewPage:
				log.Println("DEBUG Applying new page for: ", lr.PageID)
				// Ensure the underlying disk space is allocated for this PageID if it's beyond current numPages.
				// This is similar to LogManager.Recover's handling for NewPage.
				if lr.PageID.GetID() >= rm.dbInstance.GetNumPages() {
					log.Printf("INFO: ReplicationManager: Extending disk file for new page %d during replay.", lr.PageID)
					emptyPage := make([]byte, rm.dbInstance.GetPageSize())
					if writeErr := rm.dbInstance.WritePage(lr.PageID, emptyPage); writeErr != nil {
						log.Printf("ERROR: ReplicationManager: Failed to allocate/write empty page %d on disk: %v", lr.PageID, writeErr)
						rm.dbLock.Unlock()
						continue
					}
					// Update DiskManager's numPages
					if lr.PageID >= btree_core.PageID(rm.dbInstance.GetNumPages()) {
						rm.dbInstance.SetNumPages(uint64(lr.PageID) + 1)
					}
				}

				// Now fetch the page into the buffer pool. It should exist on disk or be newly allocated.
				page, fetchErr := rm.dbInstance.FetchPage(lr.PageID)
				if fetchErr != nil {
					log.Printf("ERROR: ReplicationManager: Failed to fetch page %d from BPM for NEW_PAGE record after disk ensuring: %v", lr.PageID, fetchErr)
					rm.dbLock.Unlock()
					continue
				}

				// The 'page' is already pinned by FetchPage.
				// Apply the new data.
				page.Lock()              // Acquire X-latch for writing to the page
				page.SetData(lr.NewData) // Overwrite page data with new data
				page.SetDirty(true)      // Mark page as dirty
				page.Unlock()            // Release X-latch

				if errUnpin := rm.dbInstance.UnpinPage(page.GetPageID(), true); errUnpin != nil {
					log.Printf("WARNING: ReplicationManager: Failed to unpin page %d after NEW_PAGE application: %v", page.GetPageID(), errUnpin)
				}

				// UNCOMMENT THIS LINE: Ensure the page is flushed to disk immediately.
				if errFlush := rm.dbInstance.FlushPage(page.GetPageID()); errFlush != nil {
					log.Printf("ERROR: ReplicationManager: Failed to flush page %d after NEW_PAGE application: %v", page.GetPageID(), errFlush)
				}

			case btree_core.LogRecordTypeUpdate:
				log.Println("DEBUG Applying update page for: ", lr.PageID)
				page, fetchErr := rm.dbInstance.FetchPage(lr.PageID)
				if fetchErr != nil {
					log.Printf("ERROR: ReplicationManager: Failed to fetch page %d from BPM for UPDATE record: %v", lr.PageID, fetchErr)
					rm.dbLock.Unlock()
					continue
				}

				page.Lock()              // Acquire X-latch for writing
				page.SetData(lr.NewData) // Overwrite page data with new data
				page.SetDirty(true)
				page.Unlock() // Release X-latch
				// lsn, err := rm.logManager.Append(&lr)
				// if err != nil {
				// 	log.Println("ERROR: Update ReplicationManager: failed to append logrecord New Page.", err)
				// }
				// log.Println("Update Appened log record using LM. ", lsn)
				if errUnpin := rm.dbInstance.UnpinPage(page.GetPageID(), true); errUnpin != nil {
					log.Printf("WARNING: ReplicationManager: Failed to unpin page %d after UPDATE application: %v", page.GetPageID(), errUnpin)
				}

				// UNCOMMENT THIS LINE: Ensure the page is flushed to disk immediately.
				if errFlush := rm.dbInstance.FlushPage(page.GetPageID()); errFlush != nil {
					log.Printf("ERROR: ReplicationManager: Failed to flush page %d after UPDATE application: %v", page.GetPageID(), errFlush)
				}

			case btree_core.LogRecordTypeRootChange: // NEW: Handle root page ID changes
				newRootPageID := btree_core.PageID(binary.LittleEndian.Uint64(lr.NewData))
				log.Printf("INFO: ReplicationManager: Replica applying root page ID change to %d (from LSN %d).", newRootPageID, lr.LSN)

				// Update the B-tree's in-memory rootPageID
				rm.dbInstance.SetRootPageID(newRootPageID, lr.TxnID) // Pass TxnID

				// Update the disk header for persistence
				if err := rm.dbInstance.GetDiskManager().UpdateHeaderField(func(h *btree_core.DBFileHeader) {
					h.RootPageID = newRootPageID
				}); err != nil {
					log.Printf("ERROR: ReplicationManager: Failed to update disk header with new root page ID %d: %v", newRootPageID, err)
				}

			case btree_core.LogRecordTypePrepare:
				log.Printf("DEBUG: ReplicationManager: Replica received PREPARE for Txn %d. (V1: no active processing beyond data apply)", lr.TxnID)
			case btree_core.LogRecordTypeCommitTxn:
				log.Printf("DEBUG: ReplicationManager: Replica received COMMIT for Txn %d.", lr.TxnID)
			case btree_core.LogRecordTypeAbortTxn:
				log.Printf("DEBUG: ReplicationManager: Replica received ABORT for Txn %d.", lr.TxnID)
			case btree_core.LogRecordTypeInsertKey, btree_core.LogRecordTypeDeleteKey, btree_core.LogRecordTypeNodeSplit, btree_core.LogRecordTypeNodeMerge:
				log.Printf("WARNING: ReplicationManager: Received structural/logical log record type %v (LSN %d). Physical page changes should already be covered by UPDATE/NEW_PAGE. Skipping.", lr.Type, lr.LSN)
			default:
				log.Printf("WARNING: ReplicationManager: Unhandled log record type %v during replay for LSN %d. Skipping.", lr.Type, lr.LSN)
			}
			rm.dbLock.Unlock() // Release DB write lock
		}
	}
}

func getReplicaPort(replicaAddr string) (string, error) {
	log.Println("Connecting to: ", replicaAddr)
	conn, err := net.DialTimeout("tcp", replicaAddr, storageNodeDialTimeout)
	replicaPort := replicationManager.replicationListenPort
	if err != nil {
		log.Printf("ERROR: ReplicationManager: Failed to connect to replica to get the port at %s: %v", replicaAddr, err)
		return replicaPort, err
	}
	log.Println("Sending command to ", replicaAddr)
	_, err = fmt.Fprint(conn, "SHOW CONFIG REPLICA_PORT \n")
	if err != nil {
		log.Printf("ERROR: ReplicationManager: Failed to send command to replica to get the port at %s: %v", replicaAddr, err)
		return replicaPort, err
	}
	log.Println("Sent command to ", replicaAddr)
	reader := bufio.NewReader(conn)
	defer conn.Close()
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("ERROR: ReplicationManager: Failed to send command %v", err)
		return replicaPort, err
	}
	log.Println("Read command from ", replicaAddr)
	log.Println("Replica port: ", line)
	parts := strings.Split(line, " ")
	if len(parts) < 2 {
		return replicaPort, err
	}
	if strings.TrimSpace(parts[0]) == "OK" {
		replicaPort = strings.TrimSpace(parts[1])
	}
	return replicaPort, nil
}

// manageOutboundReplication identifies primary roles and streams logs to replicas.
func (rm *ReplicationManager) manageOutboundReplication() {
	defer rm.wg.Done()

	ticker := time.NewTicker(replicationStreamInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rm.stopChan:
			log.Println("INFO: Outbound replication manager stopping.")
			return
		case <-ticker.C:
			// 1. Get latest slot assignments from Controller (via main's fetchShardMapFromController)
			// This is implicitly updated in `myAssignedSlots`.

			// 2. Determine if this node is primary for any slot ranges
			currentPrimarySlots := make(map[string]fsm.SlotRangeInfo)
			myAssignedSlotsMu.RLock()
			for rangeID, slotInfo := range myAssignedSlots {
				if slotInfo.PrimaryNodeID == rm.nodeID { // This node is primary for this range
					currentPrimarySlots[rangeID] = slotInfo
				}
			}
			myAssignedSlotsMu.RUnlock()

			rm.primaryMu.Lock()
			rm.primaryForSlots = currentPrimarySlots // Update local primary role cache
			rm.primaryMu.Unlock()

			// 3. For each primary slot, stream logs to its replicas
			for rangeID, slotInfo := range currentPrimarySlots {
				for _, replicaID := range slotInfo.ReplicaNodeIDs {
					// Get replica's address from storageNodeAddresses (from main's cache)
					rm.storageNodeAddressesMu.RLock()
					replicaAddr, ok := rm.storageNodeAddresses[replicaID]
					rm.storageNodeAddressesMu.RUnlock()

					if !ok || replicaAddr == "" {
						log.Printf("WARNING: ReplicationManager: Replica %s address not found for slot %s. Cannot stream logs.", replicaID, rangeID)
						continue
					}

					// Get or establish connection to replica
					rm.primaryMu.Lock()
					conn, connExists := rm.replicaClients[replicaAddr]
					if !connExists {
						var err error
						replicaPort, err := getReplicaPort(replicaAddr)
						if err != nil {
							log.Println("Failed to get the replication port using default replication port: ", replicaPort)
						}
						if rm.replicationListenPort == replicaPort {
							log.Println("Couldn't get the replica port: ", replicaPort)
							replicaPort = "9116"
						}
						log.Println("Sending logstreams to: ", "localhost:"+replicaPort)
						conn, err = net.DialTimeout("tcp", "localhost:"+replicaPort, storageNodeDialTimeout)
						if err != nil {
							log.Printf("ERROR: ReplicationManager: Failed to connect to replica %s at %s: %v", replicaID, replicaAddr, err)
							rm.primaryMu.Unlock()
							continue
						}
						rm.replicaClients[replicaAddr] = conn
						rm.lastSentLSN[replicaAddr] = 0 // Start from beginning of log for new connection
						log.Printf("INFO: ReplicationManager: Established new replication client connection to replica %s at %s.", replicaID, replicaAddr)
						// Stream logs to this replica
						go rm.streamLogsToReplica(conn, replicaAddr, replicaID)
					}
					rm.primaryMu.Unlock()
				}
			}
		}
	}
}

// streamLogsToReplica sends new log records to a specific replica.
func (rm *ReplicationManager) streamLogsToReplica(conn net.Conn, replicaAddr, replicaID string) {
	rm.primaryMu.Lock()
	lastLSN := rm.lastSentLSN[replicaAddr]
	rm.primaryMu.Unlock()

	log.Printf("DEBUG: ReplicationManager: Streaming logs to replica %s from LSN %d.", replicaID, lastLSN)

	// Get a log stream from LogManager
	logStream, err := rm.logManager.StartLogStream(lastLSN)
	if err != nil {
		log.Printf("ERROR: ReplicationManager: Failed to start log stream for replica %s: %v", replicaID, err)
		rm.closeReplicaConnection(replicaAddr)
		return
	}

	for lr := range logStream {
		serializedRecord, err := lr.Serialize()
		if err != nil {
			log.Printf("ERROR: ReplicationManager: Failed to serialize log record LSN %d for replica %s: %v", lr.LSN, replicaID, err)
			rm.closeReplicaConnection(replicaAddr)
			return
		}

		// Send serialized log record over TCP
		_, err = conn.Write(serializedRecord)
		if err != nil {
			log.Printf("ERROR: ReplicationManager: Failed to send log record LSN %d to replica %s: %v", lr.LSN, replicaID, err)
			rm.closeReplicaConnection(replicaAddr)
			return
		}

		rm.primaryMu.Lock()
		rm.lastSentLSN[replicaAddr] = lr.LSN // Update last sent LSN
		rm.primaryMu.Unlock()
		log.Printf("DEBUG: ReplicationManager: Sent log record LSN %d to replica %s %s.", lr.LSN, replicaID, replicaAddr)
	}
	log.Printf("INFO: ReplicationManager: Log stream to replica %s ended.", replicaID)
}

// closeReplicaConnection closes an outbound connection to a replica.
func (rm *ReplicationManager) closeReplicaConnection(addr string) {
	rm.primaryMu.Lock()
	defer rm.primaryMu.Unlock()
	if conn, ok := rm.replicaClients[addr]; ok {
		conn.Close()
		delete(rm.replicaClients, addr)
		delete(rm.lastSentLSN, addr)
		log.Printf("INFO: ReplicationManager: Closed outbound connection to replica %s.", addr)
	}
}

// initStorageNode initializes the Storage Node's database, identity, and background tasks.
func initStorageNode() error {
	var err error

	// 1. Get Storage Node Identity from environment variables
	myStorageNodeID = os.Getenv("STORAGE_NODE_ID")
	myStorageNodeAddr = os.Getenv("STORAGE_NODE_ADDR")
	if myStorageNodeID == "" || myStorageNodeAddr == "" {
		return fmt.Errorf("STORAGE_NODE_ID and STORAGE_NODE_ADDR environment variables must be set")
	}

	// Adjust dbFilePath to be unique per storage node
	uniqueDbFilePath := fmt.Sprintf("data/%s/%s_gojodb_shard.db", myStorageNodeID, myStorageNodeID)
	uniqueLogDir := fmt.Sprintf("data/%s/%s_logs", myStorageNodeID, myStorageNodeID)
	uniqueArchiveDir := fmt.Sprintf("data/%s/%s_archives", myStorageNodeID, myStorageNodeID)
	uniqueInvertedIndexFilePath := fmt.Sprintf("data/%s/%s_inverted_index.json", myStorageNodeID, myStorageNodeID) // NEW: Inverted index file path
	uniqueInvertedLogDir := fmt.Sprintf("data/%s/%s_inverted_logs", myStorageNodeID, myStorageNodeID)
	uniqueInvertedArchiveDir := fmt.Sprintf("data/%s/%s_inverted_archives", myStorageNodeID, myStorageNodeID)

	// 2. Initialize LogManager for this Storage Node
	logManager, err = btree_core.NewLogManager(uniqueLogDir, uniqueArchiveDir, logBufferSize, logSegmentSize)
	if err != nil {
		return fmt.Errorf("failed to create LogManager for storage node %s: %w", myStorageNodeID, err)
	}

	// 3. Initialize or open the B-tree database for this Storage Node
	dbInstance, err = btree_core.OpenBTreeFile[string, string]( // Use string keys
		uniqueDbFilePath,
		btree_core.DefaultKeyOrder[string], // Default order for strings
		btree_core.KeyValueSerializer[string, string]{
			SerializeKey:     btree_core.SerializeString,
			DeserializeKey:   btree_core.DeserializeString,
			SerializeValue:   btree_core.SerializeString,
			DeserializeValue: btree_core.DeserializeString,
		},
		bufferPoolSize,
		dbPageSize,
		logManager,
	)

	if err != nil {
		// If file not found, create a new one
		if os.IsNotExist(err) || strings.Contains(err.Error(), "database file not found") {
			log.Printf("INFO: Database file %s for Storage Node %s not found. Creating new database.", uniqueDbFilePath, myStorageNodeID)
			dbInstance, err = btree_core.NewBTreeFile[string, string]( // Use string keys
				uniqueDbFilePath,
				bTreeDegree,
				btree_core.DefaultKeyOrder[string],
				btree_core.KeyValueSerializer[string, string]{
					SerializeKey:     btree_core.SerializeString,
					DeserializeKey:   btree_core.DeserializeString,
					SerializeValue:   btree_core.SerializeString,
					DeserializeValue: btree_core.DeserializeString,
				},
				bufferPoolSize,
				dbPageSize,
				logManager,
			)
			if err != nil {
				return fmt.Errorf("failed to create new BTree file for storage node %s: %w", myStorageNodeID, err)
			}
		} else {
			// Other errors during open are fatal
			return fmt.Errorf("failed to open existing BTree file for storage node %s: %w", myStorageNodeID, err)
		}
	}
	log.Printf("INFO: Storage Node %s database initialized successfully. Root PageID: %d", myStorageNodeID, dbInstance.GetRootPageID())

	// NEW: 4. Initialize Inverted Index for this Storage Node
	invertedIndexInstance, err = inverted_index.NewInvertedIndex(uniqueInvertedIndexFilePath, uniqueInvertedLogDir, uniqueInvertedArchiveDir)
	if err != nil {
		return fmt.Errorf("failed to create InvertedIndex for storage node %s: %w", myStorageNodeID, err)
	}
	log.Printf("INFO: Storage Node %s inverted index initialized successfully.", myStorageNodeID)

	// 5. Initialize local shard map cache
	myAssignedSlots = make(map[string]fsm.SlotRangeInfo)

	// --- NEW: Initialize Replication Manager ---
	replicationManager = NewReplicationManager(myStorageNodeID, logManager, dbInstance, &dbLock)
	replicationManager.Start() // Start replication background tasks
	// --- END NEW ---

	// 6. Start background tasks
	go sendHeartbeatsToController()
	go fetchShardMapFromController(replicationManager)

	return nil
}

// closeStorageNode closes the B-tree, LogManager, and InvertedIndex cleanly.
func closeStorageNode() {
	log.Println("INFO: Shutting down Storage Node database...")
	if dbInstance != nil {
		if err := dbInstance.Close(); err != nil {
			log.Printf("ERROR: Failed to close BTree for Storage Node %s: %v", myStorageNodeID, err)
		}
	}
	if logManager != nil {
		if err := logManager.Close(); err != nil {
			log.Printf("ERROR: Failed to close LogManager for Storage Node %s: %v", myStorageNodeID, err)
		}
	}
	// NEW: Close Inverted Index
	if invertedIndexInstance != nil {
		if err := invertedIndexInstance.Close(); err != nil {
			log.Printf("ERROR: Failed to close InvertedIndex for Storage Node %s: %v", myStorageNodeID, err)
		}
	}
	log.Println("INFO: Storage Node shutdown complete.")
}

// sendHeartbeatsToController periodically sends heartbeats to all known Controller addresses.
func sendHeartbeatsToController() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	controllerAddrs := strings.Split(controllerHTTPAddresses, ",")
	heartbeatMessage := fmt.Sprintf("%s:%s", myStorageNodeID, myStorageNodeAddr)

	for range ticker.C {
		// Try sending heartbeat to all known controllers
		for _, addr := range controllerAddrs {
			// Extract port from controller HTTP address and add 1 for heartbeat UDP port
			parts := strings.Split(addr, ":")
			if len(parts) != 2 {
				log.Printf("WARNING: Invalid controller HTTP address format: %s", addr)
				continue
			}
			_, err := strconv.Atoi(parts[1])
			if err != nil {
				log.Printf("WARNING: Invalid port in controller HTTP address %s: %v", addr, err)
				continue
			}
			heartbeatTargetPort := controllerHeartbeatTargetPort // Assuming heartbeat listener is HTTP port + 1

			conn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: heartbeatTargetPort})
			if err != nil {
				log.Printf("ERROR: Failed to dial UDP for heartbeat to %s (port %d): %v", addr, heartbeatTargetPort, err)
				continue
			}
			_, err = conn.Write([]byte(heartbeatMessage))
			if err != nil {
				log.Printf("ERROR: Failed to send heartbeat to Controller %s (port %d): %v", addr, heartbeatTargetPort, err)
			} else {
				//log.Printf("DEBUG: Storage Node %s sent heartbeat to Controller %s (port %d).", myStorageNodeID, addr, heartbeatTargetPort)
			}
			conn.Close() // Close connection after sending
		}
	}
}

// fetchShardMapFromController periodically fetches the latest slot assignments from the Controller leader.
func fetchShardMapFromController(rm *ReplicationManager) {
	ticker := time.NewTicker(shardMapFetchInterval)
	defer ticker.Stop()

	controllerAddrs := strings.Split(controllerHTTPAddresses, ",")

	for range ticker.C {
		leaderAddr := ""
		// 1. Find the current Controller leader
		for _, addr := range controllerAddrs {
			resp, err := http.Get(fmt.Sprintf("http://%s/status", addr))
			if err != nil {
				// log.Printf("DEBUG: Failed to reach Controller %s for status: %v", addr, err) // Too noisy
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				bodyBytes, _ := io.ReadAll(resp.Body)
				bodyString := string(bodyBytes)
				if strings.Contains(bodyString, "State: Leader") {
					// Extract leader address from status response (assuming it's in "Leader: <addr>")
					leaderLine := ""
					for _, line := range strings.Split(bodyString, "\n") {
						if strings.HasPrefix(line, "Leader:") {
							leaderLine = strings.TrimSpace(strings.TrimPrefix(line, "Leader:"))
							break
						}
					}
					if leaderLine != "" && leaderLine != "unknown" && leaderLine != "none" {
						leaderAddr = addr
						break // Found leader, stop searching
					}
				}
			}
		}

		if leaderAddr == "" {
			//log.Printf("WARNING: No Controller leader found. Cannot fetch shard map.")
			continue
		}
		//log.Printf("DEBUG: Controller leader found at: %s", leaderAddr)

		// 2. Fetch all slot assignments from the leader
		resp, err := http.Get(fmt.Sprintf("http://%s/admin/get_all_slot_assignments", leaderAddr)) // New endpoint needed
		if err != nil {
			log.Printf("ERROR: Failed to fetch slot assignments from Controller leader %s: %v", leaderAddr, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			log.Printf("ERROR: Failed to fetch slot assignments: HTTP Status %s", resp.Status)
			continue
		}

		var allSlotAssignments map[string]fsm.SlotRangeInfo
		if err := json.NewDecoder(resp.Body).Decode(&allSlotAssignments); err != nil {
			log.Printf("ERROR: Failed to decode slot assignments from Controller: %v", err)
			continue
		}

		// 3. Update local cache with assignments relevant to this node
		newAssignedSlots := make(map[string]fsm.SlotRangeInfo)
		for rangeID, slotInfo := range allSlotAssignments {
			if slotInfo.AssignedNodeID == myStorageNodeID {
				newAssignedSlots[rangeID] = slotInfo
			}
			// 2. Fetch all slot assignments from the leader
			for _, nodeID := range slotInfo.ReplicaNodeIDs {
				resp, err = http.Get(fmt.Sprintf("http://%s/admin/get_storage_node_address?nodeID=%s", leaderAddr, nodeID)) // New endpoint needed
				if err != nil {
					log.Printf("ERROR: Failed to fetch slot assignments from Controller leader %s: %v", leaderAddr, err)
					continue
				}
				body, _ := ioutil.ReadAll(resp.Body)
				rm.storageNodeAddressesMu.Lock()
				rm.storageNodeAddresses[nodeID] = string(body)
				rm.storageNodeAddressesMu.Unlock()
			}
		}

		myAssignedSlotsMu.Lock()
		myAssignedSlots = newAssignedSlots
		myAssignedSlotsMu.Unlock()

		//rm.primaryForSlots = newAssignedSlots
		//log.Printf("INFO: Storage Node %s fetched and cached %d assigned slot ranges.", myStorageNodeID, len(myAssignedSlots))
	}
}

// parseRequest parses a raw string command into a Request struct.
// It's updated to handle 2PC commands and TEXT_SEARCH.
func parseRequest(raw string) (Request, error) {
	parts := strings.Fields(raw)
	if len(parts) == 0 {
		return Request{}, fmt.Errorf("empty command")
	}

	command := strings.ToUpper(parts[0])
	req := Request{Command: command, TxnID: 0} // Default TxnID to 0 for auto-commit

	switch command {
	case "PUT", "PUT_TEXT": // NEW: Handle PUT_TEXT
		if len(parts) < 3 {
			return Request{}, fmt.Errorf("%s requires key and value", command)
		}
		req.Key = parts[1]
		req.Value = strings.Join(parts[2:], " ")
	case "GET", "DELETE":
		if len(parts) < 2 {
			return Request{}, fmt.Errorf("%s requires a key", command)
		}
		req.Key = parts[1]
	case "SHOW":
		if len(parts) < 3 {
			return Request{}, fmt.Errorf("%s requires a key and value", command)
		}
		req.Key = parts[1]
		req.Value = parts[2]
	case "SIZE":
		// No additional arguments needed

	case "GET_RANGE":
		log.Println("RANGE QUERY: ", raw, parts)
		if len(parts) < 2 {
			return Request{}, fmt.Errorf("invalid range query, %s", req.Command)
		}
		req.StartKey = parts[0]
		req.EndKey = parts[1]

	case "TEXT_SEARCH": // NEW: Handle TEXT_SEARCH
		if len(parts) < 2 {
			return Request{}, fmt.Errorf("TEXT_SEARCH requires a query string")
		}
		req.Query = strings.Join(parts[1:], " ") // The rest is the query
	case "PREPARE":
		if len(parts) < 3 {
			return Request{}, fmt.Errorf("PREPARE requires TxnID and operations JSON")
		}
		txnID, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid TxnID format: %w", err)
		}
		req.TxnID = txnID
		req.Value = strings.Join(parts[2:], " ") // Operations JSON string
	case "COMMIT", "ABORT":
		if len(parts) < 2 {
			return Request{}, fmt.Errorf("%s requires TxnID", command)
		}
		txnID, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid TxnID format: %w", err)
		}
		req.TxnID = txnID
	default:
		return Request{}, fmt.Errorf("unknown command: %s", command)
	}
	return req, nil
}

// handleRequest processes a parsed Request and returns a Response.
// It now includes 2PC command handling and passes TxnID to B-tree operations.
// It also handles new PUT_TEXT and TEXT_SEARCH commands for the inverted index.
func handleRequest(req Request) Response {
	var resp Response
	var err error

	// --- Sharding Awareness: Check if key belongs to this node (for data ops) ---
	// 2PC commands (PREPARE, COMMIT, ABORT) are sent directly to the participant,
	// so they don't need shard routing check here.
	// TEXT_SEARCH is handled by any node (for V1), so it doesn't need shard routing check.
	isDataOperation := req.Command == "PUT" || req.Command == "PUT_TEXT" || req.Command == "GET" || req.Command == "DELETE"
	if isDataOperation {
		targetSlot := fsm.GetSlotForHashKey(req.Key)

		myAssignedSlotsMu.RLock()
		assignedToMe := false
		for _, slotInfo := range myAssignedSlots {
			if targetSlot >= slotInfo.StartSlot && targetSlot <= slotInfo.EndSlot {
				if slotInfo.AssignedNodeID == myStorageNodeID {
					assignedToMe = true
					break
				}
				// If assigned to another node, we could return a REDIRECT message here
				resp = Response{Status: "REDIRECT", Message: fmt.Sprintf("Key '%s' (slot %d) belongs to node %s.", req.Key, targetSlot, slotInfo.AssignedNodeID)}
				myAssignedSlotsMu.RUnlock()
				return resp
			}
		}
		myAssignedSlotsMu.RUnlock()
		log.Println("DEBUG: assignedToMe: ", assignedToMe)
		// if !assignedToMe {
		// 	resp = Response{Status: "ERROR", Message: fmt.Sprintf("Key '%s' (slot %d) does not belong to this node. No assignment found or assigned to unknown node.", req.Key, targetSlot)}
		// 	return resp
		// }
	}
	// --- End Sharding Awareness ---

	switch req.Command {
	case "PUT":
		dbLock.Lock()                                          // Acquire write lock for PUT
		err = dbInstance.Insert(req.Key, req.Value, req.TxnID) // Pass TxnID
		dbLock.Unlock()                                        // Release write lock
		if err != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("PUT failed: %v", err)}
		} else {
			resp = Response{Status: "OK", Message: "Key-value pair inserted/updated."}
		}
	case "PUT_TEXT": // NEW: Handle PUT_TEXT command
		// Extract raw text from GOJODB.TEXT() wrapper
		pureText := ""
		if strings.HasPrefix(req.Value, gojoDBTextPrefix) && strings.HasSuffix(req.Value, gojoDBTextSuffix) {
			pureText = req.Value[len(gojoDBTextPrefix) : len(req.Value)-len(gojoDBTextSuffix)]
		} else {
			resp = Response{Status: "ERROR", Message: "PUT_TEXT command requires value wrapped in GOJODB.TEXT()."}
			return resp
		}

		// Insert into Inverted Index (non-transactional for V1)
		invertedIndexInstance.Insert(pureText, req.Key)
		log.Printf("INFO: Inverted Index: Indexed key '%s' with text '%s'", req.Key, pureText)

		// Then, insert the original value (including wrapper) into the B-tree
		dbLock.Lock()
		err = dbInstance.Insert(req.Key, req.Value, req.TxnID)
		dbLock.Unlock()
		if err != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("PUT_TEXT (B-tree part) failed: %v", err)}
		} else {
			resp = Response{Status: "OK", Message: "Key-value pair and text indexed."}
		}
	case "GET":
		dbLock.RLock() // Acquire read lock for GET
		val, found, searchErr := dbInstance.Search(req.Key)
		dbLock.RUnlock() // Release read lock
		if searchErr != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("GET failed: %v", searchErr)}
		} else if found {
			resp = Response{Status: "OK", Message: val}
		} else {
			resp = Response{Status: "NOT_FOUND", Message: fmt.Sprintf("Key '%s' not found.", req.Key)}
		}
	case "GET_RANGE":
		if strings.TrimSpace(req.StartKey) == "" || strings.TrimSpace(req.EndKey) == "" {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("Invalid startKey = '%s'and endKey = '%s'", req.StartKey, req.EndKey)}
		}
		dbLock.RLock() // Abort releases locks, not acquires DB lock
		var iterator btree_core.BTreeIterator[string, string]
		var err error
		if req.StartKey == "*" || req.EndKey == "*" {
			iterator, err = dbInstance.FullScan()
		} else {
			iterator, err = dbInstance.Iterator(req.StartKey, req.EndKey)
		}
		dbLock.RUnlock()
		if err != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("Failed to create iterator: %v", err)}
		} else {
			var result []Entry
			for {
				key, val, isNext, iterErr := iterator.Next()
				if iterErr != nil || !isNext {
					log.Println("ITERATOR NEXT: ", isNext, iterErr)
					break
				}
				result = append(result, Entry{Key: key, Value: val})
				log.Println("RESPONSE: ", result)
			}
			b, _ := json.Marshal(result)
			resp = Response{Status: "OK", Message: string(b)}
			iterator.Close()
		}
	case "TEXT_SEARCH": // NEW: Handle TEXT_SEARCH command
		// Perform search on the inverted index
		resultKeys, err := invertedIndexInstance.Search(req.Query)
		if err != nil {
			log.Println("Encountered error while inverted index search: ", err)
		}
		log.Printf("INFO: Inverted Index: Search for '%s' returned %d keys.", req.Query, len(resultKeys))

		var foundEntries []Entry
		// For each key found in the inverted index, retrieve the full entry from the B-tree
		for _, key := range resultKeys {
			dbLock.RLock() // Acquire read lock for B-tree GET
			val, found, searchErr := dbInstance.Search(key)
			dbLock.RUnlock() // Release read lock
			if searchErr != nil {
				log.Printf("ERROR: TEXT_SEARCH: Failed to retrieve key '%s' from B-tree: %v", key, searchErr)
				continue // Continue to next key
			}
			if found {
				foundEntries = append(foundEntries, Entry{Key: key, Value: val})
			}
		}

		// Marshal the list of found entries into JSON
		entriesJSON, marshalErr := json.Marshal(foundEntries)
		if marshalErr != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("Failed to marshal search results: %v", marshalErr)}
		} else {
			resp = Response{Status: "OK", Message: string(entriesJSON)}
		}
	case "DELETE":
		dbLock.Lock()                               // Acquire write lock for DELETE
		err = dbInstance.Delete(req.Key, req.TxnID) // Pass TxnID
		dbLock.Unlock()                             // Release write lock
		if err != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("DELETE failed: %v", err)}
		} else {
			resp = Response{Status: "OK", Message: "Key deleted."}
		}
	case "SIZE":
		// SIZE command is global to this node's local B-tree, not sharded.
		dbLock.RLock() // Acquire read lock for SIZE
		size, sizeErr := dbInstance.GetSize()
		dbLock.RUnlock() // Release read lock
		if sizeErr != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("SIZE failed: %v", sizeErr)}
		} else {
			resp = Response{Status: "OK", Message: fmt.Sprintf("%d", size)}
		}
	// --- NEW: 2PC Command Handlers ---
	case "PREPARE":
		// Operations are passed as a JSON string in req.Value
		log.Println("Server Operations: ", req.Value)
		ops, jsonErr := deserializeOperations(req.Value)
		if jsonErr != nil {
			resp = Response{Status: "VOTE_ABORT", Message: fmt.Sprintf("Invalid operations JSON for PREPARE: %v", jsonErr)}
			return resp
		}
		log.Println("DEBUG: deserialize: ", ops, jsonErr)
		dbLock.Lock() // Prepare needs to acquire locks on keys, not the whole DB
		// The B-tree's Prepare method will handle key-level locking.
		err = dbInstance.Prepare(req.TxnID, ops)
		dbLock.Unlock()
		if err != nil {
			resp = Response{Status: "VOTE_ABORT", Message: fmt.Sprintf("PREPARE failed for Txn %d: %v", req.TxnID, err)}
		} else {
			resp = Response{Status: "VOTE_COMMIT", Message: fmt.Sprintf("Txn %d prepared.", req.TxnID)}
		}
	case "COMMIT":
		// dbLock.Lock() // Commit releases locks, not acquires DB lock
		err = dbInstance.Commit(req.TxnID)
		// dbLock.Unlock()
		if err != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("COMMIT failed for Txn %d: %v", req.TxnID, err)}
		} else {

			resp = Response{Status: "COMMITTED", Message: fmt.Sprintf("Txn %d committed.", req.TxnID)}

		}
	case "ABORT":
		// dbLock.Lock() // Abort releases locks, not acquires DB lock
		err = dbInstance.Abort(req.TxnID)
		// dbLock.Unlock()
		if err != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("ABORT failed for Txn %d: %v", req.TxnID, err)}
		} else {
			resp = Response{Status: "ABORTED", Message: fmt.Sprintf("Txn %d aborted.", req.TxnID)}
		}

	case "SHOW":
		switch req.Key {
		case "CONFIG":
			switch req.Value {
			case "REPLICA_PORT":
				resp = Response{Status: "OK", Message: fmt.Sprint(os.Getenv("REPLICATION_LISTEN_PORT"))}
				log.Println("SHOW CONFIG REPLICA_PORT: ", resp)
			default:
				resp = Response{Status: "ERROR", Message: fmt.Sprintf("Unsupported command: %s", req.Value)}
			}
		default:
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("Unsupported command: %s", req.Key)}
		}
	default:
		resp = Response{Status: "ERROR", Message: fmt.Sprintf("Unsupported command: %s", req.Command)}
	}
	return resp
}

// handleConnection manages a single client connection.
func handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("INFO: Client connected to Storage Node %s: %s", myStorageNodeID, conn.RemoteAddr().String())

	reader := bufio.NewReader(conn)
	for {
		// Read client command, delimited by newline
		netData, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Printf("INFO: Client disconnected from Storage Node %s: %s", myStorageNodeID, conn.RemoteAddr().String())
			} else {
				log.Printf("ERROR: Error reading from client %s: %v", conn.RemoteAddr().String(), err)
			}
			return
		}

		rawCommand := strings.TrimSpace(netData)
		if rawCommand == "" {
			continue // Ignore empty lines
		}
		log.Printf("DEBUG: Received command from %s: '%s'", conn.RemoteAddr().String(), rawCommand)

		// Parse request
		req, err := parseRequest(rawCommand)
		if err != nil {
			resp := Response{Status: "ERROR", Message: fmt.Sprintf("Invalid request: %v", err)}
			_, writeErr := conn.Write([]byte(fmt.Sprintf("%s %s\n", resp.Status, resp.Message)))
			if writeErr != nil {
				log.Printf("ERROR: Error writing response to client: %v", writeErr)
			}
			continue
		}

		// Process request
		resp := handleRequest(req)

		// Send response back to client
		responseString := fmt.Sprintf("%s %s\n", resp.Status, resp.Message)
		_, writeErr := conn.Write([]byte(responseString))
		if writeErr != nil {
			log.Printf("ERROR: Error writing response to client: %v", writeErr)
		}
	}
}

// serializeOperations is a helper to serialize a list of operations for sending to Storage Nodes.
// This is used by the API Service (Coordinator) to send operations to participants.
func serializeOperations(ops []btree_core.TransactionOperation) string {
	var builder strings.Builder
	for i, op := range ops {
		opJSON, _ := json.Marshal(op) // Should handle errors in production
		builder.WriteString(string(opJSON))
		if i < len(ops)-1 {
			builder.WriteString("|") // Delimiter between operations
		}
	}
	return builder.String()
}

// deserializeOperations is a helper to deserialize a list of operations from a string.
// This is used by the Storage Node (Participant) to receive operations from the Coordinator.
func deserializeOperations(s string) ([]btree_core.TransactionOperation, error) {
	var ops TransactionOperations
	if err := json.Unmarshal([]byte(s), &ops); err != nil {
		return nil, fmt.Errorf("failed to unmarshal operation part '%s': %w", s, err)
	}

	return ops.Operations, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Include file and line number in logs for debugging

	// --- Initialize Storage Node ---
	if err := initStorageNode(); err != nil {
		log.Fatalf("FATAL: Storage Node initialization failed: %v", err)
	}
	defer closeStorageNode() // Ensure database is closed on program exit

	// Start TCP listener for client requests
	listener, err := net.Listen("tcp", myStorageNodeAddr)
	if err != nil {
		log.Fatalf("FATAL: Error listening on %s: %v", myStorageNodeAddr, err)
	}
	defer listener.Close()

	log.Printf("INFO: GojoDB Storage Node %s listening for client traffic on %s", myStorageNodeID, myStorageNodeAddr)
	log.Println("INFO: Commands: PUT <key> <value>, PUT_TEXT <key> GOJODB.TEXT(<value>), GET <key>, DELETE <key>, SIZE, TEXT_SEARCH <query>") // NEW: Commands
	log.Println("INFO: 2PC Commands (from Coordinator only): PREPARE <TxnID> <ops_json>, COMMIT <TxnID>, ABORT <TxnID>")

	for {
		// Accept incoming connections
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("ERROR: Error accepting connection: %v", err)
			continue
		}
		// Handle connections in a new goroutine
		go handleConnection(conn)
	}
}
