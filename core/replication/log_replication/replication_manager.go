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
	fsm "github.com/sushant-115/gojodb/core/replication/raft_consensus"
)

const (
	LOG_BTREE = iota + 1
	LOG_INVERTED_INDEX
)

var (
	replicationStreamInterval = 10 * time.Second // How often primary checks for new logs to send
	storageNodeDialTimeout    = 2 * time.Second  // Timeout for connecting to a storage node
	// Sharding awareness
	myAssignedSlotsMu sync.RWMutex
)

// ReplicationManager manages sending and receiving log streams for replication.
type ReplicationManager struct {
	nodeID                             string
	logManager                         *btree_core.LogManager // B-tree's log manager
	dbInstance                         *btree_core.BTree[string, string]
	dbLock                             *sync.RWMutex
	invertedIndex                      *inverted_index.InvertedIndex // NEW: Inverted index instance
	replicationBTreeListenPort         string
	replicationInvertedIndexListenPort string

	// State for Primary role (sending logs)
	primaryForSlots        map[string]fsm.SlotRangeInfo // SlotRangeID -> SlotRangeInfo (where this node is primary)
	myAssignedSlots        map[string]fsm.SlotRangeInfo
	replicaClients         map[string]net.Conn       // ReplicaNodeAddr -> TCP connection to replica
	lastSentLSN            map[string]btree_core.LSN // ReplicaNodeAddr + "_btree" or "_inverted_index" -> Last LSN sent to this replica
	primaryMu              sync.Mutex                // Protects primary role state
	storageNodeAddressesMu sync.RWMutex
	storageNodeAddresses   map[string]string

	// State for Replica role (receiving logs)
	bTreeReplicationListener       net.Listener // TCP listener for incoming replication streams
	invertedIdxReplicationListener net.Listener // TCP listener for incoming replication streams
	stopChan                       chan struct{}
	wg                             sync.WaitGroup
}

// NewReplicationManager creates and initializes a ReplicationManager.
func NewReplicationManager(nodeID string, lm *btree_core.LogManager, db *btree_core.BTree[string, string], lock *sync.RWMutex, ii *inverted_index.InvertedIndex) *ReplicationManager {
	return &ReplicationManager{
		nodeID:                             nodeID,
		logManager:                         lm,
		dbInstance:                         db,
		dbLock:                             lock,
		invertedIndex:                      ii, // Store the inverted index instance
		primaryForSlots:                    make(map[string]fsm.SlotRangeInfo),
		replicaClients:                     make(map[string]net.Conn),
		lastSentLSN:                        make(map[string]btree_core.LSN),
		stopChan:                           make(chan struct{}),
		storageNodeAddresses:               make(map[string]string),
		replicationBTreeListenPort:         os.Getenv("BTREE_REPLICATION_LISTEN_PORT"),
		replicationInvertedIndexListenPort: os.Getenv("INVERTED_IDX_REPLICATION_LISTEN_PORT"),
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
		delete(rm.lastSentLSN, addr+"_btree") // Clean up both LSNs
		delete(rm.lastSentLSN, addr+"_inverted_index")
	}
	rm.primaryMu.Unlock()

	if rm.bTreeReplicationListener != nil {
		rm.bTreeReplicationListener.Close()
	}

	if rm.invertedIdxReplicationListener != nil {
		rm.invertedIdxReplicationListener.Close()
	}
	log.Printf("INFO: ReplicationManager for %s stopped.", rm.nodeID)
}

// startReplicationListener starts a TCP listener for incoming replication streams (for Replica role).
func (rm *ReplicationManager) startReplicationListener() {
	defer rm.wg.Done()
	rm.wg.Add(2)
	go rm.startReplicationListenerInternal(rm.replicationBTreeListenPort, LOG_BTREE)

	go rm.startReplicationListenerInternal(rm.replicationInvertedIndexListenPort, LOG_INVERTED_INDEX)
}

func (rm *ReplicationManager) startReplicationListenerInternal(port string, logType int) {
	defer rm.wg.Done()
	log.Println("DEBUG: Listening logs from: ", rm.replicaClients)
	addr := fmt.Sprintf(":%s", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("FATAL: ReplicationManager: Failed to start replication listener on %s: %v", addr, err)
	}
	if logType == LOG_BTREE {
		rm.bTreeReplicationListener = listener
	} else {
		rm.invertedIdxReplicationListener = listener
	}
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
		go rm.handleInboundReplicationStream(conn, logType)
	}
}

// handleInboundReplicationStream processes a single incoming replication stream (for Replica role).
// It reads log records from the primary and applies them locally.
func (rm *ReplicationManager) handleInboundReplicationStream(conn net.Conn, logType int) {
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
			// Use the B-tree's log manager's ReadLogRecord as a generic reader for the stream
			// Assuming all log records (B-tree and Inverted Index) are serialized in the same format.
			err := rm.logManager.ReadLogRecord(reader, &lr)
			if err == io.EOF {
				log.Printf("INFO: ReplicationManager: Primary %s closed replication stream.", conn.RemoteAddr().String())
				return
			}
			if err != nil {
				log.Printf("ERROR: ReplicationManager: Failed to read log record from stream %s: %v", conn.RemoteAddr().String(), err)
				return
			}

			log.Printf("DEBUG: ReplicationManager: Received log record LSN %d (Type: %v, Txn: %d, Page: %d) from Primary (%s) ",
				lr.LSN, lr.Type, lr.TxnID, lr.PageID, conn.RemoteAddr().String())

			// Differentiate between B-tree and Inverted Index log records
			if logType == LOG_INVERTED_INDEX {
				// This is likely an Inverted Index term dictionary update
				// The NewData contains: term (null-terminated) + metadata JSON
				parts := bytes.SplitN(lr.NewData, []byte{0}, 2)
				if len(parts) != 2 {
					log.Printf("ERROR: ReplicationManager: Invalid Inverted Index log record format for LSN %d. Skipping.", lr.LSN)
					continue
				}
				term := string(parts[0])
				metadataBytes := string(parts[1])

				log.Printf("INFO: ReplicationManager: Replica applied Inverted Index update for term '%s' (LSN %d).", term, lr.LSN)

				if err := rm.invertedIndex.Insert(metadataBytes, term); err != nil {
					log.Println("failed to write the log entry: ", term, metadataBytes)
				}

			} else {
				// This is a B-tree log record, apply as before
				rm.dbLock.Lock() // Acquire global DB write lock for B-tree operations
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

					// Ensure the page is flushed to disk immediately.
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
					if errUnpin := rm.dbInstance.UnpinPage(page.GetPageID(), true); errUnpin != nil {
						log.Printf("WARNING: ReplicationManager: Failed to unpin page %d after UPDATE application: %v", page.GetPageID(), errUnpin)
					}

					// Ensure the page is flushed to disk immediately.
					if errFlush := rm.dbInstance.FlushPage(page.GetPageID()); errFlush != nil {
						log.Printf("ERROR: ReplicationManager: Failed to flush page %d after UPDATE application: %v", page.GetPageID(), errFlush)
					}

				case btree_core.LogRecordTypeRootChange: // Handle root page ID changes
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
					log.Printf(`WARNING: ReplicationManager: Received structural/logical log record type %v (LSN %d). 
						Physical page changes should already be covered by UPDATE/NEW_PAGE. Skipping.`, lr.Type, lr.LSN)
				default:
					log.Printf("WARNING: ReplicationManager: Unhandled log record type %v during replay for LSN %d. Skipping.", lr.Type, lr.LSN)
				}
				rm.dbLock.Unlock() // Release DB write lock
			}
		}
	}
}

func getReplicaPort(replicaAddr string) (string, string, error) {
	// log.Println("Connecting to: ", replicaAddr)
	conn, err := net.DialTimeout("tcp", replicaAddr, storageNodeDialTimeout)
	bTreeReplicaPort := ""
	idxReplicaPort := ""
	if err != nil {
		log.Printf("ERROR: ReplicationManager: Failed to connect to replica to get the port at %s: %v", replicaAddr, err)
		return bTreeReplicaPort, idxReplicaPort, err
	}
	// log.Println("Sending command to ", replicaAddr)
	_, err = fmt.Fprint(conn, "SHOW CONFIG REPLICA_PORT \n")
	if err != nil {
		log.Printf("ERROR: ReplicationManager: Failed to send command to replica to get the port at %s: %v", replicaAddr, err)
		return bTreeReplicaPort, idxReplicaPort, err
	}
	// log.Println("Sent command to ", replicaAddr)
	reader := bufio.NewReader(conn)
	defer conn.Close()
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("ERROR: ReplicationManager: Failed to send command %v", err)
		return bTreeReplicaPort, idxReplicaPort, err
	}
	// log.Println("Read command from ", replicaAddr)
	// log.Println("Replica port: ", line)
	parts := strings.Split(line, " ")
	if len(parts) < 3 {
		return bTreeReplicaPort, idxReplicaPort, err
	}
	if strings.TrimSpace(parts[0]) == "OK" {
		bTreeReplicaPort = strings.TrimSpace(parts[1])
		idxReplicaPort = strings.TrimSpace(parts[2])
	}
	return bTreeReplicaPort, idxReplicaPort, nil
}

func (rm *ReplicationManager) SetStorageNodeAddress(nodeID, address string) {
	rm.storageNodeAddressesMu.Lock()
	rm.storageNodeAddresses[nodeID] = string(address)
	rm.storageNodeAddressesMu.Unlock()
}

func (rm *ReplicationManager) SetAssignedNodeAddress(myAssignedSlots map[string]fsm.SlotRangeInfo) {
	rm.primaryMu.Lock()
	rm.myAssignedSlots = myAssignedSlots
	rm.primaryMu.Unlock()
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
			for rangeID, slotInfo := range rm.myAssignedSlots {
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
					bTreeReplicaPort, idxReplicaPort, err := getReplicaPort(replicaAddr)
					if err != nil {
						log.Println("Failed to get the replication port using default replication port: ", bTreeReplicaPort, idxReplicaPort)
					}
					if rm.replicationBTreeListenPort == bTreeReplicaPort || rm.replicationInvertedIndexListenPort == idxReplicaPort {
						log.Println("Couldn't get the replica port: ", bTreeReplicaPort, idxReplicaPort)
					}
					// log.Println("Sending logstreams to: ", "localhost:"+bTreeReplicaPort, " for Btree and to localhost:"+idxReplicaPort, " for InvertedIdx")
					rm.primaryMu.Lock()
					bTreeConn, connExists := rm.replicaClients[replicaAddr+"_btree_conn"]
					if !connExists {
						var err error

						bTreeConn, err = net.DialTimeout("tcp", "localhost:"+bTreeReplicaPort, storageNodeDialTimeout)
						if err != nil {
							log.Printf("ERROR: ReplicationManager: Failed to connect to replica %s at %s: %v", replicaID, replicaAddr, err)
							rm.primaryMu.Unlock()
							continue
						}
						rm.replicaClients[replicaAddr+"_btree_conn"] = bTreeConn
						rm.lastSentLSN[replicaAddr+"_btree"] = 0 // Initialize B-tree LSN
						log.Printf("INFO: ReplicationManager: Established new replication client connection to replica %s at %s.", replicaID, replicaAddr)
						// Stream logs to this replica
						go rm.streamLogsToReplica(bTreeConn, replicaAddr, replicaID, LOG_BTREE)
					}
					idxConn, connExists := rm.replicaClients[replicaAddr+"_inverted_index_conn"]
					if !connExists {
						var err error

						idxConn, err = net.DialTimeout("tcp", "localhost:"+idxReplicaPort, storageNodeDialTimeout)
						if err != nil {
							log.Printf("ERROR: ReplicationManager: Failed to connect to replica %s at %s: %v", replicaID, replicaAddr, err)
							rm.primaryMu.Unlock()
							continue
						}
						rm.replicaClients[replicaAddr+"_inverted_index_conn"] = idxConn
						rm.lastSentLSN[replicaAddr+"_inverted_index"] = 0 // Initialize Inverted Index LSN
						log.Printf("INFO: ReplicationManager: Established new replication client connection to replica %s at %s.", replicaID, replicaAddr)
						// Stream logs to this replica
						go rm.streamLogsToReplica(idxConn, replicaAddr, replicaID, LOG_INVERTED_INDEX)
					}
					rm.primaryMu.Unlock()
				}
			}
		}
	}
}

// streamLogsToReplica sends new log records to a specific replica.
// It concurrently streams logs from both the B-tree's log manager and the inverted index's log manager.
func (rm *ReplicationManager) streamLogsToReplica(conn net.Conn, replicaAddr, replicaID string, logType int) {
	var wg sync.WaitGroup
	stopStreaming := make(chan struct{}) // Channel to signal goroutines to stop

	// Stream B-tree logs
	if logType == LOG_BTREE {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rm.primaryMu.Lock()
			lastBTreeLSN := rm.lastSentLSN[replicaAddr+"_btree"]
			rm.primaryMu.Unlock()

			log.Printf("DEBUG: ReplicationManager: Streaming B-tree logs to replica %s from LSN %d.", replicaID, lastBTreeLSN)
			bTreeLogStream, err := rm.logManager.StartLogStream(lastBTreeLSN)
			if err != nil {
				log.Printf("ERROR: ReplicationManager: Failed to start B-tree log stream for replica %s: %v", replicaID, err)
				rm.closeReplicaConnection(replicaAddr)
				return
			}

			for {
				select {
				case lr, ok := <-bTreeLogStream:
					if !ok {
						log.Printf("INFO: ReplicationManager: B-tree log stream for replica %s closed.", replicaID)
						return
					}
					serializedRecord, err := lr.Serialize()
					if err != nil {
						log.Printf("ERROR: ReplicationManager: Failed to serialize B-tree log record LSN %d for replica %s: %v", lr.LSN, replicaID, err)
						rm.closeReplicaConnection(replicaAddr)
						return
					}
					_, err = conn.Write(serializedRecord)
					if err != nil {
						log.Printf("ERROR: ReplicationManager: Failed to send B-tree log record LSN %d to replica %s: %v", lr.LSN, replicaID, err)
						rm.closeReplicaConnection(replicaAddr)
						return
					}
					rm.primaryMu.Lock()
					rm.lastSentLSN[replicaAddr+"_btree"] = lr.LSN
					rm.primaryMu.Unlock()
					log.Printf("DEBUG: ReplicationManager: Sent B-tree log record LSN %d to replica %s.", lr.LSN, replicaID)
				case <-stopStreaming:
					log.Printf("INFO: B-tree log streaming goroutine for replica %s stopping.", replicaID)
					return
				}
			}
		}()
	}
	if logType == LOG_INVERTED_INDEX {
		// Stream Inverted Index logs
		wg.Add(1)
		go func() {
			defer wg.Done()
			rm.primaryMu.Lock()
			lastInvertedIndexLSN := rm.lastSentLSN[replicaAddr+"_inverted_index"]
			rm.primaryMu.Unlock()

			log.Printf("DEBUG: ReplicationManager: Streaming Inverted Index logs to replica %s from LSN %d.", replicaID, lastInvertedIndexLSN)
			invertedIndexLogStream, err := rm.invertedIndex.GetLogManager().StartLogStream(lastInvertedIndexLSN) // Access inverted index's log manager
			if err != nil {
				log.Printf("ERROR: ReplicationManager: Failed to start Inverted Index log stream for replica %s: %v", replicaID, err)
				rm.closeReplicaConnection(replicaAddr)
				return
			}

			for {
				select {
				case lr, ok := <-invertedIndexLogStream:
					if !ok {
						log.Printf("INFO: ReplicationManager: Inverted Index log stream for replica %s closed.", replicaID)
						return
					}
					serializedRecord, err := lr.Serialize()
					if err != nil {
						log.Printf("ERROR: ReplicationManager: Failed to serialize Inverted Index log record LSN %d for replica %s: %v", lr.LSN, replicaID, err)
						rm.closeReplicaConnection(replicaAddr)
						return
					}
					_, err = conn.Write(serializedRecord)
					if err != nil {
						log.Printf("ERROR: ReplicationManager: Failed to send Inverted Index log record LSN %d to replica %s: %v", lr.LSN, replicaID, err)
						rm.closeReplicaConnection(replicaAddr)
						return
					}
					rm.primaryMu.Lock()
					rm.lastSentLSN[replicaAddr+"_inverted_index"] = lr.LSN
					rm.primaryMu.Unlock()
					log.Printf("DEBUG: ReplicationManager: Sent Inverted Index log record LSN %d to replica %s.", lr.LSN, replicaID)
				case <-stopStreaming:
					log.Printf("INFO: Inverted Index log streaming goroutine for replica %s stopping.", replicaID)
					return
				}
			}
		}()
	}
	// Wait for both streaming goroutines to finish or for a stop signal
	wg.Wait()
	close(stopStreaming) // Signal goroutines to stop if they haven't already
	log.Printf("INFO: All log streams to replica %s ended.", replicaID)
}

// closeReplicaConnection closes an outbound connection to a replica.
func (rm *ReplicationManager) closeReplicaConnection(addr string) {
	rm.primaryMu.Lock()
	defer rm.primaryMu.Unlock()
	if conn, ok := rm.replicaClients[addr]; ok {
		conn.Close()
		delete(rm.replicaClients, addr)
		delete(rm.lastSentLSN, addr+"_btree") // Clean up both LSNs
		delete(rm.lastSentLSN, addr+"_inverted_index")
		log.Printf("INFO: ReplicationManager: Closed outbound connection to replica %s.", addr)
	}
}
