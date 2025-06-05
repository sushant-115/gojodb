package main

import (
	"bufio" // Added for bytes.SplitN
	"context"
	"encoding/json" // Needed for (de)serializing TransactionOperations
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

	btree_core "github.com/sushant-115/gojodb/core/indexing/btree"               // B-tree core package
	"github.com/sushant-115/gojodb/core/indexing/inverted_index"                 // Inverted Index package
	"github.com/sushant-115/gojodb/core/indexing/spatial"                        // NEW: Spatial Index package
	replication "github.com/sushant-115/gojodb/core/replication/log_replication" // B-tree core package
	fsm "github.com/sushant-115/gojodb/core/replication/raft_consensus"          // FSM package for sharding info
	"github.com/sushant-115/gojodb/core/write_engine/wal"
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

	// Controller interaction configuration
	controllerHeartbeatTargetPort = 8086             // Default UDP port on Controller for heartbeats
	heartbeatInterval             = 5 * time.Second  // How often to send heartbeats
	shardMapFetchInterval         = 10 * time.Second // How often to fetch shard map
	storageNodeDialTimeout        = 2 * time.Second  // Timeout for connecting to a storage node

	// GOJODB.TEXT() wrapper prefix
	gojoDBTextPrefix = "GOJODB.TEXT("
	gojoDBTextSuffix = ")"
)

// Global database instance and its lock for simple concurrency control
var (
	dbInstance            *btree_core.BTree[string, string] // Using string keys
	dbLock                sync.RWMutex                      // Global lock for the entire B-Tree operations
	logManager            *wal.LogManager
	invertedIndexInstance *inverted_index.InvertedIndex // Global inverted index instance
	spatialIndexManager   *spatial.SpatialIndexManager  // NEW: Global spatial index instance

	// Storage Node identity
	myStorageNodeID   string
	myStorageNodeAddr string // e.g., "localhost:9000"

	// Sharding awareness
	myAssignedSlotsMu sync.RWMutex
	myAssignedSlots   map[string]fsm.SlotRangeInfo // Cache of slot ranges assigned to THIS node
	// Replication Manager instance
	replicationManager      *replication.ReplicationManager
	controllerHeartbeatAddr = os.Getenv("RAFT_HEARTBEAT_ADDRESS")
	raftControllerAddress   = os.Getenv("RAFT_HTTP_ADDRESS")
)

// Request represents a parsed client request for single operations.
type Request struct {
	Command  string
	Key      string // Key is now string
	Value    string // Only for PUT
	TxnID    uint64 // NEW: Transaction ID for 2PC operations (0 for auto-commit)
	StartKey string // For range query
	EndKey   string // For range query
	Query    string // For text search queries
	// NEW: Spatial query parameters
	MinX float64
	MinY float64
	MaxX float64
	MaxY float64
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

type HeartbeatMessage struct {
	NodeID    string `json:"node_id"`
	Address   string `json:"address"`
	Timestamp int64  `json:"timestamp"` // Unix timestamp
}

type TransactionOperations struct {
	Operations []btree_core.TransactionOperation `json:"operations"`
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

	// Ensure the base data directory exists
	baseDataDir := "data" // Assuming all data files/logs go here
	if err := os.MkdirAll(baseDataDir, 0755); err != nil {
		return fmt.Errorf("failed to create base data directory %s: %w", baseDataDir, err)
	}

	// Adjust dbFilePath to be unique per storage node
	uniqueDbFilePath := fmt.Sprintf("data/%s_gojodb_shard.db", myStorageNodeID)
	uniqueLogDir := fmt.Sprintf("data/%s_logs", myStorageNodeID)
	uniqueArchiveDir := fmt.Sprintf("data/%s_archives", myStorageNodeID)
	// Use a unique file path for the inverted index data file (changed from .json to .db)
	uniqueInvertedIndexFilePath := fmt.Sprintf("data/%s_inverted_index.db", myStorageNodeID)
	uniqueInvertedIndexLogDir := fmt.Sprintf("data/%s_inverted_index_logs", myStorageNodeID)
	uniqueInvertedIndexArchiveDir := fmt.Sprintf("data/%s_inverted_index_archives", myStorageNodeID)

	// Use a unique file path for the inverted index data file (changed from .json to .db)
	uniqueSpatialIndexFilePath := fmt.Sprintf("data/%s_spatial_index.db", myStorageNodeID)
	uniqueSpatialIndexLogDir := fmt.Sprintf("data/%s_spatial_index_logs", myStorageNodeID)
	uniqueSpatialIndexArchiveDir := fmt.Sprintf("data/%s_spatial_index_archives", myStorageNodeID)

	// 2. Initialize LogManager for this Storage Node
	logManager, err = wal.NewLogManager(uniqueLogDir, uniqueArchiveDir, logBufferSize, logSegmentSize)
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

	// 4. Initialize Inverted Index for this Storage Node
	// Pass unique log and archive directories for the inverted index
	invertedIndexInstance, err = inverted_index.NewInvertedIndex(uniqueInvertedIndexFilePath, uniqueInvertedIndexLogDir, uniqueInvertedIndexArchiveDir)
	if err != nil {
		return fmt.Errorf("failed to create InvertedIndex for storage node %s: %w", myStorageNodeID, err)
	}
	log.Printf("INFO: Storage Node %s inverted index initialized successfully.", myStorageNodeID)

	// NEW: 5. Initialize Spatial Index for this Storage Node
	spatialIndexManager, err = spatial.NewSpatialIndexManager(uniqueSpatialIndexArchiveDir, uniqueSpatialIndexLogDir, uniqueSpatialIndexFilePath, logBufferSize, logSegmentSize, dbPageSize, 6, 2)
	if err != nil {
		return fmt.Errorf("failed to create SpatialIndex for storage node %s: %w", myStorageNodeID, err)
	}
	log.Printf("INFO: Storage Node %s spatial index initialized successfully.", myStorageNodeID)

	// 6. Initialize local shard map cache
	myAssignedSlots = make(map[string]fsm.SlotRangeInfo)

	// 7. Initialize Replication Manager
	// Pass the invertedIndexInstance to the ReplicationManager
	replicationManager = replication.NewReplicationManager(myStorageNodeID, logManager, dbInstance, &dbLock, invertedIndexInstance, spatialIndexManager)
	replicationManager.Start() // Start replication background tasks

	// 8. Start background tasks
	// go sendHeartbeatsToController()
	for _, controllerAddress := range strings.Split(controllerHeartbeatAddr, ",") {
		log.Println("Setup heartbeat for :", controllerAddress)
		go SendHeartbeatsTCP(context.Background(), myStorageNodeID, myStorageNodeAddr, controllerAddress, heartbeatInterval)
	}
	log.Println("Calling fetch shard map")
	go fetchShardMapFromController(replicationManager)

	return nil
}

// closeStorageNode closes the B-tree, LogManager, InvertedIndex, and SpatialIndex cleanly.
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
	// Close Inverted Index
	if invertedIndexInstance != nil {
		if err := invertedIndexInstance.Close(); err != nil {
			log.Printf("ERROR: Failed to close InvertedIndex for Storage Node %s: %v", myStorageNodeID, err)
		}
	}
	// Spatial Index does not have a Close method in the provided code, but if it did, call it here.
	// if spatialIndexManager != nil {
	// 	if err := spatialIndexManager.Close(); err != nil {
	// 		log.Printf("ERROR: Failed to close SpatialIndex for Storage Node %s: %v", myStorageNodeID, err)
	// 	}
	// }

	// Stop Replication Manager
	if replicationManager != nil {
		replicationManager.Stop()
	}
	log.Println("INFO: Storage Node shutdown complete.")
}

// SendHeartbeatsTCP sends periodic heartbeat messages to a target address over TCP.
// It runs in a goroutine and sends heartbeats every `interval`.
// The context allows for graceful shutdown.
func SendHeartbeatsTCP(ctx context.Context, nodeID string, localAddr string, targetAddr string, interval time.Duration) {
	log.Printf("Starting TCP heartbeat sender for NodeID: %s, LocalAddr: %s, TargetAddr: %s", nodeID, localAddr, targetAddr)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Heartbeat sender for NodeID %s stopped due to context cancellation.", nodeID)
			return
		case <-ticker.C:
			// Create a new connection for each heartbeat.
			// For a more robust system, consider maintaining a persistent connection
			// or using connection pooling.
			conn, err := net.Dial("tcp", targetAddr)
			if err != nil {
				log.Printf("Failed to connect to %s for heartbeat (NodeID: %s): %v", targetAddr, nodeID, err)
				continue
			}

			// Create the heartbeat message
			msg := HeartbeatMessage{
				NodeID:    nodeID,
				Address:   localAddr,
				Timestamp: time.Now().UnixNano(),
			}

			// Encode the message to JSON
			jsonData, err := json.Marshal(msg)
			if err != nil {
				log.Printf("Failed to marshal heartbeat message for NodeID %s: %v", nodeID, err)
				conn.Close()
				continue
			}

			// Send the message followed by a newline delimiter
			_, err = conn.Write(append(jsonData, '\n'))
			if err != nil {
				log.Printf("Failed to send heartbeat to %s (NodeID: %s): %v", targetAddr, nodeID, err)
			} else {
				// log.Printf("Sent heartbeat from NodeID: %s to %s", nodeID, targetAddr)
			}
			conn.Close() // Close the connection after sending
		}
	}
}

// sendHeartbeatsToController periodically sends heartbeats to all known Controller addresses.
func sendHeartbeatsToController() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	controllerAddrs := strings.Split(controllerHeartbeatAddr, ",")
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
			heartbeatTargetPort, _ = strconv.Atoi(parts[1])
			// conn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP(parts[0]), Port: heartbeatTargetPort})
			// if err != nil {
			// 	log.Printf("ERROR: Failed to dial UDP for heartbeat to %s (port %d): %v", addr, heartbeatTargetPort, err)
			// 	continue
			// }
			// _, err = conn.Write([]byte(heartbeatMessage))
			// if err != nil {
			// 	log.Printf("ERROR: Failed to send heartbeat to Controller %s (port %d): %v", addr, heartbeatTargetPort, err)
			// } else {
			// 	//log.Printf("DEBUG: Storage Node %s sent heartbeat to Controller %s (port %d).", myStorageNodeID, addr, heartbeatTargetPort)
			// }
			tcpConn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Printf("Failed to send heartbeat to connect: %v", err)
				continue
			}
			_, err = fmt.Fprintln(tcpConn, heartbeatMessage) // Sends message with newline
			if err != nil {
				log.Printf("Heartbeat error: %v", err)
			}
			log.Println("Sent heartbeat to: ", parts[0], heartbeatTargetPort, heartbeatMessage)
			// conn.Close() // Close connection after sending
			tcpConn.Close()
		}
	}
}

// fetchShardMapFromController periodically fetches the latest slot assignments from the Controller leader.
func fetchShardMapFromController(rm *replication.ReplicationManager) {
	log.Println("Called shard map")
	ticker := time.NewTicker(shardMapFetchInterval)
	defer ticker.Stop()

	controllerAddrs := strings.Split(raftControllerAddress, ",")

	for {
		select {
		case <-ticker.C:
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
				log.Println("No leader found")
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
			// bodyBytes, _ := io.ReadAll(resp.Body)
			// bodyString := string(bodyBytes)
			// log.Println("Controller Address 418:  ", bodyString)
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
					rm.SetStorageNodeAddress(nodeID, string(body))
				}
			}

			myAssignedSlotsMu.Lock()
			myAssignedSlots = newAssignedSlots
			log.Println("Fetched slot assignments: ", newAssignedSlots)
			rm.SetAssignedNodeAddress(myAssignedSlots)
			myAssignedSlotsMu.Unlock()

			//rm.primaryForSlots = newAssignedSlots
			//log.Printf("INFO: Storage Node %s fetched and cached %d assigned slot ranges.", myStorageNodeID, len(myAssignedSlots))
		}
	}
}

// parseRequest parses a raw string command into a Request struct.
// It's updated to handle 2PC commands, TEXT_SEARCH, and new SPATIAL commands.
func parseRequest(raw string) (Request, error) {
	parts := strings.Fields(raw)
	if len(parts) == 0 {
		return Request{}, fmt.Errorf("empty command")
	}

	command := strings.ToUpper(parts[0])
	req := Request{Command: command, TxnID: 0} // Default TxnID to 0 for auto-commit

	switch command {
	case "PUT", "PUT_TEXT": // Handle PUT_TEXT
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

	case "TEXT_SEARCH": // Handle TEXT_SEARCH
		if len(parts) < 2 {
			return Request{}, fmt.Errorf("TEXT_SEARCH requires a query string")
		}
		req.Query = strings.Join(parts[1:], " ") // The rest is the query
	case "PUT_SPATIAL": // NEW: Handle PUT_SPATIAL
		if len(parts) < 6 {
			return Request{}, fmt.Errorf("PUT_SPATIAL requires key, minX, minY, maxX, maxY")
		}
		req.Key = parts[1]
		var err error
		req.MinX, err = strconv.ParseFloat(parts[2], 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid minX: %w", err)
		}
		req.MinY, err = strconv.ParseFloat(parts[3], 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid minY: %w", err)
		}
		req.MaxX, err = strconv.ParseFloat(parts[4], 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid maxX: %w", err)
		}
		req.MaxY, err = strconv.ParseFloat(parts[5], 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid maxY: %w", err)
		}
	case "DELETE_SPATIAL": // NEW: Handle DELETE_SPATIAL
		if len(parts) < 6 {
			return Request{}, fmt.Errorf("DELETE_SPATIAL requires key, minX, minY, maxX, maxY")
		}
		req.Key = parts[1]
		var err error
		req.MinX, err = strconv.ParseFloat(parts[2], 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid minX: %w", err)
		}
		req.MinY, err = strconv.ParseFloat(parts[3], 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid minY: %w", err)
		}
		req.MaxX, err = strconv.ParseFloat(parts[4], 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid maxX: %w", err)
		}
		req.MaxY, err = strconv.ParseFloat(parts[5], 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid maxY: %w", err)
		}
	case "QUERY_SPATIAL": // NEW: Handle QUERY_SPATIAL
		if len(parts) < 5 {
			return Request{}, fmt.Errorf("QUERY_SPATIAL requires minX, minY, maxX, maxY")
		}
		var err error
		req.MinX, err = strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid minX: %w", err)
		}
		req.MinY, err = strconv.ParseFloat(parts[2], 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid minY: %w", err)
		}
		req.MaxX, err = strconv.ParseFloat(parts[3], 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid maxX: %w", err)
		}
		req.MaxY, err = strconv.ParseFloat(parts[4], 64)
		if err != nil {
			return Request{}, fmt.Errorf("invalid maxY: %w", err)
		}
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
// It now includes 2PC command handling, TEXT_SEARCH, and SPATIAL commands.
func handleRequest(req Request) Response {
	var resp Response
	var err error

	// --- Sharding Awareness: Check if key belongs to this node (for data ops) ---
	// 2PC commands (PREPARE, COMMIT, ABORT) are sent directly to the participant,
	// so they don't need shard routing check here.
	// TEXT_SEARCH and SPATIAL_QUERY are handled by any node (for V1), so they don't need shard routing check.
	isDataOperation := req.Command == "PUT" || req.Command == "PUT_TEXT" || req.Command == "GET" || req.Command == "DELETE" ||
		req.Command == "PUT_SPATIAL" || req.Command == "DELETE_SPATIAL" // Spatial operations also need sharding check for their key
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
	case "PUT_TEXT": // Handle PUT_TEXT command
		// Extract raw text from GOJODB.TEXT() wrapper
		pureText := ""
		if strings.HasPrefix(req.Value, gojoDBTextPrefix) && strings.HasSuffix(req.Value, gojoDBTextSuffix) {
			pureText = req.Value[len(gojoDBTextPrefix) : len(req.Value)-len(gojoDBTextSuffix)]
		} else {
			resp = Response{Status: "ERROR", Message: "PUT_TEXT command requires value wrapped in GOJODB.TEXT()."}
			return resp
		}

		// Insert into Inverted Index (non-transactional for V1)
		if err := invertedIndexInstance.Insert(pureText, req.Key); err != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("PUT_TEXT (inverted index part) failed: %v", err)}
			return resp
		}
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
	case "PUT_SPATIAL": // NEW: Handle PUT_SPATIAL command
		rect := spatial.Rect{req.MinX, req.MinY, req.MaxX, req.MaxY}
		sd := spatial.SpatialData{req.Key}
		log.Println("Info: spatial request: ", req, sd)
		if err := spatialIndexManager.Insert(rect, sd); err != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("PUT_SPATIAL (spatial index part) failed: %v", err)}
			return resp
		}
		log.Printf("INFO: Spatial Index: Indexed key '%s' with rect %v", req.Key, rect)

		// Also store the spatial data as a JSON string in the B-tree
		rectJSON, marshalErr := json.Marshal(rect)
		if marshalErr != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("PUT_SPATIAL failed to marshal rect to JSON: %v", marshalErr)}
			return resp
		}
		dbLock.Lock()
		err = dbInstance.Insert(req.Key, string(rectJSON), req.TxnID)
		dbLock.Unlock()
		if err != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("PUT_SPATIAL (B-tree part) failed: %v", err)}
		} else {
			resp = Response{Status: "OK", Message: "Spatial data indexed and stored."}
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
	case "TEXT_SEARCH": // Handle TEXT_SEARCH command
		// Perform search on the inverted index
		resultKeys, searchErr := invertedIndexInstance.Search(req.Query) // Capture error from InvertedIndex.Search
		if searchErr != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("Text search failed: %v", searchErr)}
			return resp
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
	case "QUERY_SPATIAL": // NEW: Handle QUERY_SPATIAL command
		queryRect := spatial.Rect{
			req.MinX, req.MinY, req.MaxX, req.MaxY,
		}
		results, queryErr := spatialIndexManager.Query(queryRect)
		if queryErr != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("QUERY_SPATIAL failed: %v", queryErr)}
			return resp
		}
		log.Printf("INFO: Spatial Index: Query for %v returned %d results.", queryRect, len(results))

		// Marshal the spatial data results (SpatialData contains ID and Rect)
		resultsJSON, marshalErr := json.Marshal(results)
		if marshalErr != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("Failed to marshal spatial query results: %v", marshalErr)}
		} else {
			resp = Response{Status: "OK", Message: string(resultsJSON)}
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
	case "DELETE_SPATIAL": // NEW: Handle DELETE_SPATIAL command
		rect := spatial.Rect{req.MinX, req.MinY, req.MaxX, req.MaxY}
		// if err := spatialIndexManager.DeleteSpatialData(req.Key, rect); err != nil {
		// 	resp = Response{Status: "ERROR", Message: fmt.Sprintf("DELETE_SPATIAL (spatial index part) failed: %v", err)}
		// 	return resp
		// }
		log.Printf("INFO: Spatial Index: Deleted key '%s' with rect %v", req.Key, rect)

		// Also delete from the B-tree
		dbLock.Lock()
		err = dbInstance.Delete(req.Key, req.TxnID)
		dbLock.Unlock()
		if err != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("DELETE_SPATIAL (B-tree part) failed: %v", err)}
		} else {
			resp = Response{Status: "OK", Message: "Spatial data deleted."}
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
	// --- 2PC Command Handlers ---
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
				parts := strings.Split(myStorageNodeAddr, ":")
				response := fmt.Sprintf("%s:%s %s:%s", parts[0], os.Getenv("BTREE_REPLICATION_LISTEN_PORT"), parts[0], os.Getenv("INVERTED_IDX_REPLICATION_LISTEN_PORT"))
				resp = Response{Status: "OK", Message: response}
				// log.Println("SHOW CONFIG REPLICA_PORT: ", resp)
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
	// log.Printf("INFO: Client connected to Storage Node %s: %s", myStorageNodeID, conn.RemoteAddr().String())

	reader := bufio.NewReader(conn)
	for {
		// Read client command, delimited by newline
		netData, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				// log.Printf("INFO: Client disconnected from Storage Node %s: %s", myStorageNodeID, conn.RemoteAddr().String())
			} else {
				log.Printf("ERROR: Error reading from client %s: %v", conn.RemoteAddr().String(), err)
			}
			return
		}

		rawCommand := strings.TrimSpace(netData)
		if rawCommand == "" {
			continue // Ignore empty lines
		}
		// log.Printf("DEBUG: Received command from %s: '%s'", conn.RemoteAddr().String(), rawCommand)

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
	log.Println("INFO: Commands:")
	log.Println("  - PUT <key> <value>")
	log.Println("  - PUT_TEXT <key> GOJODB.TEXT(<value>)")
	log.Println("  - PUT_SPATIAL <key> <minX> <minY> <maxX> <maxY>")
	log.Println("  - GET <key>")
	log.Println("  - GET_RANGE <start_key> <end_key>")
	log.Println("  - DELETE <key>")
	log.Println("  - DELETE_SPATIAL <key> <minX> <minY> <maxX> <maxY>")
	log.Println("  - SIZE")
	log.Println("  - TEXT_SEARCH <query>")
	log.Println("  - QUERY_SPATIAL <minX> <minY> <maxX> <maxY>")
	log.Println("  - 2PC Commands (from Coordinator only): PREPARE <TxnID> <ops_json>, COMMIT <TxnID>, ABORT <TxnID>")

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
