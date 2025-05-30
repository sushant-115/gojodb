package main

import (
	"bufio"
	"encoding/json" // Needed for (de)serializing TransactionOperations
	"fmt"
	"io"
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
)

// Global database instance and its lock for simple concurrency control
var (
	dbInstance *btree_core.BTree[string, string] // Using string keys now
	dbLock     sync.RWMutex                      // Global lock for the entire B-Tree operations
	logManager *btree_core.LogManager

	// Storage Node identity
	myStorageNodeID   string
	myStorageNodeAddr string // e.g., "localhost:9000"

	// Sharding awareness
	myAssignedSlotsMu sync.RWMutex
	myAssignedSlots   map[string]fsm.SlotRangeInfo // Cache of slot ranges assigned to THIS node
)

// Request represents a parsed client request for single operations.
type Request struct {
	Command string
	Key     string // Key is now string
	Value   string // Only for PUT
	TxnID   uint64 // NEW: Transaction ID for 2PC operations (0 for auto-commit)
}

// Response represents a server's reply to a client request.
type Response struct {
	Status  string // OK, ERROR, NOT_FOUND, REDIRECT, VOTE_COMMIT, VOTE_ABORT, COMMITTED, ABORTED
	Message string // Details or value for GET, or target node for REDIRECT
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

	// Adjust dbFilePath to be unique per storage node
	uniqueDbFilePath := fmt.Sprintf("data/%s_gojodb_shard.db", myStorageNodeID)
	uniqueLogDir := fmt.Sprintf("data/%s_logs", myStorageNodeID)
	uniqueArchiveDir := fmt.Sprintf("data/%s_archives", myStorageNodeID)

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

	// 4. Initialize local shard map cache
	myAssignedSlots = make(map[string]fsm.SlotRangeInfo)

	// 5. Start background tasks
	go sendHeartbeatsToController()
	go fetchShardMapFromController()

	return nil
}

// closeStorageNode closes the B-tree and LogManager cleanly.
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
func fetchShardMapFromController() {
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
		log.Printf("DEBUG: Controller leader found at: %s", leaderAddr)

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
		}

		myAssignedSlotsMu.Lock()
		myAssignedSlots = newAssignedSlots
		myAssignedSlotsMu.Unlock()
		log.Printf("INFO: Storage Node %s fetched and cached %d assigned slot ranges.", myStorageNodeID, len(myAssignedSlots))
	}
}

// parseRequest parses a raw string command into a Request struct.
// It's updated to handle 2PC commands.
func parseRequest(raw string) (Request, error) {
	parts := strings.Fields(raw)
	if len(parts) == 0 {
		return Request{}, fmt.Errorf("empty command")
	}

	command := strings.ToUpper(parts[0])
	req := Request{Command: command, TxnID: 0} // Default TxnID to 0 for auto-commit

	switch command {
	case "PUT":
		if len(parts) < 3 {
			return Request{}, fmt.Errorf("PUT requires key and value")
		}
		req.Key = parts[1]
		req.Value = strings.Join(parts[2:], " ")
	case "GET", "DELETE":
		if len(parts) < 2 {
			return Request{}, fmt.Errorf("%s requires a key", command)
		}
		req.Key = parts[1]
	case "SIZE":
		// No additional arguments needed
	// --- NEW: 2PC Commands ---
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
	// --- END NEW ---
	default:
		return Request{}, fmt.Errorf("unknown command: %s", command)
	}
	return req, nil
}

// handleRequest processes a parsed Request and returns a Response.
// It now includes 2PC command handling and passes TxnID to B-tree operations.
func handleRequest(req Request) Response {
	var resp Response
	var err error

	// --- Sharding Awareness: Check if key belongs to this node (for data ops) ---
	// 2PC commands (PREPARE, COMMIT, ABORT) are sent directly to the participant,
	// so they don't need shard routing check here.
	isDataOperation := req.Command == "PUT" || req.Command == "GET" || req.Command == "DELETE"
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

		if !assignedToMe {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("Key '%s' (slot %d) does not belong to this node. No assignment found or assigned to unknown node.", req.Key, targetSlot)}
			return resp
		}
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
	// --- END NEW ---
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
	log.Println("INFO: Commands: PUT <key> <value>, GET <key>, DELETE <key>, SIZE")
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
