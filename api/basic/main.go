package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sushant-115/gojodb/cmd/gojodb_controller/fsm" // Import the FSM for sharding info (e.g., GetSlotForHashKey, SlotRangeInfo)
)

const (
	apiServiceHost            = "localhost"
	apiServicePort            = "8090"                                         // Default port for API Service
	controllerHTTPAddresses   = "localhost:8080,localhost:8083,localhost:8085" // Comma-separated list of Controller HTTP addresses
	shardMapFetchInterval     = 5 * time.Second                                // How often to fetch shard map
	controllerMonitorInterval = 2 * time.Second                                // How often to monitor controller cluster
	storageNodeDialTimeout    = 2 * time.Second                                // Timeout for connecting to a storage node
	CLIENT_TIMEOUT            = 5 * time.Second
	// Basic API Key for admin endpoints
	adminAPIKey = "GOJODB_ADMIN_KEY" // Replace with a strong, secret key in production

	// 2PC Coordinator configuration
	prepareTimeout = 5 * time.Second // Timeout for participants to respond to PREPARE
)

// APIRequest represents a client request received by the API service.
type APIRequest struct {
	Command  string `json:"command"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`     // Optional for GET/DELETE
	StartKey string `json:"start_key,omitempty"` // For range ops
	EndKey   string `json:"end_key,omitempty"`   // For range ops
}

// APIResponse represents a response sent by the API service to the client.
type APIResponse struct {
	Status  string          `json:"status"`            // OK, ERROR, NOT_FOUND, REDIRECT, COMMITTED, ABORTED
	Message string          `json:"message,omitempty"` // Details or value for GET, or target node address for REDIRECT
	Data    json.RawMessage `json:"data,omitempty"`    // Flexible field for returning structured data (e.g., array of entries, aggregation result)
}

// TransactionOperation defines a single operation within a distributed transaction.
type TransactionOperation struct {
	Command string `json:"command"` // PUT or DELETE
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
}

// TransactionRequest represents a request for a distributed transaction.
type TransactionRequest struct {
	Operations []TransactionOperation `json:"operations"`
}

// ControllerStatus represents the status of a single controller node.
type ControllerStatus struct {
	NodeID   string
	State    string // Raft state: leader, follower, candidate, etc.
	LeaderID string // ID of the current Raft leader
	Address  string // HTTP address of this controller node
}

// APIService manages the API endpoints and routing logic.
type APIService struct {
	controllerAddrs []string

	// Controller Cluster State
	currentControllerLeader string // Address of the current leader
	leaderMu                sync.RWMutex
	controllerStatesMu      sync.RWMutex
	controllerStates        map[string]ControllerStatus // Controller HTTP Address -> ControllerStatus

	// Storage Node Addresses (fetched from Controller)
	storageNodeAddressesMu sync.RWMutex
	storageNodeAddresses   map[string]string // StorageNodeID -> StorageNodeAddress (e.g., "storage1" -> "localhost:9000")

	// Cached Shard Map
	slotAssignmentsMu sync.RWMutex
	slotAssignments   map[string]fsm.SlotRangeInfo // Cached slot ranges (RangeID -> SlotRangeInfo)

	// Cache of active connections to Storage Nodes
	storageNodeConnsMu sync.Mutex
	storageNodeConns   map[string]net.Conn // storageNodeAddress -> net.Conn

	// Transaction Coordinator state (in-memory for V1)
	nextTxnID   int64
	nextTxnIDMu sync.Mutex
}

// NewAPIService creates and initializes a new API Service.
func NewAPIService(controllerHTTPAddrs string) *APIService {
	service := &APIService{
		controllerAddrs:      strings.Split(controllerHTTPAddrs, ","),
		controllerStates:     make(map[string]ControllerStatus),
		storageNodeAddresses: make(map[string]string),
		slotAssignments:      make(map[string]fsm.SlotRangeInfo),
		storageNodeConns:     make(map[string]net.Conn),
		nextTxnID:            time.Now().UnixNano(), // Basic unique ID for transactions
	}
	go service.monitorControllerCluster() // Start monitoring controller cluster and fetching shard map
	go service.manageStorageNodeConns()   // Start managing connections
	return service
}

// findControllerLeader periodically pings controller nodes to find the current Raft leader,
// fetch their status, and update the cached storage node addresses and shard map.
func (s *APIService) monitorControllerCluster() {
	ticker := time.NewTicker(controllerMonitorInterval)
	defer ticker.Stop()

	for range ticker.C {
		currentLeader := ""
		tempControllerStates := make(map[string]ControllerStatus)
		tempStorageNodeAddresses := make(map[string]string)

		// 1. Ping all controllers to get their status and find the leader
		for _, addr := range s.controllerAddrs {
			resp, err := http.Get(fmt.Sprintf("http://%s/status", addr))
			if err != nil {
				// log.Printf("DEBUG: API Service: Failed to reach Controller %s for status: %v", addr, err) // Too noisy
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				bodyBytes, _ := io.ReadAll(resp.Body)
				bodyString := string(bodyBytes)

				// Parse Controller Status
				nodeIDLine := ""
				stateLine := ""
				leaderLine := ""

				for _, line := range strings.Split(bodyString, "\n") {
					if strings.HasPrefix(line, "Node ID:") {
						nodeIDLine = strings.TrimSpace(strings.TrimPrefix(line, "Node ID:"))
					} else if strings.HasPrefix(line, "State:") {
						stateLine = strings.TrimSpace(strings.TrimPrefix(line, "State:"))
					} else if strings.HasPrefix(line, "Leader:") {
						leaderLine = strings.TrimSpace(strings.TrimPrefix(line, "Leader:"))
					} else if strings.HasPrefix(line, "  - ID:") { // Parse Storage Node addresses
						parts := strings.SplitN(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "- ID:")), ", Addr:", 2)
						if len(parts) == 2 {
							storageID := strings.TrimSpace(parts[0])
							storageAddr := strings.TrimSpace(parts[1])
							tempStorageNodeAddresses[storageID] = storageAddr
						}
					}
				}

				status := ControllerStatus{
					NodeID:   nodeIDLine,
					State:    stateLine,
					LeaderID: leaderLine, // This is the ID of the leader, not its address
					Address:  addr,       // This controller's HTTP address
				}
				tempControllerStates[addr] = status

				if stateLine == "Leader" {
					currentLeader = addr // Store the HTTP address of the leader
				}
			}
		}

		// Update cached controller states and leader address
		s.controllerStatesMu.Lock()
		s.controllerStates = tempControllerStates
		s.controllerStatesMu.Unlock()

		s.leaderMu.Lock()
		s.currentControllerLeader = currentLeader
		s.leaderMu.Unlock()

		if currentLeader == "" {
			log.Println("WARNING: API Service: No Controller leader currently reachable or identified. Some operations may fail.")
			// Clear shard map and storage node addresses if no leader
			s.slotAssignmentsMu.Lock()
			s.slotAssignments = make(map[string]fsm.SlotRangeInfo)
			s.slotAssignmentsMu.Unlock()

			s.storageNodeAddressesMu.Lock()
			s.storageNodeAddresses = make(map[string]string)
			s.storageNodeAddressesMu.Unlock()
			continue // Skip fetching shard map if no leader
		}

		// Update cached storage node addresses
		s.storageNodeAddressesMu.Lock()
		s.storageNodeAddresses = tempStorageNodeAddresses
		s.storageNodeAddressesMu.Unlock()
		log.Printf("INFO: API Service: Cached %d Storage Node addresses.", len(tempStorageNodeAddresses))

		// 2. Fetch and cache shard map from the leader
		resp, err := http.Get(fmt.Sprintf("http://%s/admin/get_all_slot_assignments", currentLeader))
		if err != nil {
			log.Printf("ERROR: API Service: Failed to fetch slot assignments from Controller leader %s: %v", currentLeader, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			log.Printf("ERROR: API Service: Failed to fetch slot assignments: HTTP Status %s", resp.Status)
			continue
		}

		var allSlotAssignments map[string]fsm.SlotRangeInfo
		if err := json.NewDecoder(resp.Body).Decode(&allSlotAssignments); err != nil {
			log.Printf("ERROR: API Service: Failed to decode slot assignments from Controller: %v", err)
			continue
		}

		s.slotAssignmentsMu.Lock()
		s.slotAssignments = allSlotAssignments // Overwrite with latest map
		s.slotAssignmentsMu.Unlock()
		log.Printf("INFO: API Service: Fetched and cached %d slot assignments from Controller leader %s.", len(allSlotAssignments), currentLeader)
	}
}

// getStorageNodeConn gets or establishes a TCP connection to a Storage Node.
func (s *APIService) getStorageNodeConn(nodeAddr string) (net.Conn, error) {
	s.storageNodeConnsMu.Lock()
	defer s.storageNodeConnsMu.Unlock()

	if conn, ok := s.storageNodeConns[nodeAddr]; ok {
		// Basic check if connection is still alive (e.g., by sending a small ping)
		// For now, assume it's alive. Real system would do more robust health checks.
		return conn, nil
	}

	log.Printf("DEBUG: API Service: Dialing new connection to Storage Node at %s", nodeAddr)
	conn, err := net.DialTimeout("tcp", nodeAddr, storageNodeDialTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to dial storage node %s: %w", nodeAddr, err)
	}
	s.storageNodeConns[nodeAddr] = conn
	return conn, nil
}

// closeStorageNodeConn closes and removes a connection from the cache.
func (s *APIService) closeStorageNodeConn(nodeAddr string) {
	s.storageNodeConnsMu.Lock()
	defer s.storageNodeConnsMu.Unlock()
	if conn, ok := s.storageNodeConns[nodeAddr]; ok {
		conn.Close()
		delete(s.storageNodeConns, nodeAddr)
		log.Printf("DEBUG: API Service: Closed connection to Storage Node at %s", nodeAddr)
	}
}

// manageStorageNodeConns periodically checks and cleans up stale connections.
func (s *APIService) manageStorageNodeConns() {
	ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
	defer ticker.Stop()

	for range ticker.C {
		s.storageNodeConnsMu.Lock()
		for addr, conn := range s.storageNodeConns {
			// A simple check: attempt a non-blocking write or read.
			// A real system would use a dedicated ping/pong or idle connection pool.
			conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			_, err := conn.Read(make([]byte, 1)) // Try a small non-blocking read
			conn.SetReadDeadline(time.Time{})    // Clear deadline

			if err != nil && !strings.Contains(err.Error(), "timeout") && !strings.Contains(err.Error(), "i/o timeout") {
				log.Printf("WARNING: API Service: Stale connection detected to %s, closing: %v", addr, err)
				conn.Close()
				delete(s.storageNodeConns, addr)
			}
		}
		s.storageNodeConnsMu.Unlock()
	}
}

// authenticate checks for a valid API key for admin requests.
func authenticate(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("X-API-Key")
		if apiKey != adminAPIKey { // Simple hardcoded key check
			http.Error(w, "Unauthorized: Invalid API Key", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	}
}

// handleAPIRequest is the main router for all incoming HTTP requests to the API service.
func (s *APIService) handleAPIRequest(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// --- Check for Controller Leader (Fail Fast if no leader for critical ops) ---
	s.leaderMu.RLock()
	leaderAddr := s.currentControllerLeader
	s.leaderMu.RUnlock()

	// Operations that depend on leader or shard map:
	isCriticalPath := strings.HasPrefix(path, "/admin/") || path == "/api/data"
	if isCriticalPath && leaderAddr == "" {
		http.Error(w, "ERROR: No Controller leader available. Cannot process request.", http.StatusServiceUnavailable)
		log.Printf("ERROR: Request for %s failed: No Controller leader available.", path)
		return
	}
	// --- END Controller Leader Check ---

	if strings.HasPrefix(path, "/admin/") {
		authenticate(s.handleAdminRequest).ServeHTTP(w, r)
		return
	} else if path == "/status" {
		s.handleClusterStatus(w, r)
		return
	} else if path == "/api/data" {
		s.handleDataRequest(w, r)
		return
	} else if path == "/api/transaction" { // New endpoint for distributed transactions
		s.handleTransactionRequest(w, r)
		return
	} else if path == "/api/query" { // NEW: Endpoint for range queries and aggregations
		s.handleQueryRequest(w, r)
		return
	} else {
		http.Error(w, "Not Found", http.StatusNotFound)
	}
}

// handleAdminRequest routes authenticated admin requests to the Controller Leader.
func (s *APIService) handleAdminRequest(w http.ResponseWriter, r *http.Request) {
	s.leaderMu.RLock()
	leaderAddr := s.currentControllerLeader
	s.leaderMu.RUnlock()

	// leaderAddr check already done in handleAPIRequest, but defensive here.
	if leaderAddr == "" {
		http.Error(w, "ERROR: No Controller leader available. Cannot process admin request.", http.StatusServiceUnavailable)
		return
	}

	// Proxy the request to the Controller Leader
	proxyURL := fmt.Sprintf("http://%s%s?%s", leaderAddr, r.URL.Path, r.URL.RawQuery)
	log.Printf("DEBUG: API Service: Proxying admin request to Controller Leader: %s", proxyURL)

	req, err := http.NewRequest(r.Method, proxyURL, r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error creating proxy request: %v", err), http.StatusInternalServerError)
		return
	}
	// Copy headers from original request, excluding X-API-Key for internal proxy
	for name, values := range r.Header {
		if name == "X-Api-Key" { // Don't forward the API key to internal controller
			continue
		}
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}

	client := &http.Client{Timeout: CLIENT_TIMEOUT}
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error proxying request to Controller Leader: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy response back to client
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// handleClusterStatus queries all Controller nodes and aggregates their status.
func (s *APIService) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	s.controllerStatesMu.RLock()
	states := s.controllerStates // Get a copy of the current states
	s.controllerStatesMu.RUnlock()

	if len(states) == 0 {
		http.Error(w, "ERROR: No Controller nodes currently reachable.", http.StatusServiceUnavailable)
		return
	}

	var responseBuilder strings.Builder
	responseBuilder.WriteString("GojoDB Controller Cluster Status:\n")

	// Sort controller addresses for consistent output
	var sortedAddrs []string
	for addr := range states {
		sortedAddrs = append(sortedAddrs, addr)
	}
	sort.Strings(sortedAddrs)

	for _, addr := range sortedAddrs {
		status := states[addr]
		responseBuilder.WriteString(fmt.Sprintf("  - Controller: %s (Addr: %s)\n", status.NodeID, status.Address))
		responseBuilder.WriteString(fmt.Sprintf("    State: %s, Leader: %s\n", status.State, status.LeaderID))
	}

	// Add Storage Node summary (from leader's perspective, which is cached)
	s.storageNodeAddressesMu.RLock()
	storageNodes := s.storageNodeAddresses
	s.storageNodeAddressesMu.RUnlock()

	responseBuilder.WriteString(fmt.Sprintf("\nRegistered Storage Nodes (%d):\n", len(storageNodes)))
	if len(storageNodes) == 0 {
		responseBuilder.WriteString("  (None)\n")
	} else {
		var sortedStorageIDs []string
		for id := range storageNodes {
			sortedStorageIDs = append(sortedStorageIDs, id)
		}
		sort.Strings(sortedStorageIDs)
		for _, id := range sortedStorageIDs {
			responseBuilder.WriteString(fmt.Sprintf("  - ID: %s, Addr: %s\n", id, storageNodes[id]))
		}
	}

	// Add Slot Assignments summary
	s.slotAssignmentsMu.RLock()
	slotAssignments := s.slotAssignments
	s.slotAssignmentsMu.RUnlock()

	responseBuilder.WriteString(fmt.Sprintf("\nSlot Assignments (%d ranges):\n", len(slotAssignments)))
	if len(slotAssignments) == 0 {
		responseBuilder.WriteString("  (None)\n")
	} else {
		var sortedRanges []fsm.SlotRangeInfo
		for _, sr := range slotAssignments {
			sortedRanges = append(sortedRanges, sr)
		}
		sort.Slice(sortedRanges, func(i, j int) bool {
			return sortedRanges[i].StartSlot < sortedRanges[j].StartSlot
		})

		for _, sr := range sortedRanges {
			responseBuilder.WriteString(fmt.Sprintf("  - RangeID: %s (%d-%d), Assigned To: %s, Status: %s\n",
				sr.RangeID, sr.StartSlot, sr.EndSlot, sr.AssignedNodeID, sr.Status))
		}
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, responseBuilder.String())
}

// handleDataRequest processes client data requests (PUT/GET/DELETE) and routes them to Storage Nodes.
func (s *APIService) handleDataRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { // Using POST for all commands for simplicity
		http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
		return
	}

	var apiReq APIRequest
	if err := json.NewDecoder(r.Body).Decode(&apiReq); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request format: %v", err), http.StatusBadRequest)
		return
	}

	log.Printf("INFO: API Service: Received data command: %s Key: '%s' Value: '%s'", apiReq.Command, apiReq.Key, apiReq.Value)

	// Leader check already done in handleAPIRequest

	// 1. Determine target slot
	targetSlot := fsm.GetSlotForHashKey(apiReq.Key)
	log.Printf("DEBUG: API Service: Key '%s' maps to slot %d", apiReq.Key, targetSlot)

	// 2. Look up slot assignment in local cache
	s.slotAssignmentsMu.RLock()
	var targetSlotInfo fsm.SlotRangeInfo
	foundAssignment := false
	for _, slotInfo := range s.slotAssignments {
		if targetSlot >= slotInfo.StartSlot && targetSlot <= slotInfo.EndSlot {
			targetSlotInfo = slotInfo
			foundAssignment = true
			break
		}
	}
	s.slotAssignmentsMu.RUnlock()

	if !foundAssignment {
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("No assignment found for slot %d (key '%s'). Cluster not fully sharded or shard map not yet fetched.", targetSlot, apiReq.Key)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// 3. Get Storage Node address for the assigned slot
	storageNodeID := targetSlotInfo.AssignedNodeID
	s.storageNodeAddressesMu.RLock()
	actualStorageNodeAddr, storageNodeFound := s.storageNodeAddresses[storageNodeID]
	s.storageNodeAddressesMu.RUnlock()

	if !storageNodeFound || actualStorageNodeAddr == "" {
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Storage Node %s (for slot %d) address not found in Controller cache. Node might be down or not registered.", storageNodeID, targetSlot)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// 4. Get or establish connection to the target Storage Node
	conn, err := s.getStorageNodeConn(actualStorageNodeAddr)
	if err != nil {
		log.Printf("ERROR: API Service: Failed to connect to Storage Node %s at %s: %v", storageNodeID, actualStorageNodeAddr, err)
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Failed to connect to Storage Node %s: %v", storageNodeID, err)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// 5. Send request to Storage Node
	storageReq := fmt.Sprintf("%s %s %s\n", apiReq.Command, apiReq.Key, apiReq.Value) // Format matches Storage Node's expected input
	_, err = conn.Write([]byte(storageReq))
	if err != nil {
		log.Printf("ERROR: API Service: Failed to send request to Storage Node %s: %v", storageNodeID, err)
		s.closeStorageNodeConn(actualStorageNodeAddr) // Close stale connection
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Failed to send request to Storage Node %s: %v", storageNodeID, err)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// 6. Read response from Storage Node
	reader := bufio.NewReader(conn)
	storageRespRaw, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("ERROR: API Service: Failed to read response from Storage Node %s: %v", storageNodeID, err)
		s.closeStorageNodeConn(actualStorageNodeAddr)
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Failed to read response from Storage Node %s: %v", storageNodeID, err)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Parse Storage Node's response (e.g., "OK value", "ERROR message")
	storageRespParts := strings.Fields(strings.TrimSpace(storageRespRaw))
	if len(storageRespParts) == 0 {
		resp := APIResponse{Status: "ERROR", Message: "Empty response from Storage Node."}
		json.NewEncoder(w).Encode(resp)
		return
	}

	apiResp := APIResponse{Status: storageRespParts[0]}
	if len(storageRespParts) > 1 {
		apiResp.Message = strings.Join(storageRespParts[1:], " ")
	}

	log.Printf("INFO: API Service: Routed key '%s' to %s, received response status: %s", apiReq.Key, storageNodeID, apiResp.Status)
	json.NewEncoder(w).Encode(apiResp) // Send JSON response to original client
}

// handleTransactionRequest handles distributed transaction requests (2PC Coordinator).
func (s *APIService) handleTransactionRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
		return
	}

	var txReq TransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&txReq); err != nil {
		http.Error(w, fmt.Sprintf("Invalid transaction request format: %v", err), http.StatusBadRequest)
		return
	}
	log.Println("DEBUG: Transaction request: ", txReq)
	s.nextTxnIDMu.Lock()
	txnID := fmt.Sprintf("%d", s.nextTxnID)
	s.nextTxnID++
	s.nextTxnIDMu.Unlock()
	log.Printf("INFO: API Service: Starting new distributed transaction: %s", txnID)

	// Map to track unique participants involved in this transaction
	participants := make(map[string]string) // storageNodeID -> storageNodeAddr

	// 1. Pre-check: Determine all participants and ensure key assignments are known
	for i, op := range txReq.Operations {
		targetSlot := fsm.GetSlotForHashKey(op.Key)
		s.slotAssignmentsMu.RLock()
		var targetSlotInfo fsm.SlotRangeInfo
		foundAssignment := false
		for _, slotInfo := range s.slotAssignments {
			if targetSlot >= slotInfo.StartSlot && targetSlot <= slotInfo.EndSlot {
				targetSlotInfo = slotInfo
				foundAssignment = true
				break
			}
		}
		s.slotAssignmentsMu.RUnlock()

		if !foundAssignment {
			resp := APIResponse{Status: "ABORTED", Message: fmt.Sprintf("Txn %s: No assignment found for slot %d (key '%s') in operation %d.", txnID, targetSlot, op.Key, i)}
			json.NewEncoder(w).Encode(resp)
			log.Printf("ERROR: Txn %s aborted due to unknown slot assignment.", txnID)
			return
		}

		storageNodeID := targetSlotInfo.AssignedNodeID
		s.storageNodeAddressesMu.RLock()
		actualStorageNodeAddr, storageNodeFound := s.storageNodeAddresses[storageNodeID]
		s.storageNodeAddressesMu.RUnlock()

		if !storageNodeFound || actualStorageNodeAddr == "" {
			resp := APIResponse{Status: "ABORTED", Message: fmt.Sprintf("Txn %s: Storage Node %s address not found for operation %d.", txnID, storageNodeID, i)}
			json.NewEncoder(w).Encode(resp)
			log.Printf("ERROR: Txn %s aborted due to unknown storage node address.", txnID)
			return
		}
		participants[storageNodeID] = actualStorageNodeAddr
	}

	// --- Phase 1: Prepare ---
	log.Printf("INFO: Txn %s: Phase 1 (Prepare) initiated with %d participants.", txnID, len(participants))
	prepareVotes := make(chan bool, len(participants))
	var wg sync.WaitGroup

	for nodeID, nodeAddr := range participants {
		wg.Add(1)
		go func(id, addr string, ops []TransactionOperation) {
			defer wg.Done()

			// Send PREPARE command to Storage Node
			prepareCmd := fmt.Sprintf("PREPARE %s %s\n", txnID, serializeOperations(ops)) // ops need to be serialized

			// This command will go to the Storage Node which needs to parse it
			// For simplicity, we are passing all operations to each participant for now.
			// A smarter coordinator would send only relevant ops per participant.

			resp, err := s.sendStorageNodeCommand(addr, prepareCmd)
			if err != nil {
				log.Printf("ERROR: Txn %s: Participant %s failed to PREPARE: %v", txnID, id, err)
				prepareVotes <- false
				return
			}
			if resp.Status == "VOTE_COMMIT" {
				log.Printf("DEBUG: Txn %s: Participant %s VOTE_COMMIT.", txnID, id)
				prepareVotes <- true
			} else {
				log.Printf("INFO: Txn %s: Participant %s VOTE_ABORT: %s", txnID, id, resp.Message)
				prepareVotes <- false
			}
		}(nodeID, nodeAddr, txReq.Operations) // Pass all operations for now, SN will filter
	}

	// Wait for all prepare responses or timeout
	allVoteCommit := true
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All participants responded
		close(prepareVotes)
		for vote := range prepareVotes {
			if !vote {
				allVoteCommit = false
				break
			}
		}
	case <-time.After(prepareTimeout):
		log.Printf("WARNING: Txn %s: Prepare phase timed out after %v.", txnID, prepareTimeout)
		allVoteCommit = false // Timeout implies abort
	}

	// --- Phase 2: Commit / Abort ---
	var finalStatus string
	var finalMessage string
	commitCmd := ""

	if allVoteCommit {
		finalStatus = "COMMITTED"
		finalMessage = fmt.Sprintf("Transaction %s committed.", txnID)
		commitCmd = fmt.Sprintf("COMMIT %s\n", txnID)
		log.Printf("INFO: Txn %s: Phase 2 (COMMIT) initiated.", txnID)
	} else {
		finalStatus = "ABORTED"
		finalMessage = fmt.Sprintf("Transaction %s aborted.", txnID)
		commitCmd = fmt.Sprintf("ABORT %s\n", txnID)
		log.Printf("INFO: Txn %s: Phase 2 (ABORT) initiated.", txnID)
	}

	// Send final command to all participants
	for nodeID, nodeAddr := range participants {
		go func(id, addr string, cmd string) {
			resp, err := s.sendStorageNodeCommand(addr, cmd)
			if err != nil {
				log.Printf("ERROR: Txn %s: Participant %s failed to send %s command: %v", txnID, id, strings.TrimSpace(cmd), err)
				// Coordinator crash recovery (later) needed if this fails.
				return
			}
			log.Printf("DEBUG: Txn %s: Participant %s responded to %s with: %s", txnID, id, strings.TrimSpace(cmd), resp.Status)
		}(nodeID, nodeAddr, commitCmd)
	}

	// Respond to original client
	json.NewEncoder(w).Encode(APIResponse{Status: finalStatus, Message: finalMessage})
	log.Printf("INFO: Txn %s completed with status: %s", txnID, finalStatus)
}

// sendStorageNodeCommand is a helper to send a command string to a Storage Node and get its response.
func (s *APIService) sendStorageNodeCommand(nodeAddr string, command string) (APIResponse, error) {
	conn, err := s.getStorageNodeConn(nodeAddr)
	if err != nil {
		return APIResponse{Status: "ERROR"}, fmt.Errorf("failed to connect to storage node %s: %w", nodeAddr, err)
	}

	_, err = conn.Write([]byte(command))
	if err != nil {
		s.closeStorageNodeConn(nodeAddr)
		return APIResponse{Status: "ERROR"}, fmt.Errorf("failed to send command to storage node %s: %w", nodeAddr, err)
	}

	reader := bufio.NewReader(conn)
	respRaw, err := reader.ReadString('\n')
	if err != nil {
		s.closeStorageNodeConn(nodeAddr)
		return APIResponse{Status: "ERROR"}, fmt.Errorf("failed to read response from storage node %s: %w", nodeAddr, err)
	}

	parts := strings.Fields(strings.TrimSpace(respRaw))
	if len(parts) == 0 {
		return APIResponse{Status: "ERROR"}, fmt.Errorf("empty response from storage node %s", nodeAddr)
	}

	apiResp := APIResponse{Status: parts[0]}
	if len(parts) > 1 {
		apiResp.Message = strings.Join(parts[1:], " ")
	}
	return apiResp, nil
}

// serializeOperations is a helper to serialize a list of operations for sending to Storage Nodes.
func serializeOperations(ops []TransactionOperation) string {
	req := TransactionRequest{
		Operations: ops,
	}
	b, _ := json.Marshal(req)
	log.Println("Serialized Request: ", string(b))
	return string(b)
}

// deserializeOperations is a helper to deserialize a list of operations from a string.
func deserializeOperations(s string) ([]TransactionOperation, error) {
	parts := strings.Split(s, "|")
	var ops []TransactionOperation
	for _, part := range parts {
		if part == "" {
			continue
		}
		var op TransactionOperation
		if err := json.Unmarshal([]byte(part), &op); err != nil {
			return nil, fmt.Errorf("failed to unmarshal operation part '%s': %w", part, err)
		}
		ops = append(ops, op)
	}
	return ops, nil
}

// --- NEW: handleQueryRequest for range queries and aggregations ---
// handleQueryRequest processes client range query and aggregation requests.
func (s *APIService) handleQueryRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
		return
	}

	var apiReq APIRequest
	if err := json.NewDecoder(r.Body).Decode(&apiReq); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request format: %v", err), http.StatusBadRequest)
		return
	}

	log.Printf("INFO: API Service: Received query command: %s StartKey: '%s' EndKey: '%s'", apiReq.Command, apiReq.StartKey, apiReq.EndKey)

	// Leader check already done in handleAPIRequest

	// 1. Determine target slot(s) for the range
	startSlot := fsm.GetSlotForHashKey(apiReq.StartKey)
	endSlot := fsm.GetSlotForHashKey(apiReq.EndKey) // Note: EndKey is exclusive for range, but for slot it's inclusive

	// --- V1 Sharding Routing for Range Queries: Only support single-shard ranges ---
	s.slotAssignmentsMu.RLock()
	var responsibleNodeID string
	//var responsibleNodeAddr string
	foundResponsibleShard := false

	// Find the shard that contains the StartKey
	var startKeySlotInfo fsm.SlotRangeInfo
	foundStartKeyShard := false
	for _, slotInfo := range s.slotAssignments {
		if startSlot >= slotInfo.StartSlot && startSlot <= slotInfo.EndSlot {
			startKeySlotInfo = slotInfo
			foundStartKeyShard = true
			break
		}
	}
	s.slotAssignmentsMu.RUnlock()

	if !foundStartKeyShard {
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("No assignment found for start_key '%s' (slot %d).", apiReq.StartKey, startSlot)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Check if the entire range falls within this single shard
	// if endSlot > startKeySlotInfo.EndSlot || endSlot < startKeySlotInfo.StartSlot { // End slot outside this shard's range
	// 	resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Range query spans multiple shards or is invalid. StartKey slot %d (shard %s) and EndKey slot %d. V1 only supports single-shard range queries.", startSlot, startKeySlotInfo.RangeID, endSlot)}
	// 	json.NewEncoder(w).Encode(resp)
	// 	return
	// }

	responsibleNodeID = startKeySlotInfo.AssignedNodeID
	s.storageNodeAddressesMu.RLock()
	actualStorageNodeAddr, storageNodeFound := s.storageNodeAddresses[responsibleNodeID]
	s.storageNodeAddressesMu.RUnlock()

	if !storageNodeFound || actualStorageNodeAddr == "" {
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Storage Node %s (for range) address not found in Controller cache. Node might be down or not registered.", responsibleNodeID)}
		json.NewEncoder(w).Encode(resp)
		return
	}
	foundResponsibleShard = true // Confirmed single shard and node found
	// --- END V1 Sharding Routing ---

	if !foundResponsibleShard {
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Could not determine responsible storage node for range %s-%s.", apiReq.StartKey, apiReq.EndKey)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// 2. Send query command to the responsible Storage Node
	// Format: COMMAND <start_key> <end_key>
	storageQueryCmd := fmt.Sprintf("%s %s %s\n", apiReq.Command, apiReq.StartKey, apiReq.EndKey)

	log.Printf("DEBUG: API Service: Routing range query '%s' to Storage Node %s (%s)", apiReq.Command, responsibleNodeID, actualStorageNodeAddr)
	respFromSN, err := s.sendStorageNodeCommand(actualStorageNodeAddr, storageQueryCmd)
	if err != nil {
		log.Printf("ERROR: API Service: Failed to get response from Storage Node %s for query: %v", responsibleNodeID, err)
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Failed to query Storage Node %s: %v", responsibleNodeID, err)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// 3. Parse and return response from Storage Node
	// Assuming Storage Node will return JSON data in its message for queries.
	// For example, for GET_RANGE, it might return "OK [{"key":"a","value":"1"},{"key":"b","value":"2"}]"
	// For COUNT_RANGE, it might return "OK 5"
	apiResp := APIResponse{Status: respFromSN.Status}
	if respFromSN.Status == "OK" {
		// Attempt to unmarshal message into Data field, if it's JSON
		if json.Valid([]byte(respFromSN.Message)) {
			apiResp.Data = json.RawMessage(respFromSN.Message)
		} else {
			apiResp.Message = respFromSN.Message // Fallback to plain message
		}
	} else {
		apiResp.Message = respFromSN.Message
	}

	json.NewEncoder(w).Encode(apiResp)
	log.Printf("INFO: API Service: Range query '%s' completed with status: %s", apiReq.Command, apiResp.Status)
}

// --- END NEW ---

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Include file and line number in logs for debugging

	// Create API service instance
	service := NewAPIService(controllerHTTPAddresses)

	// Set up HTTP handler for the main router
	http.HandleFunc("/", service.handleAPIRequest) // Route all incoming requests through handleAPIRequest

	log.Printf("INFO: GojoDB API Service listening on %s:%s", apiServiceHost, apiServicePort)
	log.Println("INFO: API Endpoints:")
	log.Println("  - /api/data (POST): { \"command\": \"PUT/GET/DELETE\", \"key\": \"...\", \"value\": \"...\" }")
	log.Println("  - /api/transaction (POST): { \"operations\": [ {\"command\":\"PUT/DELETE\", \"key\":\"...\", \"value\":\"...\"}, ... ] }")
	log.Println("  - /api/query (POST): { \"command\": \"GET_RANGE/COUNT_RANGE/SUM_RANGE/MIN_RANGE/MAX_RANGE\", \"start_key\": \"...\", \"end_key\": \"...\" }")
	log.Println("  - /admin/assign_slot_range (POST, authenticated): Proxy to Controller Leader")
	log.Println("  - /admin/get_node_for_key (GET, authenticated): Proxy to Controller Leader")
	log.Println("  - /admin/set_metadata (POST, authenticated): Proxy to Controller Leader")
	log.Println("  - /status (GET): Get aggregated status of Controller cluster and Storage Nodes")
	log.Printf("  Admin API Key: %s (for X-API-Key header)", adminAPIKey)

	log.Fatal(http.ListenAndServe(apiServiceHost+":"+apiServicePort, nil))
}
