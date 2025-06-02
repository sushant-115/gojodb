package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
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
	prepareTimeout      = 5 * time.Second // Timeout for participants to respond to PREPARE
	healthCheckInterval = 5 * time.Second // How often to health check storage nodes

	// GOJODB.TEXT() wrapper prefix
	gojoDBTextPrefix = "GOJODB.TEXT("
	gojoDBTextSuffix = ")"
)

// APIRequest represents a client request received by the API service.
type APIRequest struct {
	Command  string `json:"command"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`     // Optional for GET/DELETE/PUT
	StartKey string `json:"start_key,omitempty"` // For range ops
	EndKey   string `json:"end_key,omitempty"`   // For range ops
	Query    string `json:"query,omitempty"`     // NEW: For text search queries
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

	// Storage Node Health
	storageNodeHealthMu sync.RWMutex
	storageNodeHealth   map[string]bool // StorageNodeAddress -> true (healthy) / false (unhealthy)

	// Cached Shard Map
	slotAssignmentsMu sync.RWMutex
	slotAssignments   map[string]fsm.SlotRangeInfo // Cached slot ranges (RangeID -> SlotRangeInfo)

	// Connection Pool for Storage Nodes
	storageNodePoolsMu sync.Mutex
	storageNodePools   map[string]*sync.Pool // storageNodeAddress -> *sync.Pool of net.Conn

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
		storageNodeHealth:    make(map[string]bool),
		slotAssignments:      make(map[string]fsm.SlotRangeInfo),
		storageNodePools:     make(map[string]*sync.Pool),
		nextTxnID:            time.Now().UnixNano(), // Basic unique ID for transactions
	}
	go service.monitorControllerCluster() // Start monitoring controller cluster and fetching shard map
	go service.monitorStorageNodeHealth() // Start monitoring storage node health
	return service
}

// monitorControllerCluster periodically pings controller nodes to find the current Raft leader,
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
			log.Println("WARNING: API Service: No Controller leader currently reachable. Some operations may fail.")
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

// monitorStorageNodeHealth periodically pings all known storage nodes to update their health status.
func (s *APIService) monitorStorageNodeHealth() {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.storageNodeAddressesMu.RLock()
		addresses := make([]string, 0, len(s.storageNodeAddresses))
		for _, addr := range s.storageNodeAddresses {
			addresses = append(addresses, addr)
		}
		s.storageNodeAddressesMu.RUnlock()

		for _, addr := range addresses {
			// Perform a lightweight health check (e.g., try to establish a connection)
			conn, err := net.DialTimeout("tcp", addr, storageNodeDialTimeout)
			s.storageNodeHealthMu.Lock()
			if err != nil {
				if s.storageNodeHealth[addr] { // Only log if status changed
					log.Printf("WARNING: Storage Node %s (%s) is unhealthy: %v", s.getNodeIDFromAddress(addr), addr, err)
				} else {
					log.Printf("DEBUG: Storage Node %s (%s) remains unhealthy: %v", s.getNodeIDFromAddress(addr), addr, err)
				}
				s.storageNodeHealth[addr] = false
				// Close and remove any existing connections from the pool for this unhealthy node
				s.storageNodePoolsMu.Lock()
				if pool, ok := s.storageNodePools[addr]; ok {
					for { // Drain the pool
						if c := pool.Get(); c != nil {
							c.(net.Conn).Close()
						} else {
							break
						}
					}
					delete(s.storageNodePools, addr)
				}
				s.storageNodePoolsMu.Unlock()

			} else {
				if !s.storageNodeHealth[addr] { // Only log if status changed
					log.Printf("INFO: Storage Node %s (%s) is now healthy.", s.getNodeIDFromAddress(addr), addr)
				} else {
					log.Printf("DEBUG: Storage Node %s (%s) remains healthy.", s.getNodeIDFromAddress(addr), addr)
				}
				s.storageNodeHealth[addr] = true
				conn.Close() // Close the health check connection immediately
			}
			s.storageNodeHealthMu.Unlock()
		}
	}
}

// getNodeIDFromAddress is a helper to get the NodeID from its address.
// This is a reverse lookup and might be inefficient for large numbers of nodes.
// For logging purposes, it's acceptable.
func (s *APIService) getNodeIDFromAddress(addr string) string {
	s.storageNodeAddressesMu.RLock()
	defer s.storageNodeAddressesMu.RUnlock()
	for id, a := range s.storageNodeAddresses {
		if a == addr {
			return id
		}
	}
	return "UNKNOWN_NODE"
}

// getStorageNodeConn gets or establishes a TCP connection to a Storage Node from the pool.
func (s *APIService) getStorageNodeConn(nodeAddr string) (net.Conn, error) {
	s.storageNodeHealthMu.RLock()
	isHealthy := s.storageNodeHealth[nodeAddr]
	s.storageNodeHealthMu.RUnlock()

	if !isHealthy {
		return nil, fmt.Errorf("storage node %s is unhealthy", nodeAddr)
	}

	s.storageNodePoolsMu.Lock()
	pool, ok := s.storageNodePools[nodeAddr]
	if !ok {
		// Initialize pool for this node if it doesn't exist
		pool = &sync.Pool{
			New: func() interface{} {
				log.Printf("DEBUG: API Service: Creating new connection for pool to Storage Node at %s", nodeAddr)
				conn, err := net.DialTimeout("tcp", nodeAddr, storageNodeDialTimeout)
				if err != nil {
					log.Printf("ERROR: API Service: Failed to create new connection for pool to %s: %v", nodeAddr, err)
					s.storageNodeHealthMu.Lock()
					s.storageNodeHealth[nodeAddr] = false // Mark unhealthy if connection fails
					s.storageNodeHealthMu.Unlock()
					return nil // Return nil if connection failed
				}
				//conn = setConnDeadline(conn)
				return conn
			},
		}
		s.storageNodePools[nodeAddr] = pool
	}
	s.storageNodePoolsMu.Unlock()

	// Get a connection from the pool
	conn := pool.Get()
	if conn != nil {
		// Basic check if connection is still alive (e.g., by sending a small ping)
		// For now, assume it's alive. Real system would do more robust health checks.
		// If it's not alive, pool.Get() might return nil or a closed connection.
		// We rely on the health monitor to mark nodes unhealthy.
		if isConnWritable(conn.(net.Conn)) {
			return conn.(net.Conn), nil
		}

	}

	// If pool.Get() returned nil (meaning New failed), try creating a fresh connection
	// directly, but this should be rare if the health check is working.
	log.Printf("WARNING: API Service: Pool for %s returned nil. Attempting direct dial.", nodeAddr)
	directConn, err := net.DialTimeout("tcp", nodeAddr, storageNodeDialTimeout)
	if err != nil {
		log.Printf("ERROR: API Service: Failed to dial directly to %s: %v", nodeAddr, err)
		s.storageNodeHealthMu.Lock()
		s.storageNodeHealth[nodeAddr] = false // Mark unhealthy if direct dial fails
		s.storageNodeHealthMu.Unlock()
		return nil, fmt.Errorf("failed to get or create connection to %s: %w", nodeAddr, err)
	}
	return directConn, nil
}

func isConnWritable(conn net.Conn) bool {
	err := conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	if err != nil {
		return false
	}
	_, err = conn.Write([]byte{})
	if err != nil {
		return false
	}
	return true
}

// returnStorageNodeConn returns a connection to its pool.
func (s *APIService) returnStorageNodeConn(nodeAddr string, conn net.Conn) {
	s.storageNodePoolsMu.Lock()
	defer s.storageNodePoolsMu.Unlock()

	if pool, ok := s.storageNodePools[nodeAddr]; ok {
		pool.Put(conn)
	} else {
		// Should not happen if pools are managed correctly, but close if no pool exists.
		conn.Close()
	}
}

// manageStorageNodeConns is removed, replaced by monitorStorageNodeHealth and sync.Pool.

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
	isCriticalPath := strings.HasPrefix(path, "/admin/") || path == "/api/data" || path == "/api/transaction" || path == "/api/query" || path == "/api/text_search" // NEW: Add text_search
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
	} else if path == "/api/query" { // Endpoint for range queries and aggregations
		s.handleQueryRequest(w, r)
		return
	} else if path == "/api/text_search" { // NEW: Endpoint for text-based search
		s.handleTextSearchRequest(w, r)
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

	s.storageNodeHealthMu.RLock()
	storageNodeHealth := s.storageNodeHealth
	s.storageNodeHealthMu.RUnlock()

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
			addr := storageNodes[id]
			healthStatus := "UNKNOWN"
			if healthy, ok := storageNodeHealth[addr]; ok {
				if healthy {
					healthStatus = "HEALTHY"
				} else {
					healthStatus = "UNHEALTHY"
				}
			}
			responseBuilder.WriteString(fmt.Sprintf("  - ID: %s, Addr: %s, Health: %s\n", id, addr, healthStatus))
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
			responseBuilder.WriteString(fmt.Sprintf("  - RangeID: %s (%d-%d), Assigned To: %s, Status: %s, Primary: %s, Replicas: %s\n",
				sr.RangeID, sr.StartSlot, sr.EndSlot, sr.AssignedNodeID, sr.Status, sr.PrimaryNodeID, strings.Join(sr.ReplicaNodeIDs, ",")))
		}
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, responseBuilder.String())
}

func setConnDeadline(conn net.Conn) net.Conn {
	conn.SetReadDeadline(time.Now().Add(CLIENT_TIMEOUT))
	conn.SetWriteDeadline(time.Now().Add(CLIENT_TIMEOUT))
	return conn
}

// getResponsibleNodesForSlot returns a list of healthy storage node addresses responsible for a given slot.
// If `forWrite` is true, it returns only the primary. If false, it returns primary + replicas.
func (s *APIService) getResponsibleNodesForSlot(slot int, forWrite bool) ([]string, error) {
	log.Printf("DEBUG: getResponsibleNodesForSlot called for slot %d, forWrite: %t", slot, forWrite)

	s.slotAssignmentsMu.RLock()
	var targetSlotInfo fsm.SlotRangeInfo
	foundAssignment := false
	for _, slotInfo := range s.slotAssignments {
		if slot >= slotInfo.StartSlot && slot <= slotInfo.EndSlot {
			targetSlotInfo = slotInfo
			foundAssignment = true
			break
		}
	}
	s.slotAssignmentsMu.RUnlock()

	if !foundAssignment {
		log.Printf("ERROR: No slot assignment found for slot %d", slot)
		return nil, fmt.Errorf("no assignment found for slot %d", slot)
	}
	log.Printf("DEBUG: Slot %d assigned to RangeID %s (Primary: %s, Replicas: %v)", slot, targetSlotInfo.RangeID, targetSlotInfo.PrimaryNodeID, targetSlotInfo.ReplicaNodeIDs)

	s.storageNodeAddressesMu.RLock()
	defer s.storageNodeAddressesMu.RUnlock()

	s.storageNodeHealthMu.RLock()
	defer s.storageNodeHealthMu.RUnlock()

	var candidateNodes []string

	// Check Primary
	primaryAddr, primaryOk := s.storageNodeAddresses[targetSlotInfo.PrimaryNodeID]
	if primaryOk {
		if s.storageNodeHealth[primaryAddr] {
			candidateNodes = append(candidateNodes, primaryAddr)
			log.Printf("DEBUG: Added primary %s (%s) to candidates (healthy).", targetSlotInfo.PrimaryNodeID, primaryAddr)
		} else {
			log.Printf("WARNING: Primary %s (%s) for slot %d is unhealthy. Not adding to candidates.", targetSlotInfo.PrimaryNodeID, primaryAddr, slot)
		}
	} else {
		log.Printf("WARNING: Primary %s address not found in cache for slot %d.", targetSlotInfo.PrimaryNodeID, slot)
	}

	if forWrite {
		if len(candidateNodes) == 0 { // Primary is unhealthy or not found
			log.Printf("ERROR: No healthy primary available for write to slot %d.", slot)
			return nil, fmt.Errorf("primary node for slot %d is unhealthy or not found", slot)
		}
		log.Printf("DEBUG: For write, returning only primary: %v", candidateNodes)
		return candidateNodes, nil // For writes, only primary
	}

	// For reads, add healthy replicas
	for _, replicaID := range targetSlotInfo.ReplicaNodeIDs {
		replicaAddr, replicaOk := s.storageNodeAddresses[replicaID]
		if replicaOk {
			if s.storageNodeHealth[replicaAddr] {
				candidateNodes = append(candidateNodes, replicaAddr)
				log.Printf("DEBUG: Added replica %s (%s) to candidates (healthy).", replicaID, replicaAddr)
			} else {
				log.Printf("WARNING: Replica %s (%s) for slot %d is unhealthy. Not adding to candidates.", replicaID, replicaAddr, slot)
			}
		} else {
			log.Printf("WARNING: Replica %s address not found in cache for slot %d.", replicaID, slot)
		}
	}

	if len(candidateNodes) == 0 {
		log.Printf("ERROR: No healthy storage nodes (primary or replicas) found for read to slot %d.", slot)
		return nil, fmt.Errorf("no healthy storage nodes found for slot %d", slot)
	}

	log.Printf("INFO: Final healthy candidate nodes for slot %d (forWrite: %t): %v", slot, forWrite, candidateNodes)
	return candidateNodes, nil
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

	targetSlot := fsm.GetSlotForHashKey(apiReq.Key)
	var responsibleNodes []string
	var err error

	isWriteOperation := (apiReq.Command == "PUT" || apiReq.Command == "DELETE")
	log.Printf("DEBUG: handleDataRequest: Command '%s', isWriteOperation: %t", apiReq.Command, isWriteOperation)

	if isWriteOperation {
		responsibleNodes, err = s.getResponsibleNodesForSlot(targetSlot, true) // Get only primary for writes
	} else { // Read operations (GET)
		responsibleNodes, err = s.getResponsibleNodesForSlot(targetSlot, false) // Get primary + replicas for reads
	}

	if err != nil {
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Routing error for key '%s' (slot %d): %v", apiReq.Key, targetSlot, err)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	if len(responsibleNodes) == 0 {
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("No healthy storage nodes found for key '%s' (slot %d).", apiReq.Key, targetSlot)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Select a target node (primary for writes, random for reads)
	targetNodeAddr := ""
	if isWriteOperation {
		targetNodeAddr = responsibleNodes[0] // Primary is always the first element
	} else {
		// Randomly select a node for reads
		rand.Seed(time.Now().UnixNano()) // once in init()
		index := rand.Intn(len(responsibleNodes))
		targetNodeAddr = responsibleNodes[index]
	}
	log.Printf("INFO: handleDataRequest: Selected target node %s for command '%s' (key '%s').", targetNodeAddr, apiReq.Command, apiReq.Key)

	// 4. Get or establish connection to the target Storage Node
	conn, err := s.getStorageNodeConn(targetNodeAddr)
	if err != nil {
		log.Printf("ERROR: API Service: Failed to get connection to Storage Node %s: %v", targetNodeAddr, err)
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Failed to connect to Storage Node %s: %v", targetNodeAddr, err)}
		json.NewEncoder(w).Encode(resp)
		return
	}
	defer s.returnStorageNodeConn(targetNodeAddr, conn) // Return connection to pool

	// 5. Send request to Storage Node
	// Check for GOJODB.TEXT() wrapper for PUT operations
	commandToSend := apiReq.Command
	valueToSend := apiReq.Value
	if apiReq.Command == "PUT" && strings.HasPrefix(apiReq.Value, gojoDBTextPrefix) && strings.HasSuffix(apiReq.Value, gojoDBTextSuffix) {
		// If it's a text value, modify the command to instruct the Storage Node
		// to also index it in the inverted index.
		// The Storage Node will then parse the actual text and store it.
		commandToSend = "PUT_TEXT" // New command for Storage Node
		log.Printf("DEBUG: API Service: Detected GOJODB.TEXT() wrapper for key '%s'. Sending as PUT_TEXT.", apiReq.Key)
	}

	storageReq := fmt.Sprintf("%s %s %s\n", commandToSend, apiReq.Key, valueToSend) // Format matches Storage Node's expected input
	_, err = conn.Write([]byte(storageReq))
	if err != nil {
		log.Printf("ERROR: API Service: Failed to send request to Storage Node %s: %v", targetNodeAddr, err)
		// Mark node unhealthy if write fails (connection issue)
		s.storageNodeHealthMu.Lock()
		s.storageNodeHealth[targetNodeAddr] = false
		s.storageNodeHealthMu.Unlock()
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Failed to send request to Storage Node %s: %v", targetNodeAddr, err)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// 6. Read response from Storage Node
	reader := bufio.NewReader(conn)
	storageRespRaw, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("ERROR: API Service: Failed to read response from Storage Node %s: %v", targetNodeAddr, err)
		// Mark node unhealthy if read fails (connection issue)
		s.storageNodeHealthMu.Lock()
		s.storageNodeHealth[targetNodeAddr] = false
		s.storageNodeHealthMu.Unlock()
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Failed to read response from Storage Node %s: %v", targetNodeAddr, err)}
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

	log.Printf("INFO: API Service: Routed key '%s' to %s in responsible nodes %v, received response status: %s, isWrite: %v", apiReq.Key, targetNodeAddr, responsibleNodes, apiResp.Status, isWriteOperation)
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

	// Map to track unique participants involved in this transaction and their relevant operations
	participantsOps := make(map[string][]TransactionOperation) // storageNodeAddress -> []TransactionOperation
	participantAddrs := make(map[string]struct{})              // Set of unique participant addresses

	// 1. Pre-check: Determine all participants and their operations
	for i, op := range txReq.Operations {
		targetSlot := fsm.GetSlotForHashKey(op.Key)

		// Get primary node address for the slot (writes always go to primary)
		primaryNodes, err := s.getResponsibleNodesForSlot(targetSlot, true)
		if err != nil || len(primaryNodes) == 0 {
			resp := APIResponse{Status: "ABORTED", Message: fmt.Sprintf("Txn %s: No healthy primary found for slot %d (key '%s') in operation %d: %v.", txnID, targetSlot, op.Key, i, err)}
			json.NewEncoder(w).Encode(resp)
			log.Printf("ERROR: Txn %s aborted due to unhealthy or missing primary for key '%s'.", txnID, op.Key)
			return
		}
		primaryAddr := primaryNodes[0] // Always the primary for writes

		participantsOps[primaryAddr] = append(participantsOps[primaryAddr], op)
		participantAddrs[primaryAddr] = struct{}{}
	}

	// --- Phase 1: Prepare ---
	log.Printf("INFO: Txn %s: Phase 1 (Prepare) initiated with %d unique participants.", txnID, len(participantAddrs))
	prepareVotes := make(chan bool, len(participantAddrs))
	var wg sync.WaitGroup

	for addr := range participantAddrs {
		wg.Add(1)
		go func(pAddr string, ops []TransactionOperation) {
			defer wg.Done()

			// Send PREPARE command to Storage Node
			// The operations are serialized as a JSON array within the command string.
			opsJSON, err := json.Marshal(ops)
			if err != nil {
				log.Printf("ERROR: Txn %s: Failed to marshal operations for participant %s: %v", txnID, pAddr, err)
				prepareVotes <- false
				return
			}
			prepareCmd := fmt.Sprintf("PREPARE %s %s\n", txnID, string(opsJSON))

			resp, err := s.sendStorageNodeCommand(pAddr, prepareCmd)
			if err != nil {
				log.Printf("ERROR: Txn %s: Participant %s failed to PREPARE: %v", txnID, pAddr, err)
				prepareVotes <- false
				return
			}
			if resp.Status == "VOTE_COMMIT" {
				log.Printf("DEBUG: Txn %s: Participant %s VOTE_COMMIT.", txnID, pAddr)
				prepareVotes <- true
			} else {
				log.Printf("INFO: Txn %s: Participant %s VOTE_ABORT: %s", txnID, pAddr, resp.Message)
				prepareVotes <- false
			}
		}(addr, participantsOps[addr])
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
	for addr := range participantAddrs {
		go func(pAddr string, cmd string) {
			resp, err := s.sendStorageNodeCommand(pAddr, cmd)
			if err != nil {
				log.Printf("ERROR: Txn %s: Participant %s failed to send %s command: %v", txnID, pAddr, strings.TrimSpace(cmd), err)
				// Coordinator crash recovery (later) needed if this fails.
				return
			}
			log.Printf("DEBUG: Txn %s: Participant %s responded to %s with: %s", txnID, pAddr, strings.TrimSpace(cmd), resp.Status)
		}(addr, commitCmd)
	}

	// Respond to original client
	json.NewEncoder(w).Encode(APIResponse{Status: finalStatus, Message: finalMessage})
	log.Printf("INFO: Txn %s completed with status: %s", txnID, finalStatus)
}

// sendStorageNodeCommand is a helper to send a command string to a Storage Node and get its response.
func (s *APIService) sendStorageNodeCommand(nodeAddr string, command string) (APIResponse, error) {
	conn, err := s.getStorageNodeConn(nodeAddr)
	if err != nil {
		return APIResponse{Status: "ERROR"}, fmt.Errorf("failed to get connection to storage node %s: %w", nodeAddr, err)
	}
	defer s.returnStorageNodeConn(nodeAddr, conn) // Return connection to pool

	_, err = conn.Write([]byte(command))
	if err != nil {
		// Mark node unhealthy if write fails (connection issue)
		s.storageNodeHealthMu.Lock()
		s.storageNodeHealth[nodeAddr] = false
		s.storageNodeHealthMu.Unlock()
		return APIResponse{Status: "ERROR"}, fmt.Errorf("failed to send command to storage node %s: %w", nodeAddr, err)
	}

	reader := bufio.NewReader(conn)
	respRaw, err := reader.ReadString('\n')
	if err != nil {
		// Mark node unhealthy if read fails (connection issue)
		s.storageNodeHealthMu.Lock()
		s.storageNodeHealth[nodeAddr] = false
		s.storageNodeHealthMu.Unlock()
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
// This is used by the API Service (Coordinator) to send operations to participants.
func serializeOperations(ops []TransactionOperation) string {
	// For 2PC, the Storage Node expects a JSON array of operations directly.
	b, _ := json.Marshal(ops) // Marshal the slice of operations directly
	log.Println("Serialized Request: ", string(b))
	return string(b)
}

// deserializeOperations is a helper to deserialize a list of operations from a string.
// This is used by the Storage Node (Participant) to receive operations from the Coordinator.
// NOTE: This function is in cmd/gojodb_cluster_server/main.go. This one is for the API service.
func deserializeOperations(s string) ([]TransactionOperation, error) {
	var ops []TransactionOperation
	if err := json.Unmarshal([]byte(s), &ops); err != nil { // Unmarshal directly into slice
		return nil, fmt.Errorf("failed to unmarshal operations JSON: %w", err)
	}
	return ops, nil
}

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

	// Determine target slot(s) for the range
	startSlot := fsm.GetSlotForHashKey(apiReq.StartKey)

	// Get healthy nodes for read (primary + replicas)
	responsibleNodes, err := s.getResponsibleNodesForSlot(startSlot, false)
	if err != nil {
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Routing error for range query (start key '%s', slot %d): %v", apiReq.StartKey, startSlot, err)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	if len(responsibleNodes) == 0 {
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("No healthy storage nodes found for range query (start key '%s', slot %d).", apiReq.StartKey, startSlot)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Select a random healthy node for the query
	targetNodeAddr := responsibleNodes[time.Now().Nanosecond()%len(responsibleNodes)]

	// Send query command to the responsible Storage Node
	// Format: COMMAND <start_key> <end_key>
	storageQueryCmd := fmt.Sprintf("%s %s %s\n", apiReq.Command, apiReq.StartKey, apiReq.EndKey)

	log.Printf("DEBUG: API Service: Routing range query '%s' to Storage Node %s", apiReq.Command, targetNodeAddr)
	respFromSN, err := s.sendStorageNodeCommand(targetNodeAddr, storageQueryCmd)
	if err != nil {
		log.Printf("ERROR: API Service: Failed to get response from Storage Node %s for query: %v", targetNodeAddr, err)
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Failed to query Storage Node %s: %v", targetNodeAddr, err)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Parse and return response from Storage Node
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

// handleTextSearchRequest processes client text-based search requests.
func (s *APIService) handleTextSearchRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
		return
	}

	var apiReq APIRequest
	if err := json.NewDecoder(r.Body).Decode(&apiReq); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request format: %v", err), http.StatusBadRequest)
		return
	}

	if apiReq.Query == "" {
		http.Error(w, "Query parameter is required for text search.", http.StatusBadRequest)
		return
	}

	log.Printf("INFO: API Service: Received text search query: '%s'", apiReq.Query)

	// For text search, we don't shard by key. We need to query ALL storage nodes
	// that might contain relevant inverted index data.
	// For simplicity in V1, we'll pick a random node and assume it can forward/coordinate
	// or that the inverted index is replicated across all nodes and any can serve.
	// A more robust solution would involve a dedicated text search service or
	// broadcasting the query to all relevant nodes and aggregating results.

	s.storageNodeAddressesMu.RLock()
	addresses := make([]string, 0, len(s.storageNodeAddresses))
	for _, addr := range s.storageNodeAddresses {
		// Only consider healthy nodes for the query
		s.storageNodeHealthMu.RLock()
		if s.storageNodeHealth[addr] {
			addresses = append(addresses, addr)
		}
		s.storageNodeHealthMu.RUnlock()
	}
	s.storageNodeAddressesMu.RUnlock()

	if len(addresses) == 0 {
		resp := APIResponse{Status: "ERROR", Message: "No healthy storage nodes available for text search."}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Select a random healthy node to send the text search query to.
	// In a real system, this might be a dedicated text search node or a broadcast.
	targetNodeAddr := addresses[rand.Intn(len(addresses))]
	log.Printf("DEBUG: API Service: Routing text search '%s' to Storage Node %s", apiReq.Query, targetNodeAddr)

	// Send TEXT_SEARCH command to the selected Storage Node
	storageSearchCmd := fmt.Sprintf("TEXT_SEARCH %s\n", apiReq.Query)
	respFromSN, err := s.sendStorageNodeCommand(targetNodeAddr, storageSearchCmd)
	if err != nil {
		log.Printf("ERROR: API Service: Failed to get response from Storage Node %s for text search: %v", targetNodeAddr, err)
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Failed to perform text search on Storage Node %s: %v", targetNodeAddr, err)}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Parse and return response from Storage Node
	apiResp := APIResponse{Status: respFromSN.Status}
	if respFromSN.Status == "OK" {
		// The Storage Node should return a JSON array of Entry objects (key-value pairs)
		if json.Valid([]byte(respFromSN.Message)) {
			apiResp.Data = json.RawMessage(respFromSN.Message)
		} else {
			apiResp.Message = respFromSN.Message // Fallback to plain message if not JSON
		}
	} else {
		apiResp.Message = respFromSN.Message
	}

	json.NewEncoder(w).Encode(apiResp)
	log.Printf("INFO: API Service: Text search '%s' completed with status: %s", apiReq.Query, apiResp.Status)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Include file and line number in logs for debugging

	// Create API service instance
	service := NewAPIService(controllerHTTPAddresses)

	// Set up HTTP handler for the main router
	http.HandleFunc("/", service.handleAPIRequest) // Route all incoming requests through handleAPIRequest

	log.Printf("INFO: GojoDB API Service listening on %s:%s", apiServiceHost, apiServicePort)
	log.Println("INFO: API Endpoints:")
	log.Println("  - /api/data (POST): { \"command\": \"PUT/GET/DELETE\", \"key\": \"...\", \"value\": \"...\" }")
	log.Println("    (Use GOJODB.TEXT(your text) for text indexing in PUT value)")
	log.Println("  - /api/transaction (POST): { \"operations\": [ {\"command\":\"PUT/DELETE\", \"key\":\"...\", \"value\":\"...\"}, ... ] }")
	log.Println("  - /api/query (POST): { \"command\": \"GET_RANGE/COUNT_RANGE/SUM_RANGE/MIN_RANGE/MAX_RANGE\", \"start_key\": \"...\", \"end_key\": \"...\" }")
	log.Println("  - /api/text_search (POST): { \"query\": \"your search terms\" }") // NEW: Text search endpoint
	log.Println("  - /admin/assign_slot_range (POST, authenticated): Proxy to Controller Leader")
	log.Println("  - /admin/get_node_for_key (GET, authenticated): Proxy to Controller Leader")
	log.Println("  - /admin/set_metadata (POST, authenticated): Proxy to Controller Leader")
	log.Println("  - /status (GET): Get aggregated status of Controller cluster and Storage Nodes")
	log.Printf("  Admin API Key: %s (for X-API-Key header)", adminAPIKey)

	log.Fatal(http.ListenAndServe(apiServiceHost+":"+apiServicePort, nil))
}
