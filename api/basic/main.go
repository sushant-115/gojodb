package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort" // For sorting controller statuses
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
	clientTimeout             = 5 * time.Second
	// Basic API Key for admin endpoints
	adminAPIKey = "GOJODB_ADMIN_KEY" // Replace with a strong, secret key in production
)

// APIRequest represents a client request received by the API service.
type APIRequest struct {
	Command string `json:"command"`
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"` // Optional for GET/DELETE
}

// APIResponse represents a response sent by the API service to the client.
type APIResponse struct {
	Status  string `json:"status"`            // OK, ERROR, NOT_FOUND, REDIRECT
	Message string `json:"message,omitempty"` // Details or value for GET, or target node address for REDIRECT
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
}

// NewAPIService creates and initializes a new API Service.
func NewAPIService(controllerHTTPAddrs string) *APIService {
	service := &APIService{
		controllerAddrs:      strings.Split(controllerHTTPAddrs, ","),
		controllerStates:     make(map[string]ControllerStatus),
		storageNodeAddresses: make(map[string]string),
		slotAssignments:      make(map[string]fsm.SlotRangeInfo),
		storageNodeConns:     make(map[string]net.Conn),
	}
	go service.monitorControllerCluster() // Start monitoring controller cluster and fetching shard map
	go service.manageStorageNodeConns()   // Start managing connections
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
				log.Printf("DEBUG: API Service: Failed to reach Controller %s for status: %v", addr, err)
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
						log.Println("Before Line: ", line)
						parts := strings.SplitN(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "- ID:")), ", Addr:", 2)
						log.Println("Line: ", line, "parts: 0 ", parts[0], "parts: 1 ", parts[1])
						if len(parts) == 2 {
							storageID := strings.TrimSpace(parts[0])
							// storageID = strings.Trim("- ID:", storageID)
							// storageID = strings.TrimSpace(storageID)
							storageAddr := strings.TrimSpace(parts[1])
							tempStorageNodeAddresses[storageID] = storageAddr
							log.Println("parts: 0 ", storageID, "parts: 1 ", storageAddr, tempStorageNodeAddresses)
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

// handleAPIRequest processes incoming client requests and routes them.
func (s *APIService) handleAPIRequest(w http.ResponseWriter, r *http.Request) {
	// --- Path-based Routing ---
	path := r.URL.Path

	if strings.HasPrefix(path, "/admin/") {
		authenticate(s.handleAdminRequest).ServeHTTP(w, r)
		return
	} else if path == "/status" {
		s.handleClusterStatus(w, r)
		return
	} else if path == "/api/data" {
		s.handleDataRequest(w, r)
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

	client := &http.Client{Timeout: clientTimeout}
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

	log.Printf("INFO: API Service: Received command: %s Key: '%s' Value: '%s'", apiReq.Command, apiReq.Key, apiReq.Value)

	// --- CRITICAL: Check for Controller Leader before processing sharded requests ---
	s.leaderMu.RLock()
	leaderAddr := s.currentControllerLeader
	s.leaderMu.RUnlock()
	if leaderAddr == "" {
		resp := APIResponse{Status: "ERROR", Message: "No Controller leader available. Cannot route sharded data request."}
		json.NewEncoder(w).Encode(resp)
		return
	}
	// --- END CRITICAL CHECK ---

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
		resp := APIResponse{Status: "ERROR", Message: fmt.Sprintf("Storage Node %s (for slot %d), addr: %s, %v address not found in Controller cache. Node might be down or not registered.", storageNodeID, targetSlot, actualStorageNodeAddr, storageNodeFound)}
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

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Include file and line number in logs for debugging

	// Create API service instance
	service := NewAPIService(controllerHTTPAddresses)

	// Set up HTTP handler for the main router
	http.HandleFunc("/", service.handleAPIRequest) // Route all incoming requests through handleAPIRequest

	log.Printf("INFO: GojoDB API Service listening on %s:%s", apiServiceHost, apiServicePort)
	log.Println("INFO: API Endpoints:")
	log.Println("  - /api/data (POST): { \"command\": \"PUT/GET/DELETE\", \"key\": \"...\", \"value\": \"...\" }")
	log.Println("  - /admin/assign_slot_range (POST, authenticated): Proxy to Controller Leader")
	log.Println("  - /admin/get_node_for_key (GET, authenticated): Proxy to Controller Leader")
	log.Println("  - /status (GET): Get aggregated status of Controller cluster and Storage Nodes")
	log.Printf("  Admin API Key: %s (for X-API-Key header)", adminAPIKey)

	log.Fatal(http.ListenAndServe(apiServiceHost+":"+apiServicePort, nil))
}
