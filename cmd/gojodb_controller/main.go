package gojodbcontroller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv" // Added for parsing slot IDs from JSON keys
	"sync"
	"time"

	"github.com/hashicorp/raft"
	fsm "github.com/sushant-115/gojodb/core/replication/raft_consensus"
)

const (
	RaftDir        = "./raft"
	RaftBindAddr   = "localhost:7000"
	NodeID         = "nodeA"
	HTTPListenAddr = "localhost:8080"
	// HEARTBEAT_LISTEN_ADDR is the address where the controller expects heartbeats from storage nodes.
	HEARTBEAT_LISTEN_ADDR = ":8081"
	HeartbeatTimeout      = 15 * time.Second // Timeout for storage node heartbeats
)

// Controller manages the Raft cluster and storage node metadata.
type Controller struct {
	raft                 *raft.Raft
	fsm                  *fsm.FSM
	httpServer           *http.Server
	leaderControllerAddr string
	// Track active storage nodes based on heartbeats
	activeStorageNodes     map[string]time.Time // nodeId -> lastHeartbeatTime
	storageNodeInfoMutex   sync.RWMutex
	storageNodeIDToAddress map[string]string // nodeID -> gRPC Address received via heartbeat/registration
	lastHeartbeatReceived  map[string]time.Time
	heartbeatAddress       string
	HeartbeatServer        *http.Server
	mu                     sync.Mutex
}

// NewController creates a new Controller instance.
func NewController(raftFSM *fsm.FSM, raftNode *raft.Raft, heartbeatAddr string, leaderAddr string) (*Controller, error) {
	c := &Controller{
		fsm:                    raftFSM,
		activeStorageNodes:     make(map[string]time.Time),
		storageNodeIDToAddress: make(map[string]string),
		lastHeartbeatReceived:  make(map[string]time.Time),
		heartbeatAddress:       heartbeatAddr,
		leaderControllerAddr:   leaderAddr,
	}

	c.raft = raftNode

	c.httpServer = &http.Server{
		Addr: HTTPListenAddr,
	}

	// Start monitoring storage node heartbeats
	go c.monitorStorageNodes()
	go c.startHeartbeatListener(heartbeatAddr)

	return c, nil
}

// startHeartbeatListener starts an HTTP server to receive heartbeats from storage nodes.
func (c *Controller) startHeartbeatListener(heartbeatAddr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/heartbeat", c.handleHeartbeat)

	heartbeatServer := &http.Server{
		Addr:    heartbeatAddr,
		Handler: mux,
	}
	c.HeartbeatServer = heartbeatServer
	// log.Printf("Heartbeat listener starting on %s", heartbeatAddr)
	if err := heartbeatServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Heartbeat listener failed: %v", err)
	}
	// log.Println("Heartbeat listener stopped.")
}

// handleHeartbeat processes heartbeat requests from storage nodes.
func (c *Controller) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	nodeID := r.URL.Query().Get("nodeId")
	address := r.URL.Query().Get("address") // gRPC address of the storage node
	replicationAddr := r.URL.Query().Get("replication_addr")
	grpcAddr := r.URL.Query().Get("grpc_addr")
	// ("Received heartbeat: ", nodeID, address)
	if nodeID == "" || address == "" {
		http.Error(w, "nodeId and address are required", http.StatusBadRequest)
		return
	}

	c.storageNodeInfoMutex.Lock()
	c.lastHeartbeatReceived[nodeID] = time.Now()
	// Update storage node address if it changes or is new
	if c.storageNodeIDToAddress[nodeID] != address {
		c.storageNodeIDToAddress[nodeID] = address
	}
	c.storageNodeInfoMutex.Unlock()

	rawMessage := fmt.Sprintf(`{"status":"active", "replication_addr": "%s", "grpc_addr": "%s"}`, replicationAddr, grpcAddr)

	// Submit a command to Raft FSM to update node status and address
	cmd := fsm.Command{
		Type:     fsm.CommandRegisterNode, // Use RegisterNode for heartbeats to update status/address
		NodeID:   nodeID,
		NodeAddr: address,
		Payload:  json.RawMessage(rawMessage),
	}
	err := c.applyCommand(cmd)
	if err != nil {
		http.Error(w, "failed to apply raft command: "+err.Error(), http.StatusBadRequest)
	}

	//w.WriteHeader(http.StatusOK)
	w.Write([]byte("Heartbeat received"))
}

// monitorStorageNodes periodically checks for expired heartbeats and updates node status.
func (c *Controller) monitorStorageNodes() {
	ticker := time.NewTicker(HeartbeatTimeout / 2) // Check more frequently than timeout
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.storageNodeInfoMutex.Lock()
			for nodeID, lastHeartbeat := range c.lastHeartbeatReceived {
				if time.Since(lastHeartbeat) > HeartbeatTimeout {
					// log.Printf("Storage node %s heartbeat expired. Marking as down.", nodeID)
					// Submit Raft command to mark node as down
					cmd := fsm.Command{
						Type:    fsm.CommandUpdateNodeStatus,
						NodeID:  nodeID,
						Payload: json.RawMessage(`{"status":"down"}`),
					}
					c.applyCommand(cmd)
				} else {
					// Mark as active if it was previously down and now heartbeating
					nodeStatus, found := c.fsm.GetNodeStatus(nodeID)
					if nodeStatus != "active" && found {
						cmd := fsm.Command{
							Type:    fsm.CommandUpdateNodeStatus,
							NodeID:  nodeID,
							Payload: json.RawMessage(`{"status":"active"}`),
						}
						c.applyCommand(cmd)
					}
				}
			}
			c.storageNodeInfoMutex.Unlock()
		case <-c.fsm.ShutdownChan(): // Use a real quit channel from Controller struct if available
			return
		}
	}
}

func (c *Controller) forwardRaftCommandToLeader(cmd fsm.Command) error {
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("follower: failed to marshal command for forwarding: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/admin/apply_forwarded_command", c.leaderControllerAddr), bytes.NewBuffer(cmdBytes))
	if err != nil {
		return fmt.Errorf("follower: failed to create forwarding request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("follower: failed to forward command to leader %s: %v", c.leaderControllerAddr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("follower: leader returned non-OK status %d for forwarded command: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// applyCommand submits a command to the Raft leader.
func (c *Controller) applyCommand(cmd fsm.Command) error {
	if c.raft.State() != raft.Leader {
		log.Println("Can't apply command not leader, forwarding it to leader: ", c.leaderControllerAddr)
		if err := c.forwardRaftCommandToLeader(cmd); err != nil {
			return fmt.Errorf("failed to forward the apply command to leader %s , error: %v", c.leaderControllerAddr, err)
		}
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %v", err)
	}

	future := c.raft.Apply(cmdBytes, 5*time.Second) // Apply with a timeout
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to apply command to Raft: %v", err)
	}
	// Check if the FSM application itself returned an error
	if response := future.Response(); response != nil {
		if applyErr, ok := response.(error); ok {
			return fmt.Errorf("FSM apply returned error: %v", applyErr)
		}
	}
	return nil
}

// --- HTTP Handlers for Admin API ---

func (c *Controller) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/join", c.handleJoin)
	mux.HandleFunc("/remove_peer", c.handleRemovePeer)
	// mux.HandleFunc("/metadata", c.handleMetadata)
	mux.HandleFunc("/status", c.handleStatus) // Corresponds to GetClusterStatus

	mux.HandleFunc("/admin/assign_slot_range", c.handleAdminAssignSlotRange)              // Corresponds to AssignShardSlot
	mux.HandleFunc("/admin/get_all_slot_assignments", c.handleAdminGetAllSlotAssignments) // Corresponds to GetShardMap
	mux.HandleFunc("/admin/get_node_for_key", c.handleAdminGetNodeForKey)
	mux.HandleFunc("/admin/set_slot_primary", c.handleAdminSetSlotPrimary) // Renamed for clarity

	// New handlers for cluster management (receiving from Gateway)
	mux.HandleFunc("/admin/register_storage_node", c.handleAdminRegisterStorageNode) // Corresponds to AddStorageNode
	mux.HandleFunc("/admin/remove_storage_node", c.handleAdminRemoveStorageNode)     // Corresponds to RemoveStorageNode
	mux.HandleFunc("/admin/initiate_replica_onboarding", c.handleAdminInitiateReplicaOnboarding)
	mux.HandleFunc("/admin/update_replica_onboarding_state", c.handleAdminUpdateReplicaOnboardingState)
	mux.HandleFunc("/admin/initiate_shard_migration", c.handleAdminInitiateShardMigration)
	mux.HandleFunc("/admin/commit_shard_migration", c.handleAdminCommitShardMigration)
	// New internal endpoint for forwarded commands
	mux.HandleFunc("/admin/apply_forwarded_command", c.handleApplyForwardedCommand)
}

// handleJoin handles requests to join a new Raft peer.
func (c *Controller) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	peerAddress := r.URL.Query().Get("peerAddress")
	nodeID := r.URL.Query().Get("nodeId")

	if peerAddress == "" || nodeID == "" {
		http.Error(w, "peerAddress and nodeId are required", http.StatusBadRequest)
		return
	}

	if c.raft.State() != raft.Leader {
		http.Error(w, "Not the Raft leader", http.StatusForbidden)
		return
	}

	future := c.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(peerAddress), 0, 0)
	if err := future.Error(); err != nil {
		// log.Printf("Failed to add voter %s (%s): %v", nodeID, peerAddress, err)
		http.Error(w, fmt.Sprintf("Failed to add voter: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Node %s (%s) joined the Raft cluster", nodeID, peerAddress)))
}

// handleRemovePeer handles requests to remove a Raft peer.
func (c *Controller) handleRemovePeer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	nodeID := r.URL.Query().Get("nodeId")
	if nodeID == "" {
		http.Error(w, "nodeId is required", http.StatusBadRequest)
		return
	}

	if c.raft.State() != raft.Leader {
		http.Error(w, "Not the Raft leader", http.StatusForbidden)
		return
	}

	future := c.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if err := future.Error(); err != nil {
		// log.Printf("Failed to remove peer %s: %v", nodeID, err)
		http.Error(w, fmt.Sprintf("Failed to remove peer: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Node %s removed from the Raft cluster", nodeID)))
}

// handleMetadata handles requests to set or get generic cluster metadata.
// func (c *Controller) handleMetadata(w http.ResponseWriter, r *http.Request) {
// 	if r.Method == http.MethodPost {
// 		var data struct {
// 			Key   string `json:"key"`
// 			Value string `json:"value"`
// 		}
// 		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
// 			http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
// 			return
// 		}
// 		cmd := fsm.Command{
// 			Type:  fsm.CommandUpdateMetadata,
// 			Key:   data.Key,
// 			Value: data.Value,
// 		}
// 		if err := c.applyCommand(cmd); err != nil {
// 			http.Error(w, fmt.Sprintf("Failed to update metadata: %v", err), http.StatusInternalServerError)
// 			return
// 		}
// 		w.WriteHeader(http.StatusOK)
// 		w.Write([]byte("Metadata updated"))
// 	} else if r.Method == http.MethodGet {
// 		key := r.URL.Query().Get("key")
// 		if key == "" {
// 			// Return all metadata
// 			json.NewEncoder(w).Encode(c.fsm.GetMetadata())
// 		} else {
// 			// Return specific key
// 			metadata := c.fsm.GetMetadata()
// 			if val, ok := metadata[key]; ok {
// 				json.NewEncoder(w).Encode(map[string]string{key: val})
// 			} else {
// 				http.Error(w, "Key not found", http.StatusNotFound)
// 			}
// 		}
// 	} else {
// 		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
// 	}
// }

// handleStatus provides a comprehensive overview of the cluster state.
// This handles the GetClusterStatus request from the Gateway.
func (c *Controller) handleStatus(w http.ResponseWriter, r *http.Request) {
	nodeStatuses := c.fsm.GetNodeStatuses()
	nodeAddresses := c.fsm.GetNodeAddresses()
	nodeGrpcAddresses := c.fsm.GetNodeGrpcAddresses()
	slotAssignments := c.fsm.GetSlotAssignments()
	onboardingStates := c.fsm.GetReplicaOnboardingStates()
	migrationStates := c.fsm.GetShardMigrationStates()

	activeNodesInfo := make(map[string]struct {
		Address       string `json:"address"`
		GrpcAddress   string `json:"grpc_addr"`
		Status        string `json:"status"`
		LastHeartbeat string `json:"last_heartbeat"`
	})

	c.storageNodeInfoMutex.RLock()
	for nodeID, address := range nodeAddresses { // Changed to iterate over nodeAddresses from FSM
		// Get status from FSM
		status := nodeStatuses[nodeID]
		// Get local last heartbeat time, if available
		lastHeartbeat, heartbeatFound := c.lastHeartbeatReceived[nodeID]

		activeNodesInfo[nodeID] = struct {
			Address       string `json:"address"`
			GrpcAddress   string `json:"grpc_addr"`
			Status        string `json:"status"`
			LastHeartbeat string `json:"last_heartbeat"`
		}{
			Address:     address, // This is directly from FSM
			GrpcAddress: nodeGrpcAddresses[nodeID],
			Status:      status, // This is directly from FSM
			LastHeartbeat: func() string {
				if heartbeatFound {
					return lastHeartbeat.Format(time.RFC3339)
				}
				return "N/A" // Node known by FSM, but this controller hasn't received a heartbeat yet
			}(),
		}
	}
	c.storageNodeInfoMutex.RUnlock()

	// Convert slotAssignments from map[uint64] to map[string] for JSON serialization
	// This is because JSON keys must be strings.
	serializableSlotAssignments := make(map[string]fsm.SlotAssignment)
	for slotID, assign := range slotAssignments {
		serializableSlotAssignments[strconv.FormatUint(uint64(slotID), 10)] = assign
	}

	// Convert migrationStates from map[uint64] to map[string] for JSON serialization
	serializableMigrationStates := make(map[string]fsm.ShardMigrationState)
	for slotID, mig := range migrationStates {
		serializableMigrationStates[slotID] = mig
	}

	status := struct {
		RaftState   string        `json:"raft_state"`
		RaftLeader  string        `json:"raft_leader"`
		RaftPeers   []raft.Server `json:"raft_peers"`
		ActiveNodes map[string]struct {
			Address       string `json:"address"`
			GrpcAddress   string `json:"grpc_addr"`
			Status        string `json:"status"`
			LastHeartbeat string `json:"last_heartbeat"`
		} `json:"active_nodes"`
		SlotAssignments  map[string]fsm.SlotAssignment         `json:"slot_assignments"`
		OnboardingStates map[string]fsm.ReplicaOnboardingState `json:"onboarding_states"`
		MigrationStates  map[string]fsm.ShardMigrationState    `json:"migration_states"`
	}{
		RaftState:        c.raft.State().String(),
		RaftLeader:       string(c.raft.Leader()),
		RaftPeers:        c.raft.GetConfiguration().Configuration().Servers,
		ActiveNodes:      activeNodesInfo,
		SlotAssignments:  serializableSlotAssignments,
		OnboardingStates: onboardingStates,
		MigrationStates:  serializableMigrationStates,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// handleAdminAssignSlotRange handles requests to assign a slot range to a primary and replicas.
// This handles the AssignShardSlot request from the Gateway.
func (c *Controller) handleAdminAssignSlotRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		SlotID         uint64   `json:"slotID"`
		PrimaryNodeID  string   `json:"primaryNodeId"`
		ReplicaNodeIDs []string `json:"replicaNodeIds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	cmd := fsm.Command{
		Type:           fsm.CommandAssignSlot,
		SlotID:         req.SlotID,
		PrimaryNodeID:  req.PrimaryNodeID,
		ReplicaNodeIDs: req.ReplicaNodeIDs,
	}

	if err := c.applyCommand(cmd); err != nil {
		http.Error(w, fmt.Sprintf("Failed to assign slot: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Slot %d assigned to primary %s with replicas %v \n", req.SlotID, req.PrimaryNodeID, req.ReplicaNodeIDs)))
}

// handleAdminGetAllSlotAssignments returns the current shard slot assignments.
// This handles the GetShardMap request from the Gateway.
func (c *Controller) handleAdminGetAllSlotAssignments(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	assignments := c.fsm.GetSlotAssignments()
	// Convert map[uint64] to map[string] for JSON serialization
	serializableAssignments := make(map[string]fsm.SlotAssignment)
	for slotID, assign := range assignments {
		serializableAssignments[strconv.FormatUint(uint64(slotID), 10)] = assign
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(serializableAssignments)
}

// handleAdminGetNodeForKey returns the node responsible for a given key.
func (c *Controller) handleAdminGetNodeForKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	slotID := fsm.GetSlotForHashKey(key)
	assignments := c.fsm.GetSlotAssignments()

	assignment, found := assignments[uint64(slotID)]
	if !found {
		http.Error(w, fmt.Sprintf("No assignment found for key's slot %d", slotID), http.StatusNotFound)
		return
	}

	// For simplicity, return the primary node for queries. A real system might return a replica for reads.
	respData := map[string]string{
		"slotId":        strconv.FormatUint(uint64(slotID), 10),
		"primaryNodeId": assignment.PrimaryNodeID,
	}
	if len(assignment.ReplicaNodes) > 0 {
		respData["replicaNodeIds"] = fmt.Sprintf("%v", assignment.ReplicaNodes)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(respData)
}

// handleAdminSetSlotPrimary handles requests to change the primary for a slot.
func (c *Controller) handleAdminSetSlotPrimary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		SlotID           uint64   `json:"slotId"`
		NewPrimaryNodeID string   `json:"newPrimaryNodeId"`
		ReplicaNodeIDs   []string `json:"replicaNodeIds"` // Optional: provide updated replicas
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	cmd := fsm.Command{
		Type:           fsm.CommandSetSlotPrimary,
		SlotID:         req.SlotID,
		PrimaryNodeID:  req.NewPrimaryNodeID,
		ReplicaNodeIDs: req.ReplicaNodeIDs, // Pass through new replicas
	}

	if err := c.applyCommand(cmd); err != nil {
		http.Error(w, fmt.Sprintf("Failed to set slot primary: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Slot %d primary set to %s", req.SlotID, req.NewPrimaryNodeID)))
}

// handleAdminRegisterStorageNode allows explicit registration of a storage node.
// This handles the AddStorageNode request from the Gateway.
func (c *Controller) handleAdminRegisterStorageNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		NodeID          string `json:"nodeId"`
		Address         string `json:"address"`
		ReplicationAddr string `json:"replication_addr"`
		GrpcAddr        string `json:"grpc_addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}
	payload := fmt.Sprintf(`{"status": "registered", "replication_addr": "%s", "grpc_addr", "%s"}`, req.ReplicationAddr, req.GrpcAddr)

	cmd := fsm.Command{
		Type:     fsm.CommandRegisterNode,
		NodeID:   req.NodeID,
		NodeAddr: req.Address,
		Payload:  json.RawMessage(payload), // Initial status
	}
	if err := c.applyCommand(cmd); err != nil {
		http.Error(w, fmt.Sprintf("Failed to register storage node: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Storage node %s registered with address %s", req.NodeID, req.Address)))
}

// handleAdminRemoveStorageNode allows explicit removal of a storage node.
// This handles the RemoveStorageNode request from the Gateway.
func (c *Controller) handleAdminRemoveStorageNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		NodeID string `json:"nodeId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	cmd := fsm.Command{
		Type:   fsm.CommandDeregisterNode,
		NodeID: req.NodeID,
	}
	if err := c.applyCommand(cmd); err != nil {
		http.Error(w, fmt.Sprintf("Failed to remove storage node: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Storage node %s removed", req.NodeID)))
}

// handleAdminInitiateReplicaOnboarding initiates a replica onboarding process.
// This handles the InitiateReplicaOnboarding request from the Gateway.
func (c *Controller) handleAdminInitiateReplicaOnboarding(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		SlotID         uint64 `json:"slotId"`
		ReplicaNodeID  string `json:"replicaNodeId"`
		PrimaryNodeID  string `json:"primaryNodeId"`
		ReplicaAddress string `json:"replicaAddress"`
		PrimaryAddress string `json:"primaryAddress"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}
	replicas := make(map[string]string)
	replicas[req.ReplicaNodeID] = req.ReplicaAddress

	onboardingID := fmt.Sprintf("%d-%s", req.SlotID, req.ReplicaNodeID)

	cmd := fsm.Command{
		Type:          fsm.CommandInitiateReplicaOnboarding,
		SlotID:        req.SlotID,
		PrimaryNodeID: req.PrimaryNodeID,
		Replicas:      replicas,
		NodeAddr:      req.PrimaryAddress,
		OperationID:   onboardingID,
	}
	if err := c.applyCommand(cmd); err != nil {
		http.Error(w, fmt.Sprintf("Failed to initiate replica onboarding: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"message":      "Replica onboarding initiated",
		"onboardingId": onboardingID,
	})
}

// handleAdminUpdateReplicaOnboardingState updates the state of an ongoing replica onboarding.
// This handles the UpdateReplicaOnboardingState request from the Gateway.
func (c *Controller) handleAdminUpdateReplicaOnboardingState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		OnboardingID string `json:"onboardingId"`
		Status       string `json:"status"`
		CurrentLSN   uint64 `json:"currentLsn"`
		TargetLSN    uint64 `json:"targetLsn"`
		ErrorMessage string `json:"errorMessage"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	payload := map[string]string{
		"status":     req.Status,
		"currentLSN": strconv.FormatUint(req.CurrentLSN, 10),
		"targetLSN":  strconv.FormatUint(req.TargetLSN, 10),
		"error":      req.ErrorMessage,
	}

	b, _ := json.Marshal(payload)

	cmd := fsm.Command{
		Type:        fsm.CommandUpdateReplicaOnboardingState,
		OperationID: req.OnboardingID,
		Payload:     json.RawMessage(b),
	}
	if err := c.applyCommand(cmd); err != nil {
		http.Error(w, fmt.Sprintf("Failed to update replica onboarding state: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Replica onboarding state updated"))
}

// handleApplyForwardedCommand receives forwarded commands from followers.
// This endpoint is only intended for internal Raft communication, not external clients.
func (c *Controller) handleApplyForwardedCommand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if c.raft.State() != raft.Leader {
		// This should theoretically not happen if followers correctly forward to the leader,
		// but it's a good safeguard.
		http.Error(w, "Not the Raft leader", http.StatusForbidden)
		return
	}

	var cmd fsm.Command
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, fmt.Sprintf("Invalid forwarded command body: %v", err), http.StatusBadRequest)
		return
	}

	// Apply the command received from the follower.
	// This will go through the regular Raft Apply path on the leader.
	if err := c.applyCommand(cmd); err != nil {
		// Log the error but return 200 to the follower, as the follower's job
		// was just to forward. The error is now the leader's responsibility.
		// A more robust system might relay the exact error back.
		// log.Printf("Leader failed to apply forwarded command type %d: %v", cmd.Type, err)
		http.Error(w, fmt.Sprintf("Leader failed to apply forwarded command: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Command applied by leader"))
}

// handleAdminInitiateShardMigration initiates a shard migration process.
// This handles the InitiateShardMigration request from the Gateway.
func (c *Controller) handleAdminInitiateShardMigration(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		SlotID        uint64 `json:"slotId"`
		SourceNodeID  string `json:"sourceNodeId"`
		TargetNodeID  string `json:"targetNodeId"`
		SourceAddress string `json:"sourceAddress"`
		TargetAddress string `json:"targetAddress"`
		OperationID   string `json:"operation_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	migrationID := fmt.Sprintf("%d-%s-%s", req.SlotID, req.SourceNodeID, req.TargetNodeID)

	cmd := fsm.Command{
		Type:         fsm.CommandInitiateShardMigration,
		SlotID:       req.SlotID,
		ShardID:      strconv.Itoa(int(req.SlotID)),
		NodeID:       req.SourceNodeID,
		TargetNodeID: req.TargetNodeID,
		NodeAddr:     req.SourceAddress,
		OperationID:  migrationID,
		MigrationState: &fsm.ShardMigrationState{
			OperationID:  migrationID,
			SlotID:       req.SlotID,
			SourceNodeID: req.SourceNodeID,
			TargetNodeID: req.TargetNodeID,
			CurrentPhase: "PREPARING",
		},
	}
	if err := c.applyCommand(cmd); err != nil {
		http.Error(w, fmt.Sprintf("Failed to initiate shard migration: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"message":     "Shard migration initiated",
		"migrationId": migrationID,
	})
}

// handleAdminCommitShardMigration commits a pending shard migration.
// This handles the CommitShardMigration request from the Gateway.
func (c *Controller) handleAdminCommitShardMigration(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		SlotID            uint64   `json:"slotId"`
		NewPrimaryNodeID  string   `json:"newPrimaryNodeId"`
		NewReplicaNodeIDs []string `json:"newReplicaNodeIds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	cmd := fsm.Command{
		Type:           fsm.CommandCommitShardMigration,
		SlotID:         req.SlotID,
		NodeID:         req.NewPrimaryNodeID,
		ReplicaNodeIDs: req.NewReplicaNodeIDs,
	}
	if err := c.applyCommand(cmd); err != nil {
		http.Error(w, fmt.Sprintf("Failed to commit shard migration: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Shard migration committed"))
}
