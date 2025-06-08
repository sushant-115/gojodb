package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	pb "github.com/sushant-115/gojodb/api/proto" // Assuming your proto package is named 'proto'
	fsm "github.com/sushant-115/gojodb/core/replication/raft_consensus"
)

const (
	// DefaultGatewayPort is the port on which the GojoDB Gateway listens.
	DefaultGatewayPort = ":50051"
	// DefaultControllerAddr is the default address of the GojoDB Controller.
	DefaultControllerAddr = "localhost:8080"
	// ShardMapUpdateInterval is the interval at which the gateway polls the controller for shard map updates.
	ShardMapUpdateInterval = 5 * time.Second
	// NumShardSlots defines the total number of hash slots for sharding.
	NumShardSlots = 1024 // Must match fsm.NumShardSlots
)

// GatewayService implements the gRPC GatewayService.
type GatewayService struct {
	pb.UnimplementedGatewayServiceServer // Embed for forward compatibility
	controllerAddr                       string
	mu                                   sync.RWMutex
	slotAssignments                      map[uint32]*fsm.SlotAssignment
	storageNodeAddresses                 map[string]string // nodeId -> gRPC address (e.g., "node1" -> "localhost:50052")
	nodeConns                            *sync.Pool        // Pool of gRPC client connections to storage nodes
	quit                                 chan struct{}
	httpClient                           *http.Client // HTTP client for controller API calls
}

// NewGatewayService creates a new GatewayService instance.
func NewGatewayService(controllerAddr string) *GatewayService {
	gs := &GatewayService{
		controllerAddr:       controllerAddr,
		slotAssignments:      make(map[uint32]*fsm.SlotAssignment),
		storageNodeAddresses: make(map[string]string),
		quit:                 make(chan struct{}),
		httpClient:           &http.Client{Timeout: 10 * time.Second},
	}

	// Initialize gRPC connection pool for storage nodes
	gs.nodeConns = &sync.Pool{
		New: func() interface{} {
			// This function will be called when a new connection is needed in the pool.
			// The actual connection will be established dynamically when needed in getStorageNodeClient.
			return nil // Return nil, connection will be established on demand
		},
	}

	// Start goroutine to monitor the controller cluster for shard map updates
	go gs.monitorControllerCluster()

	log.Printf("GojoDB Gateway Service initialized, controller address: %s", controllerAddr)
	return gs
}

// monitorControllerCluster periodically fetches the cluster status and shard assignments from the controller.
func (gs *GatewayService) monitorControllerCluster() {
	ticker := time.NewTicker(ShardMapUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Fetch shard assignments
			resp, err := gs.httpClient.Get(fmt.Sprintf("http://%s/admin/get_all_slot_assignments", gs.controllerAddr))
			if err != nil {
				log.Printf("Error fetching slot assignments from controller: %v", err)
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				bodyBytes, _ := io.ReadAll(resp.Body)
				log.Printf("Controller returned non-OK status for slot assignments (%d): %s", resp.StatusCode, string(bodyBytes))
				continue
			}

			var assignments map[string]*fsm.SlotAssignment
			if err := json.NewDecoder(resp.Body).Decode(&assignments); err != nil {
				log.Printf("Error decoding slot assignments from controller: %v", err)
				continue
			}

			// Convert map[string] to map[uint32]
			updatedAssignments := make(map[uint32]*fsm.SlotAssignment)
			for slotIDStr, assignment := range assignments {
				slotID, err := strconv.ParseUint(slotIDStr, 10, 32)
				if err != nil {
					log.Printf("Error parsing slot ID string '%s': %v", slotIDStr, err)
					continue
				}
				updatedAssignments[uint32(slotID)] = assignment
			}

			// Fetch cluster status (to get active node addresses)
			statusResp, err := gs.httpClient.Get(fmt.Sprintf("http://%s/status", gs.controllerAddr))
			if err != nil {
				log.Printf("Error fetching cluster status from controller: %v", err)
				continue
			}
			defer statusResp.Body.Close()

			if statusResp.StatusCode != http.StatusOK {
				bodyBytes, _ := io.ReadAll(statusResp.Body)
				log.Printf("Controller returned non-OK status for cluster status (%d): %s", statusResp.StatusCode, string(bodyBytes))
				continue
			}

			var clusterStatus struct {
				ActiveNodes map[string]struct {
					Address string `json:"address"`
					// ... other fields not used here
				} `json:"active_nodes"`
				// ... other fields not used here
			}
			if err := json.NewDecoder(statusResp.Body).Decode(&clusterStatus); err != nil {
				log.Printf("Error decoding cluster status from controller: %v", err)
				continue
			}

			updatedNodeAddresses := make(map[string]string)
			for nodeID, nodeInfo := range clusterStatus.ActiveNodes {
				updatedNodeAddresses[nodeID] = nodeInfo.Address
			}

			gs.mu.Lock()
			gs.slotAssignments = updatedAssignments
			gs.storageNodeAddresses = updatedNodeAddresses
			gs.mu.Unlock()
			log.Printf("Shard map and node addresses updated. %d assignments, %d nodes.", len(updatedAssignments), len(updatedNodeAddresses))

		case <-gs.quit:
			log.Println("Stopping controller cluster monitor.")
			return
		}
	}
}

// getStorageNodeClient gets a gRPC client connection for a given node ID.
// It tries to reuse from the pool or creates a new one.
func (gs *GatewayService) getStorageNodeClient(nodeID string) (*grpc.ClientConn, error) {
	gs.mu.RLock()
	addr, ok := gs.storageNodeAddresses[nodeID]
	gs.mu.RUnlock()
	if !ok || addr == "" {
		return nil, fmt.Errorf("address for storage node %s not found", nodeID)
	}

	// The sync.Pool stores grpc.ClientConn directly
	if conn, ok := gs.nodeConns.Get().(*grpc.ClientConn); ok && conn != nil && conn.GetState() != (connectivity.TransientFailure) && conn.GetState() != (connectivity.Shutdown) {
		// Verify if the connection is still valid and for the correct address
		// This is a simple check, a more robust solution might involve connection wrappers
		// For simplicity, we just return it. If it's broken, the subsequent RPC will fail.
		return conn, nil
	}

	// No valid connection in pool, create a new one
	log.Printf("Establishing new gRPC connection to storage node %s at %s", nodeID, addr)
	conn, err := grpc.NewClient("127.0.0.1:8000", grpc.WithInsecure()) // Use WithInsecure for now, but in production use mTLS
	if err != nil {
		gs.nodeConns.Put(nil) // Put nil back to signal it's unusable
		return nil, fmt.Errorf("failed to dial storage node %s at %s: %v", nodeID, addr, err)
	}
	return conn, nil
}

// returnStorageNodeClient returns a gRPC client connection to the pool.
func (gs *GatewayService) returnStorageNodeClient(conn *grpc.ClientConn) {
	if conn != nil {
		gs.nodeConns.Put(conn)
	}
}

// resolveResponsibleNode finds the primary or a replica for a given slot.
// For writes, it always returns the primary. For reads, it can return primary or replica.
func (gs *GatewayService) resolveResponsibleNode(slotID uint32, isWrite bool) (string, error) {
	gs.mu.RLock()
	assignment, ok := gs.slotAssignments[slotID]
	gs.mu.RUnlock()

	if !ok || assignment == nil {
		return "", fmt.Errorf("no assignment found for slot %d", slotID)
	}

	if isWrite {
		if assignment.PrimaryNodeID == "" {
			return "", fmt.Errorf("no primary assigned for slot %d", slotID)
		}
		return assignment.PrimaryNodeID, nil
	} else {
		// For reads, try primary first, then replicas
		if assignment.PrimaryNodeID != "" {
			return assignment.PrimaryNodeID, nil // Simple read preference: primary
		}
		if len(assignment.ReplicaNodes) > 0 {
			// For simplicity, pick the first replica. In a real system, you'd
			// implement health checks and load balancing.
			keys := fsm.Keys(assignment.ReplicaNodes)
			return assignment.ReplicaNodes[keys[0]], nil
		}
		return "", fmt.Errorf("no primary or replica assigned for slot %d", slotID)
	}
}

// --- gRPC GatewayService Methods Implementation (Data Operations) ---

// Put routes a Put request to the primary storage node for the key's slot.
func (gs *GatewayService) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	slotID := fsm.GetSlotForHashKey(req.Key)
	nodeID, err := gs.resolveResponsibleNode(uint32(slotID), true) // true for write
	if err != nil {
		log.Printf("Error resolving node for Put key %s: %v", req.Key, err)
		return &pb.PutResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Unavailable, "node resolution failed: %v", err)
	}

	conn, err := gs.getStorageNodeClient(nodeID)
	if err != nil {
		log.Printf("Error getting client for node %s: %v", nodeID, err)
		return &pb.PutResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Unavailable, "failed to get storage client: %v", err)
	}
	defer gs.returnStorageNodeClient(conn)

	client := pb.NewIndexedWriteServiceClient(conn)
	putResp, err := client.Put(ctx, req)
	if err != nil {
		log.Printf("Error calling Put on node %s for key %s: %v", nodeID, req.Key, err)
		return &pb.PutResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Internal, "storage node error: %v", err)
	}
	return putResp, nil
}

// Get routes a Get request to a primary or replica storage node for the key's slot.
func (gs *GatewayService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	slotID := fsm.GetSlotForHashKey(req.Key)
	nodeID, err := gs.resolveResponsibleNode(slotID, false) // false for read
	if err != nil {
		log.Printf("Error resolving node for Get key %s: %v", req.Key, err)
		return &pb.GetResponse{Found: false, Value: nil}, status.Errorf(codes.Unavailable, "node resolution failed: %v", err)
	}

	conn, err := gs.getStorageNodeClient(nodeID)
	if err != nil {
		log.Printf("Error getting client for node %s: %v", nodeID, err)
		return &pb.GetResponse{Found: false, Value: nil}, status.Errorf(codes.Unavailable, "failed to get storage client: %v", err)
	}
	defer gs.returnStorageNodeClient(conn)

	client := pb.NewIndexedReadServiceClient(conn)
	getResp, err := client.Get(ctx, req)
	if err != nil {
		log.Printf("Error calling Get on node %s for key %s: %v", nodeID, req.Key, err)
		return &pb.GetResponse{Found: false, Value: nil}, status.Errorf(codes.Internal, "storage node error: %v", err)
	}
	return getResp, nil
}

// Delete routes a Delete request to the primary storage node for the key's slot.
func (gs *GatewayService) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	slotID := fsm.GetSlotForHashKey(req.Key)
	nodeID, err := gs.resolveResponsibleNode(slotID, true) // true for write
	if err != nil {
		log.Printf("Error resolving node for Delete key %s: %v", req.Key, err)
		return &pb.DeleteResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Unavailable, "node resolution failed: %v", err)
	}

	conn, err := gs.getStorageNodeClient(nodeID)
	if err != nil {
		log.Printf("Error getting client for node %s: %v", nodeID, err)
		return &pb.DeleteResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Unavailable, "failed to get storage client: %v", err)
	}
	defer gs.returnStorageNodeClient(conn)

	client := pb.NewIndexedWriteServiceClient(conn)
	deleteResp, err := client.Delete(ctx, req)
	if err != nil {
		log.Printf("Error calling Delete on node %s for key %s: %v", nodeID, req.Key, err)
		return &pb.DeleteResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Internal, "storage node error: %v", err)
	}
	return deleteResp, nil
}

// GetRange routes a GetRange request to a primary or replica storage node.
func (gs *GatewayService) GetRange(ctx context.Context, req *pb.GetRangeRequest) (*pb.GetRangeResponse, error) {
	// For range queries spanning multiple keys, we assume they belong to a single shard based on start_key.
	// A more advanced implementation might involve scatter-gather across multiple shards if range spans.
	slotID := fsm.GetSlotForHashKey(req.StartKey)           // Assuming start_key determines the shard
	nodeID, err := gs.resolveResponsibleNode(slotID, false) // false for read
	if err != nil {
		log.Printf("Error resolving node for GetRange start_key %s: %v", req.StartKey, err)
		return &pb.GetRangeResponse{}, status.Errorf(codes.Unavailable, "node resolution failed: %v", err)
	}

	conn, err := gs.getStorageNodeClient(nodeID)
	if err != nil {
		log.Printf("Error getting client for node %s: %v", nodeID, err)
		return &pb.GetRangeResponse{}, status.Errorf(codes.Unavailable, "failed to get storage client: %v", err)
	}
	defer gs.returnStorageNodeClient(conn)

	client := pb.NewIndexedReadServiceClient(conn)
	getRangeResp, err := client.GetRange(ctx, req)
	if err != nil {
		log.Printf("Error calling GetRange on node %s for start_key %s: %v", nodeID, req.StartKey, err)
		return &pb.GetRangeResponse{}, status.Errorf(codes.Internal, "storage node error: %v", err)
	}
	return getRangeResp, nil
}

// TextSearch routes a TextSearch request to a primary or replica storage node.
func (gs *GatewayService) TextSearch(ctx context.Context, req *pb.TextSearchRequest) (*pb.TextSearchResponse, error) {
	// Text search can be complex, often requiring scanning multiple shards or dedicated search indexes.
	// For simplicity, we assume text search is directed to a single representative shard (e.g., shard 0)
	// or that the query inherently contains information to route to a specific shard.
	// A real implementation would involve a distributed search engine or fan-out/gather.
	slotID := uint32(0)                                     // Example: always query shard 0, or implement more sophisticated routing
	nodeID, err := gs.resolveResponsibleNode(slotID, false) // false for read
	if err != nil {
		log.Printf("Error resolving node for TextSearch query %s: %v", req.Query, err)
		return &pb.TextSearchResponse{}, status.Errorf(codes.Unavailable, "node resolution failed: %v", err)
	}

	conn, err := gs.getStorageNodeClient(nodeID)
	if err != nil {
		log.Printf("Error getting client for node %s: %v", nodeID, err)
		return &pb.TextSearchResponse{}, status.Errorf(codes.Unavailable, "failed to get storage client: %v", err)
	}
	defer gs.returnStorageNodeClient(conn)

	client := pb.NewIndexedReadServiceClient(conn)
	textSearchResp, err := client.TextSearch(ctx, req)
	if err != nil {
		log.Printf("Error calling TextSearch on node %s for query %s: %v", nodeID, req.Query, err)
		return &pb.TextSearchResponse{}, status.Errorf(codes.Internal, "storage node error: %v", err)
	}
	return textSearchResp, nil
}

// BulkPut routes a BulkPut request by grouping keys by shard and sending to respective primaries.
func (gs *GatewayService) BulkPut(ctx context.Context, req *pb.BulkPutRequest) (*pb.BulkPutResponse, error) {
	// Group entries by primary node
	putsByNode := make(map[string][]*pb.KeyValuePair)
	for _, entry := range req.Entries {
		slotID := fsm.GetSlotForHashKey(entry.Key)
		nodeID, err := gs.resolveResponsibleNode(slotID, true)
		if err != nil {
			return &pb.BulkPutResponse{Success: false, Message: fmt.Sprintf("failed to resolve node for key %s: %v", entry.Key, err)}, status.Errorf(codes.Unavailable, "bulk put routing error")
		}
		putsByNode[nodeID] = append(putsByNode[nodeID], entry)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(putsByNode))

	for nodeID, entries := range putsByNode {
		wg.Add(1)
		go func(node string, entries []*pb.KeyValuePair) {
			defer wg.Done()

			conn, err := gs.getStorageNodeClient(node)
			if err != nil {
				errCh <- fmt.Errorf("failed to get client for node %s: %v", node, err)
				return
			}
			defer gs.returnStorageNodeClient(conn)

			client := pb.NewIndexedWriteServiceClient(conn)
			bulkReq := &pb.BulkPutRequest{Entries: entries}
			_, err = client.BulkPut(ctx, bulkReq)
			if err != nil {
				errCh <- fmt.Errorf("failed BulkPut on node %s: %v", node, err)
			}
		}(nodeID, entries)
	}

	wg.Wait()
	close(errCh)

	var allErrors []string
	for err := range errCh {
		allErrors = append(allErrors, err.Error())
	}

	if len(allErrors) > 0 {
		return &pb.BulkPutResponse{Success: false, Message: strings.Join(allErrors, "; ")}, status.Errorf(codes.Internal, "bulk put failed with errors: %s", strings.Join(allErrors, "; "))
	}

	return &pb.BulkPutResponse{Success: true, Message: "Bulk Put completed successfully"}, nil
}

// BulkDelete routes a BulkDelete request by grouping keys by shard and sending to respective primaries.
func (gs *GatewayService) BulkDelete(ctx context.Context, req *pb.BulkDeleteRequest) (*pb.BulkDeleteResponse, error) {
	// Group keys by primary node
	deletesByNode := make(map[string][]string)
	for _, key := range req.Keys {
		slotID := fsm.GetSlotForHashKey(key)
		nodeID, err := gs.resolveResponsibleNode(slotID, true)
		if err != nil {
			return &pb.BulkDeleteResponse{Success: false, Message: fmt.Sprintf("failed to resolve node for key %s: %v", key, err)}, status.Errorf(codes.Unavailable, "bulk delete routing error")
		}
		deletesByNode[nodeID] = append(deletesByNode[nodeID], key)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(deletesByNode))

	for nodeID, keys := range deletesByNode {
		wg.Add(1)
		go func(node string, keys []string) {
			defer wg.Done()

			conn, err := gs.getStorageNodeClient(node)
			if err != nil {
				errCh <- fmt.Errorf("failed to get client for node %s: %v", node, err)
				return
			}
			defer gs.returnStorageNodeClient(conn)

			client := pb.NewIndexedWriteServiceClient(conn)
			bulkReq := &pb.BulkDeleteRequest{Keys: keys}
			_, err = client.BulkDelete(ctx, bulkReq)
			if err != nil {
				errCh <- fmt.Errorf("failed BulkDelete on node %s: %v", node, err)
			}
		}(nodeID, keys)
	}

	wg.Wait()
	close(errCh)

	var allErrors []string
	for err := range errCh {
		allErrors = append(allErrors, err.Error())
	}

	if len(allErrors) > 0 {
		return &pb.BulkDeleteResponse{Success: false, Message: strings.Join(allErrors, "; ")}, status.Errorf(codes.Internal, "bulk delete failed with errors: %s", strings.Join(allErrors, "; "))
	}

	return &pb.BulkDeleteResponse{Success: true, Message: "Bulk Delete completed successfully"}, nil
}

// --- gRPC GatewayService Methods Implementation (Cluster Management - Proxying to Controller) ---

// callControllerAdminAPI makes an HTTP request to the controller's admin endpoint.
func (gs *GatewayService) callControllerAdminAPI(ctx context.Context, method, endpoint string, payload interface{}) ([]byte, error) {
	var reqBody io.Reader
	if payload != nil {
		jsonPayload, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload: %v", err)
		}
		reqBody = bytes.NewBuffer(jsonPayload)
	}

	url := fmt.Sprintf("http://%s%s", gs.controllerAddr, endpoint)
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %v", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := gs.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to call controller API %s: %v", url, err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("controller API %s returned non-OK status %d: %s", url, resp.StatusCode, string(bodyBytes))
	}

	return bodyBytes, nil
}

// AddStorageNode registers a new storage node with the controller.
func (gs *GatewayService) AddStorageNode(ctx context.Context, req *pb.AddStorageNodeRequest) (*pb.AddStorageNodeResponse, error) {
	payload := map[string]string{
		"nodeId":  req.NodeId,
		"address": req.Address,
	}
	_, err := gs.callControllerAdminAPI(ctx, "POST", "/admin/register_storage_node", payload)
	if err != nil {
		log.Printf("Error adding storage node %s: %v", req.NodeId, err)
		return &pb.AddStorageNodeResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Internal, "failed to add storage node: %v", err)
	}
	return &pb.AddStorageNodeResponse{Success: true, Message: "Storage node registered successfully"}, nil
}

// RemoveStorageNode removes a storage node from the controller.
func (gs *GatewayService) RemoveStorageNode(ctx context.Context, req *pb.RemoveStorageNodeRequest) (*pb.RemoveStorageNodeResponse, error) {
	payload := map[string]string{
		"nodeId": req.NodeId,
	}
	_, err := gs.callControllerAdminAPI(ctx, "POST", "/admin/remove_storage_node", payload)
	if err != nil {
		log.Printf("Error removing storage node %s: %v", req.NodeId, err)
		return &pb.RemoveStorageNodeResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Internal, "failed to remove storage node: %v", err)
	}
	return &pb.RemoveStorageNodeResponse{Success: true, Message: "Storage node removed successfully"}, nil
}

// AssignShardSlot assigns a shard slot to a primary and replicas.
func (gs *GatewayService) AssignShardSlot(ctx context.Context, req *pb.AssignShardSlotRequest) (*pb.AssignShardSlotResponse, error) {
	payload := struct {
		SlotID         uint32   `json:"slotId"`
		PrimaryNodeID  string   `json:"primaryNodeId"`
		ReplicaNodeIDs []string `json:"replicaNodeIds"`
	}{
		SlotID:         req.SlotId,
		PrimaryNodeID:  req.PrimaryNodeId,
		ReplicaNodeIDs: req.ReplicaNodeIds,
	}
	_, err := gs.callControllerAdminAPI(ctx, "POST", "/admin/assign_slot_range", payload)
	if err != nil {
		log.Printf("Error assigning shard slot %d: %v", req.SlotId, err)
		return &pb.AssignShardSlotResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Internal, "failed to assign shard slot: %v", err)
	}
	return &pb.AssignShardSlotResponse{Success: true, Message: "Shard slot assigned successfully"}, nil
}

// InitiateReplicaOnboarding initiates a replica onboarding process.
func (gs *GatewayService) InitiateReplicaOnboarding(ctx context.Context, req *pb.InitiateReplicaOnboardingRequest) (*pb.InitiateReplicaOnboardingResponse, error) {
	payload := struct {
		SlotID         uint32 `json:"slotId"`
		ReplicaNodeID  string `json:"replicaNodeId"`
		PrimaryNodeID  string `json:"primaryNodeId"`
		ReplicaAddress string `json:"replicaAddress"`
		PrimaryAddress string `json:"primaryAddress"`
	}{
		SlotID:         req.SlotId,
		ReplicaNodeID:  req.ReplicaNodeId,
		PrimaryNodeID:  req.PrimaryNodeId,
		ReplicaAddress: req.ReplicaAddress,
		PrimaryAddress: req.PrimaryAddress,
	}
	body, err := gs.callControllerAdminAPI(ctx, "POST", "/admin/initiate_replica_onboarding", payload)
	if err != nil {
		log.Printf("Error initiating replica onboarding for slot %d, replica %s: %v", req.SlotId, req.ReplicaNodeId, err)
		return &pb.InitiateReplicaOnboardingResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Internal, "failed to initiate replica onboarding: %v", err)
	}

	var controllerResp map[string]string
	if err := json.Unmarshal(body, &controllerResp); err != nil {
		return &pb.InitiateReplicaOnboardingResponse{Success: false, Message: fmt.Sprintf("failed to parse controller response: %v", err)}, status.Errorf(codes.Internal, "failed to parse controller response")
	}

	onboardingID := controllerResp["onboardingId"]
	return &pb.InitiateReplicaOnboardingResponse{Success: true, Message: "Replica onboarding initiated", OnboardingId: onboardingID}, nil
}

// UpdateReplicaOnboardingState updates the state of an ongoing replica onboarding.
func (gs *GatewayService) UpdateReplicaOnboardingState(ctx context.Context, req *pb.UpdateReplicaOnboardingStateRequest) (*pb.UpdateReplicaOnboardingStateResponse, error) {
	payload := struct {
		OnboardingID string `json:"onboardingId"`
		Status       string `json:"status"`
		CurrentLSN   uint64 `json:"currentLsn"`
		TargetLSN    uint64 `json:"targetLsn"`
		ErrorMessage string `json:"errorMessage"`
	}{
		OnboardingID: req.OnboardingId,
		Status:       req.Status,
		CurrentLSN:   req.CurrentLsn,
		TargetLSN:    req.TargetLsn,
		ErrorMessage: req.ErrorMessage,
	}
	_, err := gs.callControllerAdminAPI(ctx, "POST", "/admin/update_replica_onboarding_state", payload)
	if err != nil {
		log.Printf("Error updating replica onboarding state %s: %v", req.OnboardingId, err)
		return &pb.UpdateReplicaOnboardingStateResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Internal, "failed to update replica onboarding state: %v", err)
	}
	return &pb.UpdateReplicaOnboardingStateResponse{Success: true, Message: "Replica onboarding state updated"}, nil
}

// InitiateShardMigration initiates a shard migration process.
func (gs *GatewayService) InitiateShardMigration(ctx context.Context, req *pb.InitiateShardMigrationRequest) (*pb.InitiateShardMigrationResponse, error) {
	payload := struct {
		SlotID        uint32 `json:"slotId"`
		SourceNodeID  string `json:"sourceNodeId"`
		TargetNodeID  string `json:"targetNodeId"`
		SourceAddress string `json:"sourceAddress"`
		TargetAddress string `json:"targetAddress"`
	}{
		SlotID:        req.SlotId,
		SourceNodeID:  req.SourceNodeId,
		TargetNodeID:  req.TargetNodeId,
		SourceAddress: req.SourceAddress,
		TargetAddress: req.TargetAddress,
	}
	body, err := gs.callControllerAdminAPI(ctx, "POST", "/admin/initiate_shard_migration", payload)
	if err != nil {
		log.Printf("Error initiating shard migration for slot %d: %v", req.SlotId, err)
		return &pb.InitiateShardMigrationResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Internal, "failed to initiate shard migration: %v", err)
	}

	var controllerResp map[string]string
	if err := json.Unmarshal(body, &controllerResp); err != nil {
		return &pb.InitiateShardMigrationResponse{Success: false, Message: fmt.Sprintf("failed to parse controller response: %v", err)}, status.Errorf(codes.Internal, "failed to parse controller response")
	}

	migrationID := controllerResp["migrationId"]
	return &pb.InitiateShardMigrationResponse{Success: true, Message: "Shard migration initiated", MigrationId: migrationID}, nil
}

// CommitShardMigration commits a pending shard migration.
func (gs *GatewayService) CommitShardMigration(ctx context.Context, req *pb.CommitShardMigrationRequest) (*pb.CommitShardMigrationResponse, error) {
	payload := struct {
		SlotID            uint32   `json:"slotId"`
		NewPrimaryNodeID  string   `json:"newPrimaryNodeId"`
		NewReplicaNodeIDs []string `json:"newReplicaNodeIds"`
	}{
		SlotID:            req.SlotId,
		NewPrimaryNodeID:  req.NewPrimaryNodeId,
		NewReplicaNodeIDs: req.NewReplicaNodeIds,
	}
	_, err := gs.callControllerAdminAPI(ctx, "POST", "/admin/commit_shard_migration", payload)
	if err != nil {
		log.Printf("Error committing shard migration for slot %d: %v", req.SlotId, err)
		return &pb.CommitShardMigrationResponse{Success: false, Message: err.Error()}, status.Errorf(codes.Internal, "failed to commit shard migration: %v", err)
	}
	return &pb.CommitShardMigrationResponse{Success: true, Message: "Shard migration committed"}, nil
}

// GetClusterStatus retrieves the overall cluster status from the controller.
func (gs *GatewayService) GetClusterStatus(ctx context.Context, req *pb.GetClusterStatusRequest) (*pb.GetClusterStatusResponse, error) {
	body, err := gs.callControllerAdminAPI(ctx, "GET", "/status", nil)
	if err != nil {
		log.Printf("Error getting cluster status: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to get cluster status: %v", err)
	}

	var controllerStatus struct {
		ActiveNodes map[string]struct {
			Address string `json:"address"`
			Status  string `json:"status"`
		} `json:"active_nodes"`
		SlotAssignments  map[string]*fsm.SlotAssignment         `json:"slot_assignments"`
		OnboardingStates map[string]*fsm.ReplicaOnboardingState `json:"onboarding_states"`
		MigrationStates  map[uint32]*fsm.ShardMigrationState    `json:"migration_states"`
	}
	if err := json.Unmarshal(body, &controllerStatus); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse controller status: %v", err)
	}

	resp := &pb.GetClusterStatusResponse{}
	for nodeID, nodeInfo := range controllerStatus.ActiveNodes {
		resp.ActiveNodes = append(resp.ActiveNodes, &pb.StorageNode{
			NodeId:  nodeID,
			Address: nodeInfo.Address,
			Status:  nodeInfo.Status,
		})
	}
	for _, assign := range controllerStatus.SlotAssignments {
		resp.ShardAssignments = append(resp.ShardAssignments, &pb.ShardSlotAssignment{
			SlotId:         uint32(assign.SlotID),
			PrimaryNodeId:  assign.PrimaryNodeID,
			ReplicaNodeIds: fsm.Keys(assign.ReplicaNodes),
		})
	}
	for _, onboarding := range controllerStatus.OnboardingStates {
		resp.OnboardingStates = append(resp.OnboardingStates, &pb.ReplicaOnboardingState{
			OnboardingId:  onboarding.OperationID,
			SlotId:        uint32(onboarding.SlotID),
			ReplicaNodeId: onboarding.TargetNodeID,
			PrimaryNodeId: onboarding.SourceNodeID,
			Status:        onboarding.CurrentStage,
			CurrentLsn:    uint64(onboarding.CurrentAppliedLSN),
			TargetLsn:     uint64(onboarding.SnapshotLSN),
			ErrorMessage:  onboarding.StatusMessage,
		})
	}
	for _, migration := range controllerStatus.MigrationStates {
		resp.MigrationStates = append(resp.MigrationStates, &pb.ShardMigrationState{
			MigrationId:  migration.OperationID,
			SlotId:       uint32(migration.SlotID),
			SourceNodeId: migration.SourceNodeID,
			TargetNodeId: migration.TargetNodeID,
			Status:       migration.CurrentPhase,
			CurrentLsn:   uint64(migration.TargetCaughtUpToLSN),
			TargetLsn:    uint64(migration.SnapshotLSN),
			ErrorMessage: migration.StatusMessage,
		})
	}

	return resp, nil
}

// GetShardMap retrieves the current shard map from the gateway's cache.
func (gs *GatewayService) GetShardMap(ctx context.Context, req *pb.GetShardMapRequest) (*pb.GetShardMapResponse, error) {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	resp := &pb.GetShardMapResponse{}
	for _, assign := range gs.slotAssignments {
		replicaNodeIds := fsm.Keys(assign.ReplicaNodes)
		resp.ShardAssignments = append(resp.ShardAssignments, &pb.ShardSlotAssignment{
			SlotId:         uint32(assign.SlotID),
			PrimaryNodeId:  assign.PrimaryNodeID,
			ReplicaNodeIds: replicaNodeIds,
		})
	}
	return resp, nil
}

// Main function to start the gateway service.
func main() {
	lis, err := net.Listen("tcp", DefaultGatewayPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	gatewayService := NewGatewayService(DefaultControllerAddr)
	pb.RegisterGatewayServiceServer(s, gatewayService)

	log.Printf("GojoDB Gateway server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
