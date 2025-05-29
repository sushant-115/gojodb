package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"                   // Raft library
	raftboltdb "github.com/hashicorp/raft-boltdb" // BoltDB backend for Raft log and stable store

	"github.com/sushant-115/gojodb/cmd/gojodb_controller/fsm" // Our custom FSM
)

const (
	defaultRaftPort      = 8081
	defaultHTTPPort      = 8080
	defaultNodeID        = "node1"
	defaultRaftDir       = "raft_data"
	defaultRaftSnapshots = 2                // Number of Raft snapshots to retain
	heartbeatInterval    = 5 * time.Second  // Interval for Storage Node heartbeats
	heartbeatTimeout     = 15 * time.Second // Timeout for Storage Node health

	defaultHeartbeatListenPort = 8086 // Default UDP port for Storage Node heartbeats
)

// Controller represents a single GojoDB Controller Node.
type Controller struct {
	raft         *raft.Raft     // Raft consensus mechanism
	fsm          *fsm.GojoDBFSM // Our replicated state machine
	raftDir      string
	raftBindAddr string
	nodeID       string

	// Node Manager (Initial)
	localHeartbeatMap   map[string]time.Time // Local map to track last heartbeat time for timeout detection
	storageNodesMu      sync.Mutex           // Protects local heartbeat tracking map
	heartbeatListener   *net.UDPConn         // Listener for incoming heartbeats from Storage Nodes
	heartbeatListenPort int                  // Configurable UDP port for heartbeats
}

// NewController creates and initializes a new Controller node.
func NewController(nodeID string, raftBindAddr string, httpAddr string, raftDir string, joinAddr string, heartbeatListenPort int) (*Controller, error) {
	c := &Controller{
		fsm:                 fsm.NewGojoDBFSM(), // Initialize our FSM
		raftDir:             raftDir,
		raftBindAddr:        raftBindAddr,
		nodeID:              nodeID,
		localHeartbeatMap:   make(map[string]time.Time), // Initialize local map
		heartbeatListenPort: heartbeatListenPort,        // Set configurable port
	}

	// Setup Raft
	if err := c.setupRaft(joinAddr); err != nil {
		return nil, fmt.Errorf("failed to setup Raft: %w", err)
	}

	// Start HTTP server for client/admin interaction
	go c.startHTTPServer(httpAddr)

	// Start Heartbeat Listener for Storage Nodes
	go c.startHeartbeatListener()

	// Start background goroutine for monitoring Storage Nodes
	go c.monitorStorageNodes()

	return c, nil
}

// setupRaft initializes the Raft instance.
func (c *Controller) setupRaft(joinAddr string) error {
	// Ensure Raft data directory exists
	if err := os.MkdirAll(c.raftDir, 0755); err != nil {
		return fmt.Errorf("failed to create Raft directory: %w", err)
	}

	// Setup Raft configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(c.nodeID)
	config.Logger = nil // Raft-specific logger

	// Setup Raft transport
	addr, err := net.ResolveTCPAddr("tcp", c.raftBindAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve TCP address: %w", err)
	}
	transport, err := raft.NewTCPTransport(addr.String(), addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create TCP transport: %w", err)
	}

	// Setup Raft log store (BoltDB)
	// This stores Raft's log entries and metadata persistently.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(c.raftDir, "raft-log.db"))
	if err != nil {
		return fmt.Errorf("failed to create BoltDB log store: %w", err)
	}

	// Setup Raft stable store (BoltDB)
	// This stores Raft's configuration and vote information persistently.
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(c.raftDir, "raft-stable.db"))
	if err != nil {
		return fmt.Errorf("failed to create BoltDB stable store: %w", err)
	}

	// Setup Raft snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(c.raftDir, defaultRaftSnapshots, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create file snapshot store: %w", err)
	}

	// Create the Raft instance
	ra, err := raft.NewRaft(config, c.fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return fmt.Errorf("failed to create Raft instance: %w", err)
	}
	c.raft = ra

	// Check if this is the first node or joining an existing cluster
	if joinAddr == "" {
		log.Printf("INFO: Starting new Raft cluster as leader on %s", c.raftBindAddr)
		// Bootstrap the cluster if this is the first node
		// This makes this node the initial leader
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		f := ra.BootstrapCluster(cfg)
		if f.Error() != nil && f.Error() != raft.ErrCantBootstrap {
			return fmt.Errorf("failed to bootstrap cluster: %w", f.Error())
		}
	} else {
		log.Printf("INFO: Attempting to join Raft cluster at %s", joinAddr)
		// Attempt to join an existing cluster
		if err := c.joinRaftCluster(joinAddr); err != nil {
			return fmt.Errorf("failed to join Raft cluster: %w", err)
		}
	}

	// Wait for Raft to become a leader or follower
	log.Println("INFO: Waiting for Raft cluster to stabilize...")
	select {
	case <-ra.LeaderCh():
		log.Println("INFO: This node is now the LEADER.")
	case <-time.After(10 * time.Second): // Give it some time to elect a leader
		log.Println("INFO: Raft cluster is active (may be follower).")
	}

	return nil
}

// joinRaftCluster attempts to join an existing Raft cluster.
func (c *Controller) joinRaftCluster(joinAddr string) error {
	// Send a join request to the target Raft node
	resp, err := http.Get(fmt.Sprintf("http://%s/join?peerID=%s&peerAddr=%s", joinAddr, c.nodeID, c.raftBindAddr))
	if err != nil {
		return fmt.Errorf("failed to send join request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("join request failed with status: %s", resp.Status)
	}
	return nil
}

// startHTTPServer starts the HTTP server for API endpoints.
func (c *Controller) startHTTPServer(httpAddr string) {
	log.Printf("INFO: Starting HTTP server on %s", httpAddr)

	http.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		peerID := r.URL.Query().Get("peerID")
		peerAddr := r.URL.Query().Get("peerAddr")
		if peerID == "" || peerAddr == "" {
			http.Error(w, "peerID and peerAddr are required", http.StatusBadRequest)
			return
		}

		log.Printf("INFO: Received join request from peer %s at %s", peerID, peerAddr)

		// Check if this node is the leader
		if c.raft.State() != raft.Leader {
			http.Error(w, "not leader", http.StatusServiceUnavailable)
			return
		}

		// Add the peer to the Raft cluster configuration
		f := c.raft.AddVoter(raft.ServerID(peerID), raft.ServerAddress(peerAddr), 0, 0)
		if f.Error() != nil {
			http.Error(w, fmt.Sprintf("failed to add voter: %v", f.Error()), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Successfully joined cluster")
	})

	http.HandleFunc("/metadata", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			// Set metadata
			key := r.URL.Query().Get("key")
			value := r.URL.Query().Get("value")
			if key == "" || value == "" {
				http.Error(w, "key and value are required", http.StatusBadRequest)
				return
			}

			// Apply the command through Raft
			cmd := fsm.LogCommand{
				Op:    fsm.OpSetMetadata,
				Key:   key,
				Value: value,
			}
			cmdBytes, err := json.Marshal(cmd)
			if err != nil {
				http.Error(w, fmt.Sprintf("failed to marshal command: %v", err), http.StatusInternalServerError)
				return
			}

			f := c.raft.Apply(cmdBytes, 5*time.Second) // Apply with a timeout
			if f.Error() != nil {
				http.Error(w, fmt.Sprintf("failed to apply command: %v", f.Error()), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "Metadata set successfully")

		} else if r.Method == http.MethodGet {
			// Get metadata
			key := r.URL.Query().Get("key")
			if key == "" {
				http.Error(w, "key is required", http.StatusBadRequest)
				return
			}
			val, found := c.fsm.GetMetadata(key)
			if !found {
				http.Error(w, "metadata not found", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "Metadata: %s", val)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// Endpoint to check Raft leader status and Storage Node status
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		state := c.raft.State().String()
		leaderAddr := c.raft.Leader()

		// Get Storage Node status from FSM (replicated state)
		storageNodes := c.fsm.GetStorageNodes()
		storageNodeCount := c.fsm.GetStorageNodeCount()

		// Get Slot Assignments from FSM (replicated state)
		slotAssignments := c.fsm.GetAllSlotAssignments()

		response := fmt.Sprintf("Node ID: %s\nState: %s\nLeader: %s\n\nRegistered Storage Nodes (%d):\n", c.nodeID, state, leaderAddr, storageNodeCount)
		if storageNodeCount == 0 {
			response += "  (None)\n"
		} else {
			for id, addr := range storageNodes {
				response += fmt.Sprintf("  - ID: %s, Addr: %s\n", id, addr)
			}
		}

		response += "\nSlot Assignments:\n"
		if len(slotAssignments) == 0 {
			response += "  (None)\n"
		} else {
			// Sort slot ranges for consistent output
			var sortedRanges []fsm.SlotRangeInfo
			for _, sr := range slotAssignments {
				sortedRanges = append(sortedRanges, sr)
			}
			sort.Slice(sortedRanges, func(i, j int) bool {
				return sortedRanges[i].StartSlot < sortedRanges[j].StartSlot
			})

			for _, sr := range sortedRanges {
				response += fmt.Sprintf("  - RangeID: %s (%d-%d), Assigned To: %s, Status: %s\n",
					sr.RangeID, sr.StartSlot, sr.EndSlot, sr.AssignedNodeID, sr.Status)
			}
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, response)
	})

	// Endpoint to get all slot assignments
	http.HandleFunc("/admin/get_all_slot_assignments", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		slotAssignments := c.fsm.GetAllSlotAssignments()
		respBytes, err := json.Marshal(slotAssignments)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to marshal slot assignments: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(respBytes)
	})

	// --- Sharding Endpoints ---
	// Endpoint to assign a range of slots to a Storage Node
	http.HandleFunc("/admin/assign_slot_range", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		startSlotStr := r.URL.Query().Get("startSlot")
		endSlotStr := r.URL.Query().Get("endSlot")
		assignedNodeID := r.URL.Query().Get("assignedNodeID")

		if startSlotStr == "" || endSlotStr == "" || assignedNodeID == "" {
			http.Error(w, "startSlot, endSlot, and assignedNodeID are required", http.StatusBadRequest)
			return
		}

		startSlot, err := strconv.Atoi(startSlotStr)
		if err != nil {
			http.Error(w, "invalid startSlot format", http.StatusBadRequest)
			return
		}
		endSlot, err := strconv.Atoi(endSlotStr)
		if err != nil {
			http.Error(w, "invalid endSlot format", http.StatusBadRequest)
			return
		}

		if startSlot < 0 || endSlot >= fsm.TotalHashSlots || startSlot > endSlot {
			http.Error(w, fmt.Sprintf("invalid slot range [%d, %d). Must be within [0, %d) and start <= end.", startSlot, endSlot, fsm.TotalHashSlots), http.StatusBadRequest)
			return
		}

		// Create the SlotRangeInfo
		slotInfo := fsm.SlotRangeInfo{
			RangeID:        fmt.Sprintf("%d-%d", startSlot, endSlot),
			StartSlot:      startSlot,
			EndSlot:        endSlot,
			AssignedNodeID: assignedNodeID,
			Status:         "active", // Default status
			LastUpdated:    time.Now(),
		}

		slotInfoBytes, err := json.Marshal(slotInfo)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to marshal SlotRangeInfo: %v", err), http.StatusInternalServerError)
			return
		}

		cmd := fsm.LogCommand{
			Op:    fsm.OpAssignSlotRange,
			Key:   slotInfo.RangeID, // Key is the RangeID
			Value: string(slotInfoBytes),
		}
		cmdBytes, err := json.Marshal(cmd)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to marshal command: %v", err), http.StatusInternalServerError)
			return
		}

		applyFuture := c.raft.Apply(cmdBytes, 5*time.Second)
		if applyFuture.Error() != nil {
			http.Error(w, fmt.Sprintf("failed to apply command: %v", applyFuture.Error()), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Slot range %s assigned to %s successfully.", slotInfo.RangeID, assignedNodeID)
	})

	// Endpoint to get the assigned node for a specific key
	http.HandleFunc("/admin/get_node_for_key", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key is required", http.StatusBadRequest)
			return
		}

		nodeID, found := c.fsm.GetNodeForHashKey(key)
		if !found {
			http.Error(w, fmt.Sprintf("No storage node found for key '%s' (slot %d).", key, fsm.GetSlotForHashKey(key)), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Key '%s' (slot %d) is assigned to Storage Node: %s", key, fsm.GetSlotForHashKey(key), nodeID)
	})
	// --- End Sharding Endpoints ---

	log.Fatal(http.ListenAndServe(httpAddr, nil))
}

// startHeartbeatListener starts a UDP listener for Storage Node heartbeats.
func (c *Controller) startHeartbeatListener() {
	addr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(c.heartbeatListenPort))
	if err != nil {
		log.Fatalf("FATAL: Failed to resolve UDP address for heartbeat listener: %v", err)
	}
	listener, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("FATAL: Failed to start UDP heartbeat listener: %v", err)
	}
	c.heartbeatListener = listener
	log.Printf("INFO: Heartbeat listener started on UDP %s", listener.LocalAddr().String())

	buffer := make([]byte, 1024)
	for {
		n, remoteAddr, err := listener.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("ERROR: Error reading heartbeat: %v", err)
			continue
		}
		// Assuming heartbeat message is just the Storage Node ID and its address
		// Format: "NODE_ID:ADDRESS" e.g., "storage1:localhost:9000"
		heartbeatMsg := string(buffer[:n])
		parts := strings.SplitN(heartbeatMsg, ":", 2)
		if len(parts) != 2 {
			log.Printf("WARNING: Invalid heartbeat format from %s: '%s'", remoteAddr.String(), heartbeatMsg)
			continue
		}
		storageNodeID := parts[0]
		storageNodeAddr := parts[1]

		c.recordHeartbeat(storageNodeID, storageNodeAddr)
	}
}

// recordHeartbeat updates the last heartbeat time for a Storage Node locally,
// and if this Controller is the leader, it applies the state change via Raft.
func (c *Controller) recordHeartbeat(nodeID string, addr string) {
	c.storageNodesMu.Lock()
	c.localHeartbeatMap[nodeID] = time.Now() // Update local heartbeat time
	c.storageNodesMu.Unlock()

	// If this Controller node is the Raft leader, apply the state change to the FSM.
	if c.raft.State() == raft.Leader {
		cmd := fsm.LogCommand{
			Op:    fsm.OpUpdateNodeStatus, // Use OpUpdateNodeStatus to add or update
			Key:   nodeID,
			Value: addr, // Store the address or a more complex status object
		}
		cmdBytes, err := json.Marshal(cmd)
		if err != nil {
			log.Printf("ERROR: Failed to marshal heartbeat command for %s: %v", nodeID, err)
			return
		}

		// Apply the command through Raft. This will replicate to all followers.
		applyFuture := c.raft.Apply(cmdBytes, 5*time.Second)
		if applyFuture.Error() != nil {
			log.Printf("ERROR: Failed to apply heartbeat command for %s to Raft: %v", nodeID, applyFuture.Error())
		} else {
			log.Printf("DEBUG: Applied heartbeat for Storage Node %s (%s) to Raft FSM.", nodeID, addr)
		}
	} else {
		log.Printf("DEBUG: Received heartbeat from Storage Node %s (%s) but not leader. Not applying to Raft.", nodeID, addr)
	}
}

// monitorStorageNodes periodically checks the health of registered Storage Nodes.
// It relies on the localHeartbeatMap for timeouts and applies removal commands to Raft if a node times out.
func (c *Controller) monitorStorageNodes() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		c.storageNodesMu.Lock()
		nodesToCheck := make(map[string]time.Time)
		for nodeID, lastHeartbeat := range c.localHeartbeatMap {
			nodesToCheck[nodeID] = lastHeartbeat
		}
		c.storageNodesMu.Unlock()

		for nodeID, lastHeartbeat := range nodesToCheck {
			if time.Since(lastHeartbeat) > heartbeatTimeout {
				log.Printf("WARNING: Storage Node %s timed out (last heartbeat: %v ago). Marking as unhealthy.", nodeID, time.Since(lastHeartbeat))

				// If this Controller is the leader, apply a state change to remove the node.
				if c.raft.State() == raft.Leader {
					cmd := fsm.LogCommand{
						Op:  fsm.OpRemoveStorageNode,
						Key: nodeID,
					}
					cmdBytes, err := json.Marshal(cmd)
					if err != nil {
						log.Printf("ERROR: Failed to marshal remove node command for %s: %v", nodeID, err)
						continue
					}
					applyFuture := c.raft.Apply(cmdBytes, 5*time.Second)
					if applyFuture.Error() != nil {
						log.Printf("ERROR: Failed to apply remove node command for %s to Raft: %v", nodeID, applyFuture.Error())
					} else {
						log.Printf("DEBUG: Applied removal command for Storage Node %s to Raft FSM.", nodeID)
					}
				} else {
					log.Printf("DEBUG: Storage Node %s timed out, but not leader. Not applying removal to Raft.", nodeID)
				}

				// Remove from local map regardless of leader status (it's just a local cache)
				c.storageNodesMu.Lock()
				delete(c.localHeartbeatMap, nodeID)
				c.storageNodesMu.Unlock()
			}
		}
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Command-line arguments for node configuration
	// Example usage:
	// Controller Node 1 (Leader):
	// NODE_ID=node1 RAFT_BIND_ADDR=localhost:8081 HTTP_ADDR=localhost:8080 HEARTBEAT_LISTEN_PORT=8086 go run controller/main.go
	//
	// Controller Node 2 (Follower, joins node1):
	// NODE_ID=node2 RAFT_BIND_ADDR=localhost:8082 HTTP_ADDR=localhost:8083 JOIN_ADDR=localhost:8080 HEARTBEAT_LISTEN_PORT=8087 go run controller/main.go
	//
	// Controller Node 3 (Follower, joins node1):
	// NODE_ID=node3 RAFT_BIND_ADDR=localhost:8084 HTTP_ADDR=localhost:8085 JOIN_ADDR=localhost:8080 HEARTBEAT_LISTEN_PORT=8088 go run controller/main.go
	//
	// Simulated Storage Node client (sends heartbeats to Controller heartbeat port 8086):
	// STORAGE_NODE_ID=storage_alpha STORAGE_NODE_ADDR=localhost:9000 HEARTBEAT_TARGET_PORT=8086 go run controller/main.go

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = defaultNodeID // Default for single node testing
	}
	raftBindAddr := os.Getenv("RAFT_BIND_ADDR")
	if raftBindAddr == "" {
		raftBindAddr = fmt.Sprintf("localhost:%d", defaultRaftPort)
	}
	httpAddr := os.Getenv("HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = fmt.Sprintf("localhost:%d", defaultHTTPPort)
	}
	joinAddr := os.Getenv("JOIN_ADDR") // Optional: address of an existing node to join

	// Configurable Heartbeat Listen Port for Controller
	heartbeatListenPortStr := os.Getenv("HEARTBEAT_LISTEN_PORT")
	heartbeatListenPort := defaultHeartbeatListenPort
	if heartbeatListenPortStr != "" {
		parsedPort, err := strconv.Atoi(heartbeatListenPortStr)
		if err != nil {
			log.Fatalf("FATAL: Invalid HEARTBEAT_LISTEN_PORT: %v", err)
		}
		heartbeatListenPort = parsedPort
	}

	// Simulated Storage Node parameters (for testing heartbeat integration)
	storageNodeID := os.Getenv("STORAGE_NODE_ID")
	storageNodeAddr := os.Getenv("STORAGE_NODE_ADDR") // e.g., "localhost:9000"

	// Configurable Heartbeat Target Port for Simulated Storage Node
	heartbeatTargetPortStr := os.Getenv("HEARTBEAT_TARGET_PORT")
	heartbeatTargetPort := defaultHeartbeatListenPort // Default to Controller's default if not specified
	if heartbeatTargetPortStr != "" {
		parsedPort, err := strconv.Atoi(heartbeatTargetPortStr)
		if err != nil {
			log.Fatalf("FATAL: Invalid HEARTBEAT_TARGET_PORT: %v", err)
		}
		heartbeatTargetPort = parsedPort
	}

	// Clean up old Raft data for a fresh start (for testing purposes)
	raftDataPath := filepath.Join(defaultRaftDir, nodeID)
	if err := os.RemoveAll(raftDataPath); err != nil {
		log.Printf("WARNING: Failed to clean up old Raft data for %s: %v", nodeID, err)
	}
	log.Printf("INFO: Raft data directory for %s: %s", nodeID, raftDataPath)

	_, err := NewController(nodeID, raftBindAddr, httpAddr, raftDataPath, joinAddr, heartbeatListenPort) // Pass heartbeatListenPort
	if err != nil {
		log.Fatalf("FATAL: Failed to create controller: %v", err)
	}

	// If this Controller is also simulating a Storage Node, start sending heartbeats
	if storageNodeID != "" && storageNodeAddr != "" {
		go func() {
			log.Printf("INFO: Simulating Storage Node %s sending heartbeats to Controller heartbeat listener on port %d", storageNodeID, heartbeatTargetPort)
			conn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: heartbeatTargetPort}) // Use heartbeatTargetPort
			if err != nil {
				log.Fatalf("FATAL: Failed to dial UDP for simulated Storage Node heartbeat: %v", err)
			}
			defer conn.Close()

			ticker := time.NewTicker(heartbeatInterval)
			defer ticker.Stop()

			heartbeatMessage := fmt.Sprintf("%s:%s", storageNodeID, storageNodeAddr)

			for range ticker.C {
				_, err := conn.Write([]byte(heartbeatMessage))
				if err != nil {
					log.Printf("ERROR: Failed to send heartbeat from simulated Storage Node %s: %v", storageNodeID, err)
				} else {
					log.Printf("DEBUG: Simulated Storage Node %s sent heartbeat.", storageNodeID)
				}
			}
		}()
	}

	// Keep the main goroutine alive
	select {}
}
