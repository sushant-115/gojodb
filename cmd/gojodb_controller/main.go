package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"                   // Raft library
	raftboltdb "github.com/hashicorp/raft-boltdb" // BoltDB backend for Raft log and stable store
	// Our custom FSM
)

// NODE_ID=node1 RAFT_BIND_ADDR=localhost:8081 HTTP_ADDR=localhost:8080 HEARTBEAT_ADDR=localhost:8082 go run main.go

const (
	defaultRaftPort      = 8081
	defaultHTTPPort      = 8082
	defaultUDPPort       = 8083
	defaultNodeID        = "node1"
	defaultRaftDir       = "raft_data"
	defaultRaftSnapshots = 2                // Number of Raft snapshots to retain
	heartbeatInterval    = 5 * time.Second  // Interval for Storage Node heartbeats
	heartbeatTimeout     = 15 * time.Second // Timeout for Storage Node health
)

// Controller represents a single GojoDB Controller Node.
type Controller struct {
	raft         *raft.Raft // Raft consensus mechanism
	fsm          *GojoDBFSM // Our replicated state machine
	raftDir      string
	raftBindAddr string
	nodeID       string

	// Node Manager (Initial)
	storageNodes      map[string]time.Time // NodeID -> LastHeartbeatTime
	storageNodesMu    sync.Mutex
	heartbeatListener *net.UDPConn // Listener for incoming heartbeats from Storage Nodes
}

// NewController creates and initializes a new Controller node.
func NewController(nodeID string, raftBindAddr string, httpAddr string, raftDir string, joinAddr string, udpAddr string) (*Controller, error) {
	c := &Controller{
		fsm:          NewGojoDBFSM(), // Initialize our FSM
		raftDir:      raftDir,
		raftBindAddr: raftBindAddr,
		nodeID:       nodeID,
		storageNodes: make(map[string]time.Time),
	}

	// Setup Raft
	if err := c.setupRaft(joinAddr); err != nil {
		return nil, fmt.Errorf("failed to setup Raft: %w", err)
	}

	// Start HTTP server for client/admin interaction
	go c.startHTTPServer(httpAddr)

	// Start Heartbeat Listener for Storage Nodes
	go c.startHeartbeatListener(udpAddr)

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
	config.Logger = nil

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
			cmd := LogCommand{
				Op:    OpSetMetadata,
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

	// Add endpoint to check Raft leader status
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		state := c.raft.State().String()
		leaderAddr := c.raft.Leader()
		storageNodes := c.fsm.GetStorageNodes()
		status := fmt.Sprintf("Node ID: %s, State: %s, Leader: %s , Cluster Status: %s", c.nodeID, state, leaderAddr, c.raft.String())
		status += fmt.Sprintf("Cluster Details")
		for k, v := range storageNodes {
			status += fmt.Sprintf("Node ID: %s, State: %s, Leader: %s ", k, v, c.raft.String())
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, status)
	})

	log.Fatal(http.ListenAndServe(httpAddr, nil))
}

// startHeartbeatListener starts a UDP listener for Storage Node heartbeats.
func (c *Controller) startHeartbeatListener(udpAddr string) {
	addr, err := net.ResolveUDPAddr("udp", udpAddr)
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
		// Assuming heartbeat message is just the Storage Node ID
		storageNodeID := string(buffer[:n])
		c.recordHeartbeat(storageNodeID, remoteAddr.String())
	}
}

// recordHeartbeat updates the last heartbeat time for a Storage Node.
func (c *Controller) recordHeartbeat(nodeID string, addr string) {
	c.storageNodesMu.Lock()
	defer c.storageNodesMu.Unlock()

	// Update last heartbeat time
	c.storageNodes[nodeID] = time.Now()

	// Optionally, apply this state change through Raft if node membership needs to be replicated.
	// For now, we'll keep it in-memory for simplicity of heartbeat tracking,
	// but for true fault-tolerance, active node list should be in
	// Example:
	// if c.raft.State() == raft.Leader {
	//     cmd := LogCommand{
	//         Op:    OpUpdateNodeStatus,
	//         Key:   nodeID,
	//         Value: addr, // Or a more complex status object
	//     }
	//     cmdBytes, _ := json.Marshal(cmd)
	//     c.raft.Apply(cmdBytes, 5*time.Second)
	// }
	log.Printf("DEBUG: Heartbeat received from Storage Node: %s (%s)", nodeID, addr)
}

// monitorStorageNodes periodically checks the health of registered Storage Nodes.
func (c *Controller) monitorStorageNodes() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		c.storageNodesMu.Lock()
		for nodeID, lastHeartbeat := range c.storageNodes {
			if time.Since(lastHeartbeat) > heartbeatTimeout {
				log.Printf("WARNING: Storage Node %s timed out (last heartbeat: %v ago). Marking as unhealthy.", nodeID, time.Since(lastHeartbeat))
				// Optionally, apply a state change through Raft to remove/mark unhealthy
				if c.raft.State() == raft.Leader {
					cmd := LogCommand{
						Op:  OpRemoveNode,
						Key: nodeID,
					}
					cmdBytes, _ := json.Marshal(cmd)
					c.raft.Apply(cmdBytes, 5*time.Second)
				}
				delete(c.storageNodes, nodeID) // Remove from in-memory map
			}
		}
		c.storageNodesMu.Unlock()
	}
}

// LogCommand defines the structure of commands applied to the FSM via Raft.
// This is what gets replicated.
type LogCommand struct {
	Op    string `json:"op"`    // Operation type (e.g., "set_metadata", "add_node", "remove_node")
	Key   string `json:"key"`   // Key for the operation
	Value string `json:"value"` // Value for the operation (e.g., metadata value, node address)
	// Add other fields as needed for specific command types
}

// Operation types for the FSM
const (
	OpSetMetadata       = "set_metadata"
	OpDeleteMetadata    = "delete_metadata"
	OpAddStorageNode    = "add_storage_node"
	OpRemoveStorageNode = "remove_storage_node"
	OpUpdateNodeStatus  = "update_node_status" // For more granular node status
	OpRemoveNode        = "remove_node"
)

// GojoDBFSM implements the raft.FSM interface.
// It holds the replicated state of the Control Plane.
type GojoDBFSM struct {
	mu               sync.RWMutex
	metadata         map[string]string // Replicated metadata (e.g., table schema, config)
	storageNodes     map[string]string // Replicated active Storage Node IDs -> Address (or more complex status)
	lastAppliedIndex uint64            // The last Raft log index applied to this FSM
}

// NewGojoDBFSM creates a new instance of GojoDBFSM.
func NewGojoDBFSM() *GojoDBFSM {
	return &GojoDBFSM{
		metadata:     make(map[string]string),
		storageNodes: make(map[string]string),
	}
}

// Apply applies a Raft log entry to the FSM.
// This method is called by Raft on the leader and followers to update the state machine.
func (f *GojoDBFSM) Apply(logEntry *raft.Log) interface{} {
	var cmd LogCommand
	if err := json.Unmarshal(logEntry.Data, &cmd); err != nil {
		log.Printf("ERROR: Failed to unmarshal Raft log entry: %v", err)
		return nil // Should not happen with valid commands
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.lastAppliedIndex = logEntry.Index // Update the last applied index

	switch cmd.Op {
	case OpSetMetadata:
		f.metadata[cmd.Key] = cmd.Value
		log.Printf("DEBUG: FSM applied: Set metadata '%s' = '%s' (Index: %d)", cmd.Key, cmd.Value, logEntry.Index)
		return nil
	case OpDeleteMetadata:
		delete(f.metadata, cmd.Key)
		log.Printf("DEBUG: FSM applied: Deleted metadata '%s' (Index: %d)", cmd.Key, logEntry.Index)
		return nil
	case OpAddStorageNode:
		f.storageNodes[cmd.Key] = cmd.Value // Key is NodeID, Value is Address
		log.Printf("DEBUG: FSM applied: Added Storage Node '%s' at '%s' (Index: %d)", cmd.Key, cmd.Value, logEntry.Index)
		return nil
	case OpRemoveStorageNode:
		delete(f.storageNodes, cmd.Key)
		log.Printf("DEBUG: FSM applied: Removed Storage Node '%s' (Index: %d)", cmd.Key, logEntry.Index)
		return nil
	case OpUpdateNodeStatus:
		// This could be a more complex update, e.g., for health status
		f.storageNodes[cmd.Key] = cmd.Value // Update status/address
		log.Printf("DEBUG: FSM applied: Updated Storage Node '%s' status to '%s' (Index: %d)", cmd.Key, cmd.Value, logEntry.Index)
		return nil
	default:
		log.Printf("WARNING: Unknown FSM command operation: %s (Index: %d)", cmd.Op, logEntry.Index)
		return fmt.Errorf("unknown FSM command operation: %s", cmd.Op)
	}
}

// Snapshot returns a snapshot of the FSM's state.
// This is used by Raft to truncate the log and recover faster.
func (f *GojoDBFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Create a deep copy of the current state
	metadataCopy := make(map[string]string)
	for k, v := range f.metadata {
		metadataCopy[k] = v
	}
	storageNodesCopy := make(map[string]string)
	for k, v := range f.storageNodes {
		storageNodesCopy[k] = v
	}

	log.Printf("DEBUG: FSM Snapshot created at index %d", f.lastAppliedIndex)
	return &gojoDBFSMSnapshot{
		metadata:     metadataCopy,
		storageNodes: storageNodesCopy,
	}, nil
}

// Restore restores the FSM's state from a snapshot.
// This is used by Raft when a node joins a cluster or recovers from a crash.
func (f *GojoDBFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snapshotData struct {
		Metadata     map[string]string `json:"metadata"`
		StorageNodes map[string]string `json:"storage_nodes"`
	}

	if err := json.NewDecoder(rc).Decode(&snapshotData); err != nil {
		return fmt.Errorf("failed to decode FSM snapshot: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.metadata = snapshotData.Metadata
	f.storageNodes = snapshotData.StorageNodes
	// Note: lastAppliedIndex is typically restored by Raft itself, not the FSM.
	// The FSM's Apply method will continue from the next log entry.

	log.Println("INFO: FSM state restored from snapshot.")
	return nil
}

// --- FSM Query Methods (Read-only access to the state) ---

// GetMetadata retrieves a metadata value.
func (f *GojoDBFSM) GetMetadata(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	val, ok := f.metadata[key]
	return val, ok
}

// GetStorageNodes returns a copy of the current storage node map.
func (f *GojoDBFSM) GetStorageNodes() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	copyMap := make(map[string]string)
	for k, v := range f.storageNodes {
		copyMap[k] = v
	}
	return copyMap
}

// --- FSMSnapshot Implementation ---

// gojoDBFSMSnapshot implements the raft.FSMSnapshot interface.
type gojoDBFSMSnapshot struct {
	metadata     map[string]string
	storageNodes map[string]string
}

// Persist writes the snapshot to the given sink.
func (s *gojoDBFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

	snapshotData := struct {
		Metadata     map[string]string `json:"metadata"`
		StorageNodes map[string]string `json:"storage_nodes"`
	}{
		Metadata:     s.metadata,
		StorageNodes: s.storageNodes,
	}

	bytes, err := json.Marshal(snapshotData)
	if err != nil {
		return fmt.Errorf("failed to marshal FSM snapshot: %w", err)
	}

	if _, err := sink.Write(bytes); err != nil {
		return fmt.Errorf("failed to write FSM snapshot to sink: %w", err)
	}

	log.Println("DEBUG: FSM Snapshot persisted.")
	return nil
}

// Release is called when the snapshot is no longer needed.
func (s *gojoDBFSMSnapshot) Release() {
	log.Println("DEBUG: FSM Snapshot released.")
	// No-op for in-memory snapshot data after it's persisted.
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Command-line arguments for node configuration
	// go run main.go --id node1 --raft-addr localhost:8081 --http-addr localhost:8080
	// go run main.go --id node2 --raft-addr localhost:8082 --http-addr localhost:8083 --join-addr localhost:8080
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

	udpAddr := os.Getenv("HEARTBEAT_ADDR")
	if udpAddr == "" {
		udpAddr = fmt.Sprintf("localhost:%d", defaultUDPPort)
	}
	// Clean up old Raft data for a fresh start (for testing purposes)
	raftDataPath := filepath.Join(defaultRaftDir, nodeID)
	if err := os.RemoveAll(raftDataPath); err != nil {
		log.Printf("WARNING: Failed to clean up old Raft data for %s: %v", nodeID, err)
	}
	log.Printf("INFO: Raft data directory for %s: %s", nodeID, raftDataPath)

	_, err := NewController(nodeID, raftBindAddr, httpAddr, raftDataPath, joinAddr, udpAddr)
	if err != nil {
		log.Fatalf("FATAL: Failed to create controller: %v", err)
	}

	// Keep the main goroutine alive
	select {}
}
