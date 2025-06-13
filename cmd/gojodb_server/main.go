package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log" // Standard log, consider replacing all with zap eventually
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath" // Added for filepath.Join
	"strings"
	"sync"
	"syscall"
	"time"

	// GojoDB core packages
	indexedreadsservice "github.com/sushant-115/gojodb/api/indexed_reads_service"
	indexedwritesservice "github.com/sushant-115/gojodb/api/indexed_writes_service"
	pb "github.com/sushant-115/gojodb/api/proto"
	gojodbcontroller "github.com/sushant-115/gojodb/cmd/gojodb_controller"
	"github.com/sushant-115/gojodb/core/indexing/btree"
	"github.com/sushant-115/gojodb/core/indexing/inverted_index"
	"github.com/sushant-115/gojodb/core/indexing/spatial"
	"github.com/sushant-115/gojodb/core/indexmanager"
	logreplication "github.com/sushant-115/gojodb/core/replication/log_replication"
	fsm "github.com/sushant-115/gojodb/core/replication/raft_consensus"
	"github.com/sushant-115/gojodb/core/storage_engine/tiered_storage"
	"github.com/sushant-115/gojodb/core/write_engine/wal"

	// GojoDB API services
	// basic_api "github.com/sushant-115/gojodb/api/basic"
	// Alias for GraphQL server

	// External libraries

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	// Protobufs (ensure these paths are correct for your project structure)
	// Assuming a common proto package
)

var (
	dbInstance            *btree.BTree[string, string]
	logManager            *wal.LogManager
	invertedIndexInstance *inverted_index.InvertedIndex
	spatialIdx            *spatial.SpatialIndexManager
	tieredStorageManager  *tiered_storage.TieredStorageManager // Corrected type name
	indexManagers         map[string]indexmanager.IndexManager

	// OLD: replicationManager    *logreplication.ReplicationManager
	// NEW: Map of index type to its replication manager
	indexReplicationManagers map[logreplication.IndexType]logreplication.ReplicationManagerInterface

	raftNode       *raft.Raft
	raftFSM        *fsm.FSM
	grpcServer     *grpc.Server
	raftController *gojodbcontroller.Controller
	httpServer     *http.Server
	zlogger        *zap.Logger // Changed from logger to zlogger to avoid conflict with standard log package

	// Command-line flags
	nodeID            = flag.String("node_id", "node1", "Unique ID for the node")
	raftAddr          = flag.String("raft_addr", "127.0.0.1:7000", "Raft bind address")
	raftDir           = flag.String("raft_dir", "/tmp/gojodb_raft_data", "Raft data directory for logs and snapshots") // More descriptive name
	grpcAddr          = flag.String("grpc_addr", "127.0.0.1:8000", "gRPC bind address")
	httpAddr          = flag.String("http_addr", "127.0.0.1:8080", "HTTP bind address for GraphQL and health checks")
	bootstrap         = flag.Bool("bootstrap", false, "Bootstrap the Raft cluster (only for the first node)")
	controllerAddr    = flag.String("controller_addr", "127.0.0.1:8080", "Controller address for joining Raft cluster and fetching shard map")
	replicationAddr   = flag.String("replication_addr", "127.0.01:6000", "Port for replication data exchange between storage nodes") // Clarified purpose
	heartbeatAddr     = flag.String("heartbeat_addr", "127.0.0.1:8081", "Port for replication data exchange between storage nodes")  // Clarified purpose
	myStorageNodeID   string                                                                                                         // Set from nodeID flag
	myStorageNodeAddr string                                                                                                         // Address this node is reachable at by other storage nodes (e.g. for replication)
	raftTransport     *raft.NetworkTransport
	// Global wait group to manage graceful shutdown of goroutines
	globalWG sync.WaitGroup
)

const (
	DefaultPageSize       = 4096
	DefaultBufferPoolSize = 100
	RaftSnapShotRetain    = 2 // Number of snapshots to retain
	RaftTransportTimeout  = 10 * time.Second
	RaftTransportMaxPool  = 3 // Max number of connections in transport pool
	ControllerRetryDelay  = 5 * time.Second
	GrpcServerStopTimeout = 5 * time.Second
	HttpServerStopTimeout = 5 * time.Second
	HandshakeTimeout      = 5 * time.Second // Timeout for replication handshake
)

func main() {
	flag.Parse()
	myStorageNodeID = *nodeID
	// Construct the replication address (host from grpcAddr, port from replicationPort)
	host := strings.Split(*grpcAddr, ":")[0]
	myStorageNodeAddr = fmt.Sprintf("%s:%s", host, *replicationAddr)

	var err error
	// Initialize logger
	zlogger, err = zap.NewDevelopment() // Or zap.NewProduction() for structured logging
	if err != nil {
		log.Fatalf("CRITICAL: Can't initialize zap logger: %v", err)
	}
	defer func() {
		// if err := zlogger.Sync(); err != nil && !strings.Contains(err.Error(), "inappropriate ioctl for device") {
		// 	log.Printf("Failed to sync logger: %v", err)
		// }
	}() // flushes buffer, if any

	zlogger.Info("Starting GojoDB storage node",
		zap.String("nodeID", myStorageNodeID),
		zap.String("raftAddr", *raftAddr),
		zap.String("grpcAddr", *grpcAddr),
		zap.String("httpAddr", *httpAddr),
		zap.String("replicationListenAddr", ":"+*replicationAddr),
		zap.String("myStorageNodeAddrForReplication", myStorageNodeAddr),
		zap.Bool("bootstrap", *bootstrap),
		zap.String("controllerAddr", *controllerAddr),
	)

	// Initialize database components
	if err := initStorageNode(); err != nil {
		zlogger.Fatal("CRITICAL: Failed to initialize storage node", zap.Error(err))
	}

	// Initialize and start Raft
	if err := initAndStartRaft(); err != nil {
		zlogger.Fatal("CRITICAL: Failed to initialize Raft", zap.Error(err))
	}

	// Start API servers
	globalWG.Add(2) // For gRPC and HTTP servers
	go startGRPCServer()
	go startHTTPServer()

	// Start replication listener (for primaries to connect to this replica)
	globalWG.Add(1)
	go listenForReplicationRequests()

	// Initial attempt to fetch shard map. FSM will handle subsequent updates via Raft.
	globalWG.Add(1)
	go initialFetchShardMapFromController()

	globalWG.Add(1)
	stopChan := make(chan struct{})
	go sendHeartBeats(stopChan)

	// Graceful shutdown
	setupSignalHandling(stopChan)

	globalWG.Wait() // Wait for all essential goroutines to finish
	zlogger.Info("GojoDB storage node shut down gracefully.")
}

func initStorageNode() error {
	zlogger.Info("Initializing storage components...")
	var err error

	// Create base data directory if it doesn't exist
	baseDataDir := filepath.Join(*raftDir, myStorageNodeID) // Node-specific data directory
	if err := os.MkdirAll(baseDataDir, 0750); err != nil {
		return fmt.Errorf("failed to create base data directory %s: %w", baseDataDir, err)
	}

	// Initialize Log Manager (WAL)
	walPath := filepath.Join(baseDataDir, "wal")
	if err := os.MkdirAll(walPath, 0750); err != nil {
		return fmt.Errorf("failed to create WAL directory %s: %w", walPath, err)
	}
	logManager, err = wal.NewLogManager(walPath)
	if err != nil {
		return fmt.Errorf("failed to create main log manager: %w", err)
	}
	zlogger.Info("Main Log Manager initialized", zap.String("path", walPath))

	// Initialize B-tree Index
	dbPath := filepath.Join(baseDataDir, "btree.db")
	dbInstance, err = btree.NewBTreeFile[string, string](
		dbPath, 3,
		btree.DefaultKeyOrder[string],
		btree.KeyValueSerializer[string, string]{
			SerializeKey:     btree.SerializeString,
			DeserializeKey:   btree.DeserializeString,
			SerializeValue:   btree.SerializeString,
			DeserializeValue: btree.DeserializeString,
		},
		10,
		4096,
		logManager,
	)
	if err != nil {
		return fmt.Errorf("failed to create B-tree instance: %w", err)
	}
	zlogger.Info("B-tree instance initialized", zap.String("path", dbPath))

	// Initialize Inverted Index
	invertedIndexPath := filepath.Join(baseDataDir, "inverted_index")
	invertedIndexDBPath := filepath.Join(invertedIndexPath, "inverted_index.db")
	if err := os.MkdirAll(invertedIndexPath, 0750); err != nil {
		return fmt.Errorf("failed to create inverted index directory %s: %w", invertedIndexPath, err)
	}
	invertedIndexLogPath := filepath.Join(invertedIndexPath, "wal")
	if err := os.MkdirAll(invertedIndexPath, 0750); err != nil {
		return fmt.Errorf("failed to create inverted index directory %s: %w", invertedIndexPath, err)
	}
	invertedIndexArchiveLogPath := filepath.Join(invertedIndexPath, "archive")
	if err := os.MkdirAll(invertedIndexPath, 0750); err != nil {
		return fmt.Errorf("failed to create inverted index directory %s: %w", invertedIndexPath, err)
	}
	// Inverted Index's LogManager: The original NewInvertedIndex took a path AND a LogManager.
	// This suggests it might write to its own files but use the passed LogManager for actual log sequence numbers or coordination.
	// For replication, it should ideally manage its own WAL file stream or use distinct log types in a shared WAL.
	// Assuming it uses the main logManager for now for its WAL records relevant to replication.
	// If it has its OWN WAL that needs replicating, its GetLogManager() should return that.
	invertedIndexInstance, err = inverted_index.NewInvertedIndex(invertedIndexDBPath, invertedIndexLogPath, invertedIndexArchiveLogPath) // Pass main logManager
	if err != nil {
		return fmt.Errorf("failed to create inverted index instance: %w", err)
	}
	zlogger.Info("Inverted Index instance initialized", zap.String("path", invertedIndexPath))

	// Initialize Spatial Index
	spatialIndexPath := filepath.Join(baseDataDir, "spatial_index")
	spatialIndexDBPath := filepath.Join(baseDataDir, "spatial_index.db")
	if err := os.MkdirAll(spatialIndexPath, 0750); err != nil {
		return fmt.Errorf("failed to create spatial index directory %s: %w", spatialIndexPath, err)
	}
	// Spatial Index LogManager: Similar to Inverted Index, clarify its WAL strategy.
	// The original NewIndexManager for spatial took a path and its own LogManager.
	spatialLogManagerPath := filepath.Join(baseDataDir, "spatial_wal")
	if err := os.MkdirAll(spatialLogManagerPath, 0750); err != nil {
		return fmt.Errorf("failed to create spatial WAL directory %s: %w", spatialLogManagerPath, err)
	}
	spatialLm, err := wal.NewLogManager(spatialLogManagerPath) // Spatial index gets its own WAL
	if err != nil {
		return fmt.Errorf("failed to create spatial log manager: %w", err)
	}
	spatialIdx, err = spatial.NewSpatialIndexManager(spatialLogManagerPath, spatialIndexDBPath, 10, 4096) // Pass path and its own LM
	if err != nil {
		return fmt.Errorf("failed to create spatial index instance: %w", err)
	}
	zlogger.Info("Spatial Index instance initialized", zap.String("path", spatialIndexPath), zap.String("wal_path", spatialLogManagerPath))

	// Initialize Tiered Storage Manager
	// Placeholder for actual storage adapter initialization (e.g., EFS, S3)
	// For now, using file-based hot storage and no-op cold storage
	hotStoragePath := filepath.Join(baseDataDir, "hot_storage")
	if err := os.MkdirAll(hotStoragePath, 0750); err != nil {
		return fmt.Errorf("failed to create hot storage directory %s: %w", hotStoragePath, err)
	}
	// hotAdapter := hot_storage.NewFileStoreAdapter(hotStoragePath)
	// coldAdapter := cold_storage.NewNoOpColdStorageAdapter() // Replace with actual (e.g., S3Adapter)
	// tieredStorageManager = tiered_storage.NewTieredStorageManager(hotAdapter, coldAdapter, logManager, zlogger)
	zlogger.Info("Tiered Storage Manager initialized")

	// --- Initialize Index Replication Managers ---
	indexReplicationManagers = make(map[logreplication.IndexType]logreplication.ReplicationManagerInterface)

	// B-tree Replication Manager (uses the main logManager)
	bTreeRepMgr := logreplication.NewBTreeReplicationManager(myStorageNodeID, dbInstance, logManager, zlogger, baseDataDir)
	indexReplicationManagers[logreplication.BTreeIndexType] = bTreeRepMgr
	zlogger.Info("B-tree Replication Manager initialized")

	// Inverted Index Replication Manager
	// It should use the log stream relevant to its data. If it writes to the main WAL via the passed logManager, use that.
	// If InvertedIndex.GetLogManager() returns a specific WAL instance for its data, use that.
	iiLogManager := invertedIndexInstance.GetLogManager() // This needs to exist and return the correct WAL.
	if iiLogManager == nil {
		zlogger.Warn("InvertedIndex GetLogManager returned nil, using main logManager for its replication. Review InvertedIndex WAL strategy.")
		iiLogManager = logManager // Fallback, ensure this is correct for how Inverted Index logs its changes.
	}
	invertedIndexRepMgr := logreplication.NewInvertedIndexReplicationManager(myStorageNodeID, invertedIndexInstance, iiLogManager, zlogger, invertedIndexPath)
	indexReplicationManagers[logreplication.InvertedIndexType] = invertedIndexRepMgr
	zlogger.Info("Inverted Index Replication Manager initialized")

	// Spatial Index Replication Manager (uses its own `spatialLm`)
	spatialRepMgr := logreplication.NewSpatialReplicationManager(myStorageNodeID, spatialIdx, spatialLm, zlogger, spatialIndexPath)
	indexReplicationManagers[logreplication.SpatialIndexType] = spatialRepMgr
	zlogger.Info("Spatial Index Replication Manager initialized")

	// Start all replication managers
	for idxType, repMgr := range indexReplicationManagers {
		if err := repMgr.Start(); err != nil {
			// Log error but don't necessarily fail fast, node might still function for other indexes
			zlogger.Error("Failed to start replication manager", zap.String("indexType", string(idxType)), zap.Error(err))
		} else {
			zlogger.Info("Successfully started replication manager", zap.String("indexType", string(idxType)))
		}
	}
	// --- End of Replication Manager Init ---

	// Initialize FSM, passing the map of replication managers.
	// The FSM uses these managers to react to shard assignment changes from Raft log.
	var replMgrs = make(map[logreplication.IndexType]logreplication.ReplicationManagerInterface)
	replMgrs[logreplication.BTreeIndexType] = bTreeRepMgr
	replMgrs[logreplication.InvertedIndexType] = invertedIndexRepMgr
	replMgrs[logreplication.SpatialIndexType] = spatialRepMgr
	raftFSM = fsm.NewFSM(
		myStorageNodeID,
		myStorageNodeAddr,
		replMgrs,
		zlogger,
	)
	zlogger.Info("Raft FSM initialized")
	zlogger.Info("Initializing IndexManager")
	btreeIdxMgr := indexmanager.NewBTreeIndexManager(dbInstance)
	spatialIdxMgr := indexmanager.NewSpatialIndexManager(spatialIdx)
	invertedIdxMgr := indexmanager.NewInvertedIndexManager(invertedIndexInstance)
	indexManagers = make(map[string]indexmanager.IndexManager)
	indexManagers["btree"] = btreeIdxMgr
	indexManagers["spatial"] = spatialIdxMgr
	indexManagers["inverted"] = invertedIdxMgr
	return nil
}

func initAndStartRaft() error {
	zlogger.Info("Initializing Raft...")
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(myStorageNodeID) // Use myStorageNodeID directly
	config.Logger = hclog.Default()
	// Adjust Raft timings if needed, defaults are generally okay for testing
	// config.HeartbeatTimeout = 1000 * time.Millisecond
	// config.ElectionTimeout = 1000 * time.Millisecond

	// Ensure Raft data directory (base for logs and snapshots) exists
	// Raft library itself might create subdirs, but good to ensure the main one.
	raftDataPath := filepath.Join(*raftDir, myStorageNodeID, "raft_meta") // Specific subdir for raft's own db/snapshots
	if err := os.MkdirAll(raftDataPath, 0700); err != nil {
		return fmt.Errorf("failed to create Raft data directory %s: %w", raftDataPath, err)
	}

	// Setup Raft communication transport
	addr, err := net.ResolveTCPAddr("tcp", *raftAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve raft address %s: %w", *raftAddr, err)
	}
	transport, err := raft.NewTCPTransport(*raftAddr, addr, RaftTransportMaxPool, RaftTransportTimeout, config.LogOutput) // Pass a logger to transport
	if err != nil {
		return fmt.Errorf("failed to create raft TCP transport: %w", err)
	}

	raftTransport = transport

	// Create snapshot store
	snapshots, err := raft.NewFileSnapshotStore(raftDataPath, RaftSnapShotRetain, config.LogOutput)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store at %s: %w", raftDataPath, err)
	}

	// Create log store and stable store (BoltDB)
	boltDBPath := filepath.Join(raftDataPath, "raft.db")
	boltDB, err := raftboltdb.NewBoltStore(boltDBPath)
	if err != nil {
		return fmt.Errorf("failed to create bolt store at %s: %w", boltDBPath, err)
	}

	// Instantiate Raft
	raftNode, err = raft.NewRaft(config, raftFSM, boltDB, boltDB, snapshots, transport)
	if err != nil {
		return fmt.Errorf("failed to create raft node: %w", err)
	}

	if *bootstrap {
		zlogger.Info("Bootstrapping Raft cluster as the first node...")
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(), // Use the address the transport is actually listening on
				},
			},
		}
		bootstrapFuture := raftNode.BootstrapCluster(configuration)
		if err := bootstrapFuture.Error(); err != nil {
			return fmt.Errorf("failed to bootstrap raft cluster: %w", err)
		}
		zlogger.Info("Raft cluster bootstrapped successfully.")
	} else if *controllerAddr != "" {
		// Attempt to join an existing cluster via the controller.
		// This is a non-blocking attempt; FSM might also handle joining later.
		// This call to joinRaftCluster should not block startup indefinitely.
		go func() { // Run in a goroutine to avoid blocking main startup
			if err := joinRaftClusterViaController(); err != nil {
				zlogger.Warn("Failed to join Raft cluster via controller on initial attempt", zap.Error(err), zap.String("controllerAddr", *controllerAddr))
			}
		}()
	} else {
		zlogger.Warn("Node is not bootstrapping and no controller address provided. It might remain isolated unless manually joined.")
	}
	raftController, err = gojodbcontroller.NewController(raftFSM, raftNode, *heartbeatAddr, *controllerAddr)
	if err != nil {
		return fmt.Errorf("failed to create raft controller: %w", err)
	}

	return nil
}

func sendHeartBeats(stopChan chan struct{}) {
	defer globalWG.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	data := map[string]string{
		"nodeId":           *nodeID,
		"address":          *httpAddr,
		"replication_addr": *replicationAddr,
		"grpc_addr":        *grpcAddr,
	}

	// Convert to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	heartbeatPath := fmt.Sprintf("http://%s/heartbeat?nodeId=%s&address=%s&replication_addr=%s&grpc_addr=%s", *heartbeatAddr, *nodeID, *httpAddr, *replicationAddr, *grpcAddr)

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			resp, err := http.Post(heartbeatPath, "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				zlogger.Warn("Failed to send heartbeat: ", zap.Error(err), zap.String("heartbeartAddr", *heartbeatAddr))
				continue
			}
			body, _ := ioutil.ReadAll(resp.Body)
			log.Println("Heartbeat response: ", string(body), heartbeatPath)
			//zlogger.Warn("Sent heartbeat: ", zap.String("heartbeartAddr", *heartbeatAddr))
		}
	}

}

// listenForReplicationRequests handles incoming connections from primaries that want to stream logs.
func listenForReplicationRequests() {
	defer globalWG.Done()
	listenAddress := *replicationAddr // Listen on all interfaces on the replication port
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		zlogger.Error("CRITICAL: Failed to start replication listener, node cannot receive replicated data", zap.Error(err), zap.String("address", listenAddress))
		return // Cannot proceed if listener fails
	}
	defer listener.Close()
	zlogger.Info("Replication listener started, waiting for connections from primaries.", zap.String("address", listenAddress))
	stopchan := raftFSM.ShutdownChan()
	go func() {
		<-stopchan
		zlogger.Info("Shutdown signal received, closing listener...")
		listener.Close() // This will unblock Accept()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			// Check if the listener was closed, e.g., during shutdown
			select {
			case <-stopchan: // Assuming FSM has a shutdown channel or similar signal
				zlogger.Info("Replication listener shutting down.")
				return
			default:
				zlogger.Error("Failed to accept replication connection", zap.Error(err))
				// Avoid busy-looping on non-temporary errors
				if ne, ok := err.(net.Error); ok && !ne.Temporary() {
					zlogger.Error("Non-temporary error accepting replication connection, listener might be broken.", zap.Error(err))
					return
				}
				time.Sleep(100 * time.Millisecond) // Brief pause before retrying accept
				continue
			}
		}
		zlogger.Info("Accepted new replication connection", zap.String("from", conn.RemoteAddr().String()))
		globalWG.Add(1) // Increment for the connection handler goroutine
		go handleReplicationConnection(conn)
	}
}

// handleReplicationConnection demultiplexes the connection based on an initial handshake message (IndexType).
func handleReplicationConnection(conn net.Conn) {
	defer globalWG.Done() // Decrement when handler finishes
	defer conn.Close()    // Ensure connection is closed

	// Read the first message to determine the IndexType for handshake
	if err := conn.SetReadDeadline(time.Now().Add(HandshakeTimeout)); err != nil {
		zlogger.Warn("Failed to set read deadline for handshake", zap.Error(err), zap.String("remoteAddr", conn.RemoteAddr().String()))
		// Continue, but handshake might fail due to other reasons
	}

	buffer := make([]byte, 128) // Buffer for IndexType string
	n, err := conn.Read(buffer)
	if err != nil {
		if err == io.EOF {
			zlogger.Info("Replication connection closed by remote before handshake", zap.String("remoteAddr", conn.RemoteAddr().String()))
		} else {
			zlogger.Error("Failed to read handshake from replication connection", zap.Error(err), zap.String("remoteAddr", conn.RemoteAddr().String()))
		}
		return
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil { // Clear the deadline
		zlogger.Warn("Failed to clear read deadline after handshake", zap.Error(err), zap.String("remoteAddr", conn.RemoteAddr().String()))
	}

	indexTypeStr := string(buffer[:n])
	indexType := logreplication.IndexType(indexTypeStr)
	zlogger.Info("Received replication handshake", zap.String("indexType", indexTypeStr), zap.String("from", conn.RemoteAddr().String()))

	// Find the appropriate replication manager
	repMgr, ok := indexReplicationManagers[indexType]
	if !ok {
		zlogger.Error("No replication manager found for index type from remote", zap.String("indexType", indexTypeStr), zap.String("from", conn.RemoteAddr().String()))
		errMsg := fmt.Sprintf("ERROR: unsupported index type %s\n", indexTypeStr)
		_, writeErr := conn.Write([]byte(errMsg))
		if writeErr != nil {
			zlogger.Error("Failed to send error message to remote for unsupported index type", zap.Error(writeErr), zap.String("remoteAddr", conn.RemoteAddr().String()))
		}
		return
	}

	zlogger.Info("Passing connection to specific replication manager", zap.String("indexType", indexTypeStr), zap.String("managerType", fmt.Sprintf("%T", repMgr)))
	// HandleInboundStream is expected to be a blocking call or manage its own goroutines
	// for the lifetime of the stream. The connection is owned by it now.
	if err := repMgr.HandleInboundStream(conn); err != nil {
		zlogger.Error("Error from HandleInboundStream of manager", zap.Error(err), zap.String("indexType", indexTypeStr), zap.String("remoteAddr", conn.RemoteAddr().String()))
	} else {
		zlogger.Info("HandleInboundStream finished for manager", zap.String("indexType", indexTypeStr), zap.String("remoteAddr", conn.RemoteAddr().String()))
	}
}

func joinRaftClusterViaController() error {
	zlogger.Info("Attempting to join Raft cluster via controller", zap.String("controllerAddr", *controllerAddr))
	joinReq := struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}{
		NodeID:   myStorageNodeID,
		RaftAddr: *raftAddr,
	}
	reqBody, err := json.Marshal(joinReq)
	if err != nil {
		return fmt.Errorf("failed to marshal join request: %w", err)
	}

	// TODO: Make controller URL path configurable if necessary
	resp, err := http.Post(fmt.Sprintf("http://%s/raft/join", *controllerAddr), "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to send join request to controller at %s: %w", *controllerAddr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("controller returned non-OK status for join request: %s - %s", resp.Status, string(bodyBytes))
	}

	zlogger.Info("Successfully requested to join Raft cluster via controller. Leader will attempt to add this node.")
	return nil
}

// initialFetchShardMapFromController tries to get the shard map once at startup.
// Subsequent updates should come via Raft FSM.
func initialFetchShardMapFromController() {
	defer globalWG.Done()
	zlogger.Info("Attempting initial fetch of shard map from controller", zap.String("controllerAddr", *controllerAddr))

	// Wait for Raft to potentially elect a leader, or for this node to join.
	// A short delay might be useful, or check raftNode.Leader().
	time.Sleep(10 * time.Second) // Initial delay before first fetch attempt

	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		if raftNode.State() == raft.Shutdown {
			zlogger.Info("Raft is shut down, aborting initial shard map fetch.")
			return
		}

		// The FSM's Apply method is the one that should call BecomePrimary/Replica on managers.
		// This function just triggers the FSM to potentially update its state if the controller provides new info.
		// The actual shard map application must go through Raft log for consistency.
		// The FSM itself could expose a method to trigger a re-fetch/re-evaluation based on controller data,
		// which then proposes a Raft command.
		// For now, let's assume the controller provides the shard map and the FSM on the LEADER
		// processes this and disseminates it via Raft log.
		// This node, upon applying that log entry, will update its roles.

		// This is a simplified view. In a robust system, the controller might push updates,
		// or the leader periodically queries the controller and proposes changes to the FSM.
		// For now, this function will just log the intent. The FSM handles the actual role changes.

		// Example: Make a GET request to the controller for the shard map.
		// The response would then be proposed as a command to the Raft cluster.
		// This proposal should ideally be done by the leader.
		// If this node is the leader, it can propose. Otherwise, it might need to route to leader.

		resp, err := http.Get(fmt.Sprintf("http://%s/shardmap", *controllerAddr))
		if err != nil {
			zlogger.Warn("Failed to fetch shard map from controller", zap.Error(err), zap.Int("attempt", i+1))
			time.Sleep(ControllerRetryDelay)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			zlogger.Warn("Controller returned non-OK for shard map", zap.String("status", resp.Status), zap.ByteString("body", bodyBytes), zap.Int("attempt", i+1))
			time.Sleep(ControllerRetryDelay)
			continue
		}
		// var shardMap fsm.ClusterShardMap // Assuming fsm.ClusterShardMap is the structure
		// if err := json.NewDecoder(resp.Body).Decode(&shardMap); err != nil {
		//  zlogger.Error("Failed to decode shard map from controller", zap.Error(err))
		//  time.Sleep(ControllerRetryDelay)
		//  continue
		// }
		// zlogger.Info("Successfully fetched initial shard map from controller (data not applied here, FSM handles via Raft log)", zap.Any("shardMapPreview", shardMap.Version)) // Log a preview
		//
		// // Now, the leader should propose this shardMap as a Raft command.
		// // if raftNode.State() == raft.Leader {
		// //  cmd := fsm.Command{Type: fsm.CommandUpdateShardMap, ShardMap: shardMap}
		// //  cmdBytes, _ := json.Marshal(cmd)
		// //  applyFuture := raftNode.Apply(cmdBytes, 5*time.Second)
		// //  if err := applyFuture.Error(); err != nil {
		// //      zlogger.Error("Failed to apply initial shard map update via Raft", zap.Error(err))
		// //  } else {
		// //      zlogger.Info("Initial shard map update proposed to Raft cluster.")
		// //  }
		// // }
		// return // Success or handled by leader.
		zlogger.Info("Initial shard map fetch logic placeholder: FSM is responsible for updates via Raft log based on controller state or leader actions.")
		return // Placeholder for now.
	}
	zlogger.Warn("Failed to fetch initial shard map from controller after multiple retries.")
}

func closeStorageNode() {
	zlogger.Info("Initiating shutdown of GojoDB storage node components...")

	// 1. Stop Raft node first to prevent new FSM applications
	if raftTransport != nil {
		zlogger.Info("Closing Raft transport...")
		if err := raftTransport.Close(); err != nil {
			zlogger.Error("Error closing Raft transport", zap.Error(err))
		}
	}

	if raftNode != nil {
		zlogger.Info("Shutting down Raft node...")
		shutdownFuture := raftNode.Shutdown() // This is async
		if err := shutdownFuture.Error(); err != nil {
			zlogger.Error("Error during Raft node shutdown", zap.Error(err))
		} else {
			zlogger.Info("Raft node shut down successfully.")
		}
	}

	// 2. Stop all index replication managers
	if indexReplicationManagers != nil {
		for idxType, repMgr := range indexReplicationManagers {
			zlogger.Info("Stopping replication manager", zap.String("indexType", string(idxType)))
			if err := repMgr.Stop(); err != nil {
				zlogger.Error("Error stopping replication manager", zap.String("indexType", string(idxType)), zap.Error(err))
			}
		}
		zlogger.Info("All replication managers stopped.")
	}

	// 3. Close FSM (if it has resources to release)
	if raftFSM != nil {
		zlogger.Info("Closing FSM...")
		raftFSM.Close() // Assuming FSM has a Close method
		// For now, FSM's resources are tied to index instances and log managers.
	}

	// 4. Close index instances
	if dbInstance != nil {
		zlogger.Info("Closing B-tree instance...")
		if err := dbInstance.Close(); err != nil {
			zlogger.Error("Error closing B-tree instance", zap.Error(err))
		}
	}
	if invertedIndexInstance != nil {
		zlogger.Info("Closing Inverted Index instance...")
		if err := invertedIndexInstance.Close(); err != nil { // Assuming Close method exists
			zlogger.Error("Error closing Inverted Index instance", zap.Error(err))
		}
	}
	if spatialIdx != nil {
		zlogger.Info("Closing Spatial Index instance...")
		if err := spatialIdx.Close(); err != nil { // Assuming Close method exists
			zlogger.Error("Error closing Spatial Index instance", zap.Error(err))
		}
	}

	// 5. Close Tiered Storage Manager
	if tieredStorageManager != nil {
		zlogger.Info("Closing Tiered Storage Manager...")
		if err := tieredStorageManager.Close(); err != nil {
			zlogger.Error("Error closing Tiered Storage Manager", zap.Error(err))
		}
	}

	// 6. Close Log Managers (main, and any specific ones like spatialLm)
	if logManager != nil {
		zlogger.Info("Closing Main Log Manager...")
		if err := logManager.Close(); err != nil {
			zlogger.Error("Error closing Main Log Manager", zap.Error(err))
		}
	}
	// spatialLm was used for spatialIdx. If spatialIdx.Close() handles its LM, this might be redundant.
	// Assuming specific log managers are closed by their users or need explicit closing if shared.
	// For spatialLm, if spatial.NewIndexManager took ownership, its Close should handle it.
	// If not, spatialLm needs to be closed here. (Let's assume spatialIdx.Close handles its own LM)

	// 7. Stop gRPC and HTTP servers (these are stopped via their goroutines upon signal)
	// The main function waits on globalWG for these to complete.
	// Initiating graceful stop here if not already handled by signal handler's context.
	if grpcServer != nil {
		zlogger.Info("Attempting graceful stop of gRPC server...")
		grpcServer.GracefulStop() // This will signal the gRPC server goroutine to exit
	}
	if httpServer != nil {
		zlogger.Info("Attempting graceful shutdown of HTTP server...")
		ctx, cancel := context.WithTimeout(context.Background(), HttpServerStopTimeout)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			zlogger.Error("Error during HTTP server shutdown", zap.Error(err))
		}
	}
	if raftController != nil {
		zlogger.Info("Attempting graceful shutdown of heartbeat server")
		ctx, cancel := context.WithTimeout(context.Background(), HttpServerStopTimeout)
		defer cancel()
		if err := raftController.HeartbeatServer.Shutdown(ctx); err != nil {
			zlogger.Error("Error during HTTP server shutdown", zap.Error(err))
		}
	}
	zlogger.Info("Storage node components closed/stopped.")
}

// RaftLoggerAdapter adapts zap.Logger to raft.Logger interface
type RaftLoggerAdapter struct {
	logger *zap.SugaredLogger
}

func NewRaftLoggerAdapter(logger *zap.Logger) *RaftLoggerAdapter {
	return &RaftLoggerAdapter{logger: logger.Sugar()}
}
func (l *RaftLoggerAdapter) Debug(msg string, args ...interface{}) { l.logger.Debugf(msg, args...) }
func (l *RaftLoggerAdapter) Info(msg string, args ...interface{})  { l.logger.Infof(msg, args...) }
func (l *RaftLoggerAdapter) Warn(msg string, args ...interface{})  { l.logger.Warnf(msg, args...) }
func (l *RaftLoggerAdapter) Error(msg string, args ...interface{}) { l.logger.Errorf(msg, args...) }
func (l *RaftLoggerAdapter) GetLevel() hclog.Level                 { return hclog.Level(l.logger.Level()) }

func startGRPCServer() {
	defer globalWG.Done()
	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		zlogger.Fatal("Failed to listen for gRPC", zap.Error(err), zap.String("address", *grpcAddr))
	}

	// Create gRPC server with interceptors for logging and recovery
	grpcServer = grpc.NewServer(
	// grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
	// 	grpc_zap.UnaryServerInterceptor(zlogger),
	// 	// Add other interceptors like recovery, auth, etc.
	// )),
	)

	// Register services
	pb.RegisterIndexedWriteServiceServer(grpcServer, indexedwritesservice.NewIndexedWriteService(myStorageNodeID, 0, indexManagers))
	// Register the IndexedReadService to handle Get, GetRange, TextSearch
	pb.RegisterIndexedReadServiceServer(grpcServer, indexedreadsservice.NewIndexedReadService(myStorageNodeID, 0, indexManagers))
	// pb.RegisterBasicServiceServer(grpcServer, basic_api.NewBasicServer(dbInstance, zlogger))
	// pb.RegisterGatewayServiceServer(grpcServer, indwrite_api.NewIndexedWriteServer(dbInstance, invertedIndexInstance, spatialIdx, zlogger))
	// pb.RegisterSnapshotServiceServer(grpcServer, indread_api.NewIndexedReadServer(dbInstance, invertedIndexInstance, spatialIdx, zlogger))
	// pb.RegisterAggregationServiceServer(grpcServer, &aggregationServiceServerImpl{})
	// pb.RegisterBulkWriteServiceServer(grpcServer, &bulkWriteServiceServerImpl{})

	zlogger.Info("gRPC server starting", zap.String("address", *grpcAddr))
	if err := grpcServer.Serve(lis); err != nil {
		// Log error unless it's the common "server closed" error during graceful shutdown
		if !strings.Contains(err.Error(), "Server closed") {
			zlogger.Error("gRPC server failed to serve", zap.Error(err))
		} else {
			zlogger.Info("gRPC server stopped gracefully.")
		}
	}
}

func startHTTPServer() {
	defer globalWG.Done()
	mux := http.NewServeMux()

	// GraphQL endpoint
	// gqlResolver := gqlserver.NewResolver(dbInstance, invertedIndexInstance, spatialIdx, zlogger)
	// srv := handler.NewDefaultServer(generated.NewExecutableSchema(generated.Config{Resolvers: gqlResolver}))
	// mux.Handle("/query", srv)                                                     // GraphQL queries
	// mux.Handle("/playground", playground.Handler("GraphQL playground", "/query")) // Interactive playground
	addMuxHandler((mux))
	raftController.RegisterHandlers(mux)

	httpServer = &http.Server{
		Addr:    *httpAddr,
		Handler: mux,
	}

	zlogger.Info("HTTP server (GraphQL, Health) starting", zap.String("address", *httpAddr))
	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		zlogger.Error("HTTP server failed to serve or closed unexpectedly", zap.Error(err))
	} else {
		zlogger.Info("HTTP server stopped gracefully.")
	}
}

func handleRaftCommand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var cmd fsm.Command
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if cmd.Type == fsm.CommandNoOp {
		http.Error(w, "Cannot apply NoOp command via API", http.StatusBadRequest)
		return
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to marshal command: %v", err), http.StatusInternalServerError)
		return
	}

	applyFuture := raftNode.Apply(cmdBytes, 5*time.Second) // 5-second timeout
	if err := applyFuture.Error(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to apply command to Raft FSM: %v", err), http.StatusInternalServerError)
		return
	}

	response := applyFuture.Response()
	responseBytes, err := json.Marshal(response)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to marshal FSM response: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(responseBytes)
}

func setupSignalHandling(stopChan chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signals
		zlogger.Info("Received signal, initiating graceful shutdown", zap.String("signal", sig.String()))

		// Start shutdown sequence
		closeStorageNode()
		stopChan <- struct{}{}

		// If there are other goroutines managed by globalWG that closeStorageNode doesn't explicitly stop,
		// they should also check a global shutdown channel or context.
		// For now, closeStorageNode is assumed to handle stopping most things directly or indirectly.

		// Give some time for graceful shutdown before forceful exit (optional)
		// time.AfterFunc(30*time.Second, func() {
		//  zlogger.Fatal("Graceful shutdown timed out, forcing exit.")
		// })
	}()
}

func addMuxHandler(mux *http.ServeMux) error {
	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
	// 	status := raftFSM.Status()
	// 	w.WriteHeader(http.StatusOK)
	// 	_, _ = w.Write([]byte(status))
	// })

	mux.HandleFunc("/raft/join", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Read node details from the request body
		var req struct {
			NodeID   string `json:"node_id"`
			RaftAddr string `json:"raft_addr"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request: failed to decode JSON", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		if req.NodeID == "" || req.RaftAddr == "" {
			http.Error(w, "bad request: nodeId and raftAddr are required", http.StatusBadRequest)
			return
		}

		log.Printf("received join request for node %s at %s", req.NodeID, req.RaftAddr)

		// Check if the node is already a part of the cluster
		configFuture := raftNode.GetConfiguration()
		if err := configFuture.Error(); err != nil {
			log.Printf("failed to get raft configuration: %v", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		for _, srv := range configFuture.Configuration().Servers {
			if srv.ID == raft.ServerID(req.NodeID) {
				log.Printf("node %s already a member of the cluster, ignoring join request", req.NodeID)
				w.WriteHeader(http.StatusOK)
				return
			}
		}

		// Add the new node as a voter to the Raft cluster
		addVoterFuture := raftNode.AddVoter(raft.ServerID(req.NodeID), raft.ServerAddress(req.RaftAddr), 0, 0)
		if err := addVoterFuture.Error(); err != nil {
			log.Printf("failed to add voter: %v", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		log.Printf("node %s at %s joined successfully", req.NodeID, req.RaftAddr)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Raft status (example, might need more secure access)
	mux.HandleFunc("/raft/stats", func(w http.ResponseWriter, r *http.Request) {
		if raftNode == nil {
			http.Error(w, "Raft not initialized", http.StatusInternalServerError)
			return
		}
		stats := raftNode.Stats()
		json.NewEncoder(w).Encode(stats)
	})
	mux.HandleFunc("/raft/leader", func(w http.ResponseWriter, r *http.Request) {
		if raftNode == nil {
			http.Error(w, "Raft not initialized", http.StatusInternalServerError)
			return
		}
		leaderAddr, leaderID := raftNode.LeaderWithID()
		json.NewEncoder(w).Encode(map[string]interface{}{"leader_addr": leaderAddr, "leader_id": leaderID})
	})

	mux.HandleFunc("/shardmap", func(w http.ResponseWriter, r *http.Request) {
		if raftNode == nil {
			http.Error(w, "Raft not initialized", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		shardmap := raftFSM.GetShardMap()
		if shardmap == nil {
			json.NewEncoder(w).Encode("{}")
		} else {
			json.NewEncoder(w).Encode(shardmap)
		}
	})

	// Generic Raft command handler
	//mux.HandleFunc("/raft/command", handleRaftCommand)
	return nil
}
