package logreplication

import (
	"crypto/sha256" // For checksums
	"crypto/tls"
	"encoding/gob"
	"encoding/hex" // For checksums
	"fmt"
	"io"
	"net"
	"os"            // Added for file operations in snapshotting
	"path/filepath" // Added for file path manipulation
	"strings"
	"sync"
	"time"

	"github.com/google/uuid" // For snapshot IDs
	"github.com/sushant-115/gojodb/core/indexing"
	"github.com/sushant-115/gojodb/core/replication/events"
	storagecommon "github.com/sushant-115/gojodb/core/storage_engine/common"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	"go.uber.org/zap"
)

// ReplicaConnectionInfo ... (existing struct)
type ReplicaConnectionInfo struct {
	NodeID      string
	Address     string
	Conn        net.Conn
	EventSender *events.EventSender
	StopChan    chan struct{}
	Wg          sync.WaitGroup
	LastAckLSN  wal.LSN
	IsActive    bool
}

// BaseReplicationManager provides common fields and potentially methods for specific replication managers.
type BaseReplicationManager struct {
	NodeID     string
	IndexType  indexing.IndexType
	LogManager *wal.LogManager
	Logger     *zap.Logger
	mu         sync.RWMutex // Changed to RWMutex for finer-grained locking if needed

	PrimarySlotReplicas  map[uint64]map[string]*ReplicaConnectionInfo
	ReplicaSlotPrimaries map[uint64]*ReplicaConnectionInfo

	stopChan chan struct{}
	wg       sync.WaitGroup

	// Base path for storing/retrieving snapshot files for THIS index type on THIS node.
	// e.g., /var/lib/gojodb/<node_id>/snapshots/<index_type>/
	snapshotBaseDir string
	clientTLSCert   *tls.Config
}

// NewBaseReplicationManager initializes a new BaseReplicationManager.
// dataDir is the base data directory for the GojoDB node.
func NewBaseReplicationManager(nodeID string, indexType indexing.IndexType, logManager *wal.LogManager, logger *zap.Logger, nodeDataDir string, clientCert *tls.Config) BaseReplicationManager {
	snapDir := filepath.Join(nodeDataDir, "snapshots", string(indexType))
	if err := os.MkdirAll(snapDir, 0750); err != nil {
		// Log error but don't fail construction, snapshotting might just fail later
		logger.Error("Failed to create snapshot directory for base replication manager", zap.Error(err), zap.String("path", snapDir))
	}

	return BaseReplicationManager{
		NodeID:               nodeID,
		IndexType:            indexType,
		LogManager:           logManager,
		Logger:               logger.Named(string(indexType) + "_repl_manager"),
		PrimarySlotReplicas:  make(map[uint64]map[string]*ReplicaConnectionInfo),
		ReplicaSlotPrimaries: make(map[uint64]*ReplicaConnectionInfo),
		stopChan:             make(chan struct{}),
		snapshotBaseDir:      snapDir,
	}
}

// StartBase ... (existing method)
func (brm *BaseReplicationManager) StartBase() error {
	brm.Logger.Info("Base replication manager starting")
	return nil
}

// StopBase ... (existing method)
func (brm *BaseReplicationManager) StopBase() {
	brm.Logger.Info("Base replication manager stopping")
	close(brm.stopChan)

	brm.mu.Lock() // Lock for modifying shared maps
	for slotID, replicas := range brm.PrimarySlotReplicas {
		for replicaNodeID, connInfo := range replicas {
			if connInfo.IsActive {
				brm.Logger.Info("Stopping primary connection", zap.Uint64("slotID", slotID), zap.String("replicaNodeID", replicaNodeID))
				close(connInfo.StopChan)
				if connInfo.Conn != nil {
					connInfo.Conn.Close()
				}
			}
		}
	}
	brm.PrimarySlotReplicas = make(map[uint64]map[string]*ReplicaConnectionInfo) // Clear map

	for slotID, connInfo := range brm.ReplicaSlotPrimaries {
		if connInfo.IsActive {
			brm.Logger.Info("Stopping replica connection", zap.Uint64("slotID", slotID), zap.String("primaryNodeID", connInfo.NodeID))
			close(connInfo.StopChan)
			if connInfo.Conn != nil {
				connInfo.Conn.Close()
			}
		}
	}
	brm.ReplicaSlotPrimaries = make(map[uint64]*ReplicaConnectionInfo) // Clear map
	brm.mu.Unlock()

	//brm.wg.Wait()
	brm.Logger.Info("Base replication manager stopped")
}

// GetIndexType ... (existing method)
func (brm *BaseReplicationManager) GetIndexType() indexing.IndexType {
	return brm.IndexType
}

// sendHandshake ... (existing method)
func (brm *BaseReplicationManager) sendHandshake(conn net.Conn) error {
	brm.Logger.Debug("Sending handshake", zap.String("indexType", string(brm.IndexType)))
	indexTypeBytes := []byte(brm.IndexType)

	// Ensure index type name is not too long for a single byte length prefix
	if len(indexTypeBytes) > 255 {
		return fmt.Errorf("index type name is too long for handshake: %d bytes", len(indexTypeBytes))
	}

	// 1. Write the length of the upcoming indexType string as a single byte.
	lenByte := byte(len(indexTypeBytes))
	if _, err := conn.Write([]byte{lenByte}); err != nil {
		return fmt.Errorf("failed to write handshake length: %w", err)
	}

	// 2. Write the actual indexType string.
	if _, err := conn.Write(indexTypeBytes); err != nil {
		return fmt.Errorf("failed to write handshake body: %w", err)
	}
	brm.Logger.Info("Sent handshake", zap.String("indexType", string(brm.IndexType)), zap.String("remoteAddr", conn.RemoteAddr().String()))
	return nil
}

// streamLogs ... (existing method, ensure it's robust)
func (brm *BaseReplicationManager) streamLogs(
	connInfo *ReplicaConnectionInfo,
	wg *sync.WaitGroup,
	remoteNodeID string,
) {
	defer wg.Done()

	fromLSN := connInfo.LastAckLSN
	brm.Logger.Info("Starting log stream",
		zap.String("remoteNodeID", remoteNodeID),
		zap.Uint64("fromLSN", uint64(fromLSN)),
	)
	// Send a NO-OP record to establish the stream and prevent the receiver from closing it.
	noOpRecord := &wal.LogRecord{Type: wal.LogRecordTypeNoOp, IndexType: brm.IndexType}
	encodedNoOp, err := noOpRecord.Encode() // You'll need an Encode method on LogRecord
	if err != nil {
		brm.Logger.Error("Failed to encode NO-OP log record", zap.Error(err), zap.String("remoteNodeID", remoteNodeID))
		return
	}
	if err := connInfo.EventSender.Send(encodedNoOp); err != nil {
		brm.Logger.Error("Failed to send initial NO-OP record", zap.Error(err), zap.String("remoteNodeID", remoteNodeID))
		return
	}
	brm.Logger.Info("Sent initial NO-OP record to establish stream", zap.String("remoteNodeID", remoteNodeID))
	walReader, err := brm.LogManager.GetWALReaderForStreaming(fromLSN, remoteNodeID+"_"+string(brm.IndexType))
	if err != nil {
		brm.Logger.Error("Failed to get WAL reader for streaming", zap.Error(err), zap.String("remoteNodeID", remoteNodeID))
		return
	}
	defer walReader.Close()

	// go func() {
	// 	<-stopChan
	// 	brm.Logger.Info("Stopping log stream due to stop signal", zap.String("remoteNodeID", remoteNodeID))
	// 	walReader.Close()
	// 	closeConn()
	// }()

	// encoder := gob.NewEncoder(conn)

	for {
		var lr wal.LogRecord
		brm.Logger.Info("Calling walreader next", zap.Any("Index type", brm.IndexType))
		b, err := walReader.Next(&lr)
		if err != nil {
			if err != io.EOF {
				brm.Logger.Error("Failed to get next log record from WAL stream", zap.Error(err), zap.String("remoteNodeID", remoteNodeID))
			}
			return
		}
		brm.Logger.Info("Sending log record: ", zap.Any("indexType", lr.IndexType), zap.Any("page", lr.PageID), zap.Any("remote", connInfo.Address))
		if err := connInfo.EventSender.Send(b); err != nil {
			brm.Logger.Error("Failed to send log record", zap.Error(err), zap.String("remoteNodeID", remoteNodeID))
			return
		}

		// conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		// err = encoder.Encode(lr)
		// conn.SetWriteDeadline(time.Time{}) // Clear deadline

		// if err != nil {
		// 	brm.Logger.Error("Failed to send log record", zap.Error(err), zap.String("remoteNodeID", remoteNodeID))
		// 	return
		// }
	}
}

func (brm *BaseReplicationManager) receiveAndApplyLogs(
	conn net.Conn,
	stopChan chan struct{},
	wg *sync.WaitGroup,
	applyFn func(wal.LogRecord) error,
) {
	defer wg.Done()

	var closeOnce sync.Once
	closeConn := func() {
		closeOnce.Do(func() {
			conn.Close()
		})
	}
	defer closeConn()

	decoder := gob.NewDecoder(conn)
	brm.Logger.Info("Starting to receive logs", zap.String("fromPrimary", conn.RemoteAddr().String()))

	type decodeResult struct {
		lr  wal.LogRecord
		err error
	}
	decodeChan := make(chan decodeResult)

	// Start decoding in a separate goroutine
	go func() {
		for {
			var lr wal.LogRecord
			err := decoder.Decode(&lr)

			select {
			case <-stopChan:
				return
			case decodeChan <- decodeResult{lr, err}:
			}

			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-stopChan:
			brm.Logger.Info("Stopping log reception due to stop signal", zap.String("fromPrimary", conn.RemoteAddr().String()))
			return

		case result := <-decodeChan:
			//log.Println("DECODE RESULT: ", result)
			if result.err != nil {
				if result.err == io.EOF {
					brm.Logger.Info("Primary closed replication connection (EOF)", zap.String("fromPrimary", conn.RemoteAddr().String()))
				} else if !strings.Contains(result.err.Error(), "use of closed network connection") {
					brm.Logger.Error("Failed to decode log record from primary", zap.Error(result.err), zap.String("fromPrimary", conn.RemoteAddr().String()))
				}
				return
			}

			if result.lr.Type == wal.LogRecordTypeNoOp {
				brm.Logger.Debug("Received heartbeat log record", zap.String("fromPrimary", conn.RemoteAddr().String()))
				continue
			}

			if err := applyFn(result.lr); err != nil {
				brm.Logger.Error("Failed to apply log record", zap.Error(err), zap.Any("logRecordLSN", result.lr.LSN))
			}
		}
	}
}

// --- Base Snapshotting Method Stubs ---
// Specific managers (BTree, InvertedIndex, etc.) will need to override these with
// logic tailored to their data files and consistency requirements.

// PrepareSnapshot is a base implementation. Specific managers should override this
// to correctly identify their files and crucial metadata.
func (brm *BaseReplicationManager) PrepareSnapshot(shardID string) (*SnapshotManifest, error) {
	// This base method is a placeholder and likely insufficient for real index types.
	// It demonstrates the structure but not the specific file handling.
	brm.Logger.Info("Base PrepareSnapshot called (should be overridden by specific manager)", zap.String("shardID", shardID), zap.String("indexType", string(brm.IndexType)))

	// 1. Determine Snapshot LSN: This should be a consistent LSN from the LogManager.
	//    For a true fuzzy snapshot, this LSN is critical.
	//    A brief pause or lock might be needed on the source shard's writes if perfect consistency at LSN is needed before copy.
	//    Alternatively, rely entirely on WAL replay from an LSN taken *before* file copying started.
	snapshotLSN := brm.LogManager.GetCurrentLSN() // This is likely TOO LATE for a fuzzy snapshot. Needs careful coordination.
	// A better LSN is one taken *before* starting file copies.

	manifest := &SnapshotManifest{
		SnapshotID:        uuid.New().String(), // Generate a unique ID for this snapshot attempt
		ShardID:           shardID,
		IndexType:         brm.IndexType,
		SourceNodeID:      brm.NodeID,
		SnapshotLSN:       snapshotLSN,
		Files:             make([]SnapshotFile, 0),
		CreatedAt:         time.Now().UTC(),
		IndexSpecificMeta: make(map[string]string),
		GojoDBVersion:     "0.1.0", // TODO: Get actual version
	}

	// 2. Identify files for THIS index type and shard.
	//    This is highly dependent on how each index stores its data.
	//    Example: if BTree data is in `/var/lib/gojodb/<node>/data/<shardID>/btree/data.db`
	//    The specific BTreeReplicationManager would know this path structure.
	//    For the base manager, this is difficult.

	// Placeholder: Assume shard data for this index type is in a directory like:
	// brm.snapshotBaseDir/../../data/<shardID>/<index_type_specific_subdir>
	// This is a very weak assumption. Specific managers MUST override this.
	shardDataDir := filepath.Join(brm.snapshotBaseDir, "..", "..", "data", shardID, string(brm.IndexType)) // Highly speculative path

	brm.Logger.Warn("PrepareSnapshot in BaseReplicationManager using speculative data path. Override is essential.", zap.String("pathGuess", shardDataDir))

	// Create a temporary directory for this specific snapshot instance to perform fuzzy copy
	tempSnapshotInstanceDir := filepath.Join(brm.snapshotBaseDir, shardID, manifest.SnapshotID)
	if err := os.MkdirAll(tempSnapshotInstanceDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create temp snapshot instance dir %s: %w", tempSnapshotInstanceDir, err)
	}

	// Walk the (speculative) shardDataDir and copy files to tempSnapshotInstanceDir
	// This copy should be "fuzzy" - meaning the source files might be changing.
	// The WAL replay from manifest.SnapshotLSN handles consistency.
	err := filepath.Walk(shardDataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			relativePath, _ := filepath.Rel(shardDataDir, path)

			destPath := filepath.Join(tempSnapshotInstanceDir, relativePath)
			if err := os.MkdirAll(filepath.Dir(destPath), 0750); err != nil {
				return fmt.Errorf("failed to create dir for snapshot file %s: %w", destPath, err)
			}

			// Perform a "fuzzy" copy.
			if errCopy := brm.copyFile(path, destPath); errCopy != nil {
				return fmt.Errorf("failed to copy snapshot file %s to %s: %w", path, destPath, errCopy)
			}

			checksum, checksumErr := calculateSHA256(destPath)
			if checksumErr != nil {
				return fmt.Errorf("failed to calculate checksum for %s: %w", destPath, checksumErr)
			}

			manifest.Files = append(manifest.Files, SnapshotFile{
				RelativePath: relativePath,
				Size:         info.Size(), // Size of the *copied* file might be more accurate if source changes
				Checksum:     checksum,
				ChecksumType: "SHA256",
			})
		}
		return nil
	})

	if err != nil {
		os.RemoveAll(tempSnapshotInstanceDir) // Cleanup on error
		return nil, fmt.Errorf("error walking/copying shard data for snapshot %s: %w", shardID, err)
	}

	// Populate IndexSpecificMeta - This MUST be done by overriding methods.
	// e.g., for BTree, manifest.IndexSpecificMeta["rootPageID"] = btreeInstance.GetRootPageID().String()

	brm.Logger.Info("Snapshot prepared (base implementation)", zap.String("snapshotID", manifest.SnapshotID), zap.Int("fileCount", len(manifest.Files)))
	return manifest, nil
}

// copyFile performs a simple file copy.
func (brm *BaseReplicationManager) copyFile(src, dst string) error {
	rateLimitBytesPerSec := 10 * 1024 * 1024 // 10MB
	return storagecommon.CopyThrottled(src, dst, int64(rateLimitBytesPerSec), false)
	// sourceFile, err := os.Open(src)
	// if err != nil {
	// 	return err
	// }
	// defer sourceFile.Close()

	// destFile, err := os.Create(dst) // Creates or truncates
	// if err != nil {
	// 	return err
	// }
	// defer destFile.Close()

	// _, err = io.Copy(destFile, sourceFile)
	// return err
}

// calculateSHA256 computes the SHA256 checksum of a file.
func calculateSHA256(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// GetSnapshotDataStream provides a reader for a specific file from a prepared snapshot.
// It reads from the temporary snapshot instance directory.
func (brm *BaseReplicationManager) GetSnapshotDataStream(shardID string, snapshotID string, relativeFilePath string) (io.ReadCloser, int64, error) {
	// Path to the file within the specific snapshot instance directory
	filePath := filepath.Join(brm.snapshotBaseDir, shardID, snapshotID, relativeFilePath)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, fmt.Errorf("snapshot file %s not found in snapshot %s for shard %s: %w", relativeFilePath, snapshotID, shardID, err)
		}
		return nil, 0, fmt.Errorf("failed to stat snapshot file %s: %w", filePath, err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open snapshot file %s for streaming: %w", filePath, err)
	}

	brm.Logger.Info("Providing stream for snapshot file",
		zap.String("shardID", shardID),
		zap.String("snapshotID", snapshotID),
		zap.String("filePath", relativeFilePath),
		zap.Int64("size", fileInfo.Size()))

	return file, fileInfo.Size(), nil
}

// ApplySnapshot is a base implementation. Specific managers need to override this
// to correctly initialize their index structures from the snapshot files.
func (brm *BaseReplicationManager) ApplySnapshot(shardID string, manifest SnapshotManifest, fnGetFileData func(relativePath string) (io.ReadCloser, int64, error)) error {
	brm.Logger.Info("Base ApplySnapshot called (should be overridden by specific manager)",
		zap.String("shardID", shardID),
		zap.String("snapshotID", manifest.SnapshotID),
		zap.String("indexType", string(brm.IndexType)))

	// 1. Determine base directory for restoring this shard's data for this index type.
	//    e.g., /var/lib/gojodb/<node_id>/data/<shardID>/<index_type_specific_subdir>
	//    This needs to align with where the index expects its files.
	targetShardDataDir := filepath.Join(brm.snapshotBaseDir, "..", "..", "data", shardID, string(brm.IndexType)) // Highly speculative
	if err := os.RemoveAll(targetShardDataDir); err != nil {                                                     // Clear any existing old data
		brm.Logger.Warn("Failed to clear target shard data directory before applying snapshot", zap.Error(err), zap.String("path", targetShardDataDir))
	}
	if err := os.MkdirAll(targetShardDataDir, 0750); err != nil {
		return fmt.Errorf("failed to create target shard data directory %s: %w", targetShardDataDir, err)
	}

	// 2. Iterate through manifest.Files and restore them.
	for _, snapFile := range manifest.Files {
		fileReader, fileSize, err := fnGetFileData(snapFile.RelativePath)
		if err != nil {
			return fmt.Errorf("failed to get data stream for snapshot file %s: %w", snapFile.RelativePath, err)
		}
		defer fileReader.Close()

		if fileSize != snapFile.Size {
			brm.Logger.Warn("Snapshot file size mismatch from manifest vs stream",
				zap.String("file", snapFile.RelativePath),
				zap.Int64("manifestSize", snapFile.Size),
				zap.Int64("streamSize", fileSize))
			// Proceed, but this is a warning sign. Checksum validation is more important.
		}

		targetFilePath := filepath.Join(targetShardDataDir, snapFile.RelativePath)
		if err := os.MkdirAll(filepath.Dir(targetFilePath), 0750); err != nil {
			return fmt.Errorf("failed to create directory for target snapshot file %s: %w", targetFilePath, err)
		}

		outFile, err := os.Create(targetFilePath)
		if err != nil {
			return fmt.Errorf("failed to create target file %s for snapshot: %w", targetFilePath, err)
		}

		hasher := sha256.New()
		// TeeReader to write to file and hasher simultaneously
		teeReader := io.TeeReader(fileReader, hasher)

		_, err = io.Copy(outFile, teeReader)
		outFile.Close() // Close before checksum validation
		if err != nil {
			return fmt.Errorf("failed to write snapshot file %s: %w", targetFilePath, err)
		}

		// Validate checksum
		calculatedChecksum := hex.EncodeToString(hasher.Sum(nil))
		if calculatedChecksum != snapFile.Checksum {
			os.Remove(targetFilePath) // Delete corrupted file
			return fmt.Errorf("checksum mismatch for snapshot file %s: expected %s, got %s",
				snapFile.RelativePath, snapFile.Checksum, calculatedChecksum)
		}
		brm.Logger.Debug("Restored snapshot file", zap.String("path", targetFilePath), zap.Int64("size", snapFile.Size))
	}

	// 3. After restoring all files, the specific index manager needs to:
	//    - Initialize its data structures from these files.
	//    - Use manifest.IndexSpecificMeta (e.g., set B-tree root page ID).
	//    - Be ready to apply WALs starting from manifest.SnapshotLSN.
	//    This part MUST be handled by the overriding method in the specific manager.

	brm.Logger.Info("Snapshot files applied (base implementation). Specific index initialization pending.", zap.String("snapshotID", manifest.SnapshotID))
	return nil
}
