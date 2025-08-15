package logreplication

import (
	"io" // For snapshot streaming
	"net"
	"time"

	// "github.com/sushant-115/gojodb/core/replication/raft_consensus/fsm" // Keep if fsm types are used
	"github.com/sushant-115/gojodb/core/indexing"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
)

// ReplicaDetail contains information about a replica node for a specific index.
type ReplicaDetail struct {
	NodeID  string
	Address string
}

// SnapshotManifest contains metadata about a shard snapshot.
type SnapshotManifest struct {
	SnapshotID        string             `json:"snapshot_id"`         // Unique ID for this snapshot instance
	ShardID           string             `json:"shard_id"`            // The shard this snapshot is for
	IndexType         indexing.IndexType `json:"index_type"`          // The specific index type this snapshot is for
	SourceNodeID      string             `json:"source_node_id"`      // Node that created the snapshot
	SnapshotLSN       wal.LSN            `json:"snapshot_lsn"`        // WAL LSN up to which this snapshot is consistent (after WAL replay)
	Files             []SnapshotFile     `json:"files"`               // List of files in the snapshot
	CreatedAt         time.Time          `json:"created_at"`          // Timestamp of snapshot creation
	IndexSpecificMeta map[string]string  `json:"index_specific_meta"` // e.g., B-tree rootPageID
	GojoDBVersion     string             `json:"gojodb_version"`      // Version of GojoDB that created snapshot
}

// SnapshotFile describes a single file within a snapshot.
type SnapshotFile struct {
	RelativePath string `json:"relative_path"` // Path relative to a base snapshot directory for that index
	Size         int64  `json:"size"`          // File size in bytes
	Checksum     string `json:"checksum"`      // Checksum (e.g., SHA256) of the file content
	ChecksumType string `json:"checksum_type"` // e.g., "SHA256"
}

// ReplicationManagerInterface defines the contract for index-specific replication logic.
type ReplicationManagerInterface interface {
	Start() error
	Stop() error
	ApplyLogRecord(lr wal.LogRecord) error
	HandleInboundStream(conn net.Conn) error // For WAL streaming from primary

	BecomePrimaryForSlot(slotID uint64, replicas map[string]string) error
	CeasePrimaryForSlot(slotID uint64) error
	BecomeReplicaForSlot(slotID uint64, primaryNodeID string, primaryAddress string) error
	CeaseReplicaForSlot(slotID uint64) error
	GetIndexType() indexing.IndexType

	// PrepareSnapshot is called on a source node (primary or replica) to prepare a snapshot for a given shard and index type.
	// It should perform a fuzzy copy of necessary files and return a manifest.
	// The actual data streaming happens via RequestSnapshotDataStream.
	// This method should ensure all data files for this index type for the given shard are captured.
	PrepareSnapshot(shardID string) (*SnapshotManifest, error)

	// GetSnapshotDataStream is called on the source node by a target replica to stream a specific file from a prepared snapshot.
	// The manifest (obtained via PrepareSnapshot or FSM) tells the target which files to request.
	// Returns an io.ReadCloser for the file data and the total size of the file.
	GetSnapshotDataStream(shardID string, snapshotID string, relativeFilePath string) (io.ReadCloser, int64, error)

	// ApplySnapshot is called on a target node (new replica) to apply a received snapshot.
	// It takes the manifest and a function to retrieve file data (which would internally call GetSnapshotDataStream on source).
	// After applying, the replica should be ready to catch up via WAL replay from manifest.SnapshotLSN.
	ApplySnapshot(shardID string, manifest SnapshotManifest, fnGetFileData func(relativePath string) (io.ReadCloser, int64, error)) error
}
