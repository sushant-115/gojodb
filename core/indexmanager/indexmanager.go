package indexmanager

import (
	pb "github.com/sushant-115/gojodb/api/proto"
	"github.com/sushant-115/gojodb/core/indexing/spatial"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
)

// IndexManager interface defines the common operations for various index types.
type IndexManager interface {
	Put(key string, value []byte) error // Generic put, might be specialized by manager
	Get(key string) ([]byte, bool)
	Delete(key string) error // Generic delete, might be specialized by manager
	GetRange(startKey, endKey string, limit int32) ([]*pb.KeyValuePair, error)
	TextSearch(query, indexName string, limit int32) ([]*pb.TextSearchResult, error)
	// Specific methods for inverted/spatial if Put/Delete are too generic
	AddDocument(docID string, content string) error
	InsertSpatial(rect spatial.Rect, dataID string) error
	DeleteSpatial(dataID string) error // Assuming a delete method for spatial data

	// PrepareSnapshot prepares a snapshot, returning a unique ID.
	PrepareSnapshot() (string, error)
	// StreamSnapshot streams data for a given snapshot ID.
	StreamSnapshot(snapshotID string, chunkChan chan []byte) error
	// ApplySnapshot applies a streamed snapshot.
	ApplySnapshot(snapshotID string, chunkChan <-chan []byte) error
	// GetLatestLSN returns the latest Log Sequence Number for the index.
	GetLatestLSN() uint64
	// ApplyLogRecord applies a single structured log record to the index.
	ApplyLogRecord(record *wal.LogRecord) error // Changed to use structured LogEntry
	// Name returns the name/type of this index manager (e.g., "btree").
	Name() string
}
