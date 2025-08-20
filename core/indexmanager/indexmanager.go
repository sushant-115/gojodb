package indexmanager

import (
	"context"

	pb "github.com/sushant-115/gojodb/api/proto"
	"github.com/sushant-115/gojodb/core/indexing/spatial"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
)

// IndexManager interface defines the common operations for various index types.
type IndexManager interface {
	Put(ctx context.Context, key string, value []byte) error // Generic put, might be specialized by manager
	Get(ctx context.Context, key string) ([]byte, bool)
	Delete(ctx context.Context, key string) error // Generic delete, might be specialized by manager
	GetRange(ctx context.Context, startKey, endKey string, limit int32) ([]*pb.KeyValuePair, error)
	TextSearch(ctx context.Context, query, indexName string, limit int32) ([]*pb.TextSearchResult, error)
	// Specific methods for inverted/spatial if Put/Delete are too generic
	AddDocument(ctx context.Context, docID string, content string) error
	InsertSpatial(ctx context.Context, rect spatial.Rect, dataID string) error
	DeleteSpatial(ctx context.Context, dataID string) error // Assuming a delete method for spatial data

	// PrepareSnapshot prepares a snapshot, returning a unique ID.
	PrepareSnapshot(ctx context.Context) (string, error)
	// StreamSnapshot streams data for a given snapshot ID.
	StreamSnapshot(ctx context.Context, snapshotID string, chunkChan chan []byte) error
	// ApplySnapshot applies a streamed snapshot.
	ApplySnapshot(ctx context.Context, snapshotID string, chunkChan <-chan []byte) error
	// GetLatestLSN returns the latest Log Sequence Number for the index.
	GetLatestLSN() uint64
	// ApplyLogRecord applies a single structured log record to the index.
	ApplyLogRecord(ctx context.Context, record *wal.LogRecord) error // Changed to use structured LogEntry
	// Name returns the name/type of this index manager (e.g., "btree").
	Name() string
}
