package indexmanager

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/sushant-115/gojodb/api/proto"
	"github.com/sushant-115/gojodb/core/indexing/spatial"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
)

// ===============================================
// SpatialIndexManager: Implementation for Spatial Index (R-tree)
// ===============================================

type SpatialIndexManager struct {
	mu        sync.RWMutex
	tree      *spatial.SpatialIndexManager
	latestLSN uint64
	// For snapshotting (in-memory dummy)
	snapshotData map[string][]byte // snapshotID -> serialized data
}

func NewSpatialIndexManager(rtree *spatial.SpatialIndexManager) *SpatialIndexManager {
	return &SpatialIndexManager{
		tree:         rtree,
		snapshotData: make(map[string][]byte),
	}
}

func (m *SpatialIndexManager) Name() string { return "spatial" }

// Put is not directly supported for SpatialIndexManager (use InsertSpatial).
func (m *SpatialIndexManager) Put(key string, value []byte) error {
	return fmt.Errorf("Put not supported on SpatialIndexManager; use InsertSpatial")
}

// Get is not directly supported for SpatialIndexManager.
func (m *SpatialIndexManager) Get(key string) ([]byte, bool) {
	return nil, false // Spatial index doesn't store direct key-value by key
}

// Delete is not directly supported for SpatialIndexManager (needs data ID).
func (m *SpatialIndexManager) Delete(key string) error {
	// Assuming Delete method exists on RTree
	// return m.tree.Delete(key)
	return fmt.Errorf("Delete not supported on SpatialIndexManager; use DeleteSpatial(dataID)")
}

func (m *SpatialIndexManager) GetRange(startKey, endKey string, limit int32) ([]*pb.KeyValuePair, error) {
	return nil, fmt.Errorf("GetRange not supported on SpatialIndexManager")
}

func (m *SpatialIndexManager) TextSearch(query, indexName string, limit int32) ([]*pb.TextSearchResult, error) {
	return nil, fmt.Errorf("TextSearch not supported on SpatialIndexManager")
}

func (m *SpatialIndexManager) AddDocument(docID string, content string) error {
	return fmt.Errorf("AddDocument not supported on SpatialIndexManager")
}

func (m *SpatialIndexManager) InsertSpatial(rect spatial.Rect, dataID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.tree.Insert(rect, spatial.SpatialData{ID: dataID})
	if err == nil {
		m.latestLSN++
	}
	return err
}

func (m *SpatialIndexManager) DeleteSpatial(dataID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// err := m.tree.Delete(dataID) // Assuming spatial.RTree has a Delete(dataID string) method
	// if err == nil {
	// 	m.latestLSN++
	// }
	return nil
}

// PrepareSnapshot for SpatialIndexManager (dummy in-memory serialization).
func (m *SpatialIndexManager) PrepareSnapshot() (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Assuming RTree has a method to get its internal state for serialization
	// treeState := m.tree.GetState() // Dummy method call
	// snapshotBytes, err := json.Marshal(struct {
	// 	Data interface{} `json:"data"` // Generic for RTree state
	// 	LSN  uint64      `json:"lsn"`
	// }{
	// 	Data: treeState,
	// 	LSN:  m.latestLSN,
	// })
	// if err != nil {
	// 	return "", fmt.Errorf("failed to marshal spatial index snapshot: %v", err)
	// }
	snapshotID := fmt.Sprintf("spatial-snapshot-%d", time.Now().UnixNano())
	// m.snapshotData[snapshotID] = snapshotBytes
	log.Printf("SpatialIndexManager: Prepared snapshot %s with LSN %d", snapshotID, m.latestLSN)
	return snapshotID, nil
}

// StreamSnapshot for SpatialIndexManager.
func (m *SpatialIndexManager) StreamSnapshot(snapshotID string, chunkChan chan []byte) error {
	defer close(chunkChan)
	m.mu.RLock()
	snapshotBytes, ok := m.snapshotData[snapshotID]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("spatial index snapshot ID %s not found", snapshotID)
	}

	chunkSize := 4 * 1024
	for i := 0; i < len(snapshotBytes); i += chunkSize {
		end := i + chunkSize
		if end > len(snapshotBytes) {
			end = len(snapshotBytes)
		}
		chunk := snapshotBytes[i:end]
		select {
		case chunkChan <- chunk:
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout sending spatial index snapshot chunk")
		}
	}
	log.Printf("SpatialIndexManager: Streamed snapshot %s successfully", snapshotID)
	return nil
}

// ApplySnapshot for SpatialIndexManager.
func (m *SpatialIndexManager) ApplySnapshot(snapshotID string, chunkChan <-chan []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var receivedBytes []byte
	for chunk := range chunkChan {
		receivedBytes = append(receivedBytes, chunk...)
	}

	var snapshotContent struct {
		Data json.RawMessage `json:"data"` // Use RawMessage to defer unmarshaling
		LSN  uint64          `json:"lsn"`
	}
	if err := json.Unmarshal(receivedBytes, &snapshotContent); err != nil {
		return fmt.Errorf("failed to unmarshal received spatial index snapshot: %v", err)
	}

	//m.tree = spatial.NewRTree() // Clear existing
	// Assuming RTree has a method to load state from JSON
	// err := m.tree.LoadState(snapshotContent.Data) // Dummy method call
	// if err != nil {
	// 	return fmt.Errorf("failed to load spatial tree state from snapshot: %v", err)
	// }
	log.Printf("SpatialIndexManager: Assuming spatial.RTree.LoadState(json.RawMessage) exists")
	m.latestLSN = snapshotContent.LSN
	log.Printf("SpatialIndexManager: Applied snapshot %s, LSN %d", snapshotID, m.latestLSN)
	return nil
}

func (m *SpatialIndexManager) GetLatestLSN() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.latestLSN
}

// ApplyLogRecord for SpatialIndexManager.
func (m *SpatialIndexManager) ApplyLogRecord(entry *wal.LogRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry.LogType != wal.LogTypeSpatial {
		return fmt.Errorf("SpatialIndexManager cannot apply log entry for index type %v", entry.LogType)
	}
	switch entry.Type {
	case wal.LogRecordTypeInsertKey:
		// var rect spatial.Rect // Assuming spatial.Rect is known
		// if err := json.Unmarshal(entry.RectData, &rect); err != nil {
		// 	return fmt.Errorf("failed to unmarshal spatial.Rect from log entry: %v", err)
		// }
		// err := m.tree.Insert(rect, entry.DocID) // Use DocID as dataID for spatial
		// if err == nil {
		// 	m.latestLSN++
		// }
		return nil
	// case "DELETE_SPATIAL":
	// 	err := m.tree.Delete(entry.DocID) // Assuming Delete takes dataID
	// 	if err == nil {
	// 		m.latestLSN++
	// 	}
	// 	return err
	default:
		return fmt.Errorf("unsupported log entry type for SpatialIndexManager: %v", entry.Type)
	}
}
