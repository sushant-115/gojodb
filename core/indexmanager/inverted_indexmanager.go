package indexmanager

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/sushant-115/gojodb/api/proto"
	"github.com/sushant-115/gojodb/core/indexing/inverted_index"
	"github.com/sushant-115/gojodb/core/indexing/spatial"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
)

// ===============================================
// InvertedIndexManager: Implementation for Inverted Index
// ===============================================

type InvertedIndexManager struct {
	mu        sync.RWMutex
	index     *inverted_index.InvertedIndex
	latestLSN uint64
	// For snapshotting (in-memory dummy)
	snapshotData map[string]map[string][]string // snapshotID -> map[term]list[docID]
}

func NewInvertedIndexManager(index *inverted_index.InvertedIndex) *InvertedIndexManager {
	return &InvertedIndexManager{
		index:        index,
		snapshotData: make(map[string]map[string][]string),
	}
}

func (m *InvertedIndexManager) Name() string { return "inverted" }

// Put is not directly supported for InvertedIndexManager (use AddDocument).
func (m *InvertedIndexManager) Put(key string, value []byte) error {
	return fmt.Errorf("Put not supported on InvertedIndexManager; use AddDocument")
}

// Get is not directly supported for InvertedIndexManager.
func (m *InvertedIndexManager) Get(key string) ([]byte, bool) {
	return nil, false // Inverted index doesn't store direct key-value
}

// Delete is not directly supported for InvertedIndexManager (needs doc ID).
func (m *InvertedIndexManager) Delete(key string) error {
	// Assuming a DeleteDocument method exists
	// return m.index.DeleteDocument(key)
	return fmt.Errorf("Delete not supported on InvertedIndexManager; use DeleteDocument(docID)")
}

func (m *InvertedIndexManager) GetRange(startKey, endKey string, limit int32) ([]*pb.KeyValuePair, error) {
	return nil, fmt.Errorf("GetRange not supported on InvertedIndexManager")
}

func (m *InvertedIndexManager) TextSearch(query, indexName string, limit int32) ([]*pb.TextSearchResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	docIDs, err := m.index.Search(query)
	var results []*pb.TextSearchResult
	for _, docID := range docIDs {
		results = append(results, &pb.TextSearchResult{Key: docID, Score: 1.0}) // Dummy score
		if limit > 0 && len(results) >= int(limit) {
			break
		}
	}
	return results, err
}

func (m *InvertedIndexManager) AddDocument(docID string, content string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.index.Insert(content, docID)
	if err == nil {
		m.latestLSN++
	}
	return err
}

// InsertSpatial is not applicable for inverted index.
func (m *InvertedIndexManager) InsertSpatial(rect spatial.Rect, dataID string) error {
	return fmt.Errorf("InsertSpatial not supported on InvertedIndexManager")
}

// DeleteSpatial is not applicable for inverted index.
func (m *InvertedIndexManager) DeleteSpatial(dataID string) error {
	return fmt.Errorf("DeleteSpatial not supported on InvertedIndexManager")
}

// PrepareSnapshot for InvertedIndexManager (dummy in-memory serialization).
func (m *InvertedIndexManager) PrepareSnapshot() (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Assuming InvertedIndex has a method to get its internal state (e.g., term -> []docID mapping)
	// indexState := m.index.GetIndexState() // Dummy method call
	// snapshotBytes, err := json.Marshal(struct {
	// 	Data map[string][]string `json:"data"` // Simplified representation of inverted index state
	// 	LSN  uint64              `json:"lsn"`
	// }{
	// 	Data: indexState,
	// 	LSN:  m.latestLSN,
	// })
	// if err != nil {
	// 	return "", fmt.Errorf("failed to marshal inverted index snapshot: %v", err)
	// }
	snapshotID := fmt.Sprintf("inverted-snapshot-%d", time.Now().UnixNano())
	// m.snapshotData[snapshotID] = indexState // Store simplified state
	log.Printf("InvertedIndexManager: Prepared snapshot %s with LSN %d", snapshotID, m.latestLSN)
	return snapshotID, nil
}

// StreamSnapshot for InvertedIndexManager.
func (m *InvertedIndexManager) StreamSnapshot(snapshotID string, chunkChan chan []byte) error {
	defer close(chunkChan)
	m.mu.RLock()
	indexState, ok := m.snapshotData[snapshotID]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("inverted index snapshot ID %s not found", snapshotID)
	}

	snapshotBytes, err := json.Marshal(indexState) // Re-marshal for streaming
	if err != nil {
		return fmt.Errorf("failed to marshal inverted index snapshot for streaming: %v", err)
	}

	chunkSize := 4 * 1024 // 4KB chunks
	for i := 0; i < len(snapshotBytes); i += chunkSize {
		end := i + chunkSize
		if end > len(snapshotBytes) {
			end = len(snapshotBytes)
		}
		chunk := snapshotBytes[i:end]
		select {
		case chunkChan <- chunk:
			// Chunk sent
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout sending inverted index snapshot chunk")
		}
	}
	log.Printf("InvertedIndexManager: Streamed snapshot %s successfully", snapshotID)
	return nil
}

// ApplySnapshot for InvertedIndexManager.
func (m *InvertedIndexManager) ApplySnapshot(snapshotID string, chunkChan <-chan []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var receivedBytes []byte
	for chunk := range chunkChan {
		receivedBytes = append(receivedBytes, chunk...)
	}

	var snapshotContent map[string][]string // Simplified snapshot content
	if err := json.Unmarshal(receivedBytes, &snapshotContent); err != nil {
		return fmt.Errorf("failed to unmarshal received inverted index snapshot: %v", err)
	}

	// Rebuild the inverted index from the snapshot data
	// m.index = inverted_index.NewInvertedIndex() // Clear existing
	// // Assuming InvertedIndex has a method to load state
	// m.index.LoadIndexState(snapshotContent) // Dummy method call
	m.latestLSN = 0 // LSN would be part of snapshot or determined after replay
	log.Printf("InvertedIndexManager: Applied snapshot %s", snapshotID)
	return nil
}

func (m *InvertedIndexManager) GetLatestLSN() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.latestLSN
}

// ApplyLogRecord for InvertedIndexManager.
func (m *InvertedIndexManager) ApplyLogRecord(entry *wal.LogRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry.LogType != wal.LogTypeInvertedIndex {
		return fmt.Errorf("InvertedIndexManager cannot apply log entry for index type %v", entry.LogType)
	}
	switch entry.Type {
	// case "ADD_DOC":
	// 	err := m.index.AddDocument(entry.DocID, entry.Content)
	// 	if err == nil {
	// 		m.latestLSN++
	// 	}
	// 	return err
	case wal.LogRecordTypeInsertKey: // Assuming you add a DeleteDocument method to inverted_index
		// 	// err := m.index.DeleteDocument(entry.DocID)
		// 	// if err == nil {
		// 	// 	m.latestLSN++
		// 	// }
		// 	// return err
		return fmt.Errorf("DeleteDocument not yet implemented for InvertedIndexManager")
	default:
		return fmt.Errorf("unsupported log entry type for InvertedIndexManager: %v", entry.Type)
	}
}
