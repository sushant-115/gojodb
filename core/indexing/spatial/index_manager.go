package spatial

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	flushmanager "github.com/sushant-115/gojodb/core/write_engine/flush_manager"
	"github.com/sushant-115/gojodb/core/write_engine/memtable"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
)

const (
	// SpatialIndexHeaderPageID is the fixed page ID for the spatial index header.
	SpatialIndexHeaderPageID pagemanager.PageID = 0
	// SpatialIndexHeaderSize is the size of the spatial index header in bytes.
	// It stores RootPageID (uint64) and NextPageID (uint64).
	SpatialIndexHeaderSize = 16 // Two uint64s (8 bytes each) = 16 bytes
)

// SpatialIndexHeader stores metadata about the spatial index.
type SpatialIndexHeader struct {
	RootPageID pagemanager.PageID // The page ID of the R-tree's root node
	NextPageID pagemanager.PageID // The next available page ID for allocation
}

// serialize converts the header into a byte slice.
func (h *SpatialIndexHeader) serialize(page *pagemanager.Page) error {
	buf := new(bytes.Buffer)
	// Write RootPageID (uint64)
	if err := binary.Write(buf, binary.LittleEndian, uint64(h.RootPageID)); err != nil {
		return fmt.Errorf("failed to write RootPageID: %w", err)
	}
	// Write NextPageID (uint64)
	if err := binary.Write(buf, binary.LittleEndian, uint64(h.NextPageID)); err != nil {
		return fmt.Errorf("failed to write NextPageID: %w", err)
	}

	// Pad with zeros to fill the header size
	data := page.GetData()
	copy(data, buf.Bytes())
	for i := buf.Len(); i < len(data); i++ {
		data[i] = 0
	}
	return nil
}

// deserialize loads the header from a byte slice.
func (h *SpatialIndexHeader) deserialize(page *pagemanager.Page) error {
	data := page.GetData()
	if len(data) < SpatialIndexHeaderSize {
		return fmt.Errorf("spatial index header data too short: got %d bytes, need %d", len(data), SpatialIndexHeaderSize)
	}

	// Read RootPageID (uint64)
	h.RootPageID = pagemanager.PageID(binary.LittleEndian.Uint64(data[0:8]))
	// Read NextPageID (uint64)
	h.NextPageID = pagemanager.PageID(binary.LittleEndian.Uint64(data[8:16]))

	return nil
}

// SpatialIndexManager manages the R-tree index, handling its persistence
// through the BufferPoolManager, DiskManager, and LogManager.
type SpatialIndexManager struct {
	rtree             *RTree
	bufferPoolManager *memtable.BufferPoolManager
	diskManager       *flushmanager.DiskManager
	logManager        *wal.LogManager
	mu                sync.RWMutex // Protects manager state, especially during initialization/shutdown
}

// // NewSpatialIndexManager creates a new SpatialIndexManager.
// // It initializes or loads the R-tree.
// func NewSpatialIndexManager(
// 	bpm *memtable.BufferPoolManager,
// 	dm *flushmanager.DiskManager,
// 	lm *wal.LogManager,
// ) (*SpatialIndexManager, error) {
// 	sim := &SpatialIndexManager{
// 		bufferPoolManager: bpm,
// 		diskManager:       dm,
// 		logManager:        lm,
// 	}

// 	// Initialize or load the R-tree
// 	rt, err := NewRTree(bpm, dm, lm)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to initialize R-tree: %w", err)
// 	}
// 	sim.rtree = rt

// 	log.Println("INFO: SpatialIndexManager initialized successfully.")
// 	return sim, nil
// }

func NewSpatialIndexManager(
	uniqueSpatialIndexArchiveDir, uniqueSpatialIndexLogDir, uniqueSpatialIndexFilePath string,
	logBufferSize, logSegmentSize, dbPageSize int,
	maxEntries int, // Max entries per R-tree node
	minEntries int, // Min entries per R-tree node
) (*SpatialIndexManager, error) {

	logManager, err := wal.NewLogManager(uniqueSpatialIndexLogDir, uniqueSpatialIndexArchiveDir, logBufferSize, int64(logSegmentSize))
	if err != nil {
		return nil, fmt.Errorf("failed to create LogManager for spatial index: %w", err)
	}
	diskManager, err := flushmanager.NewDiskManager(uniqueSpatialIndexFilePath, dbPageSize)
	if err != nil {
		logManager.Close()
		return nil, fmt.Errorf("failed to create DiskManager for spatial index: %w", err)
	}

	// Ensure spatial index file exists or is created
	_, err = diskManager.OpenOrCreateFile(false, 0, 0)
	if err != nil {
		if os.IsNotExist(err) || strings.Contains(err.Error(), "database file not found") {
			log.Printf("INFO: Spatial index file %s not found. Creating new file.", uniqueSpatialIndexFilePath)
			_, err = diskManager.OpenOrCreateFile(true, 0, 0)
			if err != nil {
				logManager.Close()
				diskManager.Close()
				return nil, fmt.Errorf("failed to create new spatial index file: %w", err)
			}
		} else {
			logManager.Close()
			diskManager.Close()
			return nil, fmt.Errorf("failed to open existing spatial index file: %w", err)
		}
	}

	bufferPoolManager := memtable.NewBufferPoolManager(logBufferSize, diskManager, logManager)

	sim := &SpatialIndexManager{
		diskManager:       diskManager,
		bufferPoolManager: bufferPoolManager,
		logManager:        logManager,
	}

	// Try to read the header to see if an R-tree already exists on disk.
	if err != nil {
		if err == flushmanager.ErrPageNotFound || strings.Contains(err.Error(), "header page not found") { // Custom error for header not found
			log.Printf("INFO: Spatial index header not found. Creating a new R-tree.")
			// If no header, create a new R-tree and its root page.
			// The NewRTree function will now handle creating the initial root page
			// and updating the header via the BufferPoolManager.
			sim.rtree, err = NewRTree(bufferPoolManager, diskManager, logManager) // Initial root page ID 0, will be updated
			if err != nil {
				return nil, fmt.Errorf("failed to create new R-tree: %w", err)
			}

		} else {
			return nil, fmt.Errorf("failed to read spatial index header: %w", err)
		}
	} else {
		log.Printf("INFO: Spatial index header found. Loading existing R-tree from root page ID")
		// If header exists, load the existing R-tree using its root page ID.
		sim.rtree, err = NewRTree(bufferPoolManager, diskManager, logManager)
		if err != nil {
			return nil, fmt.Errorf("failed to load R-tree from disk: %w", err)
		}
		// Ensure BufferPoolManager's next page ID is consistent with the loaded header
		//sim.bufferPoolManager.SetNextPageID(header.NextPageID)
	}

	return sim, nil
}

// Insert adds a spatial entry to the R-tree.
func (sim *SpatialIndexManager) Insert(rect Rect, data SpatialData) error {
	sim.mu.RLock()
	defer sim.mu.RUnlock()
	return sim.rtree.Insert(rect, data)
}

// Delete removes a spatial entry from the R-tree.
func (sim *SpatialIndexManager) Delete(rect Rect, data SpatialData) error {
	sim.mu.RLock()
	defer sim.mu.RUnlock()
	return sim.rtree.Delete(rect, data)
}

// Query performs a spatial query on the R-tree.
func (sim *SpatialIndexManager) Query(queryRect Rect) ([]SpatialData, error) {
	sim.mu.RLock()
	defer sim.mu.RUnlock()
	return sim.rtree.Search(queryRect)
}

// Close flushes any pending changes and ensures the spatial index is gracefully shut down.
func (sim *SpatialIndexManager) Close() error {
	sim.mu.Lock()
	defer sim.mu.Unlock()

	log.Println("INFO: Closing SpatialIndexManager...")
	if sim.rtree != nil {
		if err := sim.rtree.Close(); err != nil {
			return fmt.Errorf("failed to close R-tree: %w", err)
		}
	}
	log.Println("INFO: SpatialIndexManager closed.")
	return nil
}
