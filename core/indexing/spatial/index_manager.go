package spatial

import (
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
	// This should be carefully calculated based on the actual header content.
	// For now, let's assume it stores RootPageID (uint32) and NextPageID (uint33).
	// uint32 + uint32 = 8 bytes. Add some padding or future fields.
	SpatialIndexHeaderSize = 16 // Example size, adjust as needed.
)

// SpatialIndexHeader represents the metadata for the spatial index stored on disk.
type SpatialIndexHeader struct {
	RootPageID pagemanager.PageID // The page ID of the R-tree's root node.
	NextPageID pagemanager.PageID // The next available page ID for allocation.
	// Add other metadata as needed, e.g., tree order, dimensions, etc.
}

// SpatialIndexManager manages the lifecycle and disk persistence of the R-tree.
type SpatialIndexManager struct {
	rtree *RTree // The in-memory R-tree instance

	diskManager       *flushmanager.DiskManager
	bufferPoolManager *memtable.BufferPoolManager
	logManager        *wal.LogManager

	headerMu sync.RWMutex // Mutex to protect header operations
}

// NewSpatialIndexManager creates a new SpatialIndexManager.
// It takes a DiskManager, BufferPoolManager, and LogManager for persistence.
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
	header, err := sim.readHeader()
	if err != nil {
		if err == flushmanager.ErrPageNotFound || strings.Contains(err.Error(), "header page not found") { // Custom error for header not found
			log.Printf("INFO: Spatial index header not found. Creating a new R-tree.")
			// If no header, create a new R-tree and its root page.
			// The NewRTree function will now handle creating the initial root page
			// and updating the header via the BufferPoolManager.
			sim.rtree, err = NewRTree(bufferPoolManager, logManager, maxEntries, minEntries, pagemanager.InvalidPageID) // Initial root page ID 0, will be updated
			if err != nil {
				return nil, fmt.Errorf("failed to create new R-tree: %w", err)
			}
			// After creating the new R-tree, its root page ID will be set.
			// We need to write this back to the header.
			sim.headerMu.Lock()
			newPage, _, err := sim.bufferPoolManager.NewPage() // Assuming NewPage increments next page ID
			if err != nil {
				return nil, fmt.Errorf("failed to create new page: %w", err)
			}
			sim.rtree.rootPageID = newPage.GetPageID()
			anotherNewPage, _, err := sim.bufferPoolManager.NewPage() // Assuming NewPage increments next page ID
			if err != nil {
				return nil, fmt.Errorf("failed to create new page: %w", err)
			}
			sim.headerMu.Unlock()
			if err := sim.writeHeader(&SpatialIndexHeader{
				RootPageID: sim.rtree.rootPageID,
				NextPageID: anotherNewPage.GetPageID(),
			}); err != nil {
				return nil, fmt.Errorf("failed to write initial spatial index header: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to read spatial index header: %w", err)
		}
	} else {
		log.Printf("INFO: Spatial index header found. Loading existing R-tree from root page ID %d.", header.RootPageID)
		// If header exists, load the existing R-tree using its root page ID.
		sim.rtree, err = NewRTree(bufferPoolManager, logManager, maxEntries, minEntries, header.RootPageID)
		if err != nil {
			return nil, fmt.Errorf("failed to load R-tree from disk: %w", err)
		}
		// Ensure BufferPoolManager's next page ID is consistent with the loaded header
		//sim.bufferPoolManager.SetNextPageID(header.NextPageID)
	}

	return sim, nil
}

// Close flushes the buffer pool and closes the underlying disk and log managers.
func (sim *SpatialIndexManager) Close() error {
	sim.headerMu.Lock()
	defer sim.headerMu.Unlock()

	// Update the header with the latest root and next page ID before closing
	if sim.rtree != nil {
		page, _, err := sim.bufferPoolManager.NewPage()
		if err != nil {
			return fmt.Errorf("failed to create new header page: %w", err)
		}
		if err := sim.writeHeader(&SpatialIndexHeader{
			RootPageID: sim.rtree.rootPageID,
			NextPageID: page.GetPageID(),
		}); err != nil {
			log.Printf("ERROR: Failed to write final spatial index header: %v", err)
		}
	}

	if err := sim.bufferPoolManager.FlushAllPages(); err != nil {
		return fmt.Errorf("failed to flush all pages in spatial index buffer pool: %w", err)
	}
	if err := sim.diskManager.Close(); err != nil {
		return fmt.Errorf("failed to close spatial index disk manager: %w", err)
	}
	if err := sim.logManager.Close(); err != nil {
		return fmt.Errorf("failed to close spatial index log manager: %w", err)
	}
	log.Println("INFO: SpatialIndexManager closed successfully.")
	return nil
}

// Insert inserts a new spatial entry into the R-tree.
func (sim *SpatialIndexManager) Insert(rect Rect, data SpatialData) error {
	sim.headerMu.RLock() // Use RLock for read operations on the tree structure
	defer sim.headerMu.RUnlock()

	if sim.rtree == nil {
		return fmt.Errorf("spatial index is not initialized")
	}
	return sim.rtree.Insert(rect, data)
}

// Search performs a spatial query on the R-tree.
func (sim *SpatialIndexManager) Search(queryRect Rect) ([]SpatialData, error) {
	sim.headerMu.RLock()
	defer sim.headerMu.RUnlock()

	if sim.rtree == nil {
		return nil, nil
	}
	return sim.rtree.Search(queryRect)
}

// readHeader reads the spatial index header from the header page.
func (sim *SpatialIndexManager) readHeader() (*SpatialIndexHeader, error) {
	sim.headerMu.RLock()
	defer sim.headerMu.RUnlock()

	page, err := sim.bufferPoolManager.FetchPage(SpatialIndexHeaderPageID)
	if err != nil {
		return nil, fmt.Errorf("header page not found: %w", err)
	}
	defer sim.bufferPoolManager.UnpinPage(page.GetPageID(), false) // Unpin, not dirty

	data := page.GetData()
	if len(data) < SpatialIndexHeaderSize {
		return nil, fmt.Errorf("spatial index header page is too small")
	}

	header := &SpatialIndexHeader{}
	header.RootPageID = pagemanager.PageID(binary.LittleEndian.Uint32(data[0:4]))
	header.NextPageID = pagemanager.PageID(binary.LittleEndian.Uint32(data[4:8]))
	// Add more fields if necessary

	return header, nil
}

// writeHeader writes the spatial index header to the header page.
func (sim *SpatialIndexManager) writeHeader(header *SpatialIndexHeader) error {
	sim.headerMu.Lock()
	defer sim.headerMu.Unlock()

	page, err := sim.bufferPoolManager.FetchPage(SpatialIndexHeaderPageID)
	if err != nil {
		// If header page doesn't exist, create it. This happens on first initialization.
		page, _, err = sim.bufferPoolManager.NewPage()
		if err != nil {
			return fmt.Errorf("failed to create new header page: %w", err)
		}
		if page.GetPageID() != SpatialIndexHeaderPageID {
			// This indicates an issue if page 0 isn't the first allocated page.
			// For simplicity, we assume page 0 is always the header.
			log.Printf("WARNING: Allocated page ID %d for header, expected %d. This might indicate an issue.", page.GetPageID(), SpatialIndexHeaderPageID)
		}
	}

	data := page.GetData()
	if len(data) < SpatialIndexHeaderSize {
		return fmt.Errorf("spatial index header page is too small for writing")
	}

	binary.LittleEndian.PutUint32(data[0:4], uint32(header.RootPageID))
	binary.LittleEndian.PutUint32(data[4:8], uint32(header.NextPageID))
	// Write more fields if necessary

	sim.bufferPoolManager.UnpinPage(page.GetPageID(), true) // Mark dirty
	return nil
}
