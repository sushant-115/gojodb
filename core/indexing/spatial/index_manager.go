// core/indexing/spatial/spatial_index_manager.go
package spatial

import (
	"fmt"
	"sync"
)

// SpatialIndexManager manages the spatial index (R-tree).
type SpatialIndexManager struct {
	rtree *RTree
	mu    sync.RWMutex // Mutex for concurrent access
}

// NewSpatialIndexManager creates a new SpatialIndexManager.
func NewSpatialIndexManager() *SpatialIndexManager {
	// You can adjust minEntries and maxEntries based on performance tuning.
	// Common values are between 2 and 20.
	return &SpatialIndexManager{
		rtree: NewRTree(3, 7), // Example values for a small R-tree
	}
}

// InsertSpatialData inserts spatial data into the index.
// dataID is the unique identifier of the document.
// point is the geographical point associated with the document.
func (sm *SpatialIndexManager) InsertSpatialData(dataID string, point Point) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// For a point, the MBR is just the point itself.
	entry := RTreeEntry{
		Rect:   NewRect(point, point),
		DataID: dataID,
	}
	sm.rtree.Insert(entry)
	fmt.Printf("SpatialIndexManager: Inserted dataID %s at %v\n", dataID, point)
	return nil
}

// DeleteSpatialData removes spatial data from the index.
func (sm *SpatialIndexManager) DeleteSpatialData(dataID string, point Point) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	entry := RTreeEntry{
		Rect:   NewRect(point, point),
		DataID: dataID,
	}
	if !sm.rtree.Delete(entry) {
		return fmt.Errorf("SpatialIndexManager: failed to delete dataID %s at %v, entry not found", dataID, point)
	}
	fmt.Printf("SpatialIndexManager: Deleted dataID %s at %v\n", dataID, point)
	return nil
}

// QuerySpatialData performs a spatial query within a given rectangle.
// It returns a slice of dataIDs that fall within the query rectangle.
func (sm *SpatialIndexManager) QuerySpatialData(queryRect Rect) ([]string, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	results := sm.rtree.Search(queryRect)
	fmt.Printf("SpatialIndexManager: Queried rect %v, found %d results\n", queryRect, len(results))
	return results, nil
}

// PrintTree for debugging purposes
func (sm *SpatialIndexManager) PrintTree() {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	sm.rtree.PrintTree()
}
