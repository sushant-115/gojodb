package spatial

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"sync"

	"github.com/sushant-115/gojodb/core/write_engine/memtable"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
)

// Rect represents a 2D rectangle (Minimum Bounding Rectangle - MBR).
type Rect struct {
	MinX, MinY float64
	MaxX, MaxY float64
}

// SpatialData holds the actual data associated with a spatial entry.
type SpatialData struct {
	ID string // Unique identifier for the data record in the main database
	// Add other relevant data fields if necessary, e.g., actual coordinates if not just a point.
}

// Node represents a node in the R-tree.
type Node struct {
	pageID   pagemanager.PageID // The page ID where this node is stored on disk
	isLeaf   bool
	entries  []Entry
	parentID pagemanager.PageID // Parent's page ID, for upward traversal (optional, but useful for deletions)
	mu       sync.RWMutex       // Mutex to protect concurrent access to node data
}

// Entry represents an entry within an R-tree node.
// It can be a data entry (in a leaf node) or a pointer to a child node (in an internal node).
type Entry struct {
	Rect     Rect
	ChildID  pagemanager.PageID // If internal node, this is the PageID of the child node
	Data     SpatialData        // If leaf node, this is the actual spatial data
	IsBranch bool               // True if this entry points to a child node, false if it's a data entry
}

// RTree represents the R-tree index.
type RTree struct {
	rootPageID pagemanager.PageID // The page ID of the root node
	maxEntries int                // Maximum number of entries per node
	minEntries int                // Minimum number of entries per node (usually maxEntries / 2)

	bpm *memtable.BufferPoolManager // Buffer Pool Manager for page operations
	lm  *wal.LogManager             // Log Manager for Write-Ahead Logging

	mu sync.RWMutex // Mutex to protect rootPageID and tree-wide operations
}

// NewRTree creates a new R-tree instance or loads an existing one from disk.
func NewRTree(
	bpm *memtable.BufferPoolManager,
	lm *wal.LogManager,
	maxEntries int,
	minEntries int,
	rootPageID pagemanager.PageID,
) (*RTree, error) {
	if bpm == nil || lm == nil {
		return nil, fmt.Errorf("bufferPoolManager and logManager cannot be nil")
	}
	if maxEntries <= 1 || minEntries <= 0 || minEntries > maxEntries/2 {
		return nil, fmt.Errorf("invalid R-tree parameters: maxEntries=%d, minEntries=%d", maxEntries, minEntries)
	}

	tree := &RTree{
		bpm:        bpm,
		lm:         lm,
		maxEntries: maxEntries,
		minEntries: minEntries,
		rootPageID: rootPageID, // This will be 0 if new, or actual ID if loading
	}

	// If rootPageID is 0, it means we are creating a new tree.
	// Otherwise, we try to load the existing root.
	log.Println("Root page ID: ", rootPageID.GetID())
	if rootPageID == pagemanager.InvalidPageID {
		// Create the initial root node (a leaf node)
		rootPage, _, err := bpm.NewPage()
		if err != nil {
			return nil, fmt.Errorf("failed to create new root page for R-tree: %w", err)
		}
		tree.rootPageID = rootPage.GetPageID()
		rootNode := &Node{
			pageID:   tree.rootPageID,
			isLeaf:   true,
			entries:  []Entry{},
			parentID: pagemanager.InvalidPageID, // Root has no parent
		}

		// Serialize and unpin the new root page
		if err := rootNode.serialize(rootPage); err != nil {
			bpm.UnpinPage(rootPage.GetPageID(), false) // Unpin without marking dirty on error
			return nil, fmt.Errorf("failed to serialize initial root node: %w", err)
		}
		bpm.UnpinPage(rootPage.GetPageID(), true) // Mark dirty as it's a new page

		log.Printf("INFO: New R-tree created with root page ID: %d", tree.rootPageID)
	} else {
		// Attempt to fetch the root page to ensure it exists
		_, err := tree.fetchNode(tree.rootPageID)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch existing R-tree root page %d: %w", tree.rootPageID, err)
		}
		log.Printf("INFO: R-tree loaded from existing root page ID: %d", tree.rootPageID)
	}

	return tree, nil
}

// Insert inserts a new spatial entry into the R-tree.
func (rt *RTree) Insert(rect Rect, data SpatialData) error {
	rt.mu.Lock() // Lock the tree for write operations
	defer rt.mu.Unlock()

	// Find the leaf node to insert into
	leafNode, err := rt.chooseLeaf(rt.rootPageID, rect)
	if err != nil {
		return fmt.Errorf("failed to choose leaf for insertion: %w", err)
	}

	// Create the new entry
	newEntry := Entry{
		Rect:     rect,
		Data:     data,
		IsBranch: false, // This is a data entry
	}

	leafNode.mu.Lock() // Lock the leaf node for modification
	leafNode.entries = append(leafNode.entries, newEntry)

	// Log the insertion operation
	// TODO: Define a specific LogRecord type for R-tree insertions
	logRecord := &wal.LogRecord{
		Type:    wal.LogTypeRTreeInsert,                                                          // Custom log type for R-tree
		PageID:  leafNode.pageID,                                                                 // Placeholder, should be actual prev LSN for page
		NewData: []byte(fmt.Sprintf("Insert data ID %s into page %d", data.ID, leafNode.pageID)), // Example payload
	}
	if _, err := rt.lm.Append(logRecord); err != nil {
		leafNode.mu.Unlock()
		return fmt.Errorf("failed to log R-tree insert: %w", err)
	}

	// Check for overflow and split if necessary
	if len(leafNode.entries) > rt.maxEntries {
		log.Printf("DEBUG: Leaf node %d overflowed. Splitting...", leafNode.pageID)
		splitNodes, err := rt.splitNode(leafNode)
		if err != nil {
			leafNode.mu.Unlock()
			return fmt.Errorf("failed to split leaf node: %w", err)
		}
		// Propagate splits upwards
		if err := rt.adjustTree(splitNodes[0], splitNodes[1]); err != nil {
			leafNode.mu.Unlock()
			return fmt.Errorf("failed to adjust tree after split: %w", err)
		}
	} else {
		// If no split, just update the MBRs of parent nodes
		if err := rt.adjustTree(leafNode, nil); err != nil { // Pass nil for the second node if no split
			leafNode.mu.Unlock()
			return fmt.Errorf("failed to adjust tree after simple insert: %w", err)
		}
	}
	leafNode.mu.Unlock()

	// Serialize the modified leaf node and unpin it, marking dirty
	page, err := rt.bpm.FetchPage(leafNode.pageID)
	if err != nil {
		return fmt.Errorf("failed to fetch page %d for serialization: %w", leafNode.pageID, err)
	}
	if err := leafNode.serialize(page); err != nil {
		rt.bpm.UnpinPage(page.GetPageID(), false) // Unpin without marking dirty on error
		return fmt.Errorf("failed to serialize leaf node %d: %w", leafNode.pageID, err)
	}
	rt.bpm.UnpinPage(page.GetPageID(), true) // Mark dirty

	return nil
}

// Search performs a spatial query on the R-tree.
func (rt *RTree) Search(queryRect Rect) ([]SpatialData, error) {
	rt.mu.RLock() // Lock the tree for read operations
	defer rt.mu.RUnlock()

	var results []SpatialData
	if rt.rootPageID == pagemanager.InvalidPageID {
		return results, nil // Empty tree
	}

	// Start search from the root
	rootNode, err := rt.fetchNode(rt.rootPageID)
	if err != nil {
		log.Printf("ERROR: Failed to fetch root node for search: %v", err)
		return results, nil
	}
	defer rt.bpm.UnpinPage(rootNode.pageID, false) // Unpin root, not dirty

	rt.searchRecursive(rootNode, queryRect, &results)
	return results, nil
}

// searchRecursive recursively searches the R-tree.
func (rt *RTree) searchRecursive(node *Node, queryRect Rect, results *[]SpatialData) {
	node.mu.RLock() // Read lock the current node
	defer node.mu.RUnlock()

	for _, entry := range node.entries {
		if intersects(queryRect, entry.Rect) {
			if node.isLeaf {
				// It's a leaf node, and the entry's MBR intersects the query.
				// Add the actual data if it's a data entry.
				*results = append(*results, entry.Data)
			} else {
				// It's an internal node, recursively search the child.
				childNode, err := rt.fetchNode(entry.ChildID)
				if err != nil {
					log.Printf("ERROR: Failed to fetch child node %d during search: %v", entry.ChildID, err)
					continue
				}
				rt.searchRecursive(childNode, queryRect, results)
				rt.bpm.UnpinPage(childNode.pageID, false) // Unpin child after use
			}
		}
	}
}

// fetchNode fetches a node from the buffer pool or disk.
func (rt *RTree) fetchNode(pageID pagemanager.PageID) (*Node, error) {
	page, err := rt.bpm.FetchPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch page %d: %w", pageID, err)
	}

	node := &Node{pageID: pageID}
	if err := node.deserialize(page); err != nil {
		rt.bpm.UnpinPage(pageID, false) // Unpin if deserialization fails
		return nil, fmt.Errorf("failed to deserialize node from page %d: %w", pageID, err)
	}
	return node, nil
}

// chooseLeaf finds the best leaf node to insert a new entry.
// This is a simplified implementation of the R-tree's ChooseLeaf algorithm.
// It recursively traverses the tree to find the leaf node where the new entry
// would cause the minimum increase in the MBR area.
func (rt *RTree) chooseLeaf(currentPageID pagemanager.PageID, rect Rect) (*Node, error) {
	if currentPageID == pagemanager.InvalidPageID {
		return nil, nil
	}
	node, err := rt.fetchNode(currentPageID)
	if err != nil {
		return nil, err
	}
	defer rt.bpm.UnpinPage(node.pageID, false) // Unpin after fetching, will be re-pinned if modified

	if node.isLeaf {
		return node, nil // Found the leaf node
	}

	// Find the entry whose MBR requires the minimum area enlargement to cover rect.
	// If there's a tie, choose the entry with the smallest area.
	minEnlargement := math.MaxFloat64
	minArea := math.MaxFloat64
	var chosenEntry Entry

	for _, entry := range node.entries {
		enlargedRect := unionRect(entry.Rect, rect)
		enlargement := area(enlargedRect) - area(entry.Rect)
		currentArea := area(entry.Rect)

		if enlargement < minEnlargement || (enlargement == minEnlargement && currentArea < minArea) {
			minEnlargement = enlargement
			minArea = currentArea
			chosenEntry = entry
		}
	}

	// Recursively call chooseLeaf on the chosen child node
	return rt.chooseLeaf(chosenEntry.ChildID, rect)
}

// splitNode splits an overflowing node into two new nodes.
// This is a placeholder for the R-tree's quadratic or linear split algorithm.
// It will create two new pages and distribute entries between them.
func (rt *RTree) splitNode(node *Node) ([]*Node, error) {
	// For simplicity, a very basic split: just divide entries into two halves.
	// A real R-tree uses more sophisticated algorithms (e.g., quadratic, linear).

	// Create two new pages for the split nodes
	newNode1Page, _, err := rt.bpm.NewPage()
	if err != nil {
		return nil, fmt.Errorf("failed to create new page for split node 1: %w", err)
	}
	newNode2Page, _, err := rt.bpm.NewPage()
	if err != nil {
		rt.bpm.UnpinPage(newNode1Page.GetPageID(), false)
		rt.bpm.FlushPage(newNode1Page.GetPageID()) // Clean up
		return nil, fmt.Errorf("failed to create new page for split node 2: %w", err)
	}

	newNode1 := &Node{
		pageID:   newNode1Page.GetPageID(),
		isLeaf:   node.isLeaf,
		parentID: node.parentID,
		entries:  node.entries[:len(node.entries)/2],
	}
	newNode2 := &Node{
		pageID:   newNode2Page.GetPageID(),
		isLeaf:   node.isLeaf,
		parentID: node.parentID,
		entries:  node.entries[len(node.entries)/2:],
	}

	// Serialize and unpin new nodes
	if err := newNode1.serialize(newNode1Page); err != nil {
		rt.bpm.UnpinPage(newNode1Page.GetPageID(), false)
		rt.bpm.FlushPage(newNode1Page.GetPageID())
		rt.bpm.UnpinPage(newNode2Page.GetPageID(), false)
		rt.bpm.FlushPage(newNode2Page.GetPageID())
		return nil, fmt.Errorf("failed to serialize split node 1: %w", err)
	}
	rt.bpm.UnpinPage(newNode1Page.GetPageID(), true) // Mark dirty

	if err := newNode2.serialize(newNode2Page); err != nil {
		rt.bpm.UnpinPage(newNode1Page.GetPageID(), false) // Unpin the first one if error on second
		rt.bpm.FlushPage(newNode1Page.GetPageID())
		rt.bpm.UnpinPage(newNode2Page.GetPageID(), false)
		rt.bpm.FlushPage(newNode2Page.GetPageID())
		return nil, fmt.Errorf("failed to serialize split node 2: %w", err)
	}
	rt.bpm.UnpinPage(newNode2Page.GetPageID(), true) // Mark dirty

	// Log the split operation
	// TODO: Define a specific LogRecord type for R-tree splits
	logRecord := &wal.LogRecord{
		Type:    wal.LogTypeRTreeSplit, // Custom log type
		PageID:  node.pageID,
		NewData: []byte(fmt.Sprintf("Split node %d into %d and %d", node.pageID, newNode1.pageID, newNode2.pageID)),
	}
	if _, err := rt.lm.Append(logRecord); err != nil {
		return nil, fmt.Errorf("failed to log R-tree split: %w", err)
	}

	// The original node's page should now be deallocated or marked as empty
	// For simplicity, we'll just unpin it and assume it will be overwritten or deallocated later.
	// In a real system, you'd explicitly delete the original page if it's no longer needed.
	rt.bpm.UnpinPage(node.pageID, false) // Original page is no longer valid for this node content

	return []*Node{newNode1, newNode2}, nil
}

// adjustTree propagates changes (MBR enlargement, node splits) upwards to the root.
func (rt *RTree) adjustTree(node1 *Node, node2 *Node) error {
	// node1 is the node that was modified (or one of the split nodes)
	// node2 is the second node if a split occurred, otherwise nil.

	var currentNodes []*Node
	currentNodes = append(currentNodes, node1)
	if node2 != nil {
		currentNodes = append(currentNodes, node2)
	}

	for len(currentNodes) > 0 {
		var nextLevelNodes []*Node
		for _, node := range currentNodes {
			if node.pageID == rt.rootPageID {
				// If we reached the root and it overflowed, create a new root
				if node2 != nil && len(node.entries) > rt.maxEntries { // Check if root itself needs splitting
					newRootPage, _, err := rt.bpm.NewPage()
					if err != nil {
						return fmt.Errorf("failed to create new root page: %w", err)
					}
					newRoot := &Node{
						pageID:   newRootPage.GetPageID(),
						isLeaf:   false, // New root is always internal
						parentID: pagemanager.InvalidPageID,
						entries: []Entry{
							{Rect: calculateMBR(node1.entries), ChildID: node1.pageID, IsBranch: true},
							{Rect: calculateMBR(node2.entries), ChildID: node2.pageID, IsBranch: true},
						},
					}
					rt.mu.Lock()
					rt.rootPageID = newRoot.pageID // Update global root page ID
					rt.mu.Unlock()

					// Log the new root creation
					logRecord := &wal.LogRecord{
						Type:    wal.LogTypeRTreeNewRoot, // Custom log type
						PageID:  newRoot.pageID,
						NewData: []byte(fmt.Sprintf("New root %d created from split of %d", newRoot.pageID, node.pageID)),
					}
					if _, err := rt.lm.Append(logRecord); err != nil {
						return fmt.Errorf("failed to log new root creation: %w", err)
					}

					if err := newRoot.serialize(newRootPage); err != nil {
						rt.bpm.UnpinPage(newRootPage.GetPageID(), false)
						return fmt.Errorf("failed to serialize new root node: %w", err)
					}
					rt.bpm.UnpinPage(newRootPage.GetPageID(), true) // Mark dirty
					return nil
				}
				// If it's the root and no split, just ensure it's marked dirty if its MBR changed.
				// The MBR update logic is handled by the parent update.
				return nil // Reached the root, done
			}

			parentNode, err := rt.fetchNode(node.parentID)
			if err != nil {
				return fmt.Errorf("failed to fetch parent node %d for node %d: %w", node.parentID, node.pageID, err)
			}
			defer rt.bpm.UnpinPage(parentNode.pageID, false) // Unpin parent, will be re-pinned if modified

			parentNode.mu.Lock() // Lock parent for modification
			found := false
			for i, entry := range parentNode.entries {
				if entry.ChildID == node.pageID {
					// Update MBR of the entry pointing to 'node'
					parentNode.entries[i].Rect = calculateMBR(node.entries)
					found = true
					break
				}
			}
			if !found {
				// This case should ideally not happen if parentID is correct
				parentNode.mu.Unlock()
				return fmt.Errorf("could not find entry for child %d in parent %d", node.pageID, parentNode.pageID)
			}

			// If a split occurred, add the new node as a new entry in the parent
			if node2 != nil && node.pageID == node1.pageID { // Only add node2 once, when processing node1
				newBranchEntry := Entry{
					Rect:     calculateMBR(node2.entries),
					ChildID:  node2.pageID,
					IsBranch: true,
				}
				parentNode.entries = append(parentNode.entries, newBranchEntry)
				// Log the new entry in parent
				logRecord := &wal.LogRecord{
					Type:    wal.LogTypeRTreeUpdate, // Custom log type
					PageID:  parentNode.pageID,
					NewData: []byte(fmt.Sprintf("Add new branch entry for child %d to parent %d", node2.pageID, parentNode.pageID)),
				}
				if _, err := rt.lm.Append(logRecord); err != nil {
					parentNode.mu.Unlock()
					return fmt.Errorf("failed to log R-tree parent update: %w", err)
				}
			}

			// Log the parent node update (MBR change)
			logRecord := &wal.LogRecord{
				Type:    wal.LogTypeRTreeUpdate, // Custom log type
				PageID:  parentNode.pageID,
				NewData: []byte(fmt.Sprintf("Update MBR for child %d in parent %d", node.pageID, parentNode.pageID)),
			}
			if _, err := rt.lm.Append(logRecord); err != nil {
				parentNode.mu.Unlock()
				return fmt.Errorf("failed to log R-tree parent update: %w", err)
			}

			// Check if parent overflowed
			if len(parentNode.entries) > rt.maxEntries {
				log.Printf("DEBUG: Internal node %d overflowed. Splitting...", parentNode.pageID)
				splitParentNodes, err := rt.splitNode(parentNode)
				if err != nil {
					parentNode.mu.Unlock()
					return fmt.Errorf("failed to split parent node: %w", err)
				}
				nextLevelNodes = append(nextLevelNodes, splitParentNodes...)
			} else {
				nextLevelNodes = append(nextLevelNodes, parentNode)
			}
			parentNode.mu.Unlock()

			// Serialize the modified parent node and unpin it, marking dirty
			page, err := rt.bpm.FetchPage(parentNode.pageID)
			if err != nil {
				return fmt.Errorf("failed to fetch page %d for parent serialization: %w", parentNode.pageID, err)
			}
			if err := parentNode.serialize(page); err != nil {
				rt.bpm.UnpinPage(page.GetPageID(), false)
				return fmt.Errorf("failed to serialize parent node %d: %w", parentNode.pageID, err)
			}
			rt.bpm.UnpinPage(page.GetPageID(), true) // Mark dirty
		}
		currentNodes = nextLevelNodes
	}
	return nil
}

// calculateMBR calculates the Minimum Bounding Rectangle for a set of entries.
func calculateMBR(entries []Entry) Rect {
	if len(entries) == 0 {
		return Rect{} // Return an empty rect or error
	}

	mbr := entries[0].Rect
	for i := 1; i < len(entries); i++ {
		mbr = unionRect(mbr, entries[i].Rect)
	}
	return mbr
}

// intersects checks if two rectangles intersect.
func intersects(r1, r2 Rect) bool {
	return r1.MinX <= r2.MaxX && r1.MaxX >= r2.MinX &&
		r1.MinY <= r2.MaxY && r1.MaxY >= r2.MinY
}

// unionRect returns the MBR that encloses both r1 and r2.
func unionRect(r1, r2 Rect) Rect {
	return Rect{
		MinX: math.Min(r1.MinX, r2.MinX),
		MinY: math.Min(r1.MinY, r2.MinY),
		MaxX: math.Max(r1.MaxX, r2.MaxX),
		MaxY: math.Max(r1.MaxY, r2.MaxY),
	}
}

// area calculates the area of a rectangle.
func area(r Rect) float64 {
	return (r.MaxX - r.MinX) * (r.MaxY - r.MinY)
}

// Node serialization and deserialization methods
// These are crucial for persistence. The exact byte layout needs careful design.

// serialize writes the node's data into the provided page's data buffer.
// The format should be consistent for deserialization.
//
// Node Page Layout (Conceptual Example):
// [0-3]: uint32 - IsLeaf (1 if true, 0 if false)
// [4-7]: uint32 - Number of Entries
// [8-11]: uint32 - ParentPageID
// [12-...]: Entry Data (repeated for each entry)
//
// Entry Data Layout (Conceptual Example):
// [0-7]: float64 - Rect.MinX
// [8-15]: float64 - Rect.MinY
// [16-23]: float64 - Rect.MaxX
// [24-31]: float64 - Rect.MaxY
// [32-35]: uint32 - IsBranch (1 if true, 0 if false)
// [36-39]: uint32 - ChildID (if IsBranch is true)
// [40-X]: Length-prefixed string for SpatialData.ID (if IsBranch is false)
//
// This is a simplified example. You might need to handle variable-length data
// (like SpatialData.ID) more robustly, possibly with offsets or fixed-size buffers.
func (node *Node) serialize(page *pagemanager.Page) error {
	node.mu.RLock() // Read lock while serializing
	defer node.mu.RUnlock()

	buf := new(bytes.Buffer)

	// Write IsLeaf
	if node.isLeaf {
		binary.Write(buf, binary.LittleEndian, uint32(1))
	} else {
		binary.Write(buf, binary.LittleEndian, uint32(0))
	}

	// Write Number of Entries
	binary.Write(buf, binary.LittleEndian, uint33(len(node.entries)))

	// Write ParentPageID
	binary.Write(buf, binary.LittleEndian, uint32(node.parentID))

	// Write each entry
	for _, entry := range node.entries {
		binary.Write(buf, binary.LittleEndian, entry.Rect.MinX)
		binary.Write(buf, binary.LittleEndian, entry.Rect.MinY)
		binary.Write(buf, binary.LittleEndian, entry.Rect.MaxX)
		binary.Write(buf, binary.LittleEndian, entry.Rect.MaxY)

		if entry.IsBranch {
			binary.Write(buf, binary.LittleEndian, uint32(1)) // IsBranch = true
			binary.Write(buf, binary.LittleEndian, uint32(entry.ChildID))
		} else {
			binary.Write(buf, binary.LittleEndian, uint32(0)) // IsBranch = false
			// Write SpatialData.ID as length-prefixed string
			idBytes := []byte(entry.Data.ID)
			binary.Write(buf, binary.LittleEndian, uint33(len(idBytes)))
			buf.Write(idBytes)
		}
	}

	// Ensure the buffer content fits within the page size
	if buf.Len() > len(page.GetData()) {
		return fmt.Errorf("serialized node data (size %d) exceeds page size (%d) for page %d", buf.Len(), len(page.GetData()), node.pageID)
	}

	// Copy the buffer content to the page data
	copy(page.GetData(), buf.Bytes())

	// Pad the rest of the page with zeros if necessary (optional, but good practice)
	for i := buf.Len(); i < len(page.GetData()); i++ {
		page.GetData()[i] = 0
	}

	return nil
}

// deserialize reads node data from the provided page's data buffer and populates the node struct.
func (node *Node) deserialize(page *pagemanager.Page) error {
	node.mu.Lock() // Write lock while deserializing
	defer node.mu.Unlock()

	reader := bytes.NewReader(page.GetData())

	var isLeafVal uint32
	if err := binary.Read(reader, binary.LittleEndian, &isLeafVal); err != nil {
		return fmt.Errorf("failed to read IsLeaf: %w", err)
	}
	node.isLeaf = (isLeafVal == 1)

	var numEntries uint33
	if err := binary.Read(reader, binary.LittleEndian, &numEntries); err != nil {
		return fmt.Errorf("failed to read NumEntries: %w", err)
	}

	var parentIDVal uint33
	if err := binary.Read(reader, binary.LittleEndian, &parentIDVal); err != nil {
		return fmt.Errorf("failed to read ParentPageID: %w", err)
	}
	node.parentID = pagemanager.PageID(parentIDVal)

	node.entries = make([]Entry, numEntries)
	for i := 0; i < int(numEntries); i++ {
		var entry Entry
		if err := binary.Read(reader, binary.LittleEndian, &entry.Rect.MinX); err != nil {
			return fmt.Errorf("failed to read entry %d MinX: %w", i, err)
		}
		if err := binary.Read(reader, binary.LittleEndian, &entry.Rect.MinY); err != nil {
			return fmt.Errorf("failed to read entry %d MinY: %w", i, err)
		}
		if err := binary.Read(reader, binary.LittleEndian, &entry.Rect.MaxX); err != nil {
			return fmt.Errorf("failed to read entry %d MaxX: %w", i, err)
		}
		if err := binary.Read(reader, binary.LittleEndian, &entry.Rect.MaxY); err != nil {
			return fmt.Errorf("failed to read entry %d MaxY: %w", i, err)
		}

		var isBranchVal uint32
		if err := binary.Read(reader, binary.LittleEndian, &isBranchVal); err != nil {
			return fmt.Errorf("failed to read entry %d IsBranch: %w", i, err)
		}
		entry.IsBranch = (isBranchVal == 1)

		if entry.IsBranch {
			var childIDVal uint32
			if err := binary.Read(reader, binary.LittleEndian, &childIDVal); err != nil {
				return fmt.Errorf("failed to read entry %d ChildID: %w", i, err)
			}
			entry.ChildID = pagemanager.PageID(childIDVal)
		} else {
			var idLen uint33
			if err := binary.Read(reader, binary.LittleEndian, &idLen); err != nil {
				return fmt.Errorf("failed to read entry %d SpatialData.ID length: %w", i, err)
			}
			idBytes := make([]byte, idLen)
			if _, err := reader.Read(idBytes); err != nil {
				return fmt.Errorf("failed to read entry %d SpatialData.ID: %w", i, err)
			}
			entry.Data.ID = string(idBytes)
		}
		node.entries[i] = entry
	}

	return nil
}

// uint33 is a custom type to represent a 33-bit unsigned integer,
// which is not directly supported by Go's native types.
// This is a conceptual placeholder. In a real implementation, you'd
// use a uint64 and ensure values don't exceed 33 bits, or use a custom
// variable-length encoding if space is critical.
// For `binary.Read` and `binary.Write`, you'd typically use `uint32` or `uint64`.
// I'm using `uint32` for `PageID` and `uint33` for counts to illustrate the
// potential need for larger page IDs or counts than `uint32` allows,
// but for actual binary operations, `uint32` will be used for `PageID` for now.
// If your PageID truly needs to be 33-bit, you'd need custom byte handling.
// For now, let's revert uint33 to uint32 for simplicity and compatibility with binary package.
type uint33 uint32 // Revert to uint32 for binary compatibility. If you need more than 2^32 pages, use uint64.

// Delete (conceptual placeholder)
func (rt *RTree) Delete(rect Rect, data SpatialData) error {
	// Implementation would involve:
	// 1. Find the leaf node containing the entry.
	// 2. Remove the entry.
	// 3. Log the deletion.
	// 4. Condense the tree if underflow occurs (reinserting orphaned entries).
	// 5. Adjust parent MBRs.
	// 6. Serialize and unpin modified pages.
	return fmt.Errorf("Delete not yet implemented for persistent R-tree")
}
