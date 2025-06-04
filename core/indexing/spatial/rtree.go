package spatial

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"

	flushmanager "github.com/sushant-115/gojodb/core/write_engine/flush_manager"
	"github.com/sushant-115/gojodb/core/write_engine/memtable"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
)

const (
	// MaxEntries is the maximum number of entries a node can hold.
	// This is a critical parameter for R-tree performance.
	MaxEntries = 4

	// MinEntries is the minimum number of entries a node must hold (except root).
	// Typically, this is MaxEntries / 2.
	MinEntries = MaxEntries / 2
)

// Rect represents a Minimum Bounding Rectangle (MBR) in 2D space.
type Rect struct {
	MinX, MinY float64
	MaxX, MaxY float64
}

// Intersects checks if two rectangles intersect.
func (r Rect) Intersects(other Rect) bool {
	return r.MinX <= other.MaxX && r.MaxX >= other.MinX &&
		r.MinY <= other.MaxY && r.MaxY >= other.MinY
}

// Contains checks if the rectangle contains another rectangle.
func (r Rect) Contains(other Rect) bool {
	return r.MinX <= other.MinX && r.MaxX >= other.MaxX &&
		r.MinY <= other.MinY && r.MaxY >= other.MaxY
}

// Union returns the MBR that encloses both rectangles.
func (r Rect) Union(other Rect) Rect {
	return Rect{
		MinX: math.Min(r.MinX, other.MinX),
		MinY: math.Min(r.MinY, other.MinY),
		MaxX: math.Max(r.MaxX, other.MaxX),
		MaxY: math.Max(r.MaxY, other.MaxY),
	}
}

// Area calculates the area of the rectangle.
func (r Rect) Area() float64 {
	if r.MinX > r.MaxX || r.MinY > r.MaxY {
		return 0 // Invalid rectangle
	}
	return (r.MaxX - r.MinX) * (r.MaxY - r.MinY)
}

// Enlargement calculates the increase in area if this rect were to be enlarged
// to include another rect.
func (r Rect) Enlargement(other Rect) float64 {
	unionRect := r.Union(other)
	return unionRect.Area() - r.Area()
}

// SpatialData holds the actual data associated with a spatial entry.
// For GojoDB, this typically includes the document ID or key.
type SpatialData struct {
	ID string // The document ID or key associated with this spatial entry
	// Add other relevant data fields here if needed, e.g., type, metadata
}

// Entry represents an entry in an R-tree node.
// It can either point to a child node (branch entry) or contain actual spatial data (leaf entry).
type Entry struct {
	Rect     Rect
	ChildID  pagemanager.PageID // Page ID of the child node if IsBranch is true
	Data     SpatialData        // Actual spatial data if IsBranch is false
	IsBranch bool               // True if this entry points to a child node, false if it's a data entry
}

// Node represents a node in the R-tree.
type Node struct {
	pageID   pagemanager.PageID
	isLeaf   bool
	entries  []Entry
	parentID pagemanager.PageID // Parent's page ID, InvalidPageID for root
	mu       sync.RWMutex       // Mutex for concurrent access to node data
}

// NewNode creates a new R-tree node.
func NewNode(pageID pagemanager.PageID, isLeaf bool, parentID pagemanager.PageID) *Node {
	return &Node{
		pageID:   pageID,
		isLeaf:   isLeaf,
		entries:  make([]Entry, 0, MaxEntries),
		parentID: parentID,
	}
}

// AddEntry adds an entry to the node.
func (n *Node) AddEntry(entry Entry) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.entries = append(n.entries, entry)
}

// GetMBR calculates the Minimum Bounding Rectangle for all entries in the node.
func (n *Node) GetMBR() Rect {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if len(n.entries) == 0 {
		return Rect{} // Return an empty/invalid rect for an empty node
	}
	mbr := n.entries[0].Rect
	for i := 1; i < len(n.entries); i++ {
		mbr = mbr.Union(n.entries[i].Rect)
	}
	return mbr
}

// serialize converts the node into a byte slice for persistence.
// Format:
// - PageID (uint64)
// - IsLeaf (uint8, 1 for true, 0 for false)
// - ParentID (uint64)
// - Number of Entries (uint64)
// - For each entry:
//   - Rect (MinX, MinY, MaxX, MaxY - each float64)
//   - IsBranch (uint8, 1 for true, 0 for false)
//   - If IsBranch: ChildID (uint64)
//   - If not IsBranch: Data.ID length (uint64), Data.ID (bytes)
func (node *Node) serialize(page *pagemanager.Page) error {
	node.mu.RLock()
	defer node.mu.RUnlock()

	buf := new(bytes.Buffer)

	// Write PageID
	if err := binary.Write(buf, binary.LittleEndian, uint64(node.pageID)); err != nil {
		return fmt.Errorf("failed to write node pageID: %w", err)
	}

	// Write IsLeaf
	isLeafByte := uint8(0)
	if node.isLeaf {
		isLeafByte = 1
	}
	if err := binary.Write(buf, binary.LittleEndian, isLeafByte); err != nil {
		return fmt.Errorf("failed to write node isLeaf: %w", err)
	}

	// Write ParentID
	if err := binary.Write(buf, binary.LittleEndian, uint64(node.parentID)); err != nil {
		return fmt.Errorf("failed to write node parentID: %w", err)
	}

	// Write Number of Entries (using uint64 for safety and consistency with PageID)
	if err := binary.Write(buf, binary.LittleEndian, uint64(len(node.entries))); err != nil {
		return fmt.Errorf("failed to write number of entries: %w", err)
	}

	for _, entry := range node.entries {
		// Write Rect
		if err := binary.Write(buf, binary.LittleEndian, entry.Rect.MinX); err != nil {
			return fmt.Errorf("failed to write entry MinX: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, entry.Rect.MinY); err != nil {
			return fmt.Errorf("failed to write entry MinY: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, entry.Rect.MaxX); err != nil {
			return fmt.Errorf("failed to write entry MaxX: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, entry.Rect.MaxY); err != nil {
			return fmt.Errorf("failed to write entry MaxY: %w", err)
		}

		// Write IsBranch
		isBranchByte := uint8(0)
		if entry.IsBranch {
			isBranchByte = 1
		}
		if err := binary.Write(buf, binary.LittleEndian, isBranchByte); err != nil {
			return fmt.Errorf("failed to write entry IsBranch: %w", err)
		}

		if entry.IsBranch {
			// Write ChildID (uint64)
			if err := binary.Write(buf, binary.LittleEndian, uint64(entry.ChildID)); err != nil {
				return fmt.Errorf("failed to write entry ChildID: %w", err)
			}
		} else {
			// Write Data.ID length (uint64) and Data.ID bytes
			idBytes := []byte(entry.Data.ID)
			if err := binary.Write(buf, binary.LittleEndian, uint64(len(idBytes))); err != nil {
				return fmt.Errorf("failed to write entry Data.ID length: %w", err)
			}
			if _, err := buf.Write(idBytes); err != nil {
				return fmt.Errorf("failed to write entry Data.ID: %w", err)
			}
		}
	}

	// Pad with zeros to fill the page size
	pageData := page.GetData()
	copy(pageData, buf.Bytes())
	for i := buf.Len(); i < len(pageData); i++ {
		pageData[i] = 0
	}

	return nil
}

// deserialize loads a node from a byte slice.
func (node *Node) deserialize(page *pagemanager.Page) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	reader := bytes.NewReader(page.GetData())

	// Read PageID
	var pageIDVal uint64
	if err := binary.Read(reader, binary.LittleEndian, &pageIDVal); err != nil {
		return fmt.Errorf("failed to read node pageID: %w", err)
	}
	node.pageID = pagemanager.PageID(pageIDVal)

	// Read IsLeaf
	var isLeafByte uint8
	if err := binary.Read(reader, binary.LittleEndian, &isLeafByte); err != nil {
		return fmt.Errorf("failed to read node isLeaf: %w", err)
	}
	node.isLeaf = (isLeafByte == 1)

	// Read ParentID
	var parentIDVal uint64
	if err := binary.Read(reader, binary.LittleEndian, &parentIDVal); err != nil {
		return fmt.Errorf("failed to read node parentID: %w", err)
	}
	node.parentID = pagemanager.PageID(parentIDVal)

	// Read Number of Entries
	var numEntries uint64
	if err := binary.Read(reader, binary.LittleEndian, &numEntries); err != nil {
		return fmt.Errorf("failed to read number of entries: %w", err)
	}

	node.entries = make([]Entry, numEntries)
	for i := 0; i < int(numEntries); i++ {
		var entry Entry
		// Read Rect
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

		// Read IsBranch
		var isBranchByte uint8
		if err := binary.Read(reader, binary.LittleEndian, &isBranchByte); err != nil {
			return fmt.Errorf("failed to read entry %d IsBranch: %w", i, err)
		}
		entry.IsBranch = (isBranchByte == 1)

		if entry.IsBranch {
			// Read ChildID (uint64)
			var childIDVal uint64
			if err := binary.Read(reader, binary.LittleEndian, &childIDVal); err != nil {
				return fmt.Errorf("failed to read entry %d ChildID: %w", i, err)
			}
			entry.ChildID = pagemanager.PageID(childIDVal)
		} else {
			// Read Data.ID length (uint64) and Data.ID bytes
			var idLen uint64
			if err := binary.Read(reader, binary.LittleEndian, &idLen); err != nil {
				return fmt.Errorf("failed to read entry %d Data.ID length: %w", i, err)
			}
			idBytes := make([]byte, idLen)
			if _, err := reader.Read(idBytes); err != nil {
				return fmt.Errorf("failed to read entry %d Data.ID: %w", i, err)
			}
			entry.Data.ID = string(idBytes)
		}
		node.entries[i] = entry
	}
	return nil
}

// RTree represents the R-tree index structure.
type RTree struct {
	rootPageID pagemanager.PageID
	bpm        *memtable.BufferPoolManager
	dm         *flushmanager.DiskManager // DiskManager for direct page operations (e.g., allocating new pages)
	lm         *wal.LogManager           // LogManager for WAL operations
	mu         sync.RWMutex              // Mutex for concurrent access to R-tree structure (e.g., root changes)
}

// NewRTree creates a new R-tree instance or loads an existing one.
func NewRTree(bpm *memtable.BufferPoolManager, dm *flushmanager.DiskManager, lm *wal.LogManager) (*RTree, error) {
	rt := &RTree{
		bpm: bpm,
		dm:  dm,
		lm:  lm,
	}

	// Try to load the root page ID from the header
	headerPage, err := bpm.FetchPage(SpatialIndexHeaderPageID)
	if err != nil {
		// If header page doesn't exist, it's a new tree
		if err == flushmanager.ErrPageNotFound || strings.Contains(err.Error(), "i/o error: EOF") {
			log.Println("INFO: Spatial Index: Header page not found, initializing new R-tree.")
			rt.rootPageID = pagemanager.InvalidPageID // Mark as empty tree
			return rt, nil
		}
		return nil, fmt.Errorf("failed to fetch spatial index header page: %w", err)
	}
	defer bpm.UnpinPage(headerPage.GetPageID(), false) // Unpin header page

	var header SpatialIndexHeader
	if err := header.deserialize(headerPage); err != nil {
		return nil, fmt.Errorf("failed to deserialize spatial index header: %w", err)
	}

	rt.rootPageID = header.RootPageID
	log.Printf("INFO: Spatial Index: Loaded R-tree with RootPageID: %d, NextPageID: %d", rt.rootPageID, header.NextPageID)

	// Ensure BPM knows about the next available page ID if loaded from header
	// This is crucial for allocating new pages correctly.
	if header.NextPageID > bpm.GetNumPages() {
		bpm.SetNextPageID(header.NextPageID)
	}

	return rt, nil
}

// fetchNode fetches a node from the buffer pool or disk.
func (rt *RTree) fetchNode(pageID pagemanager.PageID) (*Node, error) {
	page, err := rt.bpm.FetchPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch page %d: %w", pageID, err)
	}
	node := &Node{}
	if err := node.deserialize(page); err != nil {
		rt.bpm.UnpinPage(pageID, false) // Unpin on deserialize error
		return nil, fmt.Errorf("failed to deserialize node from page %d: %w", pageID, err)
	}
	return node, nil
}

// allocateNewPage allocates a new page and returns its ID.
func (rt *RTree) allocateNewPage() (pagemanager.PageID, error) {
	page, _, err := rt.bpm.NewPage()
	if page == nil || err != nil {
		return pagemanager.InvalidPageID, fmt.Errorf("failed to allocate new page from buffer pool")
	}
	// The page is already pinned by NewPage. It will be unpinned by the caller.
	return page.GetPageID(), nil
}

// Insert adds a new spatial entry to the R-tree.
func (rt *RTree) Insert(rect Rect, data SpatialData) error {
	rt.mu.Lock() // Lock the tree for write operations
	defer rt.mu.Unlock()

	entry := Entry{Rect: rect, Data: data, IsBranch: false}

	if rt.rootPageID == pagemanager.InvalidPageID {
		// Tree is empty, create a new root leaf node
		newRootPageID, err := rt.allocateNewPage()
		if err != nil {
			return fmt.Errorf("failed to allocate new root page: %w", err)
		}
		rootNode := NewNode(newRootPageID, true, pagemanager.InvalidPageID)
		rootNode.AddEntry(entry)

		// Serialize and unpin the new root
		page, err := rt.bpm.FetchPage(newRootPageID)
		if err != nil {
			return fmt.Errorf("failed to fetch new root page %d: %w", newRootPageID, err)
		}
		if err := rootNode.serialize(page); err != nil {
			rt.bpm.UnpinPage(newRootPageID, false)
			return fmt.Errorf("failed to serialize new root node: %w", err)
		}
		rt.bpm.UnpinPage(newRootPageID, true) // Mark dirty

		rt.rootPageID = newRootPageID
		log.Printf("INFO: Spatial Index: Created new root node at page %d for first insert. %d", newRootPageID, rt.rootPageID)

		// Log the new root creation
		logRecord := &wal.LogRecord{
			Type:    wal.LogTypeRTreeNewRoot,
			PageID:  newRootPageID,
			NewData: page.GetData(), // Store new root ID
			LSN:     rt.lm.GetCurrentLSN(),
		}
		if _, err := rt.lm.Append(logRecord); err != nil {
			return fmt.Errorf("failed to log R-tree new root: %w", err)
		}

		// Update header
		if err := rt.writeHeader(&SpatialIndexHeader{
			RootPageID: rt.rootPageID,
			NextPageID: rt.bpm.GetNumPages(),
		}); err != nil {
			return fmt.Errorf("failed to update spatial index header after new root: %w", err)
		}

		return nil
	}
	// Find the leaf node to insert into
	leafNode, _, err := rt.chooseLeaf(rt.rootPageID, entry)
	if err != nil {
		return fmt.Errorf("failed to choose leaf node: %w", err)
	}
	defer rt.bpm.UnpinPage(leafNode.pageID, false) // Unpin after operations
	//leafNode.mu.Lock() // Lock leaf node for modification
	leafNode.AddEntry(entry)
	// Log the insert operation before serialization
	logRecord := &wal.LogRecord{
		Type:    wal.LogTypeRTreeInsert,
		PageID:  leafNode.pageID,
		NewData: []byte(fmt.Sprintf(`{"Rect":{"MinX":%f,"MinY":%f,"MaxX":%f,"MaxY":%f},"Data":{"ID":"%s"}}`, rect.MinX, rect.MinY, rect.MaxX, rect.MaxY, data.ID)), // Serialize entry for WAL
		LSN:     rt.lm.GetCurrentLSN(),
	}
	if _, err := rt.lm.Append(logRecord); err != nil {
		//leafNode.mu.Unlock()
		return fmt.Errorf("failed to log R-tree insert: %w", err)
	}

	// If leaf node overflows, split it
	if len(leafNode.entries) > MaxEntries {
		log.Printf("INFO: Spatial Index: Leaf node %d overflowed, splitting.", leafNode.pageID)
		newNode, err := rt.splitNode(leafNode)
		if err != nil {
			//leafNode.mu.Unlock()
			return fmt.Errorf("failed to split leaf node: %w", err)
		}

		// Update parent entries for both original and new node
		if err := rt.adjustTree(leafNode, newNode); err != nil {
			//leafNode.mu.Unlock()
			return fmt.Errorf("failed to adjust tree after split: %w", err)
		}
	} else {
		// If no split, just update the MBRs up the tree
		if err := rt.adjustTree(leafNode, nil); err != nil {
			//leafNode.mu.Unlock()
			return fmt.Errorf("failed to adjust tree after insert: %w", err)
		}
	}

	// Serialize and unpin the modified leaf node (and potentially its parent/ancestors)
	page, err := rt.bpm.FetchPage(leafNode.pageID)
	if err != nil {
		//leafNode.mu.Unlock()
		return fmt.Errorf("failed to fetch leaf page %d for serialization: %w", leafNode.pageID, err)
	}
	if err := leafNode.serialize(page); err != nil {
		rt.bpm.UnpinPage(leafNode.pageID, false)
		//leafNode.mu.Unlock()
		return fmt.Errorf("failed to serialize leaf node %d: %w", leafNode.pageID, err)
	}
	rt.bpm.UnpinPage(leafNode.pageID, true) // Mark dirty
	//leafNode.mu.Unlock()
	return nil
}

// chooseLeaf finds the appropriate leaf node to insert a new entry.
// It returns the leaf node and the path of nodes traversed from root to leaf.
func (rt *RTree) chooseLeaf(currentPageID pagemanager.PageID, entry Entry) (*Node, []*Node, error) {
	path := []*Node{}
	currentNode, err := rt.fetchNode(currentPageID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch root node %d: %w", currentPageID, err)
	}
	for !currentNode.isLeaf {
		path = append(path, currentNode)

		// Find entry in current node whose rectangle needs least enlargement
		// to cover the new entry's rectangle. Break ties by choosing the entry
		// with the smallest area.
		var chosenEntry Entry
		minEnlargement := math.MaxFloat64
		minArea := math.MaxFloat64

		currentNode.mu.RLock()
		for _, e := range currentNode.entries {
			enlargement := e.Rect.Enlargement(entry.Rect)
			if enlargement < minEnlargement {
				minEnlargement = enlargement
				minArea = e.Rect.Area()
				chosenEntry = e
			} else if enlargement == minEnlargement {
				currentArea := e.Rect.Area()
				if currentArea < minArea {
					minArea = currentArea
					chosenEntry = e
				}
			}
		}
		currentNode.mu.RUnlock()

		// Unpin the current node as we move down
		rt.bpm.UnpinPage(currentNode.pageID, false)

		if chosenEntry.ChildID == pagemanager.InvalidPageID {
			return nil, nil, fmt.Errorf("chosen entry has invalid ChildID in non-leaf node %d", currentNode.pageID)
		}

		// Move to the child node
		currentNode, err = rt.fetchNode(chosenEntry.ChildID)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to fetch child node %d: %w", chosenEntry.ChildID, err)
		}

	}

	path = append(path, currentNode) // Add the leaf node to the path
	return currentNode, path, nil
}

// splitNode splits an overflowing node into two new nodes.
// This is a basic linear split implementation for demonstration.
// For production, consider Quadratic or R*-tree split algorithms.
func (rt *RTree) splitNode(oldNode *Node) (*Node, error) {
	// Create two new nodes
	newNodePageID, err := rt.allocateNewPage()
	if err != nil {
		return nil, fmt.Errorf("failed to allocate new page for split node: %w", err)
	}
	newNode := NewNode(newNodePageID, oldNode.isLeaf, oldNode.parentID)

	// Simple split: distribute entries roughly evenly
	// In a real R-tree, you'd pick two "seed" entries and then
	// iteratively assign remaining entries to the node whose MBR
	// requires less enlargement.
	oldNode.mu.Lock()
	defer oldNode.mu.Unlock()

	// For simplicity, just split entries into two halves
	mid := len(oldNode.entries) / 2
	newNode.entries = append(newNode.entries, oldNode.entries[mid:]...)
	oldNode.entries = oldNode.entries[:mid]

	// Update parent IDs for entries in the new node if they are branch entries
	if !oldNode.isLeaf {
		for i := range newNode.entries {
			if newNode.entries[i].IsBranch {
				childNode, err := rt.fetchNode(newNode.entries[i].ChildID)
				if err != nil {
					log.Printf("WARNING: Failed to fetch child node %d during split parentID update: %v", newNode.entries[i].ChildID, err)
					continue
				}
				childNode.mu.Lock()
				childNode.parentID = newNode.pageID // Update child's parent ID
				childNode.mu.Unlock()
				// Serialize and unpin the child node after updating its parentID
				childPage, err := rt.bpm.FetchPage(childNode.pageID)
				if err != nil {
					log.Printf("WARNING: Failed to fetch child page %d for serialization during split: %v", childNode.pageID, err)
					rt.bpm.UnpinPage(childNode.pageID, false)
					continue
				}
				if err := childNode.serialize(childPage); err != nil {
					log.Printf("WARNING: Failed to serialize child node %d after parentID update: %v", childNode.pageID, err)
					rt.bpm.UnpinPage(childNode.pageID, false)
					continue
				}
				rt.bpm.UnpinPage(childNode.pageID, true) // Mark dirty
			}
		}
	}

	// Serialize and unpin the new node
	newPage, err := rt.bpm.FetchPage(newNodePageID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch new page %d for serialization: %w", newNodePageID, err)
	}
	if err := newNode.serialize(newPage); err != nil {
		rt.bpm.UnpinPage(newNodePageID, false)
		return nil, fmt.Errorf("failed to serialize new node %d: %w", newNodePageID, err)
	}
	rt.bpm.UnpinPage(newNodePageID, true) // Mark dirty

	// Log the split operation
	logRecord := &wal.LogRecord{
		Type:   wal.LogTypeRTreeSplit,
		PageID: oldNode.pageID, // Original node page ID
		NewData: []byte(fmt.Sprintf(`{"OldNodePageID":%d,"NewNodePageID":%d,"OldNodeEntries":%d,"NewNodeEntries":%d}`,
			oldNode.pageID, newNode.pageID, len(oldNode.entries), len(newNode.entries))),
		LSN: rt.lm.GetCurrentLSN(),
	}
	if _, err := rt.lm.Append(logRecord); err != nil {
		return nil, fmt.Errorf("failed to log R-tree split: %w", err)
	}

	return newNode, nil
}

// adjustTree adjusts the MBRs of nodes up the tree after an insertion or split.
func (rt *RTree) adjustTree(node *Node, newNode *Node) error {
	var nodesToProcess []*Node
	nodesToProcess = append(nodesToProcess, node)
	if newNode != nil {
		nodesToProcess = append(nodesToProcess, newNode)
	}

	for len(nodesToProcess) > 0 {
		currentNode := nodesToProcess[0]
		nodesToProcess = nodesToProcess[1:]

		if currentNode.pageID == rt.rootPageID {
			// If root overflows, create a new root
			if len(currentNode.entries) > MaxEntries {
				log.Printf("INFO: Spatial Index: Root node %d overflowed, creating new root.", currentNode.pageID)
				newRootPageID, err := rt.allocateNewPage()
				if err != nil {
					return fmt.Errorf("failed to allocate new page for new root: %w", err)
				}
				newRoot := NewNode(newRootPageID, false, pagemanager.InvalidPageID)

				// Add entries for the old root and the new split node (if any)
				oldRootEntry := Entry{
					Rect:     currentNode.GetMBR(),
					ChildID:  currentNode.pageID,
					IsBranch: true,
				}
				newRoot.AddEntry(oldRootEntry)
				currentNode.parentID = newRootPageID // Update old root's parent

				if newNode != nil {
					newSplitEntry := Entry{
						Rect:     newNode.GetMBR(),
						ChildID:  newNode.pageID,
						IsBranch: true,
					}
					newRoot.AddEntry(newSplitEntry)
					newNode.parentID = newRootPageID // Update new node's parent
				}

				// Serialize and unpin the new root
				newRootPage, err := rt.bpm.FetchPage(newRootPageID)
				if err != nil {
					return fmt.Errorf("failed to fetch new root page %d: %w", newRootPageID, err)
				}
				if err := newRoot.serialize(newRootPage); err != nil {
					rt.bpm.UnpinPage(newRootPageID, false)
					return fmt.Errorf("failed to serialize new root node: %w", err)
				}
				rt.bpm.UnpinPage(newRootPageID, true) // Mark dirty

				// Serialize and unpin the old root (which is now a child)
				oldRootPage, err := rt.bpm.FetchPage(currentNode.pageID)
				if err != nil {
					return fmt.Errorf("failed to fetch old root page %d for serialization: %w", currentNode.pageID, err)
				}
				if err := currentNode.serialize(oldRootPage); err != nil {
					rt.bpm.UnpinPage(currentNode.pageID, false)
					return fmt.Errorf("failed to serialize old root node %d: %w", currentNode.pageID, err)
				}
				rt.bpm.UnpinPage(currentNode.pageID, true) // Mark dirty

				if newNode != nil {
					// Serialize and unpin the new split node (which is now a child)
					newSplitPage, err := rt.bpm.FetchPage(newNode.pageID)
					if err != nil {
						return fmt.Errorf("failed to fetch new split page %d for serialization: %w", newNode.pageID, err)
					}
					if err := newNode.serialize(newSplitPage); err != nil {
						rt.bpm.UnpinPage(newNode.pageID, false)
						return fmt.Errorf("failed to serialize new split node %d: %w", newNode.pageID, err)
					}
					rt.bpm.UnpinPage(newNode.pageID, true) // Mark dirty
				}

				rt.rootPageID = newRootPageID // Update the tree's root
				log.Printf("INFO: Spatial Index: New root created at page %d.", newRootPageID)

				// Log the new root creation
				logRecord := &wal.LogRecord{
					Type:    wal.LogTypeRTreeNewRoot,
					PageID:  newRootPageID,
					NewData: binary.LittleEndian.AppendUint64(nil, uint64(newRootPageID)), // Store new root ID
					LSN:     rt.lm.GetCurrentLSN(),
				}
				if _, err := rt.lm.Append(logRecord); err != nil {
					return fmt.Errorf("failed to log R-tree new root after root split: %w", err)
				}

				// Update header
				if err := rt.writeHeader(&SpatialIndexHeader{
					RootPageID: rt.rootPageID,
					NextPageID: rt.bpm.GetNumPages(),
				}); err != nil {
					return fmt.Errorf("failed to update spatial index header after root split: %w", err)
				}

				return nil // Done with adjustment if root changed
			}
			return nil // Reached root and no overflow, adjustment complete
		}

		// Adjust parent's MBR and potentially split parent
		if currentNode.parentID == pagemanager.InvalidPageID {
			return fmt.Errorf("node %d has no parent but is not root", currentNode.pageID)
		}

		parentNode, err := rt.fetchNode(currentNode.parentID)
		if err != nil {
			return fmt.Errorf("failed to fetch parent node %d for node %d: %w", currentNode.parentID, currentNode.pageID, err)
		}
		defer rt.bpm.UnpinPage(parentNode.pageID, false) // Unpin parent after operations

		parentNode.mu.Lock()
		// Find the entry in the parent node that points to currentNode
		foundEntry := false
		for i, entry := range parentNode.entries {
			if entry.IsBranch && entry.ChildID == currentNode.pageID {
				// Update the MBR of this entry to reflect the current node's MBR
				parentNode.entries[i].Rect = currentNode.GetMBR()
				foundEntry = true
				break
			}
		}

		if !foundEntry {
			parentNode.mu.Unlock()
			return fmt.Errorf("could not find entry for child %d in parent %d", currentNode.pageID, parentNode.pageID)
		}

		// If a new node was created during split, add an entry for it in the parent
		if newNode != nil {
			newEntry := Entry{
				Rect:     newNode.GetMBR(),
				ChildID:  newNode.pageID,
				IsBranch: true,
			}
			parentNode.AddEntry(newEntry)
			log.Printf("INFO: Spatial Index: Added new split node %d as entry to parent %d.", newNode.pageID, parentNode.pageID)
		}

		// Serialize and unpin the modified parent node
		parentPage, err := rt.bpm.FetchPage(parentNode.pageID)
		if err != nil {
			parentNode.mu.Unlock()
			return fmt.Errorf("failed to fetch parent page %d for serialization: %w", parentNode.pageID, err)
		}
		if err := parentNode.serialize(parentPage); err != nil {
			rt.bpm.UnpinPage(parentNode.pageID, false)
			parentNode.mu.Unlock()
			return fmt.Errorf("failed to serialize parent node %d: %w", parentNode.pageID, err)
		}
		rt.bpm.UnpinPage(parentNode.pageID, true) // Mark dirty

		// Log the node update
		logRecord := &wal.LogRecord{
			Type:    wal.LogTypeRTreeUpdate, // Or a more specific log type for MBR updates
			PageID:  parentNode.pageID,
			NewData: parentPage.GetData(), // Store the full page data for replay
			LSN:     rt.lm.GetCurrentLSN(),
		}
		if _, err := rt.lm.Append(logRecord); err != nil {
			parentNode.mu.Unlock()
			return fmt.Errorf("failed to log R-tree parent update: %w", err)
		}

		// If parent node overflows, split it and continue adjustment upwards
		if len(parentNode.entries) > MaxEntries {
			log.Printf("INFO: Spatial Index: Parent node %d overflowed, splitting.", parentNode.pageID)
			splitParentNode, err := rt.splitNode(parentNode)
			if err != nil {
				parentNode.mu.Unlock()
				return fmt.Errorf("failed to split parent node: %w", err)
			}
			nodesToProcess = append(nodesToProcess, parentNode, splitParentNode) // Continue with both parent and new split parent
		}
		parentNode.mu.Unlock()
	}
	return nil
}

// Search performs a spatial query for entries intersecting the given rectangle.
func (rt *RTree) Search(queryRect Rect) ([]SpatialData, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	if rt.rootPageID == pagemanager.InvalidPageID {
		return []SpatialData{}, nil // Empty tree, no results
	}

	results := []SpatialData{}
	nodesToVisit := []pagemanager.PageID{rt.rootPageID}

	for len(nodesToVisit) > 0 {
		currentPageID := nodesToVisit[0]
		nodesToVisit = nodesToVisit[1:]

		node, err := rt.fetchNode(currentPageID)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch node %d during search: %w", currentPageID, err)
		}
		defer rt.bpm.UnpinPage(node.pageID, false) // Unpin after reading

		node.mu.RLock()
		for _, entry := range node.entries {
			if entry.Rect.Intersects(queryRect) {
				if node.isLeaf {
					// Found a data entry in a leaf node that intersects
					results = append(results, entry.Data)
				} else {
					// It's a branch entry, add child node to visit list
					nodesToVisit = append(nodesToVisit, entry.ChildID)
				}
			}
		}
		node.mu.RUnlock()
	}
	return results, nil
}

// Delete removes a spatial entry from the R-tree.
// This is a simplified implementation that only removes the entry if found in a leaf.
// It does NOT handle node underflow (merging or re-inserting orphaned entries)
// or tree restructuring for efficiency after deletion.
func (rt *RTree) Delete(rect Rect, data SpatialData) error {
	rt.mu.Lock() // Lock the tree for write operations
	defer rt.mu.Unlock()

	if rt.rootPageID == pagemanager.InvalidPageID {
		return fmt.Errorf("cannot delete from an empty R-tree")
	}

	// Simple DFS to find and delete the entry
	found, err := rt.deleteRecursive(rt.rootPageID, rect, data)
	if err != nil {
		return fmt.Errorf("failed to delete entry: %w", err)
	}
	if !found {
		return fmt.Errorf("entry not found for deletion: Rect=%v, Data.ID=%s", rect, data.ID)
	}

	// After deletion, if the root becomes empty and is not a leaf, adjust the root.
	rootNode, err := rt.fetchNode(rt.rootPageID)
	if err != nil {
		return fmt.Errorf("failed to fetch root node after delete: %w", err)
	}
	defer rt.bpm.UnpinPage(rootNode.pageID, false)

	rootNode.mu.RLock()
	isRootEmpty := len(rootNode.entries) == 0
	isRootLeaf := rootNode.isLeaf
	rootNode.mu.RUnlock()

	if isRootEmpty && !isRootLeaf {
		// If root is empty and not a leaf, it means the tree became empty.
		// Reset rootPageID to InvalidPageID.
		rt.rootPageID = pagemanager.InvalidPageID
		log.Printf("INFO: Spatial Index: R-tree became empty, resetting rootPageID.")

		// Log the root change
		logRecord := &wal.LogRecord{
			Type:    wal.LogTypeRTreeNewRoot, // Using NewRoot to signify a change to InvalidPageID
			PageID:  pagemanager.InvalidPageID,
			NewData: binary.LittleEndian.AppendUint64(nil, uint64(pagemanager.InvalidPageID)),
			LSN:     rt.lm.GetCurrentLSN(),
		}
		if _, err := rt.lm.Append(logRecord); err != nil {
			return fmt.Errorf("failed to log R-tree empty root: %w", err)
		}

		// Update header
		if err := rt.writeHeader(&SpatialIndexHeader{
			RootPageID: rt.rootPageID,
			NextPageID: rt.bpm.GetNumPages(),
		}); err != nil {
			return fmt.Errorf("failed to update spatial index header after empty root: %w", err)
		}
	}

	return nil
}

// deleteRecursive finds and deletes an entry. Returns true if deleted, false otherwise.
// This is a simplified traversal for deletion.
func (rt *RTree) deleteRecursive(currentPageID pagemanager.PageID, targetRect Rect, targetData SpatialData) (bool, error) {
	node, err := rt.fetchNode(currentPageID)
	if err != nil {
		return false, fmt.Errorf("failed to fetch node %d for deletion: %w", currentPageID, err)
	}
	defer rt.bpm.UnpinPage(node.pageID, false) // Unpin if no modification

	node.mu.Lock() // Lock node for modification
	defer node.mu.Unlock()

	deletedLocally := false
	newEntries := []Entry{}
	for _, entry := range node.entries {
		if !entry.IsBranch && entry.Rect == targetRect && entry.Data.ID == targetData.ID {
			// Found the exact data entry in a leaf node
			if node.isLeaf {
				deletedLocally = true
				log.Printf("DEBUG: Deleted entry Rect=%v, Data.ID=%s from leaf node %d", targetRect, targetData.ID, node.pageID)
				// Log the deletion
				logRecord := &wal.LogRecord{
					Type:    wal.LogTypeRTreeDelete, // Use specific R-tree delete log type
					PageID:  node.pageID,
					NewData: []byte(fmt.Sprintf(`{"Rect":{"MinX":%f,"MinY":%f,"MaxX":%f,"MaxY":%f},"Data":{"ID":"%s"}}`, targetRect.MinX, targetRect.MinY, targetRect.MaxX, targetRect.MaxY, targetData.ID)), // Serialize entry for WAL
					LSN:     rt.lm.GetCurrentLSN(),
				}
				if _, err := rt.lm.Append(logRecord); err != nil {
					return false, fmt.Errorf("failed to log R-tree delete: %w", err)
				}
			} else {
				// This should not happen: data entry found in a branch node.
				// This indicates a tree inconsistency.
				log.Printf("WARNING: Data entry found in non-leaf node %d during delete. Skipping.", node.pageID)
				newEntries = append(newEntries, entry)
			}
		} else {
			// Keep entries that are not being deleted
			newEntries = append(newEntries, entry)
		}
	}

	if deletedLocally {
		node.entries = newEntries
		// Serialize and unpin the modified leaf node, marking dirty
		page, err := rt.bpm.FetchPage(node.pageID)
		if err != nil {
			return false, fmt.Errorf("failed to fetch page %d for serialization after delete: %w", node.pageID, err)
		}
		if err := node.serialize(page); err != nil {
			rt.bpm.UnpinPage(page.GetPageID(), false)
			return false, fmt.Errorf("failed to serialize leaf node %d after delete: %w", node.pageID, err)
		}
		rt.bpm.UnpinPage(page.GetPageID(), true) // Mark dirty
		// After a deletion in a leaf, its MBR might shrink. Propagate this up.
		if err := rt.adjustTree(node, nil); err != nil {
			log.Printf("WARNING: Failed to adjust tree after leaf deletion in node %d: %v", node.pageID, err)
		}
		return true, nil
	}

	// If not a leaf node and not deleted locally, recurse into children
	if !node.isLeaf {
		for i, entry := range node.entries {
			// Only recurse if child MBR intersects targetRect, or if it contains it for exact match
			if entry.IsBranch && entry.Rect.Intersects(targetRect) {
				childDeleted, err := rt.deleteRecursive(entry.ChildID, targetRect, targetData)
				if err != nil {
					return false, err
				}
				if childDeleted {
					// After child deletion, its MBR might have shrunk.
					// Update the parent's entry for this child.
					childNode, err := rt.fetchNode(entry.ChildID)
					if err != nil {
						return false, fmt.Errorf("failed to fetch child node %d after its deletion: %w", entry.ChildID, err)
					}
					defer rt.bpm.UnpinPage(childNode.pageID, false)
					node.entries[i].Rect = childNode.GetMBR() // Update MBR in parent

					// If the child node is now empty and it's not the root,
					// it should ideally be removed from its parent and potentially deallocated.
					// This is part of the full R-tree deletion algorithm (node underflow handling).
					// For this simplified version, we just update the MBR.
					// If the child node became empty, we might need to remove its entry from the parent.
					childNode.mu.RLock()
					childIsEmpty := len(childNode.entries) == 0
					childNode.mu.RUnlock()

					if childIsEmpty {
						log.Printf("INFO: Child node %d became empty after deletion. Removing its entry from parent %d.", childNode.pageID, node.pageID)
						// Remove the entry pointing to the empty child node
						node.entries = append(node.entries[:i], node.entries[i+1:]...)
						// Mark the child page as free in BPM (if it's truly empty and can be reused)
						rt.bpm.FlushPage(childNode.pageID)
						// Log the page free operation if WAL supports it, or rely on checkpoint.
					}

					// Re-serialize and unpin the modified parent node.
					page, err := rt.bpm.FetchPage(node.pageID)
					if err != nil {
						return false, fmt.Errorf("failed to fetch page %d for parent serialization after child delete: %w", node.pageID, err)
					}
					if err := node.serialize(page); err != nil {
						rt.bpm.UnpinPage(node.pageID, false)
						return false, fmt.Errorf("failed to serialize parent node %d after child delete: %w", node.pageID, err)
					}
					rt.bpm.UnpinPage(node.pageID, true) // Mark dirty

					// Propagate MBR changes up the tree from this modified node
					if err := rt.adjustTree(node, nil); err != nil {
						log.Printf("WARNING: Failed to adjust tree after child deletion in node %d: %v", node.pageID, err)
					}
					return true, nil
				}
			}
		}
	}

	return false, nil // Entry not found or not deleted
}

// writeHeader writes the R-tree header to disk.
func (rt *RTree) writeHeader(header *SpatialIndexHeader) error {
	headerPage, err := rt.bpm.FetchPage(SpatialIndexHeaderPageID)
	if err != nil {
		// If header page doesn't exist, allocate it
		if err == flushmanager.ErrPageNotFound {
			headerPage, _, err = rt.bpm.NewPage()
			if headerPage == nil || err != nil {
				return fmt.Errorf("failed to allocate new header page")
			}

			if headerPage.GetPageID() != SpatialIndexHeaderPageID {
				return fmt.Errorf("allocated header page has wrong ID: got %d, want %d", headerPage.GetPageID(), SpatialIndexHeaderPageID)
			}
		} else {
			return fmt.Errorf("failed to fetch spatial index header page for writing: %w", err)
		}

	}
	defer rt.bpm.UnpinPage(headerPage.GetPageID(), true) // Mark dirty
	if err := header.serialize(headerPage); err != nil {
		return fmt.Errorf("failed to serialize spatial index header: %w", err)
	}
	log.Printf("INFO: Spatial Index: Wrote header: RootPageID=%d, NextPageID=%d", header.RootPageID, header.NextPageID)
	return nil
}

// Close flushes all dirty pages of the R-tree to disk and persists the header.
func (rt *RTree) Close() error {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	log.Println("INFO: Closing Spatial Index...")

	// Write the final header state
	if err := rt.writeHeader(&SpatialIndexHeader{
		RootPageID: rt.rootPageID,
		NextPageID: rt.bpm.GetNumPages(), // Ensure NextPageID is updated to the highest allocated page
	}); err != nil {
		return fmt.Errorf("failed to write spatial index header on close: %w", err)
	}

	// Flush all dirty pages from the buffer pool
	// The BPM's FlushAllPages will handle writing all dirty pages to disk.
	// No explicit R-tree specific flush needed here, as BPM manages it.
	log.Println("INFO: Spatial Index: All dirty pages will be flushed by BufferPoolManager on shutdown.")
	return nil
}

// Helper function to check if two rectangles intersect (already defined, but good to keep local)
func intersects(r1, r2 Rect) bool {
	return r1.MinX <= r2.MaxX && r1.MaxX >= r2.MinX &&
		r1.MinY <= r2.MaxY && r1.MaxY >= r2.MinY
}
