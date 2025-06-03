package spatial

import (
	"fmt"
	"math"
	"sync"
)

const (
	// M is the maximum number of entries in a node.
	// This is a critical parameter affecting tree performance.
	M = 8
	// m is the minimum number of entries in a node.
	// m must be <= M/2.
	m = M / 2
)

// Rect represents a 2D bounding box.
type Rect struct {
	MinX, MinY float64
	MaxX, MaxY float64
}

// NewRect creates a new Rect.
func NewRect(minX, minY, maxX, maxY float64) Rect {
	return Rect{MinX: minX, MinY: minY, MaxX: maxX, MaxY: maxY}
}

// Area calculates the area of the rectangle.
func (r Rect) Area() float64 {
	return (r.MaxX - r.MinX) * (r.MaxY - r.MinY)
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

// Union returns the smallest rectangle that encloses both rectangles.
func (r Rect) Union(other Rect) Rect {
	return Rect{
		MinX: math.Min(r.MinX, other.MinX),
		MinY: math.Min(r.MinY, other.MinY),
		MaxX: math.Max(r.MaxX, other.MaxX),
		MaxY: math.Max(r.MaxY, other.MaxY),
	}
}

// Enlargement calculates the increase in area if `other` is added to `r`.
func (r Rect) Enlargement(other Rect) float64 {
	unionRect := r.Union(other)
	return unionRect.Area() - r.Area()
}

// SpatialData represents an entry in the R-tree, containing a rectangle and a data identifier.
type SpatialData struct {
	ID   string // Unique identifier for the data
	Rect Rect   // Bounding box of the data
}

// Entry represents an entry in an R-tree node. It can be a pointer to a child node
// or a SpatialData object.
type Entry struct {
	Rect  Rect
	Data  *SpatialData // For leaf nodes, points to actual data
	Child *Node        // For internal nodes, points to a child node
}

// Node represents a node in the R-tree.
type Node struct {
	IsLeaf  bool
	Entries []*Entry
	Parent  *Node // Pointer to parent node for tree traversal upwards
}

// RTree represents the R-tree structure.
type RTree struct {
	Root *Node
	mu   sync.RWMutex // Mutex for concurrent access
}

// NewRTree creates and initializes a new R-tree.
func NewRTree() *RTree {
	root := &Node{
		IsLeaf:  true,
		Entries: make([]*Entry, 0, M),
	}
	return &RTree{
		Root: root,
	}
}

// Insert adds a new spatial data entry into the R-tree.
func (rt *RTree) Insert(data SpatialData) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// Create a new entry for the data
	newEntry := &Entry{Rect: data.Rect, Data: &data}

	// Find the best leaf node to insert the new entry
	leaf := rt.chooseSubtree(rt.Root, newEntry)

	// Add the entry to the chosen leaf
	leaf.Entries = append(leaf.Entries, newEntry)

	// If the leaf overflows, split it
	var splitNode *Node
	if len(leaf.Entries) > M {
		splitNode = rt.splitNode(leaf)
	}

	// Adjust the tree from the leaf up to the root
	rt.adjustTree(leaf, splitNode)
	return nil
}

// Search finds all spatial data entries that intersect with the given query rectangle.
func (rt *RTree) Search(queryRect Rect) []SpatialData {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	results := make([]SpatialData, 0)
	rt.searchNode(rt.Root, queryRect, &results)
	return results
}

// searchNode recursively searches for intersecting entries within a node.
func (rt *RTree) searchNode(node *Node, queryRect Rect, results *[]SpatialData) {
	for _, entry := range node.Entries {
		if entry.Rect.Intersects(queryRect) {
			if node.IsLeaf {
				// If it's a leaf node and intersects, add the data to results
				*results = append(*results, *entry.Data)
			} else {
				// If it's an internal node, recursively search its child
				rt.searchNode(entry.Child, queryRect, results)
			}
		}
	}
}

// Delete removes a spatial data entry from the R-tree.
func (rt *RTree) Delete(dataID string, dataRect Rect) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// Find the leaf node containing the entry
	leaf, entryIndex := rt.findLeafAndEntry(rt.Root, dataID, dataRect)
	if leaf == nil || entryIndex == -1 {
		return fmt.Errorf("entry with ID '%s' and rect %v not found", dataID, dataRect)
	}

	// Remove the entry from the leaf node
	leaf.Entries = append(leaf.Entries[:entryIndex], leaf.Entries[entryIndex+1:]...)

	// Condense the tree from the leaf up to the root
	rt.condenseTree(leaf)

	// If the root has only one child (and is not a leaf), make the child the new root
	if rt.Root != nil && !rt.Root.IsLeaf && len(rt.Root.Entries) == 1 {
		rt.Root = rt.Root.Entries[0].Child
		rt.Root.Parent = nil // New root has no parent
	} else if rt.Root != nil && rt.Root.IsLeaf && len(rt.Root.Entries) == 0 {
		// If root becomes empty leaf, reset to empty tree
		rt.Root = &Node{IsLeaf: true, Entries: make([]*Entry, 0, M)}
	}

	return nil
}

// findLeafAndEntry recursively finds the leaf node and index of a specific entry.
func (rt *RTree) findLeafAndEntry(node *Node, dataID string, dataRect Rect) (*Node, int) {
	for i, entry := range node.Entries {
		if node.IsLeaf {
			// Check if the data matches in a leaf node
			if entry.Data != nil && entry.Data.ID == dataID && entry.Data.Rect == dataRect {
				return node, i
			}
		} else {
			// If it's an internal node and the child's MBR contains the data's MBR, recurse
			if entry.Rect.Contains(dataRect) {
				if leaf, index := rt.findLeafAndEntry(entry.Child, dataID, dataRect); leaf != nil {
					return leaf, index
				}
			}
		}
	}
	return nil, -1 // Not found
}

// chooseSubtree finds the best leaf node to insert a new entry.
// It minimizes the enlargement of the MBR of the chosen node.
func (rt *RTree) chooseSubtree(node *Node, entry *Entry) *Node {
	if node.IsLeaf {
		return node
	}

	// Find the entry whose MBR requires the minimum enlargement
	minEnlargement := math.MaxFloat64
	minArea := math.MaxFloat64
	var chosenEntry *Entry

	for _, e := range node.Entries {
		enlargement := e.Rect.Enlargement(entry.Rect)
		if enlargement < minEnlargement {
			minEnlargement = enlargement
			minArea = e.Rect.Area() // Tie-break by minimum area
			chosenEntry = e
		} else if enlargement == minEnlargement {
			// Tie-break by choosing the entry with the smallest area
			currentArea := e.Rect.Area()
			if currentArea < minArea {
				minArea = currentArea
				chosenEntry = e
			}
		}
	}
	return rt.chooseSubtree(chosenEntry.Child, entry)
}

// splitNode splits an overflowing node into two new nodes.
// Implements the Quadratic Split algorithm.
func (rt *RTree) splitNode(node *Node) *Node {
	// Pick two entries to be the seeds for the new nodes
	seed1, seed2, remainingEntries := rt.pickSeeds(node.Entries)

	newNode1 := &Node{IsLeaf: node.IsLeaf, Entries: make([]*Entry, 0, M)}
	newNode2 := &Node{IsLeaf: node.IsLeaf, Entries: make([]*Entry, 0, M)}

	newNode1.Entries = append(newNode1.Entries, seed1)
	newNode2.Entries = append(newNode2.Entries, seed2)

	// Set parent for child nodes if not leaf
	if !node.IsLeaf {
		seed1.Child.Parent = newNode1
		seed2.Child.Parent = newNode2
	}

	mbr1 := seed1.Rect
	mbr2 := seed2.Rect

	// Distribute the remaining entries
	for len(remainingEntries) > 0 {
		// If one node needs more entries to meet 'm' minimum, add all remaining to it
		if len(newNode1.Entries)+len(remainingEntries) <= m {
			for _, entry := range remainingEntries {
				newNode1.Entries = append(newNode1.Entries, entry)
				if !node.IsLeaf {
					entry.Child.Parent = newNode1
				}
			}
			break
		}
		if len(newNode2.Entries)+len(remainingEntries) <= m {
			for _, entry := range remainingEntries {
				newNode2.Entries = append(newNode2.Entries, entry)
				if !node.IsLeaf {
					entry.Child.Parent = newNode2
				}
			}
			break
		}

		// Find the entry that, if added to either group, would cause the least area enlargement
		var bestEntry *Entry
		maxDiff := -1.0 // Max difference in enlargement
		bestIndex := -1

		for i, entry := range remainingEntries {
			enlargement1 := mbr1.Enlargement(entry.Rect)
			enlargement2 := mbr2.Enlargement(entry.Rect)
			diff := math.Abs(enlargement1 - enlargement2)

			if diff > maxDiff {
				maxDiff = diff
				bestEntry = entry
				bestIndex = i
			} else if diff == maxDiff {
				// Tie-break: choose the one that requires the least absolute enlargement
				if math.Min(enlargement1, enlargement2) < math.Min(mbr1.Enlargement(bestEntry.Rect), mbr2.Enlargement(bestEntry.Rect)) {
					bestEntry = entry
					bestIndex = i
				}
			}
		}

		// Remove the best entry from remainingEntries
		remainingEntries = append(remainingEntries[:bestIndex], remainingEntries[bestIndex+1:]...)

		// Assign the best entry to the node whose MBR requires the least enlargement
		enlargement1 := mbr1.Enlargement(bestEntry.Rect)
		enlargement2 := mbr2.Enlargement(bestEntry.Rect)

		if enlargement1 < enlargement2 {
			newNode1.Entries = append(newNode1.Entries, bestEntry)
			mbr1 = mbr1.Union(bestEntry.Rect)
			if !node.IsLeaf {
				bestEntry.Child.Parent = newNode1
			}
		} else if enlargement2 < enlargement1 {
			newNode2.Entries = append(newNode2.Entries, bestEntry)
			mbr2 = mbr2.Union(bestEntry.Rect)
			if !node.IsLeaf {
				bestEntry.Child.Parent = newNode2
			}
		} else {
			// Tie-break: choose the node with the smaller area
			if mbr1.Area() < mbr2.Area() {
				newNode1.Entries = append(newNode1.Entries, bestEntry)
				mbr1 = mbr1.Union(bestEntry.Rect)
				if !node.IsLeaf {
					bestEntry.Child.Parent = newNode1
				}
			} else if mbr2.Area() < mbr1.Area() {
				newNode2.Entries = append(newNode2.Entries, bestEntry)
				mbr2 = mbr2.Union(bestEntry.Rect)
				if !node.IsLeaf {
					bestEntry.Child.Parent = newNode2
				}
			} else {
				// Tie-break: choose the node with fewer entries
				if len(newNode1.Entries) < len(newNode2.Entries) {
					newNode1.Entries = append(newNode1.Entries, bestEntry)
					mbr1 = mbr1.Union(bestEntry.Rect)
					if !node.IsLeaf {
						bestEntry.Child.Parent = newNode1
					}
				} else {
					newNode2.Entries = append(newNode2.Entries, bestEntry)
					mbr2 = mbr2.Union(bestEntry.Rect)
					if !node.IsLeaf {
						bestEntry.Child.Parent = newNode2
					}
				}
			}
		}
	}

	// Replace the original node's entries with newNode1's entries
	node.Entries = newNode1.Entries
	// Update parent pointers for children of newNode1
	if !node.IsLeaf {
		for _, entry := range node.Entries {
			entry.Child.Parent = node
		}
	}

	// Return newNode2, which will be inserted into the parent
	return newNode2
}

// pickSeeds selects two entries to initiate the split process (Quadratic PickSeeds).
func (rt *RTree) pickSeeds(entries []*Entry) (*Entry, *Entry, []*Entry) {
	maxNormalizedSeparation := -1.0
	var seed1, seed2 *Entry
	var seed1Index, seed2Index int

	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			e1 := entries[i]
			e2 := entries[j]

			// Calculate the MBR of the two entries combined
			unionRect := e1.Rect.Union(e2.Rect)
			// Calculate the dead space (area of union - area of e1 - area of e2)
			deadSpace := unionRect.Area() - e1.Rect.Area() - e2.Rect.Area()

			// Normalize by the area of the union rectangle to avoid bias towards larger rectangles
			normalizedSeparation := deadSpace / unionRect.Area()
			if unionRect.Area() == 0 { // Handle case where union area is zero (e.g., points)
				normalizedSeparation = 0
			}

			if normalizedSeparation > maxNormalizedSeparation {
				maxNormalizedSeparation = normalizedSeparation
				seed1 = e1
				seed2 = e2
				seed1Index = i
				seed2Index = j
			}
		}
	}

	// Create a new slice for remaining entries, excluding the seeds
	remainingEntries := make([]*Entry, 0, len(entries)-2)
	for i, entry := range entries {
		if i != seed1Index && i != seed2Index {
			remainingEntries = append(remainingEntries, entry)
		}
	}

	return seed1, seed2, remainingEntries
}

// adjustTree adjusts the MBRs of nodes and propagates splits up the tree.
func (rt *RTree) adjustTree(node *Node, splitNode *Node) {
	current := node
	var newParentEntry *Entry // Entry for the splitNode to be added to parent

	if splitNode != nil {
		newParentEntry = &Entry{Rect: rt.calculateMBR(splitNode), Child: splitNode}
		splitNode.Parent = current.Parent // Set parent for the new split node
	}

	for current != rt.Root {
		parent := current.Parent
		if parent == nil {
			// This should not happen unless current is the root and splitNode is not nil,
			// which is handled below.
			break
		}

		// Update the MBR of the entry pointing to 'current'
		found := false
		for _, entry := range parent.Entries {
			if entry.Child == current {
				entry.Rect = rt.calculateMBR(current)
				found = true
				break
			}
		}
		if !found {
			// This indicates a logical error in tree structure.
			// Log or handle appropriately.
			fmt.Printf("Error: Parent entry for node %p not found.\n", current)
			return
		}

		// If a split occurred, add the new entry to the parent
		if newParentEntry != nil {
			parent.Entries = append(parent.Entries, newParentEntry)
			newParentEntry.Child = parent // Set parent for the entry's child (splitNode)

			// If parent overflows, split it and propagate
			if len(parent.Entries) > M {
				splitNode = rt.splitNode(parent)
				newParentEntry = &Entry{Rect: rt.calculateMBR(splitNode), Child: splitNode}
				splitNode.Parent = parent.Parent // Set parent for the new split node
			} else {
				splitNode = nil // No further split
				newParentEntry = nil
			}
		}

		current = parent
	}

	// Handle root split
	if splitNode != nil {
		newRoot := &Node{
			IsLeaf:  false,
			Entries: make([]*Entry, 0, M),
		}
		// The original root becomes one child
		newRoot.Entries = append(newRoot.Entries, &Entry{Rect: rt.calculateMBR(rt.Root), Child: rt.Root})
		rt.Root.Parent = newRoot

		// The split node becomes the other child
		newRoot.Entries = append(newRoot.Entries, newParentEntry)
		newParentEntry.Child.Parent = newRoot

		rt.Root = newRoot
	}
}

// condenseTree removes entries from nodes and propagates changes up the tree,
// handling underflow and reinserting orphaned entries.
func (rt *RTree) condenseTree(node *Node) {
	var entriesToReinsert []*Entry // Collect entries to reinsert

	current := node
	for current != rt.Root {
		parent := current.Parent
		if parent == nil {
			// Should not happen unless current is the root
			break
		}

		// Check for underflow
		if len(current.Entries) < m {
			// Remove the entry pointing to 'current' from its parent
			// var entryToRemove *Entry
			entryIndexInParent := -1
			for i, entry := range parent.Entries {
				if entry.Child == current {
					// entryToRemove = entry
					entryIndexInParent = i
					break
				}
			}

			if entryIndexInParent != -1 {
				parent.Entries = append(parent.Entries[:entryIndexInParent], parent.Entries[entryIndexInParent+1:]...)
				// Add all entries from the underflowed node to reinsert list
				entriesToReinsert = append(entriesToReinsert, current.Entries...)
				// If internal node, also add its children's entries to reinsert list
				if !current.IsLeaf {
					entriesToReinsert = append(entriesToReinsert, current.Entries...)
				}
			} else {
				fmt.Printf("Error: Entry for node %p not found in parent %p during condense.\n", current, parent)
			}
		} else {
			// Node is not underflowing, just update its MBR in the parent
			for _, entry := range parent.Entries {
				if entry.Child == current {
					entry.Rect = rt.calculateMBR(current)
					break
				}
			}
		}
		current = parent
	}

	// Reinsert orphaned entries
	for _, entry := range entriesToReinsert {
		if entry.Data != nil { // It's a data entry (from a leaf node)
			// Temporarily unlock to avoid deadlock during re-insertion, then re-lock
			rt.mu.Unlock()
			rt.Insert(*entry.Data)
			rt.mu.Lock()
		} else if entry.Child != nil { // It's a child node entry (from an internal node)
			// When reinserting internal node entries, we need to reinsert the entire subtree
			// by inserting the MBR of the child node.
			// This is a simplification; a full reinsertion would re-add all individual data points.
			// For production-grade, consider a more robust reinsertion strategy for subtrees.
			// For now, we'll reinsert the MBR of the child, which will find a new parent for it.
			reinsertEntry := &Entry{Rect: rt.calculateMBR(entry.Child), Child: entry.Child}
			reinsertEntry.Child.Parent = nil // Clear parent for reinsertion
			rt.reinsertChildNode(rt.Root, reinsertEntry)
		}
	}
}

// reinsertChildNode is a helper for condenseTree to reinsert an orphaned child node.
// This is a specialized insert that takes an already formed Entry (with a Child node)
// and finds a suitable place for it.
func (rt *RTree) reinsertChildNode(node *Node, entryToReinsert *Entry) {
	if node.IsLeaf {
		// This case should ideally not happen if we are reinserting an internal node's child.
		// It implies an internal node's child is being reinserted into a leaf.
		// For robustness, we'll just add it here, but it might indicate a logic flaw.
		node.Entries = append(node.Entries, entryToReinsert)
		entryToReinsert.Child.Parent = node
		if len(node.Entries) > M {
			splitNode := rt.splitNode(node)
			rt.adjustTree(node, splitNode)
		}
		return
	}

	// Find the best internal node to insert the child node's entry
	minEnlargement := math.MaxFloat64
	minArea := math.MaxFloat64
	var chosenEntry *Entry

	for _, e := range node.Entries {
		enlargement := e.Rect.Enlargement(entryToReinsert.Rect)
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
	if chosenEntry != nil {
		rt.reinsertChildNode(chosenEntry.Child, entryToReinsert)
	} else {
		// This should ideally not happen if the tree is not empty.
		// If it does, it means no suitable child was found, implying the root is empty or invalid.
		// For now, we'll add it to the current node if it's not a leaf.
		node.Entries = append(node.Entries, entryToReinsert)
		entryToReinsert.Child.Parent = node
		if len(node.Entries) > M {
			splitNode := rt.splitNode(node)
			rt.adjustTree(node, splitNode)
		}
	}
}

// calculateMBR calculates the Minimum Bounding Rectangle for all entries in a node.
func (rt *RTree) calculateMBR(node *Node) Rect {
	if len(node.Entries) == 0 {
		// Return an "empty" rectangle or handle as an error
		return Rect{MinX: math.MaxFloat64, MinY: math.MaxFloat64, MaxX: -math.MaxFloat64, MaxY: -math.MaxFloat64}
	}

	mbr := node.Entries[0].Rect
	for i := 1; i < len(node.Entries); i++ {
		mbr = mbr.Union(node.Entries[i].Rect)
	}
	return mbr
}

// Validate checks the integrity of the R-tree. (For debugging/testing)
func (rt *RTree) Validate() error {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	if rt.Root == nil {
		return fmt.Errorf("root is nil")
	}
	return rt.validateNode(rt.Root, nil)
}

func (rt *RTree) validateNode(node *Node, parent *Node) error {
	if node == nil {
		return fmt.Errorf("nil node encountered during validation")
	}
	if node.Parent != parent {
		return fmt.Errorf("node %p has incorrect parent pointer. Expected %p, got %p", node, parent, node.Parent)
	}

	if node != rt.Root && len(node.Entries) < m {
		return fmt.Errorf("node %p underflowed: %d entries (min %d)", node, len(node.Entries), m)
	}
	if len(node.Entries) > M {
		return fmt.Errorf("node %p overflowed: %d entries (max %d)", node, len(node.Entries), M)
	}

	calculatedMBR := rt.calculateMBR(node)
	if parent != nil {
		// Check that the MBR in the parent's entry for this node is correct
		foundEntryInParent := false
		for _, entry := range parent.Entries {
			if entry.Child == node {
				foundEntryInParent = true
				if entry.Rect != calculatedMBR {
					return fmt.Errorf("parent entry MBR for node %p is incorrect. Expected %v, got %v", node, calculatedMBR, entry.Rect)
				}
				break
			}
		}
		if !foundEntryInParent {
			return fmt.Errorf("node %p not found in parent's entries", node)
		}
	}

	for _, entry := range node.Entries {
		if node.IsLeaf {
			if entry.Data == nil || entry.Child != nil {
				return fmt.Errorf("leaf node entry %p has nil data or non-nil child", entry)
			}
			if entry.Rect != entry.Data.Rect {
				return fmt.Errorf("leaf entry rect %v does not match data rect %v", entry.Rect, entry.Data.Rect)
			}
		} else {
			if entry.Data != nil || entry.Child == nil {
				return fmt.Errorf("internal node entry %p has non-nil data or nil child", entry)
			}
			if err := rt.validateNode(entry.Child, node); err != nil {
				return err
			}
		}
		// Check if entry's MBR contains its content
		if !entry.Rect.Contains(rt.calculateMBR(entry.Child)) && !node.IsLeaf {
			return fmt.Errorf("entry MBR %v does not contain child MBR %v for node %p", entry.Rect, rt.calculateMBR(entry.Child), entry.Child)
		}
	}
	return nil
}

// String representation for debugging
func (r Rect) String() string {
	return fmt.Sprintf("[(%.2f,%.2f)-(%.2f,%.2f)]", r.MinX, r.MinY, r.MaxX, r.MaxY)
}

func (e Entry) String() string {
	if e.Data != nil {
		return fmt.Sprintf("Data: %s %s", e.Data.ID, e.Rect.String())
	}
	if e.Child != nil {
		return fmt.Sprintf("Child: %s", e.Rect.String())
	}
	return "Empty Entry"
}

func (n Node) String() string {
	nodeType := "Internal"
	if n.IsLeaf {
		nodeType = "Leaf"
	}
	return fmt.Sprintf("%s Node (Entries: %d)", nodeType, len(n.Entries))
}

// PrintTree prints the tree structure for debugging.
func (rt *RTree) PrintTree() {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	fmt.Println("--- R-Tree Structure ---")
	rt.printNode(rt.Root, 0)
	fmt.Println("------------------------")
}

func (rt *RTree) printNode(node *Node, level int) {
	if node == nil {
		return
	}
	indent := ""
	for i := 0; i < level; i++ {
		indent += "  "
	}

	fmt.Printf("%s%s (MBR: %s)\n", indent, node.String(), rt.calculateMBR(node).String())

	for _, entry := range node.Entries {
		if node.IsLeaf {
			fmt.Printf("%s  %s\n", indent, entry.String())
		} else {
			fmt.Printf("%s  Entry MBR: %s -> Child:\n", indent, entry.Rect.String())
			rt.printNode(entry.Child, level+1)
		}
	}
}
