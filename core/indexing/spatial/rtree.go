package spatial

import (
	"fmt"
	"log"
	"math"
)

// Point represents a 2D geographical point.
type Point struct {
	Lat float64
	Lon float64
}

// Rect represents a 2D bounding box (Minimum Bounding Rectangle).
type Rect struct {
	Min Point
	Max Point
}

// NewRect creates a new Rect from two points.
func NewRect(p1, p2 Point) Rect {
	minLat := math.Min(p1.Lat, p2.Lat)
	maxLat := math.Max(p1.Lat, p2.Lat)
	minLon := math.Min(p1.Lon, p2.Lon)
	maxLon := math.Max(p1.Lon, p2.Lon)
	return Rect{
		Min: Point{Lat: minLat, Lon: minLon},
		Max: Point{Lat: maxLat, Lon: maxLon},
	}
}

// Contains checks if a point is within the rectangle.
func (r Rect) Contains(p Point) bool {
	return p.Lat >= r.Min.Lat && p.Lat <= r.Max.Lat &&
		p.Lon >= r.Min.Lon && p.Lon <= r.Max.Lon
}

// Intersects checks if two rectangles intersect.
func (r1 Rect) Intersects(r2 Rect) bool {
	return r1.Min.Lat <= r2.Max.Lat && r1.Max.Lat >= r2.Min.Lat &&
		r1.Min.Lon <= r2.Max.Lon && r1.Max.Lon >= r2.Min.Lon
}

// Union returns the smallest rectangle that contains both r1 and r2.
func (r1 Rect) Union(r2 Rect) Rect {
	return Rect{
		Min: Point{
			Lat: math.Min(r1.Min.Lat, r2.Min.Lat),
			Lon: math.Min(r1.Min.Lon, r2.Min.Lon),
		},
		Max: Point{
			Lat: math.Max(r1.Max.Lat, r2.Max.Lat),
			Lon: math.Max(r1.Max.Lon, r2.Max.Lon),
		},
	}
}

// Area returns the area of the rectangle.
func (r Rect) Area() float64 {
	return (r.Max.Lat - r.Min.Lat) * (r.Max.Lon - r.Min.Lon)
}

// Enlargement returns the increase in area if another rectangle is added.
func (r Rect) Enlargement(other Rect) float64 {
	unionRect := r.Union(other)
	return unionRect.Area() - r.Area()
}

// RTreeEntry represents an entry in the R-tree.
type RTreeEntry struct {
	Rect   Rect
	DataID string // The ID of the document associated with this spatial data
}

// RTreeNode represents a node in the R-tree.
type RTreeNode struct {
	IsLeaf   bool
	Entries  []RTreeEntry // For leaf nodes, these are data entries. For internal nodes, these are child MBRs.
	Children []*RTreeNode // For internal nodes, pointers to child nodes.
	Parent   *RTreeNode
}

// RTree is a basic R-tree implementation.
type RTree struct {
	Root       *RTreeNode
	MinEntries int // Minimum number of entries per node (m)
	MaxEntries int // Maximum number of entries per node (M)
}

// NewRTree creates a new R-tree.
func NewRTree(minEntries, maxEntries int) *RTree {
	if minEntries < 2 || maxEntries < minEntries {
		panic("RTree: minEntries must be at least 2 and maxEntries must be greater than or equal to minEntries")
	}
	return &RTree{
		Root:       &RTreeNode{IsLeaf: true},
		MinEntries: minEntries,
		MaxEntries: maxEntries,
	}
}

// Insert adds a new spatial entry to the R-tree.
func (rt *RTree) Insert(entry RTreeEntry) {
	leaf := rt.chooseLeaf(rt.Root, entry.Rect)
	leaf.Entries = append(leaf.Entries, entry)

	if len(leaf.Entries) > rt.MaxEntries {
		rt.handleOverflow(leaf)
	}
}

// chooseLeaf finds the best leaf node to insert a new rectangle.
func (rt *RTree) chooseLeaf(node *RTreeNode, rect Rect) *RTreeNode {
	if node.IsLeaf {
		return node
	}

	// Find the child whose MBR requires the minimum enlargement to cover rect.
	// Break ties by choosing the child with the smallest area.
	var bestChild *RTreeNode
	minEnlargement := math.MaxFloat64
	minArea := math.MaxFloat64

	for _, child := range node.Children {
		childMBR := calculateMBR(child.Entries)
		enlargement := childMBR.Enlargement(rect)
		if enlargement < minEnlargement {
			minEnlargement = enlargement
			minArea = childMBR.Area()
			bestChild = child
		} else if enlargement == minEnlargement {
			childArea := childMBR.Area()
			if childArea < minArea {
				minArea = childArea
				bestChild = child
			}
		}
	}
	return rt.chooseLeaf(bestChild, rect)
}

// handleOverflow handles node overflow by splitting the node or propagating up.
func (rt *RTree) handleOverflow(node *RTreeNode) {
	if len(node.Entries) <= rt.MaxEntries {
		return // No overflow
	}

	// Split the node
	newNode1, newNode2 := rt.splitNode(node)

	if node.Parent == nil {
		// Root split
		newRoot := &RTreeNode{IsLeaf: false}
		newRoot.Children = []*RTreeNode{newNode1, newNode2}
		newNode1.Parent = newRoot
		newNode2.Parent = newRoot
		rt.Root = newRoot
	} else {
		// Replace the old node with the two new nodes in the parent
		parentEntries := []RTreeEntry{}
		parentChildren := []*RTreeNode{}
		for i, child := range node.Parent.Children {
			if child == node {
				parentChildren = append(parentChildren, newNode1, newNode2)
				parentEntries = append(parentEntries, RTreeEntry{Rect: calculateMBR(newNode1.Entries)}, RTreeEntry{Rect: calculateMBR(newNode2.Entries)})
			} else {
				parentChildren = append(parentChildren, child)
				parentEntries = append(parentEntries, node.Parent.Entries[i]) // Copy existing MBRs
			}
		}
		node.Parent.Children = parentChildren
		node.Parent.Entries = parentEntries // Update parent's entries to reflect new MBRs
		newNode1.Parent = node.Parent
		newNode2.Parent = node.Parent

		if len(node.Parent.Entries) > rt.MaxEntries {
			rt.handleOverflow(node.Parent) // Propagate overflow
		}
	}
}

// splitNode splits an overflowing node into two new nodes.
// This is a simplified quadratic split algorithm.
func (rt *RTree) splitNode(node *RTreeNode) (*RTreeNode, *RTreeNode) {
	newNode1 := &RTreeNode{IsLeaf: node.IsLeaf}
	newNode2 := &RTreeNode{IsLeaf: node.IsLeaf}

	// Pick two entries to be the first elements of the new groups (seeds)
	// Find the pair of entries that would waste the most area if put in the same group.
	var seed1, seed2 RTreeEntry
	maxWaste := -1.0
	for i := 0; i < len(node.Entries); i++ {
		for j := i + 1; j < len(node.Entries); j++ {
			r1 := node.Entries[i].Rect
			r2 := node.Entries[j].Rect
			unionRect := r1.Union(r2)
			waste := unionRect.Area() - r1.Area() - r2.Area()
			if waste > maxWaste {
				maxWaste = waste
				seed1 = node.Entries[i]
				seed2 = node.Entries[j]
			}
		}
	}

	// Add seeds to new nodes
	newNode1.Entries = append(newNode1.Entries, seed1)
	newNode2.Entries = append(newNode2.Entries, seed2)

	// If node was internal, distribute children
	if !node.IsLeaf {
		// Find the corresponding children for the seeds and add them
		var child1, child2 *RTreeNode
		remainingChildren := []*RTreeNode{}
		for _, child := range node.Children {
			childMBR := calculateMBR(child.Entries)
			if childMBR == seed1.Rect && child1 == nil { // Simple check, might need more robust ID matching
				child1 = child
			} else if childMBR == seed2.Rect && child2 == nil {
				child2 = child
			} else {
				remainingChildren = append(remainingChildren, child)
			}
		}
		if child1 != nil {
			newNode1.Children = append(newNode1.Children, child1)
			child1.Parent = newNode1
		}
		if child2 != nil {
			newNode2.Children = append(newNode2.Children, child2)
			child2.Parent = newNode2
		}
		node.Children = remainingChildren // Update remaining children for distribution
	}

	// Distribute remaining entries
	remainingEntries := []RTreeEntry{}
	for _, entry := range node.Entries {
		if entry != seed1 && entry != seed2 {
			remainingEntries = append(remainingEntries, entry)
		}
	}

	for len(remainingEntries) > 0 {
		// If one group has too few entries to reach minEntries, add all remaining to it.
		if len(newNode1.Entries)+len(remainingEntries) < rt.MinEntries {
			newNode1.Entries = append(newNode1.Entries, remainingEntries...)
			if !node.IsLeaf {
				// Distribute corresponding children
				for _, entry := range remainingEntries {
					for i, child := range node.Children {
						if calculateMBR(child.Entries) == entry.Rect {
							newNode1.Children = append(newNode1.Children, child)
							child.Parent = newNode1
							node.Children = append(node.Children[:i], node.Children[i+1:]...) // Remove from original
							break
						}
					}
				}
			}
			break
		}
		if len(newNode2.Entries)+len(remainingEntries) < rt.MinEntries {
			newNode2.Entries = append(newNode2.Entries, remainingEntries...)
			if !node.IsLeaf {
				// Distribute corresponding children
				for _, entry := range remainingEntries {
					for i, child := range node.Children {
						if calculateMBR(child.Entries) == entry.Rect {
							newNode2.Children = append(newNode2.Children, child)
							child.Parent = newNode2
							node.Children = append(node.Children[:i], node.Children[i+1:]...) // Remove from original
							break
						}
					}
				}
			}
			break
		}

		// Pick next entry to assign
		var nextEntry RTreeEntry
		var bestIndex int
		maxDiff := -1.0

		for i, entry := range remainingEntries {
			enlargement1 := calculateMBR(newNode1.Entries).Enlargement(entry.Rect)
			enlargement2 := calculateMBR(newNode2.Entries).Enlargement(entry.Rect)
			diff := math.Abs(enlargement1 - enlargement2)
			if diff > maxDiff {
				maxDiff = diff
				nextEntry = entry
				bestIndex = i
			}
		}

		// Assign to the node that requires less enlargement
		enlargement1 := calculateMBR(newNode1.Entries).Enlargement(nextEntry.Rect)
		enlargement2 := calculateMBR(newNode2.Entries).Enlargement(nextEntry.Rect)

		if enlargement1 < enlargement2 {
			newNode1.Entries = append(newNode1.Entries, nextEntry)
		} else if enlargement2 < enlargement1 {
			newNode2.Entries = append(newNode2.Entries, nextEntry)
		} else {
			// Tie-breaking: choose the node with smaller area, then fewer entries
			area1 := calculateMBR(newNode1.Entries).Area()
			area2 := calculateMBR(newNode2.Entries).Area()
			if area1 < area2 {
				newNode1.Entries = append(newNode1.Entries, nextEntry)
			} else if area2 < area1 {
				newNode2.Entries = append(newNode2.Entries, nextEntry)
			} else {
				if len(newNode1.Entries) < len(newNode2.Entries) {
					newNode1.Entries = append(newNode1.Entries, nextEntry)
				} else {
					newNode2.Entries = append(newNode2.Entries, nextEntry)
				}
			}
		}

		// If node was internal, distribute corresponding child
		if !node.IsLeaf {
			for i, child := range node.Children {
				if calculateMBR(child.Entries) == nextEntry.Rect {
					if enlargement1 < enlargement2 { // Re-evaluate based on the chosen node
						newNode1.Children = append(newNode1.Children, child)
						child.Parent = newNode1
					} else if enlargement2 < enlargement1 {
						newNode2.Children = append(newNode2.Children, child)
						child.Parent = newNode2
					} else { // Tie-breaking for children distribution as well
						area1 := calculateMBR(newNode1.Entries).Area()
						area2 := calculateMBR(newNode2.Entries).Area()
						if area1 < area2 {
							newNode1.Children = append(newNode1.Children, child)
							child.Parent = newNode1
						} else if area2 < area1 {
							newNode2.Children = append(newNode2.Children, child)
							child.Parent = newNode2
						} else {
							if len(newNode1.Entries) < len(newNode2.Entries) {
								newNode1.Children = append(newNode1.Children, child)
								child.Parent = newNode1
							} else {
								newNode2.Children = append(newNode2.Children, child)
								child.Parent = newNode2
							}
						}
					}
					node.Children = append(node.Children[:i], node.Children[i+1:]...) // Remove from original
					break
				}
			}
		}

		remainingEntries = append(remainingEntries[:bestIndex], remainingEntries[bestIndex+1:]...)
	}

	return newNode1, newNode2
}

// calculateMBR calculates the Minimum Bounding Rectangle for a set of entries.
func calculateMBR(entries []RTreeEntry) Rect {
	if len(entries) == 0 {
		return Rect{} // Return an empty/zero rect
	}
	mbr := entries[0].Rect
	for i := 1; i < len(entries); i++ {
		mbr = mbr.Union(entries[i].Rect)
	}
	return mbr
}

// Delete removes a spatial entry from the R-tree.
func (rt *RTree) Delete(entry RTreeEntry) bool {
	// Find the leaf node containing the entry
	leaf := rt.findLeaf(rt.Root, entry)
	if leaf == nil {
		return false // Entry not found
	}

	// Remove the entry from the leaf node
	found := false
	for i, e := range leaf.Entries {
		if e.DataID == entry.DataID && e.Rect == entry.Rect { // Assuming DataID and Rect uniquely identify
			leaf.Entries = append(leaf.Entries[:i], leaf.Entries[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return false // Entry not found in the leaf
	}

	// Adjust tree upwards
	rt.condenseTree(leaf)

	// If root becomes empty and has one child, make the child the new root
	if rt.Root != nil && !rt.Root.IsLeaf && len(rt.Root.Entries) == 0 && len(rt.Root.Children) == 1 {
		rt.Root = rt.Root.Children[0]
		rt.Root.Parent = nil
	} else if rt.Root != nil && rt.Root.IsLeaf && len(rt.Root.Entries) == 0 {
		// If root is a leaf and becomes empty, reset to empty tree
		rt.Root = &RTreeNode{IsLeaf: true}
	}

	return true
}

// findLeaf finds the leaf node containing a specific entry.
func (rt *RTree) findLeaf(node *RTreeNode, entry RTreeEntry) *RTreeNode {
	if node.IsLeaf {
		for _, e := range node.Entries {
			if e.DataID == entry.DataID && e.Rect == entry.Rect {
				return node
			}
		}
		return nil
	}

	for _, child := range node.Children {
		childMBR := calculateMBR(child.Entries)
		if childMBR.Contains(entry.Rect.Min) && childMBR.Contains(entry.Rect.Max) { // Check if child MBR contains the entry's MBR
			if leaf := rt.findLeaf(child, entry); leaf != nil {
				return leaf
			}
		}
	}
	return nil
}

// condenseTree propagates changes up the tree after a deletion.
func (rt *RTree) condenseTree(node *RTreeNode) {
	// Collect nodes to reinsert
	reinsertList := []*RTreeNode{}

	currentNode := node
	for currentNode != rt.Root {
		parent := currentNode.Parent
		if parent == nil {
			break // Should not happen if not root
		}

		// Remove currentNode from parent's children and entries
		newParentChildren := []*RTreeNode{}
		newParentEntries := []RTreeEntry{}
		removed := false
		for i, child := range parent.Children {
			if child == currentNode {
				removed = true
				// If the node has too few entries, add its children/entries to reinsertList
				if len(currentNode.Entries) < rt.MinEntries {
					if currentNode.IsLeaf {
						for _, entry := range currentNode.Entries {
							log.Println(entry)
							// Reinsert individual entries if it's a leaf
							// This is a simplification; a more robust R-tree would reinsert the data, not the node
							// For this basic implementation, we'll just reinsert the data entries.
							// This part needs careful consideration for actual implementation.
							// For now, let's assume entries are just removed and not reinserted if the node is underflow.
						}
					} else {
						// For internal nodes, reinsert children
						reinsertList = append(reinsertList, currentNode.Children...)
					}
				}
			} else {
				newParentChildren = append(newParentChildren, child)
				newParentEntries = append(newParentEntries, parent.Entries[i]) // Copy existing MBRs
			}
		}
		parent.Children = newParentChildren
		parent.Entries = newParentEntries

		// If the node was removed due to underflow, update parent's MBR and continue condensing.
		if removed && len(currentNode.Entries) < rt.MinEntries {
			// Recalculate MBR for parent's entries
			parent.Entries = []RTreeEntry{}
			for _, child := range parent.Children {
				parent.Entries = append(parent.Entries, RTreeEntry{Rect: calculateMBR(child.Entries)})
			}
		} else if removed {
			// If not removed due to underflow, just update parent's MBR for this child
			parent.Entries = []RTreeEntry{}
			for _, child := range parent.Children {
				parent.Entries = append(parent.Entries, RTreeEntry{Rect: calculateMBR(child.Entries)})
			}
		}

		// Move up to the parent
		currentNode = parent
	}

	// Reinsert collected entries/nodes
	for _, nodeToReinsert := range reinsertList {
		// This is a simplified reinsertion. In a real R-tree, you'd reinsert the actual data entries or child nodes.
		// For this basic example, we're just demonstrating the condense phase.
		// A proper reinsertion would involve calling rt.Insert for each entry/child.
		if nodeToReinsert.IsLeaf {
			for _, entry := range nodeToReinsert.Entries {
				rt.Insert(entry) // Reinsert individual data entries
			}
		} else {
			// Reinserting internal nodes is more complex, typically you'd reinsert their MBRs and children.
			// For simplicity, we'll assume only leaf entries are reinserted.
		}
	}
}

// Search finds all DataIDs whose MBRs intersect the query rectangle.
func (rt *RTree) Search(queryRect Rect) []string {
	results := []string{}
	rt.searchNode(rt.Root, queryRect, &results)
	return results
}

// searchNode recursively searches the R-tree.
func (rt *RTree) searchNode(node *RTreeNode, queryRect Rect, results *[]string) {
	if node == nil {
		return
	}

	if node.IsLeaf {
		for _, entry := range node.Entries {
			if entry.Rect.Intersects(queryRect) {
				*results = append(*results, entry.DataID)
			}
		}
	} else {
		for _, child := range node.Children {
			childMBR := calculateMBR(child.Entries)
			if childMBR.Intersects(queryRect) {
				rt.searchNode(child, queryRect, results)
			}
		}
	}
}

// PrintTree (for debugging)
func (rt *RTree) PrintTree() {
	fmt.Println("--- R-Tree ---")
	rt.printNode(rt.Root, 0)
	fmt.Println("--------------")
}

func (rt *RTree) printNode(node *RTreeNode, level int) {
	if node == nil {
		return
	}
	indent := ""
	for i := 0; i < level; i++ {
		indent += "  "
	}

	nodeType := "Internal"
	if node.IsLeaf {
		nodeType = "Leaf"
	}
	fmt.Printf("%s[%s Node] MBR: %v, Entries: %d\n", indent, nodeType, calculateMBR(node.Entries), len(node.Entries))

	if node.IsLeaf {
		for _, entry := range node.Entries {
			fmt.Printf("%s  - DataID: %s, Rect: %v\n", indent, entry.DataID, entry.Rect)
		}
	} else {
		for i, child := range node.Children {
			fmt.Printf("%s  Child %d:\n", indent, i)
			rt.printNode(child, level+1)
		}
	}
}
