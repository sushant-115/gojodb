package spatial

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
)

// --- Tests for Rect.Intersects ---

func TestRect_Intersects_Overlapping(t *testing.T) {
	r1 := Rect{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}
	r2 := Rect{MinX: 5, MinY: 5, MaxX: 15, MaxY: 15}
	require.True(t, r1.Intersects(r2))
	require.True(t, r2.Intersects(r1))
}

func TestRect_Intersects_Adjacent_Touching(t *testing.T) {
	// Rectangles that share only a boundary edge are still considered intersecting.
	r1 := Rect{MinX: 0, MinY: 0, MaxX: 5, MaxY: 5}
	r2 := Rect{MinX: 5, MinY: 0, MaxX: 10, MaxY: 5}
	require.True(t, r1.Intersects(r2))
}

func TestRect_Intersects_NonOverlapping_Horizontal(t *testing.T) {
	r1 := Rect{MinX: 0, MinY: 0, MaxX: 4, MaxY: 4}
	r2 := Rect{MinX: 5, MinY: 0, MaxX: 9, MaxY: 4}
	require.False(t, r1.Intersects(r2))
	require.False(t, r2.Intersects(r1))
}

func TestRect_Intersects_NonOverlapping_Vertical(t *testing.T) {
	r1 := Rect{MinX: 0, MinY: 0, MaxX: 4, MaxY: 4}
	r2 := Rect{MinX: 0, MinY: 5, MaxX: 4, MaxY: 9}
	require.False(t, r1.Intersects(r2))
	require.False(t, r2.Intersects(r1))
}

func TestRect_Intersects_ContainedRect(t *testing.T) {
	outer := Rect{MinX: 0, MinY: 0, MaxX: 20, MaxY: 20}
	inner := Rect{MinX: 5, MinY: 5, MaxX: 10, MaxY: 10}
	require.True(t, outer.Intersects(inner))
	require.True(t, inner.Intersects(outer))
}

func TestRect_Intersects_SameRect(t *testing.T) {
	r := Rect{MinX: 1, MinY: 1, MaxX: 5, MaxY: 5}
	require.True(t, r.Intersects(r))
}

// --- Tests for Rect.Contains ---

func TestRect_Contains_Strictly(t *testing.T) {
	outer := Rect{MinX: 0, MinY: 0, MaxX: 20, MaxY: 20}
	inner := Rect{MinX: 5, MinY: 5, MaxX: 10, MaxY: 10}
	require.True(t, outer.Contains(inner))
}

func TestRect_Contains_ExactBoundary(t *testing.T) {
	r := Rect{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}
	require.True(t, r.Contains(r))
}

func TestRect_Contains_False_PartialOverlap(t *testing.T) {
	r1 := Rect{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}
	r2 := Rect{MinX: 5, MinY: 5, MaxX: 15, MaxY: 15}
	require.False(t, r1.Contains(r2))
}

func TestRect_Contains_False_NonOverlapping(t *testing.T) {
	r1 := Rect{MinX: 0, MinY: 0, MaxX: 5, MaxY: 5}
	r2 := Rect{MinX: 10, MinY: 10, MaxX: 20, MaxY: 20}
	require.False(t, r1.Contains(r2))
	require.False(t, r2.Contains(r1))
}

// --- Tests for Rect.Union ---

func TestRect_Union_OverlappingRects(t *testing.T) {
	r1 := Rect{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}
	r2 := Rect{MinX: 5, MinY: 5, MaxX: 20, MaxY: 20}
	u := r1.Union(r2)
	require.Equal(t, 0.0, u.MinX)
	require.Equal(t, 0.0, u.MinY)
	require.Equal(t, 20.0, u.MaxX)
	require.Equal(t, 20.0, u.MaxY)
}

func TestRect_Union_DisjointRects(t *testing.T) {
	r1 := Rect{MinX: 0, MinY: 0, MaxX: 5, MaxY: 5}
	r2 := Rect{MinX: 10, MinY: 10, MaxX: 15, MaxY: 15}
	u := r1.Union(r2)
	require.Equal(t, 0.0, u.MinX)
	require.Equal(t, 0.0, u.MinY)
	require.Equal(t, 15.0, u.MaxX)
	require.Equal(t, 15.0, u.MaxY)
}

func TestRect_Union_SameRect(t *testing.T) {
	r := Rect{MinX: 1, MinY: 2, MaxX: 3, MaxY: 4}
	u := r.Union(r)
	require.Equal(t, r, u)
}

func TestRect_Union_ContainedRect(t *testing.T) {
	outer := Rect{MinX: 0, MinY: 0, MaxX: 100, MaxY: 100}
	inner := Rect{MinX: 10, MinY: 10, MaxX: 50, MaxY: 50}
	u := outer.Union(inner)
	require.Equal(t, outer, u)
}

// --- Tests for Rect.Area ---

func TestRect_Area_Normal(t *testing.T) {
	r := Rect{MinX: 0, MinY: 0, MaxX: 5, MaxY: 4}
	require.Equal(t, 20.0, r.Area())
}

func TestRect_Area_Zero_Width(t *testing.T) {
	r := Rect{MinX: 3, MinY: 0, MaxX: 3, MaxY: 5}
	require.Equal(t, 0.0, r.Area())
}

func TestRect_Area_Zero_Height(t *testing.T) {
	r := Rect{MinX: 0, MinY: 2, MaxX: 5, MaxY: 2}
	require.Equal(t, 0.0, r.Area())
}

func TestRect_Area_InvalidRect(t *testing.T) {
	// MinX > MaxX is invalid; Area should return 0.
	r := Rect{MinX: 10, MinY: 0, MaxX: 5, MaxY: 5}
	require.Equal(t, 0.0, r.Area())
}

func TestRect_Area_FloatPrecision(t *testing.T) {
	r := Rect{MinX: 0.5, MinY: 0.5, MaxX: 1.5, MaxY: 1.5}
	require.InDelta(t, 1.0, r.Area(), 1e-9)
}

// --- Tests for Rect.Enlargement ---

func TestRect_Enlargement_OverlappingRect(t *testing.T) {
	r1 := Rect{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}
	r2 := Rect{MinX: 5, MinY: 5, MaxX: 20, MaxY: 20}
	// Union area = 20*20 = 400, original area = 100; enlargement = 300.
	require.InDelta(t, 300.0, r1.Enlargement(r2), 1e-9)
}

func TestRect_Enlargement_ContainedRect(t *testing.T) {
	outer := Rect{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}
	inner := Rect{MinX: 2, MinY: 2, MaxX: 8, MaxY: 8}
	// inner is already inside outer, so union == outer; enlargement == 0.
	require.InDelta(t, 0.0, outer.Enlargement(inner), 1e-9)
}

func TestRect_Enlargement_DisjointRect(t *testing.T) {
	r1 := Rect{MinX: 0, MinY: 0, MaxX: 5, MaxY: 5}
	r2 := Rect{MinX: 10, MinY: 10, MaxX: 15, MaxY: 15}
	// r1 area = 25; union = 15x15 = 225; enlargement = 200.
	require.InDelta(t, 200.0, r1.Enlargement(r2), 1e-9)
}

func TestRect_Enlargement_SameRect(t *testing.T) {
	r := Rect{MinX: 0, MinY: 0, MaxX: 5, MaxY: 5}
	require.InDelta(t, 0.0, r.Enlargement(r), 1e-9)
}

// --- Tests for NewNode ---

func TestNewNode_LeafNode(t *testing.T) {
	node := NewNode(pagemanager.PageID(1), true, pagemanager.PageID(0))
	require.NotNil(t, node)
	require.True(t, node.isLeaf)
	require.Equal(t, pagemanager.PageID(1), node.pageID)
	require.Equal(t, pagemanager.PageID(0), node.parentID)
	require.Empty(t, node.entries)
}

func TestNewNode_BranchNode(t *testing.T) {
	node := NewNode(pagemanager.PageID(5), false, pagemanager.PageID(2))
	require.NotNil(t, node)
	require.False(t, node.isLeaf)
	require.Equal(t, pagemanager.PageID(5), node.pageID)
	require.Equal(t, pagemanager.PageID(2), node.parentID)
}

// --- Tests for Node.AddEntry ---

func TestNode_AddEntry_Single(t *testing.T) {
	node := NewNode(pagemanager.PageID(1), true, pagemanager.PageID(0))
	entry := Entry{
		Rect:     Rect{MinX: 0, MinY: 0, MaxX: 5, MaxY: 5},
		Data:     SpatialData{ID: "doc1"},
		IsBranch: false,
	}
	node.AddEntry(entry)
	require.Len(t, node.entries, 1)
	require.Equal(t, entry.Data.ID, node.entries[0].Data.ID)
}

func TestNode_AddEntry_Multiple(t *testing.T) {
	node := NewNode(pagemanager.PageID(2), true, pagemanager.PageID(0))
	for i := 0; i < 4; i++ {
		entry := Entry{
			Rect:     Rect{MinX: float64(i), MinY: float64(i), MaxX: float64(i + 1), MaxY: float64(i + 1)},
			Data:     SpatialData{ID: "doc"},
			IsBranch: false,
		}
		node.AddEntry(entry)
	}
	require.Len(t, node.entries, 4)
}

// --- Tests for Node.GetMBR ---

func TestNode_GetMBR_Empty(t *testing.T) {
	node := NewNode(pagemanager.PageID(1), true, pagemanager.PageID(0))
	mbr := node.GetMBR()
	// Empty node returns a zero-value Rect.
	require.Equal(t, Rect{}, mbr)
}

func TestNode_GetMBR_SingleEntry(t *testing.T) {
	node := NewNode(pagemanager.PageID(1), true, pagemanager.PageID(0))
	r := Rect{MinX: 1, MinY: 2, MaxX: 3, MaxY: 4}
	node.AddEntry(Entry{Rect: r, Data: SpatialData{ID: "x"}, IsBranch: false})
	mbr := node.GetMBR()
	require.Equal(t, r, mbr)
}

func TestNode_GetMBR_MultipleEntries(t *testing.T) {
	node := NewNode(pagemanager.PageID(1), true, pagemanager.PageID(0))
	node.AddEntry(Entry{Rect: Rect{MinX: 1, MinY: 2, MaxX: 3, MaxY: 4}, Data: SpatialData{ID: "a"}, IsBranch: false})
	node.AddEntry(Entry{Rect: Rect{MinX: 5, MinY: 0, MaxX: 8, MaxY: 6}, Data: SpatialData{ID: "b"}, IsBranch: false})
	node.AddEntry(Entry{Rect: Rect{MinX: -1, MinY: -2, MaxX: 2, MaxY: 3}, Data: SpatialData{ID: "c"}, IsBranch: false})

	mbr := node.GetMBR()
	require.Equal(t, -1.0, mbr.MinX)
	require.Equal(t, -2.0, mbr.MinY)
	require.Equal(t, 8.0, mbr.MaxX)
	require.Equal(t, 6.0, mbr.MaxY)
}

func TestNode_GetMBR_IsUnionOfAllEntryRects(t *testing.T) {
	node := NewNode(pagemanager.PageID(1), true, pagemanager.PageID(0))
	rects := []Rect{
		{MinX: 0, MinY: 0, MaxX: 2, MaxY: 2},
		{MinX: 3, MinY: 3, MaxX: 5, MaxY: 5},
		{MinX: -3, MinY: 1, MaxX: 1, MaxY: 4},
	}
	for _, r := range rects {
		node.AddEntry(Entry{Rect: r, Data: SpatialData{ID: "x"}, IsBranch: false})
	}
	mbr := node.GetMBR()

	var expectedMBR Rect
	expectedMBR.MinX = math.Inf(1)
	expectedMBR.MinY = math.Inf(1)
	expectedMBR.MaxX = math.Inf(-1)
	expectedMBR.MaxY = math.Inf(-1)
	for _, r := range rects {
		expectedMBR.MinX = math.Min(expectedMBR.MinX, r.MinX)
		expectedMBR.MinY = math.Min(expectedMBR.MinY, r.MinY)
		expectedMBR.MaxX = math.Max(expectedMBR.MaxX, r.MaxX)
		expectedMBR.MaxY = math.Max(expectedMBR.MaxY, r.MaxY)
	}
	require.InDelta(t, expectedMBR.MinX, mbr.MinX, 1e-9)
	require.InDelta(t, expectedMBR.MinY, mbr.MinY, 1e-9)
	require.InDelta(t, expectedMBR.MaxX, mbr.MaxX, 1e-9)
	require.InDelta(t, expectedMBR.MaxY, mbr.MaxY, 1e-9)
}
