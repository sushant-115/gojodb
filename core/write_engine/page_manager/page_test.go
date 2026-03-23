package pagemanager

import (
	"container/list"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- Tests for NewPage ---

func TestNewPage_InitialState(t *testing.T) {
	const pageSize = 4096
	page := NewPage(PageID(1), pageSize)

	require.NotNil(t, page)
	require.Equal(t, PageID(1), page.GetPageID())
	require.Equal(t, pageSize, len(page.GetData()))
	require.Equal(t, uint32(0), page.GetPinCount())
	require.False(t, page.IsDirty())
	require.Equal(t, InvalidLSN, page.GetLSN())
}

func TestNewPage_DataIsZeroed(t *testing.T) {
	page := NewPage(PageID(5), 512)
	data := page.GetData()
	for i, b := range data {
		require.Equal(t, byte(0), b, "byte at index %d should be zero", i)
	}
}

// --- Tests for SetDirty / IsDirty ---

func TestPage_DirtyFlag(t *testing.T) {
	page := NewPage(PageID(2), 64)
	require.False(t, page.IsDirty())

	page.SetDirty(true)
	require.True(t, page.IsDirty())

	page.SetDirty(false)
	require.False(t, page.IsDirty())
}

// --- Tests for Pin / Unpin / GetPinCount ---

func TestPage_PinUnpin(t *testing.T) {
	page := NewPage(PageID(3), 64)
	require.Equal(t, uint32(0), page.GetPinCount())

	page.Pin()
	require.Equal(t, uint32(1), page.GetPinCount())

	page.Pin()
	require.Equal(t, uint32(2), page.GetPinCount())

	page.Unpin()
	require.Equal(t, uint32(1), page.GetPinCount())

	page.Unpin()
	require.Equal(t, uint32(0), page.GetPinCount())
}

func TestPage_UnpinWhenZero_DoesNotUnderflow(t *testing.T) {
	page := NewPage(PageID(4), 64)
	require.Equal(t, uint32(0), page.GetPinCount())

	// Calling Unpin when already at 0 should not underflow.
	page.Unpin()
	require.Equal(t, uint32(0), page.GetPinCount())
}

func TestPage_SetPinCount(t *testing.T) {
	page := NewPage(PageID(5), 64)
	page.SetPinCount(10)
	require.Equal(t, uint32(10), page.GetPinCount())
}

// --- Tests for GetData / SetData ---

func TestPage_SetData_CopiesContent(t *testing.T) {
	page := NewPage(PageID(6), 8)
	src := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	ok := page.SetData(src)
	require.True(t, ok)

	data := page.GetData()
	require.Equal(t, src, data)

	// Modifying src should not affect the page's data because SetData uses copy.
	src[0] = 99
	require.Equal(t, byte(1), data[0], "page data should not be affected by modifying original slice")
}

// --- Tests for GetPageID / SetPageID ---

func TestPage_GetSetPageID(t *testing.T) {
	page := NewPage(PageID(7), 64)
	require.Equal(t, PageID(7), page.GetPageID())

	page.SetPageID(PageID(42))
	require.Equal(t, PageID(42), page.GetPageID())
}

// --- Tests for GetLSN / SetLSN ---

func TestPage_GetSetLSN(t *testing.T) {
	page := NewPage(PageID(8), 64)
	require.Equal(t, InvalidLSN, page.GetLSN())

	page.SetLSN(LSN(100))
	require.Equal(t, LSN(100), page.GetLSN())
}

// --- Tests for UpdatedAt ---

func TestPage_UpdatedAt(t *testing.T) {
	page := NewPage(PageID(9), 64)
	now := time.Now()
	page.UpdatedAt(now)
	require.Equal(t, now, page.GetUpdatedAt())
}

// --- Tests for LRU Element ---

func TestPage_LruElement(t *testing.T) {
	page := NewPage(PageID(10), 64)
	require.Nil(t, page.GetLruElement())

	l := list.New()
	elem := l.PushBack(42)
	page.SetLruElement(elem)
	require.Equal(t, elem, page.GetLruElement())
}

// --- Tests for Reset ---

func TestPage_Reset(t *testing.T) {
	page := NewPage(PageID(11), 64)

	// Populate all fields.
	page.SetDirty(true)
	page.SetLSN(LSN(55))
	page.SetPinCount(3)
	page.SetData([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	l := list.New()
	page.SetLruElement(l.PushBack(99))

	// Reset should clear everything.
	page.Reset()

	require.Equal(t, InvalidPageID, page.GetPageID())
	require.Equal(t, uint32(0), page.GetPinCount())
	require.False(t, page.IsDirty())
	require.Equal(t, InvalidLSN, page.GetLSN())
	require.Nil(t, page.GetLruElement())

	data := page.GetData()
	for i, b := range data {
		require.Equal(t, byte(0), b, "byte at index %d should be zero after Reset", i)
	}
}

// --- Tests for PageID helper ---

func TestPageID_GetID(t *testing.T) {
	id := PageID(12345)
	require.Equal(t, uint64(12345), id.GetID())
}

// --- Tests for Latch (Lock / Unlock / RLock / RUnlock) ---

func TestPage_ReadLatch(t *testing.T) {
	page := NewPage(PageID(20), 64)

	// Multiple readers should work concurrently.
	page.RLock()
	page.RLock()
	page.RUnlock()
	page.RUnlock()
}

func TestPage_WriteLatch(t *testing.T) {
	page := NewPage(PageID(21), 64)

	page.Lock()
	page.Unlock()

	// Should be able to acquire again after releasing.
	page.Lock()
	page.Unlock()
}
