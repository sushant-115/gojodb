package memtable

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	flushmanager "github.com/sushant-115/gojodb/core/write_engine/flush_manager"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"go.uber.org/zap"
)

const testPageSize = 4096
const testPoolSize = 32

// --- Helpers ---

func setupBPM(t *testing.T, poolSize int) (*BufferPoolManager, *flushmanager.DiskManager) {
	t.Helper()
	logger, _ := zap.NewDevelopment()
	dbPath := filepath.Join(t.TempDir(), "test.db")

	dm, err := flushmanager.NewDiskManager(dbPath, testPageSize)
	require.NoError(t, err)

	_, err = dm.OpenOrCreateFile(true, 2, 0)
	require.NoError(t, err)

	bpm := NewBufferPoolManager(poolSize, dm, nil, logger)
	return bpm, dm
}

// --- Buffer Pool Manager Tests ---

// TestBPM_NewPageAllocatesAndPins verifies that NewPage allocates a page, returns a
// non-nil Page pointer with pin count 1, and marks it dirty.
func TestBPM_NewPageAllocatesAndPins(t *testing.T) {
	bpm, dm := setupBPM(t, testPoolSize)
	defer dm.Close()

	page, pageID, err := bpm.NewPage()
	require.NoError(t, err)
	require.NotNil(t, page)
	require.NotEqual(t, pagemanager.InvalidPageID, pageID)
	require.Equal(t, uint32(1), page.GetPinCount())
	require.True(t, page.IsDirty())
	require.Equal(t, pageID, page.GetPageID())
}

// TestBPM_FetchPageCacheHit verifies that fetching an already-buffered page returns the
// same page object without going to disk (pin count increments each time).
func TestBPM_FetchPageCacheHit(t *testing.T) {
	bpm, dm := setupBPM(t, testPoolSize)
	defer dm.Close()

	page1, pageID, err := bpm.NewPage()
	require.NoError(t, err)
	require.NoError(t, bpm.UnpinPage(pageID, false))

	// Fetch the same page; should come from buffer pool.
	page2, err := bpm.FetchPage(pageID)
	require.NoError(t, err)
	require.NotNil(t, page2)
	require.Equal(t, page1, page2, "cache hit should return the same page object")
	require.Equal(t, pageID, page2.GetPageID())
}

// TestBPM_FetchPageCacheMiss verifies that fetching a page not in memory causes a disk
// read and the correct page ID is populated.
func TestBPM_FetchPageCacheMiss(t *testing.T) {
	bpm, dm := setupBPM(t, testPoolSize)
	defer dm.Close()

	page, pageID, err := bpm.NewPage()
	require.NoError(t, err)

	// Write distinguishable data into the page.
	copy(page.GetData(), []byte("hello from disk"))
	require.NoError(t, bpm.FlushPage(pageID))
	require.NoError(t, bpm.UnpinPage(pageID, false))

	// Invalidate to force a cache miss.
	bpm.InvalidatePage(pageID)

	// Fetch should read from disk.
	fetched, err := bpm.FetchPage(pageID)
	require.NoError(t, err)
	require.Equal(t, pageID, fetched.GetPageID())
	require.Equal(t, []byte("hello from disk"), fetched.GetData()[:15])
}

// TestBPM_UnpinPageDecrementsPinCount verifies that UnpinPage decrements the pin
// counter and returns an error when the page is already at zero.
func TestBPM_UnpinPageDecrementsPinCount(t *testing.T) {
	bpm, dm := setupBPM(t, testPoolSize)
	defer dm.Close()

	page, pageID, err := bpm.NewPage()
	require.NoError(t, err)
	require.Equal(t, uint32(1), page.GetPinCount())

	require.NoError(t, bpm.UnpinPage(pageID, false))
	require.Equal(t, uint32(0), page.GetPinCount())

	// Second unpin should error (pin count already zero).
	err = bpm.UnpinPage(pageID, false)
	require.Error(t, err)
}

// TestBPM_FlushPageWritesToDisk verifies that FlushPage writes the page's data to the
// disk manager so subsequent reads return the updated content.
func TestBPM_FlushPageWritesToDisk(t *testing.T) {
	bpm, dm := setupBPM(t, testPoolSize)
	defer dm.Close()

	page, pageID, err := bpm.NewPage()
	require.NoError(t, err)

	sentinel := []byte("flush_test_sentinel")
	copy(page.GetData(), sentinel)

	require.NoError(t, bpm.FlushPage(pageID))

	// Read the raw bytes back from disk to confirm they were persisted.
	rawBuf := make([]byte, testPageSize)
	require.NoError(t, dm.ReadPage(pageID, rawBuf))
	require.Equal(t, sentinel, rawBuf[:len(sentinel)])
}

// TestBPM_FlushAllPagesFlushesAllDirty verifies that FlushAllPages persists every dirty
// page in the buffer pool to disk.
func TestBPM_FlushAllPagesFlushesAllDirty(t *testing.T) {
	bpm, dm := setupBPM(t, testPoolSize)
	defer dm.Close()

	const numPages = 5
	pageIDs := make([]pagemanager.PageID, numPages)

	for i := 0; i < numPages; i++ {
		p, pid, err := bpm.NewPage()
		require.NoError(t, err)
		copy(p.GetData(), []byte(fmt.Sprintf("page_%d_data", i)))
		pageIDs[i] = pid
	}

	require.NoError(t, bpm.FlushAllPages())

	// Verify each page was persisted.
	buf := make([]byte, testPageSize)
	for i, pid := range pageIDs {
		require.NoError(t, dm.ReadPage(pid, buf))
		expected := fmt.Sprintf("page_%d_data", i)
		require.Equal(t, expected, string(buf[:len(expected)]))
	}
}

// TestBPM_InvalidatePageForcesNextFetchFromDisk verifies that after InvalidatePage,
// the next FetchPage reads from disk rather than returning a stale in-memory copy.
func TestBPM_InvalidatePageForcesNextFetchFromDisk(t *testing.T) {
	bpm, dm := setupBPM(t, testPoolSize)
	defer dm.Close()

	page, pageID, err := bpm.NewPage()
	require.NoError(t, err)

	diskData := []byte("persisted_on_disk")
	copy(page.GetData(), diskData)
	require.NoError(t, bpm.FlushPage(pageID))
	require.NoError(t, bpm.UnpinPage(pageID, false))

	bpm.InvalidatePage(pageID)

	// Change in-memory would not matter since the frame was invalidated.
	fetched, err := bpm.FetchPage(pageID)
	require.NoError(t, err)
	require.Equal(t, diskData, fetched.GetData()[:len(diskData)])
}

// TestBPM_EvictionWhenPoolFull verifies that when the buffer pool is full and all pinned
// pages are accounted for, unpinning allows new pages to be allocated.
func TestBPM_EvictionWhenPoolFull(t *testing.T) {
	const smallPool = numBPMShards // one frame per shard
	bpm, dm := setupBPM(t, smallPool)
	defer dm.Close()

	pages := make([]*pagemanager.Page, smallPool)
	pids := make([]pagemanager.PageID, smallPool)

	for i := 0; i < smallPool; i++ {
		p, pid, err := bpm.NewPage()
		require.NoError(t, err, "page %d", i)
		pages[i] = p
		pids[i] = pid
	}

	// Pool is full and all pages are pinned — next NewPage should fail.
	_, _, err := bpm.NewPage()
	require.ErrorIs(t, err, flushmanager.ErrBufferPoolFull)

	// Unpin one page so it becomes evictable.
	require.NoError(t, bpm.UnpinPage(pids[0], false))

	// Now NewPage should succeed.
	_, _, err = bpm.NewPage()
	require.NoError(t, err, "should succeed after unpinning one page")
}

// TestBPM_ConcurrentFetchNewPages verifies that concurrent goroutines can safely call
// NewPage and FetchPage without triggering data races.
func TestBPM_ConcurrentFetchNewPages(t *testing.T) {
	bpm, dm := setupBPM(t, testPoolSize)
	defer dm.Close()

	var wg sync.WaitGroup
	const numGoroutines = 8

	var mu sync.Mutex
	pids := make([]pagemanager.PageID, 0, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, pid, err := bpm.NewPage()
			if err != nil {
				return // pool may be exhausted — that's OK for this test
			}
			copy(p.GetData(), []byte("concurrent"))

			mu.Lock()
			pids = append(pids, pid)
			mu.Unlock()
		}()
	}
	wg.Wait()

	// Fetch each allocated page; no panics or races expected.
	for _, pid := range pids {
		_, err := bpm.FetchPage(pid)
		if err == nil {
			require.NoError(t, bpm.UnpinPage(pid, false))
		}
	}
}

// TestBPM_GetPageSize verifies that GetPageSize returns the value configured at creation.
func TestBPM_GetPageSize(t *testing.T) {
	bpm, dm := setupBPM(t, testPoolSize)
	defer dm.Close()
	require.Equal(t, testPageSize, bpm.GetPageSize())
}

// TestBPM_UnpinNonexistentPage verifies that unpinning a page that is not in the buffer
// pool returns an error.
func TestBPM_UnpinNonexistentPage(t *testing.T) {
	bpm, dm := setupBPM(t, testPoolSize)
	defer dm.Close()
	err := bpm.UnpinPage(pagemanager.PageID(999), false)
	require.Error(t, err)
}

// TestBPM_FlushNonexistentPage verifies that flushing a page not in the pool returns
// an error.
func TestBPM_FlushNonexistentPage(t *testing.T) {
	bpm, dm := setupBPM(t, testPoolSize)
	defer dm.Close()
	err := bpm.FlushPage(pagemanager.PageID(999))
	require.Error(t, err)
}
