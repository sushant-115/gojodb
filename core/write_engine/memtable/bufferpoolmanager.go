package memtable

import (
	"bytes"
	"container/list" // For LRU
	"fmt"
	"log"
	"os"
	"sync"

	flushmanager "github.com/sushant-115/gojodb/core/write_engine/flush_manager"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	// Added for time.Ticker
)

// BufferPoolManager manages in-memory pages (frames) and interacts with the DiskManager.
// It implements a simple LRU (Least Recently Used) eviction policy.
type BufferPoolManager struct {
	diskManager *flushmanager.DiskManager
	logManager  *wal.LogManager // Changed from interface{} to *wal.LogManager
	poolSize    int
	pages       []*pagemanager.Page        // Page frames
	pageTable   map[pagemanager.PageID]int // PageID to frame index
	lruList     *list.List                 // Doubly linked list for LRU tracking (stores frame indices)
	lruMap      map[int]*list.Element      // Frame index to LRU list element
	mu          sync.Mutex
	pageSize    int
	numPages    pagemanager.PageID
	logType     wal.LogType
}

func (bpm *BufferPoolManager) GetNumPages() pagemanager.PageID {
	return bpm.numPages
}

func (bpm *BufferPoolManager) SetNextPageID(pageID pagemanager.PageID) {
	bpm.numPages = pageID
}

// NewBufferPoolManager creates and initializes a new BufferPoolManager.
func NewBufferPoolManager(poolSize int, diskManager *flushmanager.DiskManager, logManager *wal.LogManager) *BufferPoolManager {
	if diskManager == nil {
		log.Fatal("NewBufferPoolManager: diskManager cannot be nil")
	}
	bpm := &BufferPoolManager{
		diskManager: diskManager,
		logManager:  logManager, // Now directly *wal.LogManager
		poolSize:    poolSize,
		pages:       make([]*pagemanager.Page, poolSize),
		pageTable:   make(map[pagemanager.PageID]int),
		lruList:     list.New(),
		lruMap:      make(map[int]*list.Element),
		pageSize:    diskManager.GetPageSize(),
	}
	for i := 0; i < poolSize; i++ {
		bpm.pages[i] = pagemanager.NewPage(pagemanager.InvalidPageID, bpm.pageSize)
	}
	log.Printf("INFO: BufferPoolManager initialized with pool size %d, page size %d", poolSize, bpm.pageSize)
	return bpm
}

// FetchPage retrieves a page from the buffer pool. If not present, it fetches from disk.
// It pins the page, increments its pin count, and moves it to the front of the LRU list.
func (bpm *BufferPoolManager) FetchPage(pageID pagemanager.PageID) (*pagemanager.Page, error) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	// 1. Check if page is already in the buffer pool
	if frameIdx, ok := bpm.pageTable[pageID]; ok {
		page := bpm.pages[frameIdx]
		page.Pin()
		// Move to front of LRU to mark as recently used
		if page.GetLruElement() != nil { // Ensure it's in LRU list before moving
			bpm.lruList.MoveToFront(page.GetLruElement())
		}
		log.Printf("DEBUG: Page %d found in buffer pool (frame %d), pinCount: %d", pageID, frameIdx, page.GetPinCount())
		return page, nil
	}

	// 2. Page not in pool, find a victim frame to replace
	frameIdx, err := bpm.getVictimFrameInternal()
	if err != nil {
		log.Printf("ERROR: Failed to get victim frame for page %d: %v", pageID, err)
		return nil, err
	}
	victimPage := bpm.pages[frameIdx]
	log.Printf("DEBUG: Page %d not in buffer pool. Evicting frame %d (old page %d)", pageID, frameIdx, victimPage.GetPageID())

	// 3. If victim page is dirty, flush it to disk
	if victimPage.IsDirty() && victimPage.GetPageID() != pagemanager.InvalidPageID {
		// WAL INTEGRATION: Ensure all log records for this page up to its LSN are flushed
		if bpm.logManager != nil && victimPage.GetLSN() != pagemanager.InvalidLSN {
			if err := bpm.logManager.Sync(); err != nil {
				log.Printf("ERROR: Failed to flush log for victim page %d (LSN %d) before flushing page: %v", victimPage.GetPageID(), victimPage.GetLSN(), err)
				// This is a critical error, as we might be flushing a page before its log is durable.
				// For now, return error, but a real system might panic or try another victim.
				return nil, fmt.Errorf("failed to flush log for victim page %d: %w", victimPage.GetPageID(), err)
			}
		}
		log.Printf("DEBUG: Flushing dirty victim page %d from frame %d to disk", victimPage.GetPageID(), frameIdx)
		if err := bpm.diskManager.WritePage(victimPage.GetPageID(), victimPage.GetData()); err != nil {
			// If flush fails, we cannot reuse this frame safely.
			return nil, fmt.Errorf("failed to flush dirty victim page %d: %w", victimPage.GetPageID(), err)
		}
		victimPage.SetDirty(false) // Flushed
	}

	// 4. Remove victim page from pageTable and LRU list
	if victimPage.GetPageID() != pagemanager.InvalidPageID { // If it held a valid page
		delete(bpm.pageTable, victimPage.GetPageID())
		if victimPage.GetLruElement() != nil {
			bpm.lruList.Remove(victimPage.GetLruElement())
			delete(bpm.lruMap, frameIdx) // Also remove from lruMap
		}
	}

	// 5. Reset victim page for new content
	victimPage.Reset() // Clears data and metadata

	// 6. Load new page data from disk
	log.Printf("DEBUG: Reading page %d from disk into frame %d", pageID, frameIdx)
	if err := bpm.diskManager.ReadPage(pageID, victimPage.GetData()); err != nil {
		// If read fails, return error. The frame is now empty and not tracked in pageTable.
		return nil, fmt.Errorf("failed to read page %d from disk: %w", pageID, err)
	}

	// 7. Update new page metadata and track in buffer pool
	victimPage.SetPageID(pageID)
	victimPage.SetPinCount(1)
	victimPage.SetDirty(false)                // Just read from disk, so it's clean
	victimPage.SetLSN(pagemanager.InvalidLSN) // LSN should be read from page data if stored, or set after WAL replay. For now, assume Invalid.

	bpm.pageTable[pageID] = frameIdx
	victimPage.SetLruElement(bpm.lruList.PushFront(frameIdx)) // Add to front of LRU
	bpm.lruMap[frameIdx] = victimPage.GetLruElement()
	log.Printf("DEBUG: Page %d loaded into frame %d, pinCount: %d", pageID, frameIdx, victimPage.GetPinCount())

	return victimPage, nil
}

// getVictimFrameInternal finds an unpinned page to evict.
// This method MUST be called with lm.mu locked.
func (bpm *BufferPoolManager) getVictimFrameInternal() (int, error) {
	// First, look for an unpinned page in the LRU list (starting from the least recently used)
	for e := bpm.lruList.Back(); e != nil; e = e.Prev() {
		frameIdx := e.Value.(int)
		if bpm.pages[frameIdx].GetPinCount() == 0 {
			log.Printf("DEBUG: Found LRU victim frame %d (page %d)", frameIdx, bpm.pages[frameIdx].GetPageID())
			return frameIdx, nil
		}
	}

	// If no unpinned page found in LRU, check if there are any completely free frames (never used, or reset)
	// This covers the initial state where frames are not yet holding valid pages or added to LRU.
	for i := 0; i < bpm.poolSize; i++ {
		if bpm.pages[i].GetPageID() == pagemanager.InvalidPageID { // A truly empty frame
			log.Printf("DEBUG: Found empty frame %d as victim", i)
			return i, nil
		}
	}

	// If all pages are pinned or the pool is genuinely full with no evictable pages
	log.Printf("ERROR: Buffer pool is full, and all pages are pinned or none are evictable.")
	return -1, flushmanager.ErrBufferPoolFull
}

// UnpinPage decrements the pin count for a page. If isDirty is true, it marks the page as dirty.
// This is a key point for logging page modifications.
func (bpm *BufferPoolManager) UnpinPage(pageID pagemanager.PageID, isDirty bool) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	if frameIdx, ok := bpm.pageTable[pageID]; ok {
		page := bpm.pages[frameIdx]
		if page.GetPinCount() == 0 {
			log.Printf("WARNING: Attempted to unpin page %d with pin count 0.", pageID)
			return fmt.Errorf("cannot unpin page %d with pin count 0", pageID)
		}
		page.Unpin()

		if isDirty {
			page.SetDirty(true)
			// WAL INTEGRATION: Append a log record for this page modification.
			// This is a generic "update" record. More specific records (InsertKey, DeleteKey)
			// would ideally be generated at the B-tree Node level (Node.serialize).
			// For now, we log the entire page data as OldData/NewData for simplicity.
			if bpm.logManager != nil {
				// Create a copy of the page data for OldData and NewData for the log record.
				// In a real system, you might optimize this to only log changed bytes or use
				// physiological logging where the log record describes the operation.
				// For this generic page update, we'll log the whole page.
				// OldData would require reading the page's previous state, which is complex for BPM.
				// So, we'll log NewData only for now, or assume OldData is fetched during recovery.
				// For simplicity, we'll log an "update" with the current page data as NewData.
				// A more robust physiological log would have the B-tree layer provide OldData/NewData.

				// For now, let's just log a generic update.
				// The actual OldData would come from a copy of the page *before* the modification.
				// Since BPM doesn't track old state, this is a simplified log.
				// The LSN is assigned by the LogManager.Append method.
				logRecord := &wal.LogRecord{
					TxnID:  0, // Placeholder: real transactions would have a TxnID
					Type:   wal.LogRecordTypeUpdate,
					PageID: pageID,
					// OldData:  make([]byte, bpm.pageSize), // Requires copying old state
					Data: page.GetData(), // Copy of current page data
				}
				lsn, err := bpm.logManager.AppendRecord(logRecord, bpm.logType)
				if err != nil {
					log.Printf("ERROR: Failed to append log record for page %d update: %v", pageID, err)
					// This is a critical error. A real system would likely crash or panic.
					return fmt.Errorf("failed to append log record for page %d: %w", pageID, err)
				}
				page.SetLSN(pagemanager.LSN(lsn)) // Update the page's LSN to the LSN of this log record
			}
		}
		log.Printf("DEBUG: Unpinned page %d (frame %d), new pinCount: %d, isDirty: %t, LSN: %d", pageID, frameIdx, page.GetPinCount(), page.IsDirty(), page.GetLSN())
		// If pin count becomes 0, it's now eligible for LRU eviction. Its LRU position
		// is managed by FetchPage moving to front. No explicit LRU removal/addition needed here.
		return nil
	}
	log.Printf("ERROR: Page %d not found in buffer pool to unpin.", pageID)
	return fmt.Errorf("%w: page %d not found to unpin", flushmanager.ErrPageNotFound, pageID)
}

// NewPage allocates a new page on disk and fetches it into the buffer pool.
// It pins the new page and marks it dirty.
func (bpm *BufferPoolManager) NewPage() (*pagemanager.Page, pagemanager.PageID, error) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	// 1. Allocate a new page on disk
	newPageID, err := bpm.diskManager.AllocatePage()
	if err != nil {
		log.Printf("ERROR: Failed to allocate new page on disk: %v", err)
		return nil, pagemanager.InvalidPageID, err
	}
	log.Printf("DEBUG: Allocated new page %d on disk.", newPageID)

	// 2. Find a victim frame in the buffer pool for this new page
	frameIdx, err := bpm.getVictimFrameInternal()
	if err != nil {
		// IMPORTANT: If we can't get a frame, the allocated disk page is orphaned.
		// A robust system would deallocate this newPageID back to the disk free list.
		_ = bpm.diskManager.DeallocatePage(newPageID) // Attempt to deallocate (might be a no-op if not implemented)
		log.Printf("ERROR: Failed to get frame for new page %d, deallocated. Error: %v", newPageID, err)
		return nil, pagemanager.InvalidPageID, fmt.Errorf("failed to get frame for new page %d: %w", newPageID, err)
	}
	victimPage := bpm.pages[frameIdx]
	log.Printf("DEBUG: New page %d getting frame %d (old page %d)", newPageID, frameIdx, victimPage.GetPageID())

	// 3. If victim page is dirty, flush it before reuse
	if victimPage.IsDirty() && victimPage.GetPageID() != pagemanager.InvalidPageID {
		// WAL INTEGRATION: Ensure logs for victimPage are flushed before eviction
		if bpm.logManager != nil && victimPage.GetLSN() != pagemanager.InvalidLSN {
			if err := bpm.logManager.Sync(); err != nil {
				log.Printf("ERROR: Failed to flush log for victim page %d (LSN %d) before flushing page: %v", victimPage.GetPageID(), victimPage.GetLSN(), err)
				return nil, pagemanager.InvalidPageID, fmt.Errorf("failed to flush log for victim page %d: %w", victimPage.GetPageID(), err)
			}
		}
		log.Printf("DEBUG: Flushing dirty victim page %d for new page %d", victimPage.GetPageID(), newPageID)
		if err := bpm.diskManager.WritePage(victimPage.GetPageID(), victimPage.GetData()); err != nil {
			return nil, pagemanager.InvalidPageID, fmt.Errorf("failed to flush dirty victim %d for new page: %w", victimPage.GetPageID(), err)
		}
		victimPage.SetDirty(false)
	}

	// 4. Remove victim page from pageTable and LRU list
	if victimPage.GetPageID() != pagemanager.InvalidPageID {
		delete(bpm.pageTable, victimPage.GetPageID())
		if victimPage.GetLruElement() != nil {
			bpm.lruList.Remove(victimPage.GetLruElement())
			delete(bpm.lruMap, frameIdx)
		}
	}

	// 5. Reset and initialize the new page
	victimPage.Reset() // Clears data and metadata
	victimPage.SetPageID(newPageID)
	victimPage.SetPinCount(1)
	victimPage.SetDirty(true)                 // Just read from disk, so it's clean
	victimPage.SetLSN(pagemanager.InvalidLSN) // Will be set by the B-tree Node.serialize after a log record is written for it.

	// 6. Track the new page in buffer pool
	bpm.pageTable[newPageID] = frameIdx
	victimPage.SetLruElement(bpm.lruList.PushFront(frameIdx)) // Add to front of LRU
	bpm.lruMap[frameIdx] = victimPage.GetLruElement()
	log.Printf("DEBUG: New page %d loaded into frame %d, pinCount: %d, isDirty: %t", newPageID, frameIdx, victimPage.GetPinCount(), victimPage.IsDirty())

	// WAL INTEGRATION: Append a log record for the allocation of this new page.
	if bpm.logManager != nil {
		logRecord := &wal.LogRecord{
			TxnID:  0, // Placeholder
			Type:   wal.LogRecordTypeNewPage,
			PageID: newPageID,
			// NewData can be the initial empty page data, or just the fact of allocation.
			// For simplicity, we'll log the allocation itself.
		}
		lsn, err := bpm.logManager.AppendRecord(logRecord, bpm.logType)
		if err != nil {
			log.Printf("ERROR: Failed to append LogRecordTypeNewPage for page %d: %v", newPageID, err)
			// This is a critical error. A real system would likely crash or panic.
			return nil, pagemanager.InvalidPageID, fmt.Errorf("failed to append new page log record for %d: %w", newPageID, err)
		}
		victimPage.SetLSN(pagemanager.LSN(lsn)) // Update the page's LSN
	}

	return victimPage, newPageID, nil
}

// FlushPage flushes a specific page to disk if it's dirty.
// It ensures that all log records related to this page up to its LSN are durable first.
func (bpm *BufferPoolManager) FlushPage(pageID pagemanager.PageID) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	if frameIdx, ok := bpm.pageTable[pageID]; ok {
		page := bpm.pages[frameIdx]
		if page.IsDirty() {
			// WAL INTEGRATION: Ensure log records up to page.GetLSN() are flushed before this.
			if bpm.logManager != nil && page.GetLSN() != pagemanager.InvalidLSN {
				if err := bpm.logManager.Sync(); err != nil {
					log.Printf("ERROR: Failed to flush log for page %d (LSN %d) before flushing page: %v", pageID, page.GetLSN(), err)
					return fmt.Errorf("failed to flush log for page %d: %w", pageID, err)
				}
			}
			log.Printf("DEBUG: Flushing page %d (frame %d) to disk", pageID, frameIdx)
			if err := bpm.diskManager.WritePage(page.GetPageID(), page.GetData()); err != nil {
				log.Printf("ERROR: Failed to flush page %d: %v", pageID, err)
				return err
			}
			dd := make([]byte, len(page.GetData()))
			err := bpm.diskManager.ReadPage(page.GetPageID(), dd)
			if err != nil {
				log.Println("Error during read page: ", err)
			}
			log.Println("Page data and data read from disk is same? : ", bytes.Equal(page.GetData(), dd))

			page.SetDirty(false) // Mark clean after successful flush
		} else {
			log.Printf("DEBUG: Page %d (frame %d) is clean, no flush needed.", pageID, frameIdx)
		}
		return nil
	}
	log.Printf("WARNING: Attempted to flush page %d not found in buffer pool.", pageID)
	return fmt.Errorf("%w: page %d not found to flush", flushmanager.ErrPageNotFound, pageID)
}

// FlushAllPages flushes all dirty pages in the buffer pool to disk.
// It ensures all pending log records are durable before flushing pages.
func (bpm *BufferPoolManager) FlushAllPages() error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	var firstErr error

	// WAL INTEGRATION: Before flushing all pages, ensure all pending log records are flushed.
	if bpm.logManager != nil {
		log.Println("DEBUG: Flushing all pending log records before flushing all pages.")
		if firstErr = bpm.logManager.Sync(); firstErr != nil { // Flush all logs
			log.Printf("ERROR: Failed to flush all log records before flushing all pages: %v", firstErr)
		}
	}

	log.Println("DEBUG: Flushing all dirty pages from buffer pool...")
	for i, page := range bpm.pages { // Iterate over all frames in the pool
		if page.GetPageID() != pagemanager.InvalidPageID && page.IsDirty() {
			// WAL INTEGRATION: Although logs should be flushed above, this is a redundant check
			// for safety if a page's LSN is somehow higher than what was flushed.
			if bpm.logManager != nil && page.GetLSN() != pagemanager.InvalidLSN {
				if err := bpm.logManager.Sync(); err != nil {
					log.Printf("ERROR: Failed to flush log for page %d (LSN %d) during FlushAllPages: %v", page.GetPageID(), page.GetLSN(), err)
					if firstErr == nil {
						firstErr = err
					}
				}
			}

			log.Printf("DEBUG: Flushing page %d (frame %d) during FlushAllPages", page.GetPageID(), i)
			err := bpm.diskManager.WritePage(page.GetPageID(), page.GetData())
			if err != nil {
				if firstErr == nil {
					firstErr = err // Store the first error encountered
				}
				fmt.Fprintf(os.Stderr, "Error flushing page %d: %v\n", page.GetPageID(), err)
			} else {
				page.SetDirty(false) // Mark clean after successful flush
			}
		}
	}
	// Finally, sync the disk manager to ensure all writes are durable
	if err := bpm.diskManager.Sync(); err != nil {
		if firstErr == nil {
			firstErr = err
		}
		fmt.Fprintf(os.Stderr, "Error syncing disk manager during FlushAllPages: %v\n", err)
	}
	log.Println("DEBUG: Finished FlushAllPages.")
	return firstErr
}

// InvalidatePage removes a page from the buffer pool's hash table,
// forcing a fresh read from DiskManager on the next FetchPage.
// It also ensures the page is unpinned and its frame is available for eviction.
func (bpm *BufferPoolManager) InvalidatePage(pageID pagemanager.PageID) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	if frameID, ok := bpm.pageTable[pageID]; ok {
		// Ensure the page is unpinned so it can be evicted.
		// For recovery, pages are typically not pinned during direct writes.
		// If it's pinned by something else, this might indicate a logical error
		// or a case where the page shouldn't be invalidated yet.
		// For robustness, we forcefully unpin it here.
		for bpm.pages[frameID].GetPinCount() > 0 {
			bpm.UnpinPage(pageID, false) // Unpin without marking dirty, as it's being invalidated
		}

		// Remove from page table
		delete(bpm.pageTable, pageID)

		// Reset page state in the frame to make it truly "invalid" and reusable.
		// This also ensures the replacer can manage this frame.
		bpm.pages[pageID].SetPageID(pagemanager.InvalidPageID)
		bpm.pages[pageID].SetDirty(false)
		bpm.pages[pageID].SetPinCount(0)
		bpm.pages[pageID].SetLSN(pagemanager.InvalidLSN)

		// Inform the replacer that this frame is now unpinned and available for eviction
		// if it was previously pinned. Or if it was already unpinned, this is idempotent.
		// This is important because the replacer holds state about pinned/unpinned frames.
		bpm.UnpinPage(pageID, true)

		log.Printf("DEBUG: Invalidated page %d (frame %d) from buffer pool.", pageID, frameID)
	} else {
		log.Printf("DEBUG: Attempted to invalidate page %d not found in buffer pool. No action taken.", pageID)
	}
}

func (bpm *BufferPoolManager) GetPageSize() int {
	return bpm.pageSize
}
