package btree_core

// import (
// 	"container/list"
// 	"fmt"
// 	"log"
// 	"os"
// 	"sync"
// )

// // --- Write-Ahead Logging (WAL) ---
// type LSN uint64 // Log Sequence Number
// const InvalidLSN LSN = 0

// // LogRecordType defines the type of operation logged.
// type LogRecordType byte

// const (
// 	LogRecordTypeUpdate    LogRecordType = iota + 1 // Update existing page data
// 	LogRecordTypeInsertKey                          // B-Tree key insert (higher level)
// 	LogRecordTypeDeleteKey                          // B-Tree key delete
// 	LogRecordTypeNodeSplit                          // B-Tree node split
// 	LogRecordTypeNodeMerge                          // B-Tree node merge
// 	LogRecordTypeNewPage                            // Allocation of a new page
// 	LogRecordTypeFreePage                           // Deallocation of a page
// 	LogRecordTypeCommit                             // Transaction commit
// 	LogRecordTypeAbort                              // Transaction abort
// 	LogRecordTypeCheckpointStart
// 	LogRecordTypeCheckpointEnd
// 	// ... other types
// )

// type LogRecord struct {
// 	LSN     LSN
// 	PrevLSN LSN    // LSN of the previous log record by the same transaction (for undo)
// 	TxnID   uint64 // Transaction ID (0 if not part of a transaction or single op)
// 	Type    LogRecordType
// 	PageID  PageID // Page affected (if applicable)
// 	Offset  uint16 // Offset within the page (if applicable for UPDATE)
// 	OldData []byte // For UNDO (and REDO if needed for physiological logging)
// 	NewData []byte // For REDO
// 	// Other type-specific fields
// }

// // TODO: Implement LogManager (writes to a log file, manages LSNs, flushing)
// // type LogManager struct { ... }
// // func NewLogManager(logFilePath string) (*LogManager, error) { ... }
// // func (lm *LogManager) Append(record *LogRecord) (LSN, error) { ... } // Returns LSN of written record
// // func (lm *LogManager) Flush() error { ... }
// // The LogManager would use its own file handle, not necessarily the DiskManager for data pages.

// // --- BufferPoolManager (with LRU) ---
// type BufferPoolManager struct {
// 	diskManager *DiskManager
// 	logManager  interface{} // Placeholder for *LogManager
// 	poolSize    int
// 	pages       []*Page               // Page frames
// 	pageTable   map[PageID]int        // PageID to frame index
// 	lruList     *list.List            // Doubly linked list for LRU tracking (stores frame indices)
// 	lruMap      map[int]*list.Element // Frame index to LRU list element
// 	mu          sync.Mutex
// 	pageSize    int
// }

// func NewBufferPoolManager(poolSize int, diskManager *DiskManager, logManager interface{}) *BufferPoolManager {
// 	if diskManager == nil {
// 		log.Fatal("NewBufferPoolManager: diskManager cannot be nil")
// 	}
// 	bpm := &BufferPoolManager{
// 		diskManager: diskManager, logManager: logManager, poolSize: poolSize,
// 		pages: make([]*Page, poolSize), pageTable: make(map[PageID]int),
// 		lruList: list.New(), lruMap: make(map[int]*list.Element),
// 		pageSize: diskManager.pageSize,
// 	}
// 	for i := 0; i < poolSize; i++ {
// 		bpm.pages[i] = NewPage(InvalidPageID, bpm.pageSize)
// 	}
// 	return bpm
// }

// func (bpm *BufferPoolManager) FetchPage(pageID PageID) (*Page, error) {
// 	bpm.mu.Lock()
// 	defer bpm.mu.Unlock()

// 	// 1. Check if page is already in the buffer pool
// 	if frameIdx, ok := bpm.pageTable[pageID]; ok {
// 		page := bpm.pages[frameIdx]
// 		page.Pin()
// 		// Move to front of LRU to mark as recently used
// 		if page.lruElement != nil { // Ensure it's in LRU list before moving
// 			bpm.lruList.MoveToFront(page.lruElement)
// 		}
// 		log.Printf("DEBUG: Page %d found in buffer pool (frame %d), pinCount: %d", pageID, frameIdx, page.GetPinCount())
// 		return page, nil
// 	}

// 	// 2. Page not in pool, find a victim frame to replace
// 	frameIdx, err := bpm.getVictimFrameInternal()
// 	if err != nil {
// 		log.Printf("ERROR: Failed to get victim frame for page %d: %v", pageID, err)
// 		return nil, err
// 	}
// 	victimPage := bpm.pages[frameIdx]
// 	log.Printf("DEBUG: Page %d not in buffer pool. Evicting frame %d (old page %d)", pageID, frameIdx, victimPage.GetPageID())

// 	// 3. If victim page is dirty, flush it to disk
// 	if victimPage.IsDirty() && victimPage.GetPageID() != InvalidPageID {
// 		// TODO: WAL - Before flushing, ensure all log records for this page up to its LSN are flushed.
// 		// if lm, ok := bpm.logManager.(*LogManager); ok { lm.Flush(victimPage.GetLSN()) }
// 		log.Printf("DEBUG: Flushing dirty victim page %d from frame %d to disk", victimPage.GetPageID(), frameIdx)
// 		if err := bpm.diskManager.WritePage(victimPage.GetPageID(), victimPage.GetData()); err != nil {
// 			// If flush fails, we cannot reuse this frame safely. This is a critical error.
// 			// A real system might panic or try another victim.
// 			return nil, fmt.Errorf("failed to flush dirty victim page %d: %w", victimPage.GetPageID(), err)
// 		}
// 		victimPage.SetDirty(false) // Flushed
// 	}

// 	// 4. Remove victim page from pageTable and LRU list
// 	if victimPage.GetPageID() != InvalidPageID { // If it held a valid page
// 		delete(bpm.pageTable, victimPage.GetPageID())
// 		if victimPage.lruElement != nil {
// 			bpm.lruList.Remove(victimPage.lruElement)
// 			delete(bpm.lruMap, frameIdx) // Also remove from lruMap
// 		}
// 	}

// 	// 5. Reset victim page for new content
// 	victimPage.Reset() // Clears data and metadata

// 	// 6. Load new page data from disk
// 	log.Printf("DEBUG: Reading page %d from disk into frame %d", pageID, frameIdx)
// 	if err := bpm.diskManager.ReadPage(pageID, victimPage.GetData()); err != nil {
// 		// If read fails, return error. The frame is now empty and not tracked in pageTable.
// 		return nil, fmt.Errorf("failed to read page %d from disk: %w", pageID, err)
// 	}

// 	// 7. Update new page metadata and track in buffer pool
// 	victimPage.id = pageID
// 	victimPage.pinCount = 1
// 	victimPage.isDirty = false  // Just read from disk, so it's clean
// 	victimPage.lsn = InvalidLSN // LSN should be read from page data if stored, or set after WAL replay. For now, assume Invalid.

// 	bpm.pageTable[pageID] = frameIdx
// 	victimPage.lruElement = bpm.lruList.PushFront(frameIdx) // Add to front of LRU
// 	bpm.lruMap[frameIdx] = victimPage.lruElement
// 	log.Printf("DEBUG: Page %d loaded into frame %d, pinCount: %d", pageID, frameIdx, victimPage.GetPinCount())

// 	return victimPage, nil
// }

// // getVictimFrameInternal finds an unpinned page to evict.
// func (bpm *BufferPoolManager) getVictimFrameInternal() (int, error) {
// 	// First, look for an unpinned page in the LRU list (starting from the least recently used)
// 	for e := bpm.lruList.Back(); e != nil; e = e.Prev() {
// 		frameIdx := e.Value.(int)
// 		if bpm.pages[frameIdx].GetPinCount() == 0 {
// 			log.Printf("DEBUG: Found LRU victim frame %d (page %d)", frameIdx, bpm.pages[frameIdx].GetPageID())
// 			return frameIdx, nil
// 		}
// 	}

// 	// If no unpinned page found in LRU, check if there are any completely free frames (never used, or reset)
// 	// This covers the initial state where frames are not yet holding valid pages or added to LRU.
// 	for i := 0; i < bpm.poolSize; i++ {
// 		if bpm.pages[i].GetPageID() == InvalidPageID { // A truly empty frame
// 			log.Printf("DEBUG: Found empty frame %d as victim", i)
// 			return i, nil
// 		}
// 	}

// 	// If all pages are pinned or the pool is genuinely full with no evictable pages
// 	log.Printf("ERROR: Buffer pool is full, and all pages are pinned or none are evictable.")
// 	return -1, ErrBufferPoolFull
// }

// // UnpinPage decrements the pin count for a page. If isDirty is true, it marks the page as dirty.
// func (bpm *BufferPoolManager) UnpinPage(pageID PageID, isDirty bool) error {
// 	bpm.mu.Lock()
// 	defer bpm.mu.Unlock()
// 	if frameIdx, ok := bpm.pageTable[pageID]; ok {
// 		page := bpm.pages[frameIdx]
// 		if page.GetPinCount() == 0 {
// 			log.Printf("WARNING: Attempted to unpin page %d with pin count 0.", pageID)
// 			return fmt.Errorf("cannot unpin page %d with pin count 0", pageID)
// 		}
// 		page.Unpin()
// 		if isDirty {
// 			page.SetDirty(true)
// 		}
// 		log.Printf("DEBUG: Unpinned page %d (frame %d), new pinCount: %d, isDirty: %t", pageID, frameIdx, page.GetPinCount(), page.IsDirty())
// 		// If pin count becomes 0, it's now eligible for LRU eviction. Its LRU position
// 		// is managed by FetchPage moving to front. No explicit LRU removal/addition needed here.
// 		return nil
// 	}
// 	log.Printf("ERROR: Page %d not found in buffer pool to unpin.", pageID)
// 	return fmt.Errorf("%w: page %d not found to unpin", ErrPageNotFound, pageID)
// }

// // NewPage allocates a new page on disk and fetches it into the buffer pool.
// func (bpm *BufferPoolManager) NewPage() (*Page, PageID, error) {
// 	bpm.mu.Lock()
// 	defer bpm.mu.Unlock()

// 	// 1. Allocate a new page on disk
// 	newPageID, err := bpm.diskManager.AllocatePage()
// 	if err != nil {
// 		log.Printf("ERROR: Failed to allocate new page on disk: %v", err)
// 		return nil, InvalidPageID, err
// 	}
// 	log.Printf("DEBUG: Allocated new page %d on disk.", newPageID)

// 	// 2. Find a victim frame in the buffer pool for this new page
// 	frameIdx, err := bpm.getVictimFrameInternal()
// 	if err != nil {
// 		// IMPORTANT: If we can't get a frame, the allocated disk page is orphaned.
// 		// A robust system would deallocate this newPageID back to the disk free list.
// 		// For now, we log and return error.
// 		_ = bpm.diskManager.DeallocatePage(newPageID) // Attempt to deallocate (might be a no-op if not implemented)
// 		log.Printf("ERROR: Failed to get frame for new page %d, deallocated. Error: %v", newPageID, err)
// 		return nil, InvalidPageID, fmt.Errorf("failed to get frame for new page %d: %w", newPageID, err)
// 	}
// 	victimPage := bpm.pages[frameIdx]
// 	log.Printf("DEBUG: New page %d getting frame %d (old page %d)", newPageID, frameIdx, victimPage.GetPageID())

// 	// 3. If victim page is dirty, flush it before reuse
// 	if victimPage.IsDirty() && victimPage.GetPageID() != InvalidPageID {
// 		// TODO: WAL - flush logs for victimPage.GetLSN()
// 		log.Printf("DEBUG: Flushing dirty victim page %d for new page %d", victimPage.GetPageID(), newPageID)
// 		if err := bpm.diskManager.WritePage(victimPage.GetPageID(), victimPage.GetData()); err != nil {
// 			return nil, InvalidPageID, fmt.Errorf("failed to flush dirty victim %d for new page: %w", victimPage.GetPageID(), err)
// 		}
// 		victimPage.SetDirty(false)
// 	}

// 	// 4. Remove victim page from pageTable and LRU list
// 	if victimPage.GetPageID() != InvalidPageID {
// 		delete(bpm.pageTable, victimPage.GetPageID())
// 		if victimPage.lruElement != nil {
// 			bpm.lruList.Remove(victimPage.lruElement)
// 			delete(bpm.lruMap, frameIdx)
// 		}
// 	}

// 	// 5. Reset and initialize the new page
// 	victimPage.Reset() // Clears data and metadata
// 	victimPage.id = newPageID
// 	victimPage.pinCount = 1     // Automatically pinned when created
// 	victimPage.isDirty = true   // New page, considered dirty as it will be written to for the first time.
// 	victimPage.lsn = InvalidLSN // Will be set when first log record is written for this page.

// 	// 6. Track the new page in buffer pool
// 	bpm.pageTable[newPageID] = frameIdx
// 	victimPage.lruElement = bpm.lruList.PushFront(frameIdx) // Add to front of LRU
// 	bpm.lruMap[frameIdx] = victimPage.lruElement
// 	log.Printf("DEBUG: New page %d loaded into frame %d, pinCount: %d, isDirty: %t", newPageID, frameIdx, victimPage.GetPinCount(), victimPage.IsDirty())

// 	return victimPage, newPageID, nil
// }

// // FlushPage flushes a specific page to disk if it's dirty.
// func (bpm *BufferPoolManager) FlushPage(pageID PageID) error {
// 	bpm.mu.Lock()
// 	defer bpm.mu.Unlock()
// 	if frameIdx, ok := bpm.pageTable[pageID]; ok {
// 		page := bpm.pages[frameIdx]
// 		if page.IsDirty() {
// 			// TODO: WAL - Ensure log records up to page.GetLSN() are flushed before this.
// 			// if lm, ok := bpm.logManager.(*LogManager); ok { lm.Flush(page.GetLSN()) }
// 			log.Printf("DEBUG: Flushing page %d (frame %d) to disk", pageID, frameIdx)
// 			if err := bpm.diskManager.WritePage(page.GetPageID(), page.GetData()); err != nil {
// 				log.Printf("ERROR: Failed to flush page %d: %v", pageID, err)
// 				return err
// 			}
// 			page.SetDirty(false) // Mark clean after successful flush
// 		} else {
// 			log.Printf("DEBUG: Page %d (frame %d) is clean, no flush needed.", pageID, frameIdx)
// 		}
// 		return nil
// 	}
// 	log.Printf("WARNING: Attempted to flush page %d not found in buffer pool.", pageID)
// 	return fmt.Errorf("%w: page %d not found to flush", ErrPageNotFound, pageID)
// }

// // FlushAllPages flushes all dirty pages in the buffer pool to disk.
// func (bpm *BufferPoolManager) FlushAllPages() error {
// 	bpm.mu.Lock()
// 	defer bpm.mu.Unlock()
// 	var firstErr error

// 	// TODO: WAL - Before flushing all, ensure all pending log records are flushed.
// 	// if lm, ok := bpm.logManager.(*LogManager); ok { lm.FlushAll() }

// 	log.Println("DEBUG: Flushing all dirty pages from buffer pool...")
// 	for i, page := range bpm.pages { // Iterate over all frames in the pool
// 		if page.GetPageID() != InvalidPageID && page.IsDirty() {
// 			// TODO: WAL - Ensure log records up to page.GetLSN() are flushed.
// 			log.Printf("DEBUG: Flushing page %d (frame %d) during FlushAllPages", page.GetPageID(), i)
// 			err := bpm.diskManager.WritePage(page.GetPageID(), page.GetData())
// 			if err != nil {
// 				if firstErr == nil {
// 					firstErr = err // Store the first error encountered
// 				}
// 				fmt.Fprintf(os.Stderr, "Error flushing page %d: %v\n", page.GetPageID(), err)
// 			} else {
// 				page.SetDirty(false) // Mark clean after successful flush
// 			}
// 		}
// 	}
// 	// Finally, sync the disk manager to ensure all writes are durable
// 	if err := bpm.diskManager.Sync(); err != nil {
// 		if firstErr == nil {
// 			firstErr = err
// 		}
// 		fmt.Fprintf(os.Stderr, "Error syncing disk manager during FlushAllPages: %v\n", err)
// 	}
// 	log.Println("DEBUG: Finished FlushAllPages.")
// 	return firstErr
// }
