package memtable

import (
	"bytes"
	"container/list" // For LRU
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	flushmanager "github.com/sushant-115/gojodb/core/write_engine/flush_manager"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	"go.uber.org/zap"
	// Added for time.Ticker
)

// BufferPoolManager manages in-memory pages (frames) and interacts with the DiskManager.
// It implements a simple LRU (Least Recently Used) eviction policy.
//
// The page table is sharded across numBPMShards buckets to reduce lock contention
// for concurrent FetchPage / UnpinPage calls on different page IDs.
// Each shard has its own mutex, page table map, and LRU structures.
// A global eviction mutex (evictMu) serialises victim selection across shards.

const numBPMShards = 16

type bpmShard struct {
	mu      sync.Mutex
	pages   map[pagemanager.PageID]int // PageID → frame index
	lruList *list.List
	lruMap  map[int]*list.Element // frame index → list element
}

type BufferPoolManager struct {
	diskManager *flushmanager.DiskManager
	logManager  *wal.LogManager // Changed from interface{} to *wal.LogManager
	poolSize    int
	pages       []*pagemanager.Page // Page frames (indexed by frame ID)
	shards      [numBPMShards]bpmShard
	evictMu     sync.Mutex // Serialises victim selection across shards
	pageSize    int
	numPages    pagemanager.PageID
	logType     wal.LogType
	logger      *zap.Logger
}

// shardFor returns the shard index for a given page ID.
func shardFor(pageID pagemanager.PageID) int {
	return int(pageID) % numBPMShards
}

func (bpm *BufferPoolManager) GetNumPages() pagemanager.PageID {
	return bpm.numPages
}

func (bpm *BufferPoolManager) SetNextPageID(pageID pagemanager.PageID) {
	bpm.numPages = pageID
}

// NewBufferPoolManager creates and initializes a new BufferPoolManager.
func NewBufferPoolManager(poolSize int, diskManager *flushmanager.DiskManager, logManager *wal.LogManager, logger *zap.Logger) *BufferPoolManager {
	if diskManager == nil {
		log.Fatal("NewBufferPoolManager: diskManager cannot be nil")
	}
	bpm := &BufferPoolManager{
		diskManager: diskManager,
		logManager:  logManager, // Now directly *wal.LogManager
		poolSize:    poolSize,
		pages:       make([]*pagemanager.Page, poolSize),
		pageSize:    diskManager.GetPageSize(),
		logger:      logger,
	}
	for i := 0; i < numBPMShards; i++ {
		bpm.shards[i] = bpmShard{
			pages:   make(map[pagemanager.PageID]int),
			lruList: list.New(),
			lruMap:  make(map[int]*list.Element),
		}
	}
	for i := 0; i < poolSize; i++ {
		bpm.pages[i] = pagemanager.NewPage(pagemanager.InvalidPageID, bpm.pageSize)
	}
	bpm.logger.Info("INFO: BufferPoolManager initialized with pool size %d, page size %d", zap.Int("pool_size", poolSize), zap.Int("page_size", bpm.pageSize))
	return bpm
}

// FetchPage retrieves a page from the buffer pool. If not present, it fetches from disk.
// It pins the page, increments its pin count, and moves it to the front of the LRU list.
func (bpm *BufferPoolManager) FetchPage(pageID pagemanager.PageID) (*pagemanager.Page, error) {
	s := &bpm.shards[shardFor(pageID)]
	s.mu.Lock()

	// 1. Check if page is already in the buffer pool
	if frameIdx, ok := s.pages[pageID]; ok {
		page := bpm.pages[frameIdx]
		page.Pin()
		// Move to front of LRU to mark as recently used
		if page.GetLruElement() != nil {
			s.lruList.MoveToFront(page.GetLruElement())
		}
		s.mu.Unlock()
		bpm.logger.Debug("Page found in buffer pool", zap.Any("page_id", pageID), zap.Int("frame_index", frameIdx), zap.Uint32("pin_count", page.GetPinCount()))
		return page, nil
	}
	s.mu.Unlock()

	// 2. Page not in pool – acquire eviction lock and find a victim frame.
	bpm.evictMu.Lock()
	defer bpm.evictMu.Unlock()

	// Re-check under evictMu (another goroutine may have loaded the page).
	s.mu.Lock()
	if frameIdx, ok := s.pages[pageID]; ok {
		page := bpm.pages[frameIdx]
		page.Pin()
		if page.GetLruElement() != nil {
			s.lruList.MoveToFront(page.GetLruElement())
		}
		s.mu.Unlock()
		return page, nil
	}
	s.mu.Unlock()

	frameIdx, victimPageID, err := bpm.getVictimFrameInternal()
	if err != nil {
		bpm.logger.Error("Failed to get victim frame", zap.Any("page_id", pageID), zap.Error(err))
		return nil, err
	}
	victimPage := bpm.pages[frameIdx]
	bpm.logger.Debug("Page not found in buffer pool, evicting old_page", zap.Any("page_id", pageID), zap.Int("frame_index", frameIdx), zap.Uint32("old_page", uint32(victimPage.GetPageID())))

	// 3. If victim page is dirty, flush it to disk (outside any shard lock – disk I/O is slow).
	if victimPage.IsDirty() && victimPageID != pagemanager.InvalidPageID {
		if bpm.logManager != nil && victimPage.GetLSN() != pagemanager.InvalidLSN {
			if err := bpm.logManager.Sync(); err != nil {
				bpm.logger.Error("Failed to flush log for victim page before flushing page:", zap.Any("victim_page_id", victimPageID), zap.Uint64("lsn", uint64(victimPage.GetLSN())), zap.Error(err))
				return nil, fmt.Errorf("failed to flush log for victim page %d: %w", victimPageID, err)
			}
		}
		if err := bpm.diskManager.WritePage(victimPageID, victimPage.GetData()); err != nil {
			return nil, fmt.Errorf("failed to flush dirty victim page %d: %w", victimPageID, err)
		}
		victimPage.SetDirty(false)
	}

	// 4. Remove victim page from its shard.
	if victimPageID != pagemanager.InvalidPageID {
		vs := &bpm.shards[shardFor(victimPageID)]
		vs.mu.Lock()
		delete(vs.pages, victimPageID)
		if victimPage.GetLruElement() != nil {
			vs.lruList.Remove(victimPage.GetLruElement())
			delete(vs.lruMap, frameIdx)
		}
		vs.mu.Unlock()
	}

	// 5. Reset victim page for new content.
	victimPage.Reset()

	// 6. Load new page data from disk (outside shard lock – I/O).
	bpm.logger.Debug("reading page from disk into frame", zap.Any("page_id", pageID), zap.Any("frame_idx", frameIdx))
	if err := bpm.diskManager.ReadPage(pageID, victimPage.GetData()); err != nil {
		return nil, fmt.Errorf("failed to read page %d from disk: %w", pageID, err)
	}

	// 7. Update metadata and register in the target shard.
	victimPage.SetPageID(pageID)
	victimPage.SetPinCount(1)
	victimPage.SetDirty(false)
	victimPage.SetLSN(pagemanager.InvalidLSN)

	s.mu.Lock()
	s.pages[pageID] = frameIdx
	victimPage.SetLruElement(s.lruList.PushFront(frameIdx))
	s.lruMap[frameIdx] = victimPage.GetLruElement()
	s.mu.Unlock()

	return victimPage, nil
}

// getVictimFrameInternal finds an unpinned frame to evict across all shards.
// MUST be called with bpm.evictMu held.
func (bpm *BufferPoolManager) getVictimFrameInternal() (frameIdx int, evictedPageID pagemanager.PageID, err error) {
	// Scan all shards in order, checking their LRU tails.
	for i := 0; i < numBPMShards; i++ {
		s := &bpm.shards[i]
		s.mu.Lock()
		for e := s.lruList.Back(); e != nil; e = e.Prev() {
			fi := e.Value.(int)
			if bpm.pages[fi].GetPinCount() == 0 {
				pid := bpm.pages[fi].GetPageID()
				s.mu.Unlock()
				bpm.logger.Debug("Found LRU victim", zap.Int("frame_id", fi), zap.Uint64("page_id", uint64(pid)))
				return fi, pid, nil
			}
		}
		s.mu.Unlock()
	}

	// Fall back: look for completely free frames (never loaded).
	for i := 0; i < bpm.poolSize; i++ {
		if bpm.pages[i].GetPageID() == pagemanager.InvalidPageID {
			bpm.logger.Debug("Found empty frame as victim", zap.Int("frame_id", i))
			return i, pagemanager.InvalidPageID, nil
		}
	}

	bpm.logger.Error("Buffer pool full and all pages pinned")
	return -1, pagemanager.InvalidPageID, flushmanager.ErrBufferPoolFull
}

// UnpinPage decrements the pin count for a page. If isDirty is true, it marks the page as dirty.
// This is a key point for logging page modifications.
func (bpm *BufferPoolManager) UnpinPage(pageID pagemanager.PageID, isDirty bool) error {
	s := &bpm.shards[shardFor(pageID)]
	s.mu.Lock()
	frameIdx, ok := s.pages[pageID]
	if !ok {
		s.mu.Unlock()
		bpm.logger.Error("Page not in buffer pool to unpin", zap.Uint64("page_id", uint64(pageID)))
		return fmt.Errorf("%w: page %d not found to unpin", flushmanager.ErrPageNotFound, pageID)
	}
	page := bpm.pages[frameIdx]
	if page.GetPinCount() == 0 {
		s.mu.Unlock()
		bpm.logger.Warn("Attempted to unpin page with zero pin count", zap.Uint64("page_id", uint64(pageID)))
		return fmt.Errorf("cannot unpin page %d with pin count 0", pageID)
	}
	page.Unpin()

	if isDirty {
		page.SetDirty(true)
		if bpm.logManager != nil {
			logRecord := &wal.LogRecord{
				TxnID:  0,
				Type:   wal.LogRecordTypeUpdate,
				PageID: pageID,
				Data:   page.GetData(),
			}
			s.mu.Unlock() // release shard lock before WAL append (may block)
			lsn, err := bpm.logManager.AppendRecord(context.TODO(), logRecord, bpm.logType)
			if err != nil {
				bpm.logger.Error("Failed to append log record for page update", zap.Uint64("page_id", uint64(pageID)), zap.Error(err))
				return fmt.Errorf("failed to append log record for page %d: %w", pageID, err)
			}
			s.mu.Lock()
			page.SetLSN(pagemanager.LSN(lsn))
			s.mu.Unlock()
			bpm.logger.Debug("Unpinned page", zap.Uint64("page_id", uint64(pageID)), zap.Int("frame_id", frameIdx), zap.Uint32("pin_count", page.GetPinCount()), zap.Bool("is_dirty", page.IsDirty()), zap.Uint64("lsn", uint64(page.GetLSN())))
			return nil
		}
	}
	s.mu.Unlock()
	bpm.logger.Debug("Unpinned page", zap.Uint64("page_id", uint64(pageID)), zap.Int("frame_id", frameIdx), zap.Uint32("pin_count", page.GetPinCount()), zap.Bool("is_dirty", page.IsDirty()), zap.Uint64("lsn", uint64(page.GetLSN())))
	return nil
}

// NewPage allocates a new page on disk and fetches it into the buffer pool.
// It pins the new page and marks it dirty.
func (bpm *BufferPoolManager) NewPage() (*pagemanager.Page, pagemanager.PageID, error) {
	// Allocate a disk page first (outside any shard lock).
	newPageID, err := bpm.diskManager.AllocatePage()
	if err != nil {
		bpm.logger.Error("Failed to allocate new page on disk", zap.Error(err))
		return nil, pagemanager.InvalidPageID, err
	}
	bpm.logger.Debug("Allocated new page on disk", zap.Uint64("page_id", uint64(newPageID)))

	bpm.evictMu.Lock()
	defer bpm.evictMu.Unlock()

	frameIdx, victimPageID, err := bpm.getVictimFrameInternal()
	if err != nil {
		_ = bpm.diskManager.DeallocatePage(newPageID)
		bpm.logger.Error("Failed to get frame for new page, deallocated", zap.Uint64("page_id", uint64(newPageID)), zap.Error(err))
		return nil, pagemanager.InvalidPageID, fmt.Errorf("failed to get frame for new page %d: %w", newPageID, err)
	}
	victimPage := bpm.pages[frameIdx]
	bpm.logger.Debug("New page getting frame", zap.Uint64("new_page_id", uint64(newPageID)), zap.Int("frame_id", frameIdx), zap.Uint64("victim_page_id", uint64(victimPageID)))

	// Flush dirty victim outside shard lock.
	if victimPage.IsDirty() && victimPageID != pagemanager.InvalidPageID {
		if bpm.logManager != nil && victimPage.GetLSN() != pagemanager.InvalidLSN {
			if err := bpm.logManager.Sync(); err != nil {
				bpm.logger.Error("Failed to flush log for victim page before eviction", zap.Uint64("victim_page_id", uint64(victimPageID)), zap.Uint64("lsn", uint64(victimPage.GetLSN())), zap.Error(err))
				return nil, pagemanager.InvalidPageID, fmt.Errorf("failed to flush log for victim page %d: %w", victimPageID, err)
			}
		}
		bpm.logger.Debug("Flushing dirty victim page for new page", zap.Uint64("victim_page_id", uint64(victimPageID)), zap.Uint64("new_page_id", uint64(newPageID)))
		if err := bpm.diskManager.WritePage(victimPageID, victimPage.GetData()); err != nil {
			return nil, pagemanager.InvalidPageID, fmt.Errorf("failed to flush dirty victim %d for new page: %w", victimPageID, err)
		}
		victimPage.SetDirty(false)
	}

	// Remove victim from its shard.
	if victimPageID != pagemanager.InvalidPageID {
		vs := &bpm.shards[shardFor(victimPageID)]
		vs.mu.Lock()
		delete(vs.pages, victimPageID)
		if victimPage.GetLruElement() != nil {
			vs.lruList.Remove(victimPage.GetLruElement())
			delete(vs.lruMap, frameIdx)
		}
		vs.mu.Unlock()
	}

	victimPage.Reset()
	victimPage.SetPageID(newPageID)
	victimPage.SetPinCount(1)
	victimPage.SetDirty(true)
	victimPage.SetLSN(pagemanager.InvalidLSN)

	ns := &bpm.shards[shardFor(newPageID)]
	ns.mu.Lock()
	ns.pages[newPageID] = frameIdx
	victimPage.SetLruElement(ns.lruList.PushFront(frameIdx))
	ns.lruMap[frameIdx] = victimPage.GetLruElement()
	ns.mu.Unlock()

	bpm.logger.Debug("New page loaded into frame", zap.Uint64("page_id", uint64(newPageID)), zap.Int("frame_id", frameIdx), zap.Uint32("pin_count", victimPage.GetPinCount()), zap.Bool("is_dirty", victimPage.IsDirty()))

	if bpm.logManager != nil {
		logRecord := &wal.LogRecord{
			TxnID:  0,
			Type:   wal.LogRecordTypeNewPage,
			PageID: newPageID,
		}
		lsn, err := bpm.logManager.AppendRecord(context.TODO(), logRecord, bpm.logType)
		if err != nil {
			bpm.logger.Error("Failed to append new page log record", zap.Uint64("page_id", uint64(newPageID)), zap.Error(err))
			return nil, pagemanager.InvalidPageID, fmt.Errorf("failed to append new page log record for %d: %w", newPageID, err)
		}
		victimPage.SetLSN(pagemanager.LSN(lsn))
	}

	return victimPage, newPageID, nil
}

// FlushPage flushes a specific page to disk if it's dirty.
// It ensures that all log records related to this page up to its LSN are durable first.
func (bpm *BufferPoolManager) FlushPage(pageID pagemanager.PageID) error {
	s := &bpm.shards[shardFor(pageID)]
	s.mu.Lock()
	frameIdx, ok := s.pages[pageID]
	if !ok {
		s.mu.Unlock()
		bpm.logger.Warn("Attempted to flush page not in buffer pool", zap.Uint64("page_id", uint64(pageID)))
		return fmt.Errorf("%w: page %d not found to flush", flushmanager.ErrPageNotFound, pageID)
	}
	page := bpm.pages[frameIdx]
	if !page.IsDirty() {
		s.mu.Unlock()
		bpm.logger.Debug("Page is clean, no flush needed", zap.Uint64("page_id", uint64(pageID)), zap.Int("frame_id", frameIdx))
		return nil
	}
	lsn := page.GetLSN()
	s.mu.Unlock()

	// WAL flush and disk write outside shard lock.
	if bpm.logManager != nil && lsn != pagemanager.InvalidLSN {
		if err := bpm.logManager.Sync(); err != nil {
			bpm.logger.Error("Failed to flush log before flushing page", zap.Uint64("page_id", uint64(pageID)), zap.Uint64("lsn", uint64(lsn)), zap.Error(err))
			return fmt.Errorf("failed to flush log for page %d: %w", pageID, err)
		}
	}
	bpm.logger.Debug("Flushing page to disk", zap.Uint64("page_id", uint64(pageID)), zap.Int("frame_id", frameIdx))
	if err := bpm.diskManager.WritePage(page.GetPageID(), page.GetData()); err != nil {
		bpm.logger.Error("Failed to flush page", zap.Uint64("page_id", uint64(pageID)), zap.Error(err))
		return err
	}
	dd := make([]byte, len(page.GetData()))
	if err := bpm.diskManager.ReadPage(page.GetPageID(), dd); err != nil {
		bpm.logger.Error("Error during read page", zap.Error(err))
	}
	bpm.logger.Debug("Page data integrity check", zap.Bool("is_equal", bytes.Equal(page.GetData(), dd)))

	s.mu.Lock()
	page.SetDirty(false)
	s.mu.Unlock()
	return nil
}

// FlushAllPages flushes all dirty pages in the buffer pool to disk.
// It ensures all pending log records are durable before flushing pages.
func (bpm *BufferPoolManager) FlushAllPages() error {
	var firstErr error

	if bpm.logManager != nil {
		bpm.logger.Debug("Flushing all pending log records before flushing pages")
		if firstErr = bpm.logManager.Sync(); firstErr != nil {
			bpm.logger.Error("Failed to flush all log records before flushing pages", zap.Error(firstErr))
		}
	}

	bpm.logger.Debug("Flushing all dirty pages from buffer pool")
	for i, page := range bpm.pages {
		if page.GetPageID() != pagemanager.InvalidPageID && page.IsDirty() {
			bpm.logger.Debug("Flushing page during FlushAllPages", zap.Uint64("page_id", uint64(page.GetPageID())), zap.Int("frame_id", i))
			err := bpm.diskManager.WritePage(page.GetPageID(), page.GetData())
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				fmt.Fprintf(os.Stderr, "Error flushing page %d: %v\n", page.GetPageID(), err)
			} else {
				page.SetDirty(false)
			}
		}
	}
	if err := bpm.diskManager.Sync(); err != nil {
		if firstErr == nil {
			firstErr = err
		}
		fmt.Fprintf(os.Stderr, "Error syncing disk manager during FlushAllPages: %v\n", err)
	}
	bpm.logger.Debug("Finished FlushAllPages")
	return firstErr
}

// InvalidatePage removes a page from the buffer pool's hash table,
// forcing a fresh read from DiskManager on the next FetchPage.
func (bpm *BufferPoolManager) InvalidatePage(pageID pagemanager.PageID) {
	s := &bpm.shards[shardFor(pageID)]
	s.mu.Lock()
	frameID, ok := s.pages[pageID]
	if !ok {
		s.mu.Unlock()
		bpm.logger.Debug("Attempted to invalidate page not in buffer pool", zap.Uint64("page_id", uint64(pageID)))
		return
	}
	page := bpm.pages[frameID]
	page.SetPinCount(0)
	page.SetDirty(false)
	page.SetLSN(pagemanager.InvalidLSN)
	page.SetPageID(pagemanager.InvalidPageID)
	delete(s.pages, pageID)
	if page.GetLruElement() != nil {
		s.lruList.Remove(page.GetLruElement())
		delete(s.lruMap, frameID)
		page.SetLruElement(nil)
	}
	s.mu.Unlock()
	bpm.logger.Debug("Invalidated page from buffer pool", zap.Uint64("page_id", uint64(pageID)), zap.Int("frame_id", frameID))
}

func (bpm *BufferPoolManager) GetPageSize() int {
	return bpm.pageSize
}
