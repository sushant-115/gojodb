package memtable

import (
	"bytes"
	"container/list" // For LRU
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"

	flushmanager "github.com/sushant-115/gojodb/core/write_engine/flush_manager"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	"go.uber.org/zap"
)

// BufferPoolManager manages in-memory pages (frames) and interacts with the DiskManager.
// It implements a sharded LRU eviction policy.
//
// Each shard owns a disjoint subset of frames, tracked in freeStack (unloaded) and lruList
// (loaded, ordered least-to-most recently used). On a cache miss, claimFrame tries the
// target shard first (only that shard's mutex required). The global evictMu is only acquired
// when the target shard has no evictable frames and a cross-shard steal is needed.

const numBPMShards = 16

type bpmShard struct {
	mu        sync.Mutex
	pages     map[pagemanager.PageID]int // PageID → frame index
	lruList   *list.List
	lruMap    map[int]*list.Element // frame index → list element
	freeStack []int                 // frames with no page loaded, available immediately
}

// victimFrame atomically claims an evictable frame from this shard.
// It removes the frame from all shard structures so that no other goroutine can select it.
// Returns (frameIdx, evictedPageID). Returns (-1, InvalidPageID) when the shard is fully pinned.
// MUST be called with s.mu held.
func (s *bpmShard) victimFrame(allPages []*pagemanager.Page) (int, pagemanager.PageID) {
	// Free (unloaded) frames are the cheapest — no dirty flush or disk read needed.
	if n := len(s.freeStack); n > 0 {
		fi := s.freeStack[n-1]
		s.freeStack = s.freeStack[:n-1]
		return fi, pagemanager.InvalidPageID
	}
	// Walk the LRU from the least-recently-used end.
	for e := s.lruList.Back(); e != nil; e = e.Prev() {
		fi := e.Value.(int)
		if allPages[fi].GetPinCount() == 0 {
			pid := allPages[fi].GetPageID()
			// Atomically remove the frame from all shard structures.
			s.lruList.Remove(e)
			delete(s.lruMap, fi)
			if pid != pagemanager.InvalidPageID {
				delete(s.pages, pid)
			}
			return fi, pid
		}
	}
	return -1, pagemanager.InvalidPageID
}

type BufferPoolManager struct {
	diskManager *flushmanager.DiskManager
	logManager  *wal.LogManager
	poolSize    int
	pages       []*pagemanager.Page // Page frames (indexed by frame ID)
	shards      [numBPMShards]bpmShard
	evictMu     sync.Mutex    // held only during cross-shard steals
	clockHand   atomic.Uint32 // round-robin starting shard for cross-shard victim search
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
// Frames are distributed evenly across shards so that each shard can immediately
// serve cache-miss requests from its own freeStack without touching evictMu.
func NewBufferPoolManager(poolSize int, diskManager *flushmanager.DiskManager, logManager *wal.LogManager, logger *zap.Logger) *BufferPoolManager {
	if diskManager == nil {
		log.Fatal("NewBufferPoolManager: diskManager cannot be nil")
	}
	bpm := &BufferPoolManager{
		diskManager: diskManager,
		logManager:  logManager,
		poolSize:    poolSize,
		pages:       make([]*pagemanager.Page, poolSize),
		pageSize:    diskManager.GetPageSize(),
		logger:      logger,
	}
	framesPerShard := poolSize / numBPMShards
	for i := 0; i < numBPMShards; i++ {
		base := i * framesPerShard
		end := base + framesPerShard
		if i == numBPMShards-1 {
			end = poolSize // last shard absorbs any remainder
		}
		free := make([]int, 0, end-base)
		for fi := base; fi < end; fi++ {
			free = append(free, fi)
		}
		bpm.shards[i] = bpmShard{
			pages:     make(map[pagemanager.PageID]int),
			lruList:   list.New(),
			lruMap:    make(map[int]*list.Element),
			freeStack: free,
		}
	}
	for i := 0; i < poolSize; i++ {
		bpm.pages[i] = pagemanager.NewPage(pagemanager.InvalidPageID, bpm.pageSize)
	}
	bpm.logger.Info("BufferPoolManager initialized", zap.Int("pool_size", poolSize), zap.Int("page_size", bpm.pageSize), zap.Int("shards", numBPMShards), zap.Int("frames_per_shard", framesPerShard))
	return bpm
}

// claimFrame selects and atomically removes a frame from the buffer pool for use by the caller.
// It tries the target shard first (no global lock needed). If the target shard has no evictable
// frames, it acquires evictMu and steals from other shards in round-robin order.
// The returned frame has already been removed from its previous shard's structures; on failure
// the caller MUST return the frame to the target shard's freeStack.
func (bpm *BufferPoolManager) claimFrame(targetShard int) (frameIdx int, evictedPageID pagemanager.PageID, err error) {
	s := &bpm.shards[targetShard]

	// Fast path: target shard has a free or evictable frame — no global lock.
	s.mu.Lock()
	fi, pid := s.victimFrame(bpm.pages)
	s.mu.Unlock()
	if fi >= 0 {
		bpm.logger.Debug("Claimed frame from target shard", zap.Int("shard", targetShard), zap.Int("frame_id", fi), zap.Uint64("evicted_page", uint64(pid)))
		return fi, pid, nil
	}

	// Slow path: target shard fully pinned — steal from another shard.
	// Use a clock hand to avoid always hammering the same shard.
	bpm.evictMu.Lock()
	defer bpm.evictMu.Unlock()

	start := int(bpm.clockHand.Add(1) % numBPMShards)
	for i := 0; i < numBPMShards; i++ {
		si := (start + i) % numBPMShards
		if si == targetShard {
			continue // already tried
		}
		os := &bpm.shards[si]
		os.mu.Lock()
		fi, pid = os.victimFrame(bpm.pages)
		os.mu.Unlock()
		if fi >= 0 {
			bpm.logger.Debug("Stole frame from shard", zap.Int("from_shard", si), zap.Int("target_shard", targetShard), zap.Int("frame_id", fi))
			return fi, pid, nil
		}
	}
	bpm.logger.Error("Buffer pool full and all pages pinned")
	return -1, pagemanager.InvalidPageID, flushmanager.ErrBufferPoolFull
}

// FetchPage retrieves a page from the buffer pool. If not present, it fetches from disk.
// It pins the page, increments its pin count, and moves it to the front of the LRU list.
func (bpm *BufferPoolManager) FetchPage(pageID pagemanager.PageID) (*pagemanager.Page, error) {
	si := shardFor(pageID)
	s := &bpm.shards[si]

	// Fast path: cache hit — only the target shard's mutex is needed.
	s.mu.Lock()
	if frameIdx, ok := s.pages[pageID]; ok {
		page := bpm.pages[frameIdx]
		page.Pin()
		if page.GetLruElement() != nil {
			s.lruList.MoveToFront(page.GetLruElement())
		}
		s.mu.Unlock()
		bpm.logger.Debug("Page found in buffer pool", zap.Any("page_id", pageID), zap.Int("frame_index", frameIdx), zap.Uint32("pin_count", page.GetPinCount()))
		return page, nil
	}
	s.mu.Unlock()

	// Slow path: cache miss — claim a frame (atomically removed from its shard's structures).
	frameIdx, victimPageID, err := bpm.claimFrame(si)
	if err != nil {
		bpm.logger.Error("Failed to claim frame", zap.Any("page_id", pageID), zap.Error(err))
		return nil, err
	}
	victimPage := bpm.pages[frameIdx]
	bpm.logger.Debug("Cache miss: evicting frame", zap.Any("page_id", pageID), zap.Int("frame", frameIdx), zap.Uint64("evicted_page", uint64(victimPageID)))

	// Flush dirty victim to disk. The frame is already invisible to other goroutines
	// (removed from shard structures), so no lock is needed here.
	if victimPage.IsDirty() && victimPageID != pagemanager.InvalidPageID {
		if bpm.logManager != nil && victimPage.GetLSN() != pagemanager.InvalidLSN {
			if err := bpm.logManager.Sync(); err != nil {
				bpm.logger.Error("WAL sync failed before evicting dirty page", zap.Uint64("victim", uint64(victimPageID)), zap.Error(err))
				s.mu.Lock()
				s.freeStack = append(s.freeStack, frameIdx) // return claimed frame
				s.mu.Unlock()
				return nil, fmt.Errorf("failed to flush log for victim page %d: %w", victimPageID, err)
			}
		}
		if err := bpm.diskManager.WritePage(victimPageID, victimPage.GetData()); err != nil {
			s.mu.Lock()
			s.freeStack = append(s.freeStack, frameIdx)
			s.mu.Unlock()
			return nil, fmt.Errorf("failed to flush dirty victim page %d: %w", victimPageID, err)
		}
		victimPage.SetDirty(false)
	}
	victimPage.Reset()

	// Load new page from disk (no locks held — I/O is slow).
	bpm.logger.Debug("Reading page from disk into frame", zap.Any("page_id", pageID), zap.Int("frame", frameIdx))
	if err := bpm.diskManager.ReadPage(pageID, victimPage.GetData()); err != nil {
		s.mu.Lock()
		s.freeStack = append(s.freeStack, frameIdx)
		s.mu.Unlock()
		return nil, fmt.Errorf("failed to read page %d from disk: %w", pageID, err)
	}

	// Register the newly loaded page in the target shard.
	victimPage.SetPageID(pageID)
	victimPage.SetPinCount(1)
	victimPage.SetDirty(false)
	victimPage.SetLSN(pagemanager.InvalidLSN)

	s.mu.Lock()
	// Double-check: another goroutine may have raced and loaded the same page.
	if existingFrame, ok := s.pages[pageID]; ok {
		s.freeStack = append(s.freeStack, frameIdx) // return our frame
		page := bpm.pages[existingFrame]
		page.Pin()
		if page.GetLruElement() != nil {
			s.lruList.MoveToFront(page.GetLruElement())
		}
		s.mu.Unlock()
		return page, nil
	}
	s.pages[pageID] = frameIdx
	victimPage.SetLruElement(s.lruList.PushFront(frameIdx))
	s.lruMap[frameIdx] = victimPage.GetLruElement()
	s.mu.Unlock()

	return victimPage, nil
}

// getVictimFrameInternal is superseded by claimFrame + bpmShard.victimFrame.
// Kept as a no-op shim in case any internal call site was missed during refactor.

// UnpinPage decrements the pin count for a page. If isDirty is true, it marks the page as dirty.
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
			// Guard against eviction + reuse of the frame while the lock was dropped:
			// only update LSN if this frame still belongs to the same page.
			if curr, still := s.pages[pageID]; still && curr == frameIdx {
				page.SetLSN(pagemanager.LSN(lsn))
			}
			s.mu.Unlock()
			bpm.logger.Debug("Unpinned dirty page", zap.Uint64("page_id", uint64(pageID)), zap.Int("frame_id", frameIdx))
			return nil
		}
	}
	s.mu.Unlock()
	bpm.logger.Debug("Unpinned page", zap.Uint64("page_id", uint64(pageID)), zap.Int("frame_id", frameIdx), zap.Uint32("pin_count", page.GetPinCount()), zap.Bool("is_dirty", page.IsDirty()))
	return nil
}

// NewPage allocates a new page on disk and fetches it into the buffer pool.
// It pins the new page and marks it dirty.
func (bpm *BufferPoolManager) NewPage() (*pagemanager.Page, pagemanager.PageID, error) {
	// Allocate a disk page ID first (outside any lock — disk metadata operation).
	newPageID, err := bpm.diskManager.AllocatePage()
	if err != nil {
		bpm.logger.Error("Failed to allocate new page on disk", zap.Error(err))
		return nil, pagemanager.InvalidPageID, err
	}
	bpm.logger.Debug("Allocated new page on disk", zap.Uint64("page_id", uint64(newPageID)))

	si := shardFor(newPageID)
	s := &bpm.shards[si]

	// Claim a frame from the target shard (or steal from another).
	frameIdx, victimPageID, err := bpm.claimFrame(si)
	if err != nil {
		_ = bpm.diskManager.DeallocatePage(newPageID)
		bpm.logger.Error("Failed to get frame for new page, deallocated", zap.Uint64("page_id", uint64(newPageID)), zap.Error(err))
		return nil, pagemanager.InvalidPageID, fmt.Errorf("failed to get frame for new page %d: %w", newPageID, err)
	}
	victimPage := bpm.pages[frameIdx]
	bpm.logger.Debug("New page getting frame", zap.Uint64("new_page_id", uint64(newPageID)), zap.Int("frame_id", frameIdx), zap.Uint64("victim_page_id", uint64(victimPageID)))

	// Flush dirty victim outside any lock.
	if victimPage.IsDirty() && victimPageID != pagemanager.InvalidPageID {
		if bpm.logManager != nil && victimPage.GetLSN() != pagemanager.InvalidLSN {
			if err := bpm.logManager.Sync(); err != nil {
				bpm.logger.Error("WAL sync failed before evicting dirty page", zap.Uint64("victim", uint64(victimPageID)), zap.Error(err))
				s.mu.Lock()
				s.freeStack = append(s.freeStack, frameIdx)
				s.mu.Unlock()
				return nil, pagemanager.InvalidPageID, fmt.Errorf("failed to flush log for victim page %d: %w", victimPageID, err)
			}
		}
		bpm.logger.Debug("Flushing dirty victim page for new page", zap.Uint64("victim_page_id", uint64(victimPageID)), zap.Uint64("new_page_id", uint64(newPageID)))
		if err := bpm.diskManager.WritePage(victimPageID, victimPage.GetData()); err != nil {
			s.mu.Lock()
			s.freeStack = append(s.freeStack, frameIdx)
			s.mu.Unlock()
			return nil, pagemanager.InvalidPageID, fmt.Errorf("failed to flush dirty victim %d for new page: %w", victimPageID, err)
		}
		victimPage.SetDirty(false)
	}

	victimPage.Reset()
	victimPage.SetPageID(newPageID)
	victimPage.SetPinCount(1)
	victimPage.SetDirty(true)
	victimPage.SetLSN(pagemanager.InvalidLSN)

	s.mu.Lock()
	s.pages[newPageID] = frameIdx
	victimPage.SetLruElement(s.lruList.PushFront(frameIdx))
	s.lruMap[frameIdx] = victimPage.GetLruElement()
	s.mu.Unlock()

	bpm.logger.Debug("New page loaded into frame", zap.Uint64("page_id", uint64(newPageID)), zap.Int("frame_id", frameIdx), zap.Uint32("pin_count", victimPage.GetPinCount()))

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
	// Snapshot LSN under lock so we can detect concurrent modifications after the flush.
	lsnAtFlushStart := page.GetLSN()
	s.mu.Unlock()

	// WAL flush and disk write outside shard lock (slow I/O).
	if bpm.logManager != nil && lsnAtFlushStart != pagemanager.InvalidLSN {
		if err := bpm.logManager.Sync(); err != nil {
			bpm.logger.Error("Failed to flush log before flushing page", zap.Uint64("page_id", uint64(pageID)), zap.Error(err))
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

	// Only clear dirty if the page hasn't been modified since we started the flush and
	// the frame still belongs to this page (wasn't evicted and reused while I/O ran).
	s.mu.Lock()
	if curr, still := s.pages[pageID]; still && curr == frameIdx && page.GetLSN() == lsnAtFlushStart {
		page.SetDirty(false)
	}
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

	// Collect dirty frames per shard under the shard lock (avoids data races on page metadata).
	type dirtyEntry struct {
		frameIdx int
		pageID   pagemanager.PageID
		lsn      pagemanager.LSN
		si       int
	}
	var dirty []dirtyEntry
	for i := 0; i < numBPMShards; i++ {
		s := &bpm.shards[i]
		s.mu.Lock()
		for pid, fi := range s.pages {
			pg := bpm.pages[fi]
			if pg.IsDirty() {
				dirty = append(dirty, dirtyEntry{fi, pid, pg.GetLSN(), i})
			}
		}
		s.mu.Unlock()
	}

	// Flush each dirty page outside shard locks (slow I/O).
	bpm.logger.Debug("Flushing dirty pages", zap.Int("count", len(dirty)))
	for _, d := range dirty {
		pg := bpm.pages[d.frameIdx]
		if err := bpm.diskManager.WritePage(d.pageID, pg.GetData()); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			fmt.Fprintf(os.Stderr, "Error flushing page %d: %v\n", d.pageID, err)
			continue
		}
		// Clear dirty only if the frame still holds the same page and LSN.
		s := &bpm.shards[d.si]
		s.mu.Lock()
		if curr, still := s.pages[d.pageID]; still && curr == d.frameIdx && pg.GetLSN() == d.lsn {
			pg.SetDirty(false)
		}
		s.mu.Unlock()
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
// The freed frame is returned to the shard's freeStack for immediate reuse.
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
	// Return the frame to the free pool so it can be claimed without LRU eviction.
	s.freeStack = append(s.freeStack, frameID)
	s.mu.Unlock()
	bpm.logger.Debug("Invalidated page from buffer pool", zap.Uint64("page_id", uint64(pageID)), zap.Int("frame_id", frameID))
}

func (bpm *BufferPoolManager) GetPageSize() int {
	return bpm.pageSize
}
