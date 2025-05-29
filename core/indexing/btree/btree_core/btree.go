package btree_core

import (
	"bytes"
	"cmp"
	"container/list" // For LRU
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"slices"
	"sync"
	"unsafe" // For checking struct sizes at compile time (informational)
)

// --- Configuration & Constants ---

const (
	DefaultPageSize          = 4096       // Bytes
	FileHeaderPageID  PageID = 0          // Page ID for the database file header
	InvalidPageID     PageID = 0          // Also used for header, but generally indicates invalid/unallocated
	MaxFilenameLength        = 255        // Example limit
	DBMagic           uint32 = 0x6010DB00 // GoJoDB00

	// dbFileHeaderSize must be a fixed size that matches how it's written/read.
	// We'll ensure DBFileHeader struct matches this.
	dbFileHeaderSize = 128 // Increased size for more metadata
	checksumSize     = 4   // Size of CRC32 checksum
)

// --- Error Definitions ---

var (
	ErrKeyNotFound                 = errors.New("key not found")
	ErrKeyAlreadyExists            = errors.New("key already exists (for strict insert)")
	ErrInvalidDegree               = errors.New("btree degree must be at least 2")
	ErrNilKeyOrder                 = errors.New("keyOrder function must be provided")
	ErrPageNotFound                = errors.New("page not found in buffer pool")
	ErrBufferPoolFull              = errors.New("buffer pool is full and no pages can be evicted")
	ErrPagePinned                  = errors.New("page is pinned and cannot be evicted")
	ErrSerialization               = errors.New("error during serialization")
	ErrDeserialization             = errors.New("error during deserialization")
	ErrIO                          = errors.New("i/o error")
	ErrChecksumMismatch            = errors.New("page checksum mismatch, data corruption suspected")
	ErrInvalidPageData             = errors.New("invalid page data")
	ErrDBFileExists                = errors.New("database file already exists")
	ErrDBFileNotFound              = errors.New("database file not found")
	ErrUnsupportedKeyType          = errors.New("key type not supported for default serialization")
	ErrUnsupportedValueType        = errors.New("value type not supported for default serialization")
	ErrValueTooLargeForPage        = errors.New("value too large to fit in page with metadata")
	ErrMaxKeysOrChildrenExceeded   = errors.New("maximum number of keys or children per node exceeded for page size")
	ErrBTreeNotInitializedProperly = errors.New("btree not initialized properly (e.g. missing disk manager or buffer pool)")
	ErrLogRecordTooLarge           = errors.New("log record too large for log buffer")
	ErrLogFileError                = errors.New("log file operation error")
)

// --- Page Management ---

// PageID represents a unique identifier for a page on disk.
type PageID uint64

// Page represents an in-memory copy of a disk page.
type Page struct {
	id       PageID
	data     []byte
	pinCount uint32
	isDirty  bool
	lsn      LSN // LSN of the last log record that modified this page
	// For LRU
	lruElement *list.Element // Pointer to the element in LRU list
}

// NewPage creates a new Page instance.
func NewPage(id PageID, size int) *Page {
	return &Page{
		id:       id,
		data:     make([]byte, size),
		pinCount: 0,
		isDirty:  false,
		lsn:      InvalidLSN,
	}
}

func (p *Page) Reset() {
	p.id = InvalidPageID
	p.pinCount = 0
	p.isDirty = false
	p.lsn = InvalidLSN
	p.lruElement = nil
	// Zero out data to prevent old data from leaking, especially for security or integrity checks
	for i := range p.data {
		p.data[i] = 0
	}
}

func (p *Page) GetData() []byte   { return p.data }
func (p *Page) GetPageID() PageID { return p.id }
func (p *Page) IsDirty() bool     { return p.isDirty }
func (p *Page) Pin()              { p.pinCount++ }
func (p *Page) Unpin() {
	if p.pinCount > 0 {
		p.pinCount--
	}
}
func (p *Page) GetPinCount() uint32 { return p.pinCount }
func (p *Page) SetDirty(dirty bool) { p.isDirty = dirty }
func (p *Page) GetLSN() LSN         { return p.lsn }
func (p *Page) SetLSN(lsn LSN)      { p.lsn = lsn }

// --- DiskManager ---

// DBFileHeader defines the structure of the database file header.
// IMPORTANT: All fields must have fixed sizes to ensure binary.Read/Write consistency.
// We explicitly add padding to ensure the struct size matches dbFileHeaderSize.
type DBFileHeader struct {
	Magic          uint32
	Version        uint32
	PageSize       uint32
	RootPageID     PageID // uint64
	Degree         uint32
	TreeSize       uint64                               // Persisted tree size
	FreeListPageID PageID                               // uint64
	LastLSN        LSN                                  // uint64
	_              [dbFileHeaderSize - (4*4 + 4*8)]byte // Explicit padding: Corrected calculation
}

var expectedSize uintptr

// Ensure DBFileHeader is exactly dbFileHeaderSize bytes long at compile time.
// This helps prevent binary.Read/Write issues due to Go's struct padding.
func init() {
	// Calculate expected size of the defined fields
	expectedSize = unsafe.Sizeof(DBFileHeader{}.Magic) +
		unsafe.Sizeof(DBFileHeader{}.Version) +
		unsafe.Sizeof(DBFileHeader{}.PageSize) +
		unsafe.Sizeof(DBFileHeader{}.RootPageID) +
		unsafe.Sizeof(DBFileHeader{}.Degree) +
		unsafe.Sizeof(DBFileHeader{}.TreeSize) +
		unsafe.Sizeof(DBFileHeader{}.FreeListPageID) +
		unsafe.Sizeof(DBFileHeader{}.LastLSN)

	// This check will cause a compile-time error if the padding array size in the struct is wrong.
	// It ensures that the struct's size matches the declared dbFileHeaderSize.
	// if int(unsafe.Sizeof(DBFileHeader{})) != dbFileHeaderSize {
	// 	panic(fmt.Sprintf("DBFileHeader struct size mismatch! Expected %d bytes, got %d bytes. Adjust padding in DBFileHeader struct.", dbFileHeaderSize, unsafe.Sizeof(DBFileHeader{})))
	// }
}

type DiskManager struct {
	filePath string
	file     *os.File
	pageSize int
	numPages uint64 // Tracks total number of pages in the file (file size / page size)
	mu       sync.Mutex
	// For Free Space Management (conceptual)
	// freeListPageManager *FreeListPageManager // Would manage the on-disk free list
}

func NewDiskManager(filePath string, pageSize int) (*DiskManager, error) {
	if len(filePath) > MaxFilenameLength {
		return nil, fmt.Errorf("file path too long: %s", filePath)
	}
	return &DiskManager{
		filePath: filePath,
		pageSize: pageSize,
	}, nil
}

// OpenOrCreateFile attempts to open an existing database file or create a new one.
// The 'create' flag determines behavior if the file doesn't exist or already exists.
func (dm *DiskManager) OpenOrCreateFile(create bool, degree int, initialTreeSize uint64) (*DBFileHeader, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	var file *os.File
	var err error
	var header DBFileHeader

	// Check if file exists
	_, statErr := os.Stat(dm.filePath)

	if os.IsNotExist(statErr) {
		// File does not exist
		if !create {
			return nil, fmt.Errorf("%w: %s", ErrDBFileNotFound, dm.filePath)
		}
		// Create a new file, ensuring it doesn't exist already (O_EXCL)
		file, err = os.OpenFile(dm.filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			return nil, fmt.Errorf("%w: creating file %s: %v", ErrIO, dm.filePath, err)
		}
		dm.file = file

		// Initialize header for a new database file
		header = DBFileHeader{
			Magic:          DBMagic,
			Version:        1,
			PageSize:       uint32(dm.pageSize),
			RootPageID:     InvalidPageID, // Will be updated when B-tree root is created
			Degree:         uint32(degree),
			TreeSize:       initialTreeSize,
			FreeListPageID: InvalidPageID, // To be initialized later
			LastLSN:        InvalidLSN,
		}

		// Write the new header to page 0
		if err := dm.writeHeader(&header); err != nil {
			_ = os.Remove(dm.filePath) // Clean up on error
			return nil, fmt.Errorf("failed to write initial header: %w", err)
		}

		// --- CRITICAL FIX: Initialize numPages correctly after header write ---
		// PageID 0 is now occupied by the header. Subsequent allocations start from PageID 1.
		dm.numPages = 1
		// Removed: `if _, err := dm.allocateRawPageInternal(); err != nil { ... }`
		// This call was overwriting the header. The B-tree's NewPage() will allocate PageID 1.
		// --- END CRITICAL FIX ---

	} else if statErr == nil { // File exists
		if create {
			return nil, fmt.Errorf("%w: %s", ErrDBFileExists, dm.filePath)
		}
		// Open existing file
		file, err = os.OpenFile(dm.filePath, os.O_RDWR, 0666)
		if err != nil {
			return nil, fmt.Errorf("%w: opening file %s: %v", ErrIO, dm.filePath, err)
		}
		dm.file = file

		// Read and validate the header
		if err := dm.readHeader(&header); err != nil {
			dm.Close() // Close file if header read fails
			return nil, fmt.Errorf("failed to read database header: %w", err)
		}

		// --- CRITICAL MAGIC NUMBER CHECK ---
		if header.Magic != DBMagic {
			log.Printf("DEBUG: Magic number mismatch. Expected: 0x%x, Got: 0x%x for file: %s", DBMagic, header.Magic, dm.filePath)
			dm.Close() // Close file on magic number mismatch
			return nil, fmt.Errorf("invalid database file magic number")
		}
		// --- END CRITICAL MAGIC NUMBER CHECK ---

		// Validate page size from header
		if header.PageSize != uint32(dm.pageSize) {
			// If allowing dynamic page size on open, adjust dm.pageSize here.
			// For now, assume configured pageSize must match file's pageSize.
			dm.Close() // Close file on page size mismatch
			return nil, fmt.Errorf("database file page size (%d) does not match configured page size (%d)", header.PageSize, dm.pageSize)
		}
	} else {
		// Other file system errors
		return nil, fmt.Errorf("%w: stating file %s: %v", ErrIO, dm.filePath, statErr)
	}

	// Update DiskManager's internal state based on file size
	fi, err := dm.file.Stat()
	if err != nil {
		dm.Close()
		return nil, fmt.Errorf("%w: getting file info: %v", ErrIO, err)
	}
	// This `dm.numPages` calculation is for existing files. For new files, it's set to 1 above.
	// This ensures `dm.numPages` accurately reflects the highest allocated page ID + 1.
	if dm.numPages == 0 { // Only update if it wasn't set to 1 for a new file
		dm.numPages = uint64(fi.Size()) / uint64(dm.pageSize)
	}
	return &header, nil
}

// writeHeader serializes the DBFileHeader and writes it to the beginning of the file (offset 0).
func (dm *DiskManager) writeHeader(header *DBFileHeader) error {
	buf := new(bytes.Buffer)
	// Use binary.Write on the header struct directly.
	// This will write fields contiguously based on their sizes.
	if err := binary.Write(buf, binary.LittleEndian, header); err != nil {
		return fmt.Errorf("%w: serializing header: %v", ErrSerialization, err)
	}

	// Ensure the buffer length after writing the struct matches dbFileHeaderSize.
	// If it doesn't, this indicates a mismatch in struct padding or definition.
	if buf.Len() > dbFileHeaderSize {
		return fmt.Errorf("header serialization size (%d) exceeds declared header size (%d)", buf.Len(), dbFileHeaderSize)
	}
	// Add padding to reach the exact dbFileHeaderSize
	padding := make([]byte, dbFileHeaderSize-buf.Len())
	buf.Write(padding)

	log.Printf("DEBUG: Writing header of %d bytes to offset 0", buf.Len())
	if _, err := dm.file.WriteAt(buf.Bytes(), 0); err != nil {
		return fmt.Errorf("%w: writing header to disk: %v", ErrIO, err)
	}
	return dm.file.Sync() // Ensure header is flushed to disk immediately
}

// readHeader reads the DBFileHeader from the beginning of the file (offset 0).
func (dm *DiskManager) readHeader(header *DBFileHeader) error {
	data := make([]byte, dbFileHeaderSize)
	// Read exactly dbFileHeaderSize bytes from offset 0
	n, err := dm.file.ReadAt(data, 0)
	if err != nil {
		if err == io.EOF && n < dbFileHeaderSize {
			return fmt.Errorf("database file is too small or corrupted (header too short)")
		}
		return fmt.Errorf("%w: reading header from disk: %v", ErrIO, err)
	}
	if n != dbFileHeaderSize {
		return fmt.Errorf("short read for header, expected %d bytes, got %d", dbFileHeaderSize, n)
	}

	buf := bytes.NewReader(data)
	// Use binary.Read to deserialize directly into the header struct.
	if err := binary.Read(buf, binary.LittleEndian, header); err != nil {
		return fmt.Errorf("%w: deserializing header: %v", ErrDeserialization, err)
	}
	log.Printf("DEBUG: Read header from disk. Magic: 0x%x, Version: %d, PageSize: %d",
		header.Magic, header.Version, header.PageSize)
	return nil
}

// UpdateHeaderField updates a field in the DBFileHeader safely.
func (dm *DiskManager) UpdateHeaderField(updateFunc func(header *DBFileHeader)) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.file == nil {
		return fmt.Errorf("file not open")
	}
	var header DBFileHeader
	if err := dm.readHeader(&header); err != nil {
		return err
	}
	updateFunc(&header)
	return dm.writeHeader(&header)
}

// ReadPage reads a page's data from disk into the provided pageData buffer.
func (dm *DiskManager) ReadPage(pageID PageID, pageData []byte) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.file == nil {
		return fmt.Errorf("file not open")
	}
	if len(pageData) != dm.pageSize {
		return fmt.Errorf("page data buffer size (%d) != disk manager page size (%d)", len(pageData), dm.pageSize)
	}
	offset := int64(pageID) * int64(dm.pageSize)
	bytesRead, err := dm.file.ReadAt(pageData, offset)
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("%w: EOF reading page %d at offset %d, file may be corrupt or pageID out of bounds", ErrIO, pageID, offset)
		}
		return fmt.Errorf("%w: reading page %d at offset %d: %v", ErrIO, pageID, offset, err)
	}
	if bytesRead != dm.pageSize {
		return fmt.Errorf("%w: short read for page %d, expected %d, got %d", ErrIO, pageID, dm.pageSize, bytesRead)
	}
	return nil
}

// WritePage writes pageData to disk at the specified pageID's location.
func (dm *DiskManager) WritePage(pageID PageID, pageData []byte) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.file == nil {
		return fmt.Errorf("file not open")
	}
	if len(pageData) != dm.pageSize {
		return fmt.Errorf("page data buffer size (%d) != disk manager page size (%d)", len(pageData), dm.pageSize)
	}
	offset := int64(pageID) * int64(dm.pageSize)
	_, err := dm.file.WriteAt(pageData, offset)
	if err != nil {
		return fmt.Errorf("%w: writing page %d at offset %d: %v", ErrIO, pageID, offset, err)
	}
	// Note: We don't Sync() here for every page write. Syncing is handled by BufferPoolManager.FlushAllPages or BTree.Close()
	return nil
}

// allocateRawPageInternal extends the file to allocate a new page.
func (dm *DiskManager) allocateRawPageInternal() (PageID, error) {
	// TODO: Integrate Free Space Management.
	// 1. Request a page from freeListPageManager.
	// 2. If available, return that PageID.
	// 3. If not, extend file as below.
	newPageID := PageID(dm.numPages)
	emptyPageData := make([]byte, dm.pageSize)
	offset := int64(newPageID) * int64(dm.pageSize)

	// Write an empty page to extend the file
	if _, err := dm.file.WriteAt(emptyPageData, offset); err != nil {
		return InvalidPageID, fmt.Errorf("%w: extending file for new page %d: %v", ErrIO, newPageID, err)
	}
	dm.numPages++
	return newPageID, nil
}

// AllocatePage allocates a new page on disk and returns its ID.
func (dm *DiskManager) AllocatePage() (PageID, error) { // Public, locked version
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.allocateRawPageInternal()
}

// DeallocatePage marks a page as free (placeholder).
func (dm *DiskManager) DeallocatePage(pageID PageID) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	// TODO: Integrate Free Space Management.
	// Add pageID to the on-disk free list structure via freeListPageManager.
	// This involves fetching the free list page(s), modifying, and writing back.
	// For now, this is a no-op placeholder.
	return fmt.Errorf("DeallocatePage not fully implemented (needs Free Space Manager) for page %d", pageID)
}

// Sync flushes all buffered data to disk.
func (dm *DiskManager) Sync() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.file != nil {
		return dm.file.Sync()
	}
	return nil
}

// Close closes the underlying file handle.
func (dm *DiskManager) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.file != nil {
		// Attempt to sync before closing to ensure data durability
		err := dm.file.Sync()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error syncing file on close: %v\n", err)
		}
		closeErr := dm.file.Close()
		dm.file = nil // Clear file handle
		return closeErr
	}
	return nil
}

// --- Write-Ahead Logging (WAL) ---
type LSN uint64 // Log Sequence Number
const InvalidLSN LSN = 0

// LogRecordType defines the type of operation logged.
type LogRecordType byte

const (
	LogRecordTypeUpdate    LogRecordType = iota + 1 // Update existing page data
	LogRecordTypeInsertKey                          // B-Tree key insert (higher level)
	LogRecordTypeDeleteKey                          // B-Tree key delete
	LogRecordTypeNodeSplit                          // B-Tree node split
	LogRecordTypeNodeMerge                          // B-Tree node merge
	LogRecordTypeNewPage                            // Allocation of a new page
	LogRecordTypeFreePage                           // Deallocation of a page
	LogRecordTypeCommit                             // Transaction commit
	LogRecordTypeAbort                              // Transaction abort
	LogRecordTypeCheckpointStart
	LogRecordTypeCheckpointEnd
	// ... other types
)

type LogRecord struct {
	LSN     LSN
	PrevLSN LSN    // LSN of the previous log record by the same transaction (for undo)
	TxnID   uint64 // Transaction ID (0 if not part of a transaction or single op)
	Type    LogRecordType
	PageID  PageID // Page affected (if applicable)
	Offset  uint16 // Offset within the page (if applicable for UPDATE)
	OldData []byte // For UNDO (and REDO if needed for physiological logging)
	NewData []byte // For REDO
	// Other type-specific fields
}

// TODO: Implement LogManager (writes to a log file, manages LSNs, flushing)
// type LogManager struct { ... }
// func NewLogManager(logFilePath string) (*LogManager, error) { ... }
// func (lm *LogManager) Append(record *LogRecord) (LSN, error) { ... } // Returns LSN of written record
// func (lm *LogManager) Flush() error { ... }
// The LogManager would use its own file handle, not necessarily the DiskManager for data pages.

// --- BufferPoolManager (with LRU) ---
type BufferPoolManager struct {
	diskManager *DiskManager
	logManager  interface{} // Placeholder for *LogManager
	poolSize    int
	pages       []*Page               // Page frames
	pageTable   map[PageID]int        // PageID to frame index
	lruList     *list.List            // Doubly linked list for LRU tracking (stores frame indices)
	lruMap      map[int]*list.Element // Frame index to LRU list element
	mu          sync.Mutex
	pageSize    int
}

func NewBufferPoolManager(poolSize int, diskManager *DiskManager, logManager interface{}) *BufferPoolManager {
	if diskManager == nil {
		log.Fatal("NewBufferPoolManager: diskManager cannot be nil")
	}
	bpm := &BufferPoolManager{
		diskManager: diskManager, logManager: logManager, poolSize: poolSize,
		pages: make([]*Page, poolSize), pageTable: make(map[PageID]int),
		lruList: list.New(), lruMap: make(map[int]*list.Element),
		pageSize: diskManager.pageSize,
	}
	for i := 0; i < poolSize; i++ {
		bpm.pages[i] = NewPage(InvalidPageID, bpm.pageSize)
	}
	return bpm
}

func (bpm *BufferPoolManager) FetchPage(pageID PageID) (*Page, error) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	// 1. Check if page is already in the buffer pool
	if frameIdx, ok := bpm.pageTable[pageID]; ok {
		page := bpm.pages[frameIdx]
		page.Pin()
		// Move to front of LRU to mark as recently used
		if page.lruElement != nil { // Ensure it's in LRU list before moving
			bpm.lruList.MoveToFront(page.lruElement)
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
	if victimPage.IsDirty() && victimPage.GetPageID() != InvalidPageID {
		// TODO: WAL - Before flushing, ensure all log records for this page up to its LSN are flushed.
		// if lm, ok := bpm.logManager.(*LogManager); ok { lm.Flush(victimPage.GetLSN()) }
		log.Printf("DEBUG: Flushing dirty victim page %d from frame %d to disk", victimPage.GetPageID(), frameIdx)
		if err := bpm.diskManager.WritePage(victimPage.GetPageID(), victimPage.GetData()); err != nil {
			// If flush fails, we cannot reuse this frame safely. This is a critical error.
			// A real system might panic or try another victim.
			return nil, fmt.Errorf("failed to flush dirty victim page %d: %w", victimPage.GetPageID(), err)
		}
		victimPage.SetDirty(false) // Flushed
	}

	// 4. Remove victim page from pageTable and LRU list
	if victimPage.GetPageID() != InvalidPageID { // If it held a valid page
		delete(bpm.pageTable, victimPage.GetPageID())
		if victimPage.lruElement != nil {
			bpm.lruList.Remove(victimPage.lruElement)
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
	victimPage.id = pageID
	victimPage.pinCount = 1
	victimPage.isDirty = false  // Just read from disk, so it's clean
	victimPage.lsn = InvalidLSN // LSN should be read from page data if stored, or set after WAL replay. For now, assume Invalid.

	bpm.pageTable[pageID] = frameIdx
	victimPage.lruElement = bpm.lruList.PushFront(frameIdx) // Add to front of LRU
	bpm.lruMap[frameIdx] = victimPage.lruElement
	log.Printf("DEBUG: Page %d loaded into frame %d, pinCount: %d", pageID, frameIdx, victimPage.GetPinCount())

	return victimPage, nil
}

// getVictimFrameInternal finds an unpinned page to evict.
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
		if bpm.pages[i].GetPageID() == InvalidPageID { // A truly empty frame
			log.Printf("DEBUG: Found empty frame %d as victim", i)
			return i, nil
		}
	}

	// If all pages are pinned or the pool is genuinely full with no evictable pages
	log.Printf("ERROR: Buffer pool is full, and all pages are pinned or none are evictable.")
	return -1, ErrBufferPoolFull
}

// UnpinPage decrements the pin count for a page. If isDirty is true, it marks the page as dirty.
func (bpm *BufferPoolManager) UnpinPage(pageID PageID, isDirty bool) error {
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
		}
		log.Printf("DEBUG: Unpinned page %d (frame %d), new pinCount: %d, isDirty: %t", pageID, frameIdx, page.GetPinCount(), page.IsDirty())
		// If pin count becomes 0, it's now eligible for LRU eviction. Its LRU position
		// is managed by FetchPage moving to front. No explicit LRU removal/addition needed here.
		return nil
	}
	log.Printf("ERROR: Page %d not found in buffer pool to unpin.", pageID)
	return fmt.Errorf("%w: page %d not found to unpin", ErrPageNotFound, pageID)
}

// NewPage allocates a new page on disk and fetches it into the buffer pool.
func (bpm *BufferPoolManager) NewPage() (*Page, PageID, error) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	// 1. Allocate a new page on disk
	newPageID, err := bpm.diskManager.AllocatePage()
	if err != nil {
		log.Printf("ERROR: Failed to allocate new page on disk: %v", err)
		return nil, InvalidPageID, err
	}
	log.Printf("DEBUG: Allocated new page %d on disk.", newPageID)

	// 2. Find a victim frame in the buffer pool for this new page
	frameIdx, err := bpm.getVictimFrameInternal()
	if err != nil {
		// IMPORTANT: If we can't get a frame, the allocated disk page is orphaned.
		// A robust system would deallocate this newPageID back to the disk free list.
		// For now, we log and return error.
		_ = bpm.diskManager.DeallocatePage(newPageID) // Attempt to deallocate (might be a no-op if not implemented)
		log.Printf("ERROR: Failed to get frame for new page %d, deallocated. Error: %v", newPageID, err)
		return nil, InvalidPageID, fmt.Errorf("failed to get frame for new page %d: %w", newPageID, err)
	}
	victimPage := bpm.pages[frameIdx]
	log.Printf("DEBUG: New page %d getting frame %d (old page %d)", newPageID, frameIdx, victimPage.GetPageID())

	// 3. If victim page is dirty, flush it before reuse
	if victimPage.IsDirty() && victimPage.GetPageID() != InvalidPageID {
		// TODO: WAL - flush logs for victimPage.GetLSN()
		log.Printf("DEBUG: Flushing dirty victim page %d for new page %d", victimPage.GetPageID(), newPageID)
		if err := bpm.diskManager.WritePage(victimPage.GetPageID(), victimPage.GetData()); err != nil {
			return nil, InvalidPageID, fmt.Errorf("failed to flush dirty victim %d for new page: %w", victimPage.GetPageID(), err)
		}
		victimPage.SetDirty(false)
	}

	// 4. Remove victim page from pageTable and LRU list
	if victimPage.GetPageID() != InvalidPageID {
		delete(bpm.pageTable, victimPage.GetPageID())
		if victimPage.lruElement != nil {
			bpm.lruList.Remove(victimPage.lruElement)
			delete(bpm.lruMap, frameIdx)
		}
	}

	// 5. Reset and initialize the new page
	victimPage.Reset() // Clears data and metadata
	victimPage.id = newPageID
	victimPage.pinCount = 1     // Automatically pinned when created
	victimPage.isDirty = true   // New page, considered dirty as it will be written to for the first time.
	victimPage.lsn = InvalidLSN // Will be set when first log record is written for this page.

	// 6. Track the new page in buffer pool
	bpm.pageTable[newPageID] = frameIdx
	victimPage.lruElement = bpm.lruList.PushFront(frameIdx) // Add to front of LRU
	bpm.lruMap[frameIdx] = victimPage.lruElement
	log.Printf("DEBUG: New page %d loaded into frame %d, pinCount: %d, isDirty: %t", newPageID, frameIdx, victimPage.GetPinCount(), victimPage.IsDirty())

	return victimPage, newPageID, nil
}

// FlushPage flushes a specific page to disk if it's dirty.
func (bpm *BufferPoolManager) FlushPage(pageID PageID) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	if frameIdx, ok := bpm.pageTable[pageID]; ok {
		page := bpm.pages[frameIdx]
		if page.IsDirty() {
			// TODO: WAL - Ensure log records up to page.GetLSN() are flushed before this.
			// if lm, ok := bpm.logManager.(*LogManager); ok { lm.Flush(page.GetLSN()) }
			log.Printf("DEBUG: Flushing page %d (frame %d) to disk", pageID, frameIdx)
			if err := bpm.diskManager.WritePage(page.GetPageID(), page.GetData()); err != nil {
				log.Printf("ERROR: Failed to flush page %d: %v", pageID, err)
				return err
			}
			page.SetDirty(false) // Mark clean after successful flush
		} else {
			log.Printf("DEBUG: Page %d (frame %d) is clean, no flush needed.", pageID, frameIdx)
		}
		return nil
	}
	log.Printf("WARNING: Attempted to flush page %d not found in buffer pool.", pageID)
	return fmt.Errorf("%w: page %d not found to flush", ErrPageNotFound, pageID)
}

// FlushAllPages flushes all dirty pages in the buffer pool to disk.
func (bpm *BufferPoolManager) FlushAllPages() error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	var firstErr error

	// TODO: WAL - Before flushing all, ensure all pending log records are flushed.
	// if lm, ok := bpm.logManager.(*LogManager); ok { lm.FlushAll() }

	log.Println("DEBUG: Flushing all dirty pages from buffer pool...")
	for i, page := range bpm.pages { // Iterate over all frames in the pool
		if page.GetPageID() != InvalidPageID && page.IsDirty() {
			// TODO: WAL - Ensure log records up to page.GetLSN() are flushed.
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

// --- BTree Node Serialization/Deserialization ---

// Header fields within a Node's page data (offsets relative to start of page data)
const (
	nodeHeaderFlagsOffset   = 0 // For isLeaf, other flags (1 byte)
	nodeHeaderNumKeysOffset = 1 // Number of keys (2 bytes, uint16)
	// Key/Value/Child data follows
	// Checksum is at the very end of the page
)

// Node represents an in-memory B-tree node.
type Node[K any, V any] struct {
	pageID       PageID
	isLeaf       bool
	keys         []K
	values       []V
	childPageIDs []PageID
	tree         *BTree[K, V] // Reference to the parent BTree for BPM/DiskManager access
}

func (n *Node[K, V]) GetPageID() PageID {
	return n.pageID
}

// serialize converts the Node into a byte slice and writes it to the provided Page's data buffer.
// It also calculates and writes the checksum.
func (n *Node[K, V]) serialize(page *Page, keySerializer func(K) ([]byte, error), valueSerializer func(V) ([]byte, error)) error {
	if n.tree == nil || n.tree.bpm == nil {
		return ErrBTreeNotInitializedProperly
	}
	pageSize := n.tree.bpm.pageSize
	buffer := new(bytes.Buffer)

	// Write Node Header:
	// Flags (1 byte): bit 0 for isLeaf, other bits for future flags
	var flags byte
	if n.isLeaf {
		flags |= (1 << 0) // Set 0th bit if it's a leaf
	}
	if err := binary.Write(buffer, binary.LittleEndian, flags); err != nil {
		return fmt.Errorf("%w: writing flags: %v", ErrSerialization, err)
	}

	// Number of keys (uint16)
	numKeys := uint16(len(n.keys))
	if err := binary.Write(buffer, binary.LittleEndian, numKeys); err != nil {
		return fmt.Errorf("%w: writing numKeys: %v", ErrSerialization, err)
	}

	// Serialize Keys
	for _, k := range n.keys {
		keyData, err := keySerializer(k)
		if err != nil {
			return fmt.Errorf("%w: serializing key: %v", ErrSerialization, err)
		}
		if err := binary.Write(buffer, binary.LittleEndian, uint16(len(keyData))); err != nil { // Length of key data
			return err
		}
		if _, err := buffer.Write(keyData); err != nil { // Key data
			return err
		}
	}

	// Serialize Values
	for _, v := range n.values {
		valData, err := valueSerializer(v)
		if err != nil {
			return fmt.Errorf("%w: serializing value: %v", ErrSerialization, err)
		}
		if err := binary.Write(buffer, binary.LittleEndian, uint16(len(valData))); err != nil { // Length of value data
			return err
		}
		if _, err := buffer.Write(valData); err != nil { // Value data
			return err
		}
	}

	// Serialize Child Page IDs (if not a leaf)
	if !n.isLeaf {
		numChildren := uint16(len(n.childPageIDs))
		if err := binary.Write(buffer, binary.LittleEndian, numChildren); err != nil {
			return fmt.Errorf("%w: writing numChildren: %v", ErrSerialization, err)
		}
		for _, childID := range n.childPageIDs {
			if err := binary.Write(buffer, binary.LittleEndian, childID); err != nil {
				return fmt.Errorf("%w: writing childPageID: %v", ErrSerialization, err)
			}
		}
	}

	serializedData := buffer.Bytes()

	// Check if serialized data fits within the page (excluding checksum space)
	if len(serializedData)+checksumSize > pageSize {
		return fmt.Errorf("%w: node data (%d bytes) + checksum (%d) exceeds page size (%d) for page %d",
			ErrSerialization, len(serializedData), checksumSize, pageSize, n.pageID)
	}

	// Copy serialized data into the page's buffer
	pageData := page.GetData()
	copy(pageData, serializedData)

	// Pad remaining space with zeros (important for consistent checksum calculation)
	for i := len(serializedData); i < pageSize-checksumSize; i++ {
		pageData[i] = 0
	}

	// Calculate and write checksum
	// The checksum is calculated over the entire page data *excluding* the checksum itself.
	checksum := crc32.ChecksumIEEE(pageData[:pageSize-checksumSize])
	binary.LittleEndian.PutUint32(pageData[pageSize-checksumSize:], checksum)

	// Logging for debugging serialization (can be extensive)
	log.Printf("SER: PageID %d, numKeys %d, isLeaf %v, serializedLen: %d, calculatedChecksum: 0x%x",
		n.pageID, len(n.keys), n.isLeaf, len(serializedData), checksum)
	// log.Printf("SER: Page %d data (first 64 bytes): %x", n.pageID, page.GetData()[:64])
	// log.Printf("SER: Page %d data (last %d bytes, includes checksum): %x", n.pageID, checksumSize+32, page.GetData()[pageSize-checksumSize-32:])

	// Mark the page as dirty, so it will be flushed to disk by the BufferPoolManager
	page.SetDirty(true)
	return nil
}

// deserialize reads node data from the provided Page's data buffer and reconstructs the Node.
// It also verifies the checksum.
func (n *Node[K, V]) deserialize(page *Page, keyDeserializer func([]byte) (K, error), valueDeserializer func([]byte) (V, error)) error {
	if n.tree == nil || n.tree.bpm == nil {
		n.keys = make([]K, 0) // Initialize slices even on error for safety
		n.values = make([]V, 0)
		n.childPageIDs = make([]PageID, 0)
		return ErrBTreeNotInitializedProperly
	}
	pageSize := n.tree.bpm.pageSize
	pageData := page.GetData()

	// --- CRITICAL CHECKSUM VERIFICATION ---
	// Extract stored checksum from the end of the page
	storedChecksumBytes := pageData[pageSize-checksumSize:]
	storedChecksum := binary.LittleEndian.Uint32(storedChecksumBytes)

	// Calculate checksum from the rest of the page data
	calculatedChecksum := crc32.ChecksumIEEE(pageData[:pageSize-checksumSize])

	log.Printf("DESER: PageID %d. Stored Checksum: 0x%x, Calculated Checksum: 0x%x",
		page.GetPageID(), storedChecksum, calculatedChecksum)
	// log.Printf("DESER: Page %d data (first 64 bytes for checksum): %x", page.GetPageID(), pageData[:64])
	// log.Printf("DESER: Page %d data (last %d bytes, includes checksum): %x", page.GetPageID(), checksumSize+32, pageData[pageSize-checksumSize-32:])

	if storedChecksum != calculatedChecksum {
		log.Printf("ERROR: CHECKSUM MISMATCH DETECTED for PageID %d: Stored=0x%x, Calculated=0x%x. Raw page data (first 64 bytes for debug): %x",
			page.GetPageID(), storedChecksum, calculatedChecksum, pageData[:64])
		// Initialize node slices to empty to prevent using corrupt data
		n.keys = make([]K, 0)
		n.values = make([]V, 0)
		n.childPageIDs = make([]PageID, 0)
		return fmt.Errorf("%w: stored=0x%x, calculated=0x%x for page %d", ErrChecksumMismatch, storedChecksum, calculatedChecksum, page.GetPageID())
	}
	// --- END CRITICAL CHECKSUM VERIFICATION ---

	buffer := bytes.NewReader(pageData[:pageSize-checksumSize]) // Read from data *before* checksum

	// Read Node Header:
	// Flags (1 byte)
	var flags byte
	if err := binary.Read(buffer, binary.LittleEndian, &flags); err != nil {
		return fmt.Errorf("%w: reading flags: %v", ErrDeserialization, err)
	}
	n.isLeaf = (flags & (1 << 0)) != 0 // Check 0th bit for isLeaf

	// Number of keys (uint16)
	var numKeys uint16
	if err := binary.Read(buffer, binary.LittleEndian, &numKeys); err != nil {
		return fmt.Errorf("%w: reading numKeys: %v", ErrDeserialization, err)
	}
	n.keys = make([]K, numKeys)
	n.values = make([]V, numKeys)

	// Deserialize Keys
	for i := uint16(0); i < numKeys; i++ {
		var keyDataLen uint16
		if err := binary.Read(buffer, binary.LittleEndian, &keyDataLen); err != nil {
			return fmt.Errorf("%w: reading key length for key %d: %v", ErrDeserialization, i, err)
		}
		keyData := make([]byte, keyDataLen)
		if _, err := io.ReadFull(buffer, keyData); err != nil {
			return fmt.Errorf("%w: reading key data for key %d: %v", ErrDeserialization, i, err)
		}
		key, err := keyDeserializer(keyData)
		if err != nil {
			return fmt.Errorf("%w: deserializing key %d: %v", ErrDeserialization, i, err)
		}
		n.keys[i] = key
	}

	// Deserialize Values
	for i := uint16(0); i < numKeys; i++ {
		var valDataLen uint16
		if err := binary.Read(buffer, binary.LittleEndian, &valDataLen); err != nil {
			return fmt.Errorf("%w: reading value length for value %d: %v", ErrDeserialization, i, err)
		}
		valData := make([]byte, valDataLen)
		if _, err := io.ReadFull(buffer, valData); err != nil {
			return fmt.Errorf("%w: reading value data for value %d: %v", ErrDeserialization, i, err)
		}
		val, err := valueDeserializer(valData)
		if err != nil {
			return fmt.Errorf("%w: deserializing value %d: %v", ErrDeserialization, i, err)
		}
		n.values[i] = val
	}

	// Deserialize Child Page IDs (if not a leaf)
	if !n.isLeaf {
		var numChildren uint16
		if err := binary.Read(buffer, binary.LittleEndian, &numChildren); err != nil {
			return fmt.Errorf("%w: reading numChildren: %v", ErrDeserialization, err)
		}
		n.childPageIDs = make([]PageID, numChildren)
		for i := uint16(0); i < numChildren; i++ {
			if err := binary.Read(buffer, binary.LittleEndian, &n.childPageIDs[i]); err != nil {
				return fmt.Errorf("%w: reading childPageID %d: %v", ErrDeserialization, i, err)
			}
		}
	} else {
		n.childPageIDs = make([]PageID, 0) // Ensure it's an empty slice for leaves
	}

	n.pageID = page.GetPageID() // Set the node's page ID from the page object
	log.Printf("DESER: Successfully deserialized PageID %d, isLeaf: %v, numKeys: %d", n.pageID, n.isLeaf, len(n.keys))
	return nil
}

// --- BTree Structure & Operations (Refactored for Persistence) ---
type Order[K any] func(a, b K) int

type KeyValueSerializer[K any, V any] struct {
	SerializeKey     func(K) ([]byte, error)
	DeserializeKey   func([]byte) (K, error)
	SerializeValue   func(V) ([]byte, error)
	DeserializeValue func([]byte) (V, error)
}

type BTree[K any, V any] struct {
	rootPageID   PageID
	degree       int
	keyOrder     Order[K]
	kvSerializer KeyValueSerializer[K, V]
	bpm          *BufferPoolManager
	diskManager  *DiskManager
	logManager   interface{} // Placeholder for *LogManager
	// size is now read from/written to header
}

// NewBTreeFile creates a new database file and initializes a new B-tree within it.
func NewBTreeFile[K any, V any](filePath string, degree int, keyOrder Order[K], kvSerializer KeyValueSerializer[K, V], poolSize int, pageSize int, logManager interface{}) (*BTree[K, V], error) {
	if degree < 2 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidDegree, degree)
	}
	if keyOrder == nil {
		return nil, ErrNilKeyOrder
	}
	if kvSerializer.SerializeKey == nil || kvSerializer.DeserializeKey == nil || kvSerializer.SerializeValue == nil || kvSerializer.DeserializeValue == nil {
		return nil, errors.New("all key/value serializers must be provided")
	}

	dm, err := NewDiskManager(filePath, pageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk manager: %w", err)
	}

	// Open or create the file. 'create=true' means it will create if not exists, error if exists.
	_, err = dm.OpenOrCreateFile(true, degree, 0)
	if err != nil {
		dm.Close() // Ensure disk manager is closed on failure
		// If the file already exists, it's an error for NewBTreeFile
		if errors.Is(err, ErrDBFileExists) {
			return nil, fmt.Errorf("%w: file %s already exists. Use OpenBTreeFile to open an existing database.", err, filePath)
		}
		return nil, fmt.Errorf("failed to open/create database file: %w", err)
	}

	bpm := NewBufferPoolManager(poolSize, dm, logManager)

	bt := &BTree[K, V]{
		rootPageID: InvalidPageID, degree: degree, keyOrder: keyOrder,
		kvSerializer: kvSerializer, bpm: bpm, diskManager: dm, logManager: logManager,
	}

	// Create the initial root node page
	rootPageForNew, rootPageIDForNew, err := bpm.NewPage()
	if err != nil {
		dm.Close()
		_ = os.Remove(filePath) // Clean up file on failure to create root page
		return nil, fmt.Errorf("failed to create initial root page for B-tree: %w", err)
	}
	bt.rootPageID = rootPageIDForNew // Set the tree's root to the newly allocated page

	// Initialize the root node as a leaf
	rootNode := &Node[K, V]{
		pageID:       rootPageIDForNew,
		isLeaf:       true,
		tree:         bt,
		keys:         make([]K, 0),
		values:       make([]V, 0),
		childPageIDs: make([]PageID, 0),
	}

	// Serialize the new empty root node to its page
	if err := rootNode.serialize(rootPageForNew, kvSerializer.SerializeKey, kvSerializer.SerializeValue); err != nil {
		bpm.UnpinPage(rootPageIDForNew, false) // Unpin, not dirty if serialization failed
		dm.Close()
		_ = os.Remove(filePath) // Clean up file on serialization failure
		return nil, fmt.Errorf("failed to serialize initial root node: %w", err)
	}

	// Unpin the root page, marking it dirty as it's new content
	if err := bpm.UnpinPage(rootPageIDForNew, true); err != nil {
		dm.Close()
		_ = os.Remove(filePath)
		return nil, fmt.Errorf("failed to unpin initial root page: %w", err)
	}

	// Update the file header with the new root page ID and other metadata
	if err := dm.UpdateHeaderField(func(h *DBFileHeader) {
		h.RootPageID = rootPageIDForNew
		h.TreeSize = 0 // Initially empty tree
		h.Degree = uint32(degree)
	}); err != nil {
		dm.Close()
		_ = os.Remove(filePath)
		return nil, fmt.Errorf("failed to update header with root page ID: %w", err)
	}

	log.Printf("INFO: New B-tree database created at %s with root PageID %d, Degree %d", filePath, rootPageIDForNew, degree)
	return bt, nil
}

// OpenBTreeFile opens an existing database file and initializes the B-tree from its header.
func OpenBTreeFile[K any, V any](filePath string, keyOrder Order[K], kvSerializer KeyValueSerializer[K, V], poolSize int, defaultPageSize int, logManager interface{}) (*BTree[K, V], error) {
	if keyOrder == nil {
		return nil, ErrNilKeyOrder
	}
	if kvSerializer.SerializeKey == nil || kvSerializer.DeserializeKey == nil || kvSerializer.SerializeValue == nil || kvSerializer.DeserializeValue == nil {
		return nil, errors.New("all key/value serializers must be provided")
	}

	dm, err := NewDiskManager(filePath, defaultPageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk manager: %w", err)
	}

	// Open the existing file. 'create=false' means it will error if file doesn't exist.
	header, err := dm.OpenOrCreateFile(false, 0, 0) // Degree and initialTreeSize are read from header
	if err != nil {
		dm.Close() // Ensure disk manager is closed on failure
		return nil, fmt.Errorf("failed to open database file %s: %w", filePath, err)
	}

	// Ensure the DiskManager's page size matches the file's page size
	if dm.pageSize != int(header.PageSize) {
		log.Printf("WARNING: DiskManager initialized with page size %d, but file header specifies %d. Adjusting DiskManager's pageSize.", dm.pageSize, header.PageSize)
		dm.pageSize = int(header.PageSize) // Adjust DM's page size to match the file's
	}

	bpm := NewBufferPoolManager(poolSize, dm, logManager)

	// --- TODO: Recovery Process using LogManager and BPM ---
	// This is where you would implement database recovery.
	// 1. Analysis Pass (scan WAL to find last checkpoint and active transactions)
	// 2. Redo Pass (replay WAL records to bring pages up to date)
	// 3. Undo Pass (rollback uncommitted transactions if any)
	// After recovery, header.LastLSN and page LSNs would be consistent.
	// if lm, ok := logManager.(*LogManager); ok {
	//    if err := lm.Recover(bpm, header.LastLSN); err != nil {
	//        dm.Close(); return nil, fmt.Errorf("recovery failed: %w", err)
	//    }
	// }

	// Re-read header after potential recovery changes if recovery can modify it
	// For now, assume header is correct after open.
	if err := dm.readHeader(header); err != nil { // Re-read header in case recovery changed it
		dm.Close()
		return nil, fmt.Errorf("failed to re-read header after potential recovery: %w", err)
	}

	bt := &BTree[K, V]{
		rootPageID:   header.RootPageID,
		degree:       int(header.Degree),
		keyOrder:     keyOrder,
		kvSerializer: kvSerializer,
		bpm:          bpm,
		diskManager:  dm,
		logManager:   logManager,
	}
	log.Printf("INFO: Existing B-tree database opened from %s. Root PageID: %d, Degree: %d, TreeSize: %d",
		filePath, bt.rootPageID, bt.degree, header.TreeSize)
	return bt, nil
}

// fetchNode retrieves a node from the buffer pool or disk.
func (bt *BTree[K, V]) fetchNode(pageID PageID) (*Node[K, V], *Page, error) {
	if pageID == InvalidPageID {
		return nil, nil, errors.New("attempted to fetch node with InvalidPageID")
	}
	page, err := bt.bpm.FetchPage(pageID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch page %d from buffer pool: %w", pageID, err)
	}
	node := &Node[K, V]{tree: bt}
	err = node.deserialize(page, bt.kvSerializer.DeserializeKey, bt.kvSerializer.DeserializeValue)
	if err != nil {
		bt.bpm.UnpinPage(pageID, false) // Unpin page if deserialization fails
		return nil, nil, fmt.Errorf("failed to deserialize node from page %d: %w", pageID, err)
	}
	return node, page, nil
}

// GetSize reads the persisted tree size from the file header.
func (bt *BTree[K, V]) GetSize() (uint64, error) {
	var header DBFileHeader
	// Read header directly, ensures latest size from disk
	if err := bt.diskManager.readHeader(&header); err != nil {
		return 0, fmt.Errorf("failed to read tree size from header: %w", err)
	}
	return header.TreeSize, nil
}

// updatePersistedSize updates the TreeSize field in the file header.
// NOTE: This is inefficient for frequent updates. In a real DB, size is often
// updated in memory and flushed periodically or at shutdown/checkpoint.
func (bt *BTree[K, V]) updatePersistedSize(delta int64) error {
	return bt.diskManager.UpdateHeaderField(func(h *DBFileHeader) {
		if delta > 0 {
			h.TreeSize += uint64(delta)
		} else if delta < 0 {
			// Ensure TreeSize doesn't underflow
			if h.TreeSize >= uint64(-delta) {
				h.TreeSize -= uint64(-delta)
			} else {
				h.TreeSize = 0 // Should not happen if logic is correct
			}
		}
	})
}

// Search for a key in the B-tree.
func (bt *BTree[K, V]) Search(key K) (V, bool, error) {
	var zeroV V
	if bt.rootPageID == InvalidPageID {
		log.Println("DEBUG: Search on empty or uninitialized B-tree.")
		return zeroV, false, nil
	}
	// Fetch the root node to start the search
	rootNode, rootPage, err := bt.fetchNode(bt.rootPageID)
	if err != nil {
		return zeroV, false, fmt.Errorf("failed to fetch root node for search: %w", err)
	}

	// Call the recursive search function
	val, found, searchErr := bt.searchRecursive(rootNode, rootPage, key)
	// The recursive search function is responsible for unpinning pages on both success and error paths.
	if searchErr != nil {
		log.Printf("ERROR: Recursive search failed: %v", searchErr)
		return zeroV, false, searchErr
	}
	return val, found, nil
}

// searchRecursive performs a recursive search for a key.
// It takes ownership of currNode and currPage, unpinning currPage before returning.
func (bt *BTree[K, V]) searchRecursive(currNode *Node[K, V], currPage *Page, key K) (V, bool, error) {
	var zeroV V

	// Find the index where the key *could* be or where it should be if not found
	idx, foundKeyInCurrNode := slices.BinarySearchFunc(currNode.keys, key, bt.keyOrder)

	if foundKeyInCurrNode {
		// Key found in the current node
		val := currNode.values[idx]
		unpinErr := bt.bpm.UnpinPage(currPage.GetPageID(), false) // Unpin clean page
		if unpinErr != nil {
			log.Printf("WARNING: Error unpinning page %d after successful search: %v", currPage.GetPageID(), unpinErr)
		}
		return val, true, unpinErr
	}

	if currNode.isLeaf {
		// Key not found, and it's a leaf node. So, key is not in the tree.
		unpinErr := bt.bpm.UnpinPage(currPage.GetPageID(), false) // Unpin clean page
		if unpinErr != nil {
			log.Printf("WARNING: Error unpinning page %d after failed search (leaf): %v", currPage.GetPageID(), unpinErr)
		}
		return zeroV, false, unpinErr
	}

	// Key not found in current internal node, descend to the appropriate child
	childPageIDToSearch := currNode.childPageIDs[idx]

	// Unpin the current parent page BEFORE fetching the child.
	// This helps manage pin counts and allows the parent page to be evicted if needed.
	unpinErr := bt.bpm.UnpinPage(currPage.GetPageID(), false) // Unpin clean page
	if unpinErr != nil {
		// Log or handle unpin error, but proceed with search if possible, though state might be tricky
		log.Printf("WARNING: Error unpinning page %d during recursive search traversal: %v", currPage.GetPageID(), unpinErr)
		// Decide if this is a fatal error or just a warning. For production, might abort transaction.
	}

	// Fetch the child node
	childNode, childPage, fetchErr := bt.fetchNode(childPageIDToSearch)
	if fetchErr != nil {
		log.Printf("ERROR: Failed to fetch child page %d during search from parent %d: %v", childPageIDToSearch, currNode.GetPageID(), fetchErr)
		return zeroV, false, fetchErr
	}

	// Recursively call search on the child node. The recursive call will handle unpinning childPage.
	return bt.searchRecursive(childNode, childPage, key)
}

// Insert a key-value pair into the B-tree.
func (bt *BTree[K, V]) Insert(key K, value V) error {
	// Check if key already exists to determine if TreeSize should be incremented.
	// This search is non-transactional and doesn't handle concurrency or LSNs,
	// simply for determining if it's a new insert or an update.
	_, exists, searchErr := bt.Search(key)
	if searchErr != nil {
		log.Printf("WARNING: Search error during pre-insert check for key %v: %v", key, searchErr)
		// We proceed, but the actual insert path will handle if the key is found or not.
		// If search failed due to corruption (e.g. checksum), the insert might also fail.
	}

	// If the tree is empty, create the first root node
	if bt.rootPageID == InvalidPageID {
		log.Printf("DEBUG: Inserting first key %v. Creating initial root page.", key)
		rootPg, rootPgID, err := bt.bpm.NewPage() // Allocates a new page and pins it
		if err != nil {
			return fmt.Errorf("failed to create first root page for insert: %w", err)
		}
		bt.rootPageID = rootPgID // Update tree's root reference
		// Update header immediately so the root is persisted
		if err := bt.diskManager.UpdateHeaderField(func(h *DBFileHeader) { h.RootPageID = rootPgID; h.TreeSize = 0 }); err != nil {
			bt.bpm.UnpinPage(rootPgID, false)           // Unpin even on error
			_ = bt.diskManager.DeallocatePage(rootPgID) // Try to free the orphaned page
			return fmt.Errorf("failed to update header with new root page ID: %w", err)
		}
		rootNode := &Node[K, V]{pageID: rootPgID, isLeaf: true, tree: bt, keys: make([]K, 0), values: make([]V, 0), childPageIDs: make([]PageID, 0)}
		// TODO: WAL Log creation of new root page
		return bt.insertNonFull(rootNode, rootPg, key, value, !exists) // insertNonFull unpins rootPg
	}

	// Fetch the current root node
	rootNode, rootPage, err := bt.fetchNode(bt.rootPageID)
	if err != nil {
		return fmt.Errorf("failed to fetch root node during insert: %w", err)
	}

	// If the root node is full, the tree must grow in height by splitting the root
	if len(rootNode.keys) == 2*bt.degree-1 {
		log.Printf("DEBUG: Root node %d is full. Splitting root.", bt.rootPageID)
		newRootDiskPage, newRootPageID, err := bt.bpm.NewPage() // Allocates a new page for the new root
		if err != nil {
			bt.bpm.UnpinPage(rootPage.GetPageID(), false) // Unpin old root on error
			return fmt.Errorf("failed to create new root page during split: %w", err)
		}

		oldRootPageID := bt.rootPageID // Store old root ID
		bt.rootPageID = newRootPageID  // Update tree's root to the new page ID

		// Update file header with the new root page ID
		if err := bt.diskManager.UpdateHeaderField(func(h *DBFileHeader) { h.RootPageID = newRootPageID }); err != nil {
			bt.bpm.UnpinPage(rootPage.GetPageID(), false)        // Unpin old root
			bt.bpm.UnpinPage(newRootDiskPage.GetPageID(), false) // Unpin new root
			_ = bt.diskManager.DeallocatePage(newRootPageID)     // Try to free orphaned new root page
			return fmt.Errorf("failed to update header with new root page ID during split: %w", err)
		}

		// Create the new in-memory root node
		newRootNode := &Node[K, V]{
			pageID:       newRootPageID,
			isLeaf:       false,                   // New root is always an internal node
			childPageIDs: []PageID{oldRootPageID}, // Old root becomes the first child
			tree:         bt,
			keys:         make([]K, 0),
			values:       make([]V, 0),
		}
		// TODO: WAL Log new root page creation and the subsequent split operation

		// Split the old root node. `splitChild` promotes a key to `newRootNode` and unpins `rootPage` and the new sibling page.
		err = bt.splitChild(newRootNode, newRootDiskPage, 0, rootNode, rootPage)
		if err != nil {
			// If split fails, newRootDiskPage and rootPage (old root) are already unpinned (or handled by splitChild)
			// The B-tree state might be inconsistent if split fails before full completion.
			// This is a critical error path.
			return fmt.Errorf("failed to split root node: %w", err)
		}
		log.Printf("DEBUG: Root split complete. New root %d, Old root %d", newRootPageID, oldRootPageID)

		// After splitting, insert the key into the new (non-full) root node.
		// `newRootNode` is modified in memory by `splitChild`. We need to fetch it again
		// to ensure we have the latest state (as `splitChild` would serialize it).
		reloadedNewRootNode, reloadedNewRootPage, fetchErr := bt.fetchNode(newRootPageID)
		if fetchErr != nil {
			return fmt.Errorf("failed to re-fetch new root node after split: %w", fetchErr)
		}
		return bt.insertNonFull(reloadedNewRootNode, reloadedNewRootPage, key, value, !exists)
	} else {
		// Root node is not full, just insert into it directly.
		return bt.insertNonFull(rootNode, rootPage, key, value, !exists) // insertNonFull unpins rootPage
	}
}

// insertNonFull inserts a key-value pair into a node that is guaranteed not to be full.
// It takes ownership of `node` and `page`, unpinning `page` before returning.
func (bt *BTree[K, V]) insertNonFull(node *Node[K, V], page *Page, key K, value V, incrementSize bool) error {
	// Find the correct insertion position
	idx := slices.IndexFunc(node.keys, func(k K) bool { return bt.keyOrder(key, k) <= 0 })
	if idx == -1 { // Key is larger than all existing keys
		idx = len(node.keys)
	}

	updated := false // Flag to check if an existing key was updated

	if node.isLeaf {
		// If it's a leaf node:
		// Check if key already exists (update case)
		if idx < len(node.keys) && bt.keyOrder(key, node.keys[idx]) == 0 {
			node.values[idx] = value // Update existing value
			updated = true
		} else if idx > 0 && bt.keyOrder(key, node.keys[idx-1]) == 0 { // Edge case: key might be at idx-1 if new key is same as previous
			node.values[idx-1] = value
			updated = true
		}

		if updated {
			log.Printf("DEBUG: Updated existing key %v in leaf node %d", key, node.pageID)
			// TODO: WAL Log value update
			if err := node.serialize(page, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
				bt.bpm.UnpinPage(page.GetPageID(), false)
				return fmt.Errorf("failed to serialize leaf node %d after update: %w", node.pageID, err)
			}
			return bt.bpm.UnpinPage(page.GetPageID(), true) // Unpin, mark dirty
		}

		// Key not found, insert new key-value pair
		node.keys = slices.Insert(node.keys, idx, key)
		node.values = slices.Insert(node.values, idx, value)
		log.Printf("DEBUG: Inserted new key %v into leaf node %d at index %d. New numKeys: %d", key, node.pageID, idx, len(node.keys))

		// TODO: WAL Log key/value insert
		if err := node.serialize(page, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
			bt.bpm.UnpinPage(page.GetPageID(), false)
			return fmt.Errorf("failed to serialize leaf node %d after insert: %w", node.pageID, err)
		}
		if incrementSize {
			if err := bt.updatePersistedSize(1); err != nil {
				log.Printf("ERROR: Failed to update tree size after insert: %v", err)
				// This is a soft error; data is inserted, but size count might be off.
			}
		}
		return bt.bpm.UnpinPage(page.GetPageID(), true) // Unpin, mark dirty
	} else { // Internal node
		// Check if key already exists (update case in internal node)
		if idx < len(node.keys) && bt.keyOrder(key, node.keys[idx]) == 0 {
			node.values[idx] = value // Update existing value
			updated = true
		} else if idx > 0 && bt.keyOrder(key, node.keys[idx-1]) == 0 {
			node.values[idx-1] = value
			updated = true
		}

		if updated {
			log.Printf("DEBUG: Updated existing key %v in internal node %d", key, node.pageID)
			// TODO: WAL Log value update in internal node
			if err := node.serialize(page, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
				bt.bpm.UnpinPage(page.GetPageID(), false)
				return fmt.Errorf("failed to serialize internal node %d after update: %w", node.pageID, err)
			}
			return bt.bpm.UnpinPage(page.GetPageID(), true) // Unpin, mark dirty
		}

		// Key not found in current internal node, descend to child
		childPageIDToDescend := node.childPageIDs[idx]
		childNode, childPage, err := bt.fetchNode(childPageIDToDescend) // Child is now pinned
		if err != nil {
			bt.bpm.UnpinPage(page.GetPageID(), false) // Unpin parent
			return fmt.Errorf("failed to fetch child node %d from parent %d: %w", childPageIDToDescend, node.pageID, err)
		}

		// If the child node is full, split it
		if len(childNode.keys) == 2*bt.degree-1 {
			log.Printf("DEBUG: Child node %d of parent %d is full. Splitting child.", childNode.pageID, node.pageID)
			// TODO: WAL Log upcoming split operation (parent and child involved)
			err = bt.splitChild(node, page, idx, childNode, childPage) // `splitChild` unpins `childPage` and new sibling page
			if err != nil {
				bt.bpm.UnpinPage(page.GetPageID(), false) // Unpin parent if split fails
				return fmt.Errorf("failed to split child node %d: %w", childNode.pageID, err)
			}

			// After split, `node` (parent) has been modified (promoted key, new child pointer).
			// `page` is marked dirty by `splitChild` (via parentPage.SetDirty(true)).
			// The promoted key might be the one we're trying to insert/update, or it might
			// direct us to a different child.
			if bt.keyOrder(key, node.keys[idx]) > 0 {
				idx++ // Key is greater than the promoted key, so it goes to the right sibling
			} else if bt.keyOrder(key, node.keys[idx]) == 0 {
				// The key to insert is the same as the promoted key. Update its value in parent.
				node.values[idx] = value
				log.Printf("DEBUG: Key %v matches promoted key from split. Updating value in parent %d.", key, node.pageID)
				// TODO: WAL Log this update in parent node
				if errS := node.serialize(page, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); errS != nil {
					bt.bpm.UnpinPage(page.GetPageID(), false)
					return fmt.Errorf("failed to serialize parent node %d after updating promoted key value: %w", node.pageID, errS)
				}
				// Size not incremented as it's an update of a key that was part of the structure.
				return bt.bpm.UnpinPage(page.GetPageID(), true) // Unpin, mark dirty
			}

			// Parent page (`page`) is now dirty from split (or previous update).
			// Serialize and unpin it before descending further.
			if errS := node.serialize(page, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); errS != nil {
				bt.bpm.UnpinPage(page.GetPageID(), false)
				return fmt.Errorf("failed to serialize parent node %d after split: %w", node.pageID, errS)
			}
			if errU := bt.bpm.UnpinPage(page.GetPageID(), true); errU != nil {
				return fmt.Errorf("failed to unpin parent node %d after split: %w", node.pageID, errU)
			}

			// Fetch the correct child to descend into (it's now `node.childPageIDs[idx]`)
			descendChildPageID := node.childPageIDs[idx]
			descendChildNode, descendChildPage, fetchErr := bt.fetchNode(descendChildPageID) // New child is pinned
			if fetchErr != nil {
				return fmt.Errorf("failed to fetch correct child %d after split: %w", descendChildPageID, fetchErr)
			}
			return bt.insertNonFull(descendChildNode, descendChildPage, key, value, incrementSize) // Recursive call unpins
		} else {
			// Child has space. Unpin current parent node before descending.
			unpinErr := bt.bpm.UnpinPage(page.GetPageID(), false)
			if unpinErr != nil {
				bt.bpm.UnpinPage(childPage.GetPageID(), false) // Unpin child if parent unpin fails
				return fmt.Errorf("failed to unpin parent page %d before descending: %w", page.GetPageID(), unpinErr)
			}
			// Recurse into the child
			return bt.insertNonFull(childNode, childPage, key, value, incrementSize) // Recursive call unpins
		}
	}
}

// splitChild splits a full childNode, promoting a key to the parentNode.
// It takes ownership of `parentNode`, `parentPage`, `childToSplitNode`, `childToSplitPage`.
// It serializes and unpins `childToSplitPage` and the new sibling page.
// It modifies `parentNode` and marks `parentPage` dirty, but *does not unpin* `parentPage`.
// The caller (`insertNonFull`) is responsible for serializing and unpinning `parentPage`.
func (bt *BTree[K, V]) splitChild(parentNode *Node[K, V], parentPage *Page, childIdx int, childToSplitNode *Node[K, V], childToSplitPage *Page) error {
	t := bt.degree // B-tree degree/order

	// 1. Create a new page for the new sibling node
	newSiblingDiskPage, newSiblingPageID, err := bt.bpm.NewPage() // Allocates and pins
	if err != nil {
		bt.bpm.UnpinPage(childToSplitPage.GetPageID(), false) // Unpin child on error
		return fmt.Errorf("failed to create new sibling page during split: %w", err)
	}

	// 2. Create the new in-memory sibling node
	newSiblingNode := &Node[K, V]{
		pageID: newSiblingPageID,
		isLeaf: childToSplitNode.isLeaf, // Sibling is leaf if original child was
		tree:   bt,
		keys:   make([]K, t-1), // t-1 keys for the right half
		values: make([]V, t-1),
	}
	if !childToSplitNode.isLeaf {
		newSiblingNode.childPageIDs = make([]PageID, t) // t children for the right half
	} else {
		newSiblingNode.childPageIDs = make([]PageID, 0)
	}

	// 3. Find the middle key to promote to the parent
	middleKey := childToSplitNode.keys[t-1]
	middleValue := childToSplitNode.values[t-1]

	// 4. Copy the right half of keys/values/children from the child to the new sibling
	copy(newSiblingNode.keys, childToSplitNode.keys[t:])
	copy(newSiblingNode.values, childToSplitNode.values[t:])
	if !childToSplitNode.isLeaf {
		copy(newSiblingNode.childPageIDs, childToSplitNode.childPageIDs[t:])
	}

	// 5. Truncate the original child node to keep only its left half
	childToSplitNode.keys = childToSplitNode.keys[:t-1]
	childToSplitNode.values = childToSplitNode.values[:t-1]
	if !childToSplitNode.isLeaf {
		childToSplitNode.childPageIDs = childToSplitNode.childPageIDs[:t]
	}

	// 6. Insert the promoted middle key/value and new sibling's pointer into the parent node
	parentNode.keys = slices.Insert(parentNode.keys, childIdx, middleKey)
	parentNode.values = slices.Insert(parentNode.values, childIdx, middleValue)
	parentNode.childPageIDs = slices.Insert(parentNode.childPageIDs, childIdx+1, newSiblingPageID)

	// TODO: WAL - Log modifications to parentNode, childToSplitNode, newSiblingNode
	log.Printf("DEBUG: Split child %d into new sibling %d. Promoted key %v to parent %d.",
		childToSplitNode.pageID, newSiblingPageID, middleKey, parentNode.pageID)

	// 7. Serialize the modified child and the new sibling node
	var firstErr error
	if errS := childToSplitNode.serialize(childToSplitPage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); errS != nil {
		firstErr = fmt.Errorf("failed to serialize original child %d during split: %w", childToSplitNode.pageID, errS)
	}
	if errS := newSiblingNode.serialize(newSiblingDiskPage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); errS != nil {
		if firstErr == nil {
			firstErr = fmt.Errorf("failed to serialize new sibling %d during split: %w", newSiblingPageID, errS)
		}
	}

	// 8. Unpin child and new sibling pages, marking them dirty
	if e := bt.bpm.UnpinPage(childToSplitPage.GetPageID(), true); e != nil && firstErr == nil {
		firstErr = fmt.Errorf("failed to unpin original child %d during split: %w", childToSplitPage.GetPageID(), e)
	}
	if e := bt.bpm.UnpinPage(newSiblingDiskPage.GetPageID(), true); e != nil && firstErr == nil {
		firstErr = fmt.Errorf("failed to unpin new sibling %d during split: %w", newSiblingDiskPage.GetPageID(), e)
	}

	// Mark parent page dirty. It will be serialized and unpinned by the caller (insertNonFull).
	parentPage.SetDirty(true)
	return firstErr
}

// --- Deletion Implementation (Persistent) ---

// Delete removes a key-value pair from the B-tree.
func (bt *BTree[K, V]) Delete(key K) error {
	if bt.rootPageID == InvalidPageID {
		return ErrKeyNotFound // Cannot delete from an empty tree
	}

	// Pre-check: Search for the key to determine if it actually exists.
	// This helps in correctly updating the TreeSize and returning ErrKeyNotFound.
	_, keyActuallyExistsInTree, searchErr := bt.Search(key)
	if searchErr != nil {
		return fmt.Errorf("error during pre-delete search for key %v: %w", key, searchErr)
	}
	if !keyActuallyExistsInTree {
		return ErrKeyNotFound // Key not in tree
	}

	// Fetch the root node to start the recursive deletion
	rootNode, rootPage, err := bt.fetchNode(bt.rootPageID) // Root is pinned
	if err != nil {
		return fmt.Errorf("failed to fetch root node for deletion: %w", err)
	}

	// Perform the recursive deletion. It handles unpinning of pages.
	wasDeleted, err := bt.deleteRecursive(rootNode, rootPage, key)
	if err != nil {
		return fmt.Errorf("recursive deletion failed for key %v: %w", key, err)
	}
	if !wasDeleted {
		// This should theoretically not happen if pre-search found the key
		log.Printf("WARNING: Key %v found in pre-search but not deleted by recursive delete.", key)
		return ErrKeyNotFound
	}

	// After deletion, the root might have changed (e.g., if it became empty and its child became the new root).
	// We need to re-fetch the root to check its current state and update the B-tree's rootPageID if needed.
	// This re-fetch is somewhat inefficient but safer given complex deletion paths.
	currentRootNode, currentRootPage, fetchErr := bt.fetchNode(bt.rootPageID) // Current root is pinned
	if fetchErr != nil {
		return fmt.Errorf("failed to fetch root page post-delete for root check: %w", fetchErr)
	}

	rootChanged := false
	// Case: Root has 0 keys and is not a leaf (meaning it has only one child left after merges/deletions)
	if len(currentRootNode.keys) == 0 && !currentRootNode.isLeaf {
		if len(currentRootNode.childPageIDs) == 1 {
			oldRootPageID := bt.rootPageID
			bt.rootPageID = currentRootNode.childPageIDs[0] // The only child becomes the new root
			log.Printf("DEBUG: Root %d has 0 keys and 1 child. New root is child %d.", oldRootPageID, bt.rootPageID)

			// Update the file header with the new root PageID
			if err := bt.diskManager.UpdateHeaderField(func(h *DBFileHeader) { h.RootPageID = bt.rootPageID }); err != nil {
				bt.rootPageID = oldRootPageID                        // Revert on error
				bt.bpm.UnpinPage(currentRootPage.GetPageID(), false) // Unpin old root
				return fmt.Errorf("failed to update header for new root after deletion: %w", err)
			}
			// TODO: Deallocate the oldRootPageID from disk using the free list manager
			// This is critical to reclaim space.
			if err := bt.diskManager.DeallocatePage(oldRootPageID); err != nil {
				log.Printf("ERROR: Failed to deallocate old root page %d: %v", oldRootPageID, err)
				// This is a soft error, but indicates a resource leak.
			}
			rootChanged = true
		} else if len(currentRootNode.childPageIDs) == 0 {
			// This state usually means the tree became completely empty (last key deleted from a root that was also a leaf).
			// The root should remain a single empty leaf page. No change to bt.rootPageID needed if it's already pointing to this empty leaf.
			// The `deleteRecursive` should have already made `currentRootNode` an empty leaf.
			log.Printf("DEBUG: B-tree root %d became empty after deletion.", bt.rootPageID)
		}
	}

	// Unpin the current (potentially new) root page. It's only dirty if its ID changed in the header.
	unpinErr := bt.bpm.UnpinPage(currentRootPage.GetPageID(), rootChanged)
	if unpinErr != nil {
		log.Printf("WARNING: Error unpinning root page %d after deletion check: %v", currentRootPage.GetPageID(), unpinErr)
	}

	// If a key was successfully deleted from the tree structure, update the persisted size.
	if wasDeleted {
		if err := bt.updatePersistedSize(-1); err != nil {
			log.Printf("ERROR: Failed to update tree size after deletion: %v", err)
		}
	}
	return nil
}

// deleteRecursive attempts to delete a key from the subtree rooted at `node`.
// It takes ownership of `node` and `nodePage`, unpinning `nodePage` before returning.
// Returns (true if a key was deleted, error).
func (bt *BTree[K, V]) deleteRecursive(node *Node[K, V], nodePage *Page, key K) (bool, error) {
	idx, found := slices.BinarySearchFunc(node.keys, key, bt.keyOrder)
	var err error
	var actuallyDeleted bool = false

	if node.isLeaf {
		// Case 1: Key is in a leaf node.
		if found {
			// Remove the key and its value from the leaf node.
			node.keys = slices.Delete(node.keys, idx, idx+1)
			node.values = slices.Delete(node.values, idx, idx+1)
			log.Printf("DEBUG: Deleted key %v from leaf node %d. New numKeys: %d", key, node.pageID, len(node.keys))

			// TODO: WAL Log key deletion from leaf
			// Serialize the modified leaf node and mark it dirty.
			if errS := node.serialize(nodePage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); errS != nil {
				bt.bpm.UnpinPage(nodePage.GetPageID(), false) // Unpin even on serialize failure
				return false, fmt.Errorf("failed to serialize leaf node %d after deletion: %w", node.pageID, errS)
			}
			actuallyDeleted = true                             // Key was successfully deleted.
			err = bt.bpm.UnpinPage(nodePage.GetPageID(), true) // Unpin, mark dirty
		} else {
			// Key not found in this leaf node.
			log.Printf("DEBUG: Key %v not found in leaf node %d during recursive delete.", key, node.pageID)
			err = bt.bpm.UnpinPage(nodePage.GetPageID(), false) // Unpin, page is clean
		}
		return actuallyDeleted, err
	}

	// Key is not in a leaf node (this is an internal node).
	if found {
		// Case 2: Key `k` is found in this internal node `node` at `node.keys[idx]`.
		// We need to replace `k` with its predecessor or successor, and then recursively delete that predecessor/successor.
		log.Printf("DEBUG: Key %v found in internal node %d at index %d.", key, node.pageID, idx)
		actuallyDeleted, err = bt.deleteFromInternalNode(node, nodePage, key, idx)
		// `deleteFromInternalNode` is responsible for serializing `nodePage` (if modified) and unpinning it.
	} else {
		// Case 3: Key `k` is NOT in this internal node `node`. Recurse into the appropriate child.
		// `idx` is the index of the child subtree that *must* contain `k` if it exists.
		childPageID := node.childPageIDs[idx]
		childNode, childPage, fetchErr := bt.fetchNode(childPageID) // Child is pinned
		if fetchErr != nil {
			bt.bpm.UnpinPage(nodePage.GetPageID(), false) // Unpin parent on child fetch failure
			return false, fmt.Errorf("failed to fetch child page %d for key %v deletion: %w", childPageID, key, fetchErr)
		}

		// Ensure the child has enough keys (`t` keys if it's an internal node, `t-1` if it's a leaf)
		// before descending, to prevent underflow situations that require immediate merging/borrowing.
		if len(childNode.keys) < bt.degree { // Check if child is "under-full" (less than degree-1 keys)
			// Unpin the child page before calling `ensureChildHasEnoughKeys`,
			// as `ensureChildHasEnoughKeys` will re-fetch/modify it or its siblings.
			if errUnpinChild := bt.bpm.UnpinPage(childPage.GetPageID(), false); errUnpinChild != nil {
				// Log but try to continue, as `ensureChildHasEnoughKeys` might recover
				log.Printf("WARNING: Error unpinning child page %d before ensureChildHasEnoughKeys: %v", childPage.GetPageID(), errUnpinChild)
			}
			log.Printf("DEBUG: Child node %d is under-full. Calling ensureChildHasEnoughKeys from parent %d.", childNode.pageID, node.pageID)

			// This function will ensure `node.childPageIDs[idx]` now points to a child with enough keys.
			// It modifies `node` (parent) and `nodePage` (marks dirty).
			errEnsure := bt.ensureChildHasEnoughKeys(node, nodePage, idx)
			if errEnsure != nil {
				bt.bpm.UnpinPage(nodePage.GetPageID(), false) // Unpin parent if ensure fails
				return false, fmt.Errorf("failed to ensure child %d has enough keys: %w", childNode.pageID, errEnsure)
			}

			// After `ensureChildHasEnoughKeys`, the structure might have changed.
			// The parent node (`node`) might have a new key (from a merge) or updated child pointers.
			// Re-evaluate `idx` for descending and fetch the correct child again.
			// This is crucial because `idx` might now point to a *different* child after a merge or borrow.
			_, foundAfterEnsure := slices.BinarySearchFunc(node.keys, key, bt.keyOrder)
			if foundAfterEnsure {
				// Key was found in the parent after reorganization (e.g., a child merged up).
				// We now fall back to Case 2.
				log.Printf("DEBUG: Key %v found in parent %d after ensureChildHasEnoughKeys. Recursing into deleteFromInternalNode.", key, node.pageID)
				actuallyDeleted, err = bt.deleteFromInternalNode(node, nodePage, key, idx)
				// deleteFromInternalNode will serialize and unpin nodePage.
				return actuallyDeleted, err
			}
			// If not found in parent, recalculate idx for new child pointer
			idx = slices.IndexFunc(node.keys, func(k K) bool { return bt.keyOrder(key, k) <= 0 })
			if idx == -1 {
				idx = len(node.keys)
			}
			reFetchedChildPageID := node.childPageIDs[idx]
			reFetchedChildNode, reFetchedChildPage, reFetchErr := bt.fetchNode(reFetchedChildPageID) // New child is pinned
			if reFetchErr != nil {
				bt.bpm.UnpinPage(nodePage.GetPageID(), true) // Parent was dirtied by ensure
				return false, fmt.Errorf("failed to re-fetch child %d after ensureChildHasEnoughKeys: %w", reFetchedChildPageID, reFetchErr)
			}
			actuallyDeleted, err = bt.deleteRecursive(reFetchedChildNode, reFetchedChildPage, key) // Recursive call unpins child
		} else {
			// Child has enough keys, just recurse.
			log.Printf("DEBUG: Child node %d has enough keys. Recursing into child.", childNode.pageID)
			actuallyDeleted, err = bt.deleteRecursive(childNode, childPage, key) // Recursive call unpins child
		}
		// After the recursive call, unpin the parent page (`nodePage`).
		// It might have been dirtied by `ensureChildHasEnoughKeys` or a subsequent write.
		if e := bt.bpm.UnpinPage(nodePage.GetPageID(), nodePage.IsDirty()); e != nil && err == nil {
			err = fmt.Errorf("failed to unpin parent page %d after recursive deletion: %w", nodePage.GetPageID(), e)
		}
	}
	return actuallyDeleted, err
}

// deleteFromInternalNode handles the case where the key to delete (`key`) is found in an internal `node`.
// It replaces `key` with its predecessor or successor, and then recursively deletes that predecessor/successor.
// It takes ownership of `node` and `nodePage`, responsible for serializing `nodePage` (if modified) and unpinning it.
func (bt *BTree[K, V]) deleteFromInternalNode(node *Node[K, V], nodePage *Page, key K, idxInNode int) (bool, error) {
	t := bt.degree
	var err error
	keyActuallyRemoved := false // This will be true if the replacement key is successfully deleted from child

	// Fetch left child (subtree containing predecessor of key)
	leftChildPageID := node.childPageIDs[idxInNode]
	leftChildNode, leftChildPage, fetchErr := bt.fetchNode(leftChildPageID) // Left child is pinned
	if fetchErr != nil {
		bt.bpm.UnpinPage(nodePage.GetPageID(), false) // Unpin parent on error
		return false, fmt.Errorf("failed to fetch left child %d for internal node deletion: %w", leftChildPageID, fetchErr)
	}

	// Fetch right child (subtree containing successor of key)
	rightChildPageID := node.childPageIDs[idxInNode+1]
	rightChildNode, rightChildPage, fetchErr := bt.fetchNode(rightChildPageID) // Right child is pinned
	if fetchErr != nil {
		bt.bpm.UnpinPage(leftChildPage.GetPageID(), false) // Unpin left child on error
		bt.bpm.UnpinPage(nodePage.GetPageID(), false)      // Unpin parent on error
		return false, fmt.Errorf("failed to fetch right child %d for internal node deletion: %w", rightChildPageID, fetchErr)
	}

	if len(leftChildNode.keys) >= t {
		// Case 2a: Left child has at least 't' keys.
		// Find the predecessor of `key` (rightmost key in left child's subtree).
		log.Printf("DEBUG: Borrowing predecessor from left child %d for key %v in parent %d.", leftChildNode.pageID, key, node.pageID)
		predKey, predValue := bt.findPredecessor(leftChildNode, leftChildPage) // `findPredecessor` unpins `leftChildPage`

		// Replace `key` in `node` with `predKey` and `predValue`.
		node.keys[idxInNode] = predKey
		node.values[idxInNode] = predValue
		nodePage.SetDirty(true) // Parent node is now dirty.

		// Now, recursively delete `predKey` from the left child's subtree.
		// We need to re-fetch the left child because `findPredecessor` unpinned it.
		reFetchedLeftChildNode, reFetchedLeftChildPage, reFetchErr := bt.fetchNode(leftChildPageID) // Left child is pinned again
		if reFetchErr != nil {
			bt.bpm.UnpinPage(nodePage.GetPageID(), false)       // Unpin parent on error
			bt.bpm.UnpinPage(rightChildPage.GetPageID(), false) // Unpin right child
			return false, fmt.Errorf("failed to re-fetch left child %d after predecessor finding: %w", leftChildPageID, reFetchErr)
		}

		// Unpin the right child's page, as it's not used in this path.
		if e := bt.bpm.UnpinPage(rightChildPage.GetPageID(), false); e != nil {
			log.Printf("WARNING: Error unpinning right child page %d: %v", rightChildPage.GetPageID(), e)
		}

		// Recursively delete the predecessor from the left child's subtree.
		keyActuallyRemoved, err = bt.deleteRecursive(reFetchedLeftChildNode, reFetchedLeftChildPage, predKey)
		// `deleteRecursive` will serialize and unpin `reFetchedLeftChildPage`.
		if err != nil {
			bt.bpm.UnpinPage(nodePage.GetPageID(), false) // Unpin parent on error
			return false, err
		}

		// Parent `node` was modified (key/value replaced). Serialize and unpin it.
		if errS := node.serialize(nodePage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); errS != nil {
			bt.bpm.UnpinPage(nodePage.GetPageID(), false)
			return false, fmt.Errorf("failed to serialize parent node %d after predecessor replacement: %w", node.pageID, errS)
		}
		err = bt.bpm.UnpinPage(nodePage.GetPageID(), true) // Unpin, mark dirty

	} else if len(rightChildNode.keys) >= t {
		// Case 2b: Right child has at least 't' keys.
		// Find the successor of `key` (leftmost key in right child's subtree).
		log.Printf("DEBUG: Borrowing successor from right child %d for key %v in parent %d.", rightChildNode.pageID, key, node.pageID)
		succKey, succValue := bt.findSuccessor(rightChildNode, rightChildPage) // `findSuccessor` unpins `rightChildPage`

		// Replace `key` in `node` with `succKey` and `succValue`.
		node.keys[idxInNode] = succKey
		node.values[idxInNode] = succValue
		nodePage.SetDirty(true) // Parent node is now dirty.

		// Re-fetch right child because `findSuccessor` unpinned it.
		reFetchedRightChildNode, reFetchedRightChildPage, reFetchErr := bt.fetchNode(rightChildPageID) // Right child is pinned again
		if reFetchErr != nil {
			bt.bpm.UnpinPage(nodePage.GetPageID(), false)      // Unpin parent on error
			bt.bpm.UnpinPage(leftChildPage.GetPageID(), false) // Unpin left child
			return false, fmt.Errorf("failed to re-fetch right child %d after successor finding: %w", rightChildPageID, reFetchErr)
		}

		// Unpin the left child's page.
		if e := bt.bpm.UnpinPage(leftChildPage.GetPageID(), false); e != nil {
			log.Printf("WARNING: Error unpinning left child page %d: %v", leftChildPage.GetPageID(), e)
		}

		// Recursively delete the successor from the right child's subtree.
		keyActuallyRemoved, err = bt.deleteRecursive(reFetchedRightChildNode, reFetchedRightChildPage, succKey)
		// `deleteRecursive` will serialize and unpin `reFetchedRightChildPage`.
		if err != nil {
			bt.bpm.UnpinPage(nodePage.GetPageID(), false) // Unpin parent on error
			return false, err
		}

		// Parent `node` was modified. Serialize and unpin it.
		if errS := node.serialize(nodePage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); errS != nil {
			bt.bpm.UnpinPage(nodePage.GetPageID(), false)
			return false, fmt.Errorf("failed to serialize parent node %d after successor replacement: %w", node.pageID, errS)
		}
		err = bt.bpm.UnpinPage(nodePage.GetPageID(), true) // Unpin, mark dirty

	} else {
		// Case 2c: Both children (left and right) have `t-1` keys. Merge them.
		// Merge `key` (from `node`) and all of `rightChildNode` into `leftChildNode`.
		// `node` will lose `key` and its pointer to `rightChild`.
		// `leftChildNode` will now contain `2t-1` keys (full).
		log.Printf("DEBUG: Merging child %d (left) and %d (right) for key %v in parent %d.",
			leftChildNode.pageID, rightChildNode.pageID, key, node.pageID)

		// `mergeChildrenAndKey` is a high-level function that handles:
		// 1. Moving `key` and `value` from `node` to `leftChildNode`.
		// 2. Moving all contents from `rightChildNode` to `leftChildNode`.
		// 3. Updating `node` (removing `key` and `rightChild` pointer).
		// 4. Serializing `leftChildNode` and marking `nodePage` dirty.
		// 5. Unpinning `leftChildPage` and `rightChildPage`.
		// 6. Deallocating `rightChildPageID`.
		err = bt.mergeChildrenAndKey(node, nodePage, idxInNode, leftChildNode, leftChildPage, rightChildNode, rightChildPage, key)
		if err != nil {
			// If merge fails, some pages might still be pinned. Attempt to unpin for clean exit.
			bt.bpm.UnpinPage(leftChildPage.GetPageID(), false)
			bt.bpm.UnpinPage(rightChildPage.GetPageID(), false)
			bt.bpm.UnpinPage(nodePage.GetPageID(), false)
			return false, fmt.Errorf("failed to merge children %d and %d for key %v: %w", leftChildPageID, rightChildPageID, key, err)
		}
		// At this point, `nodePage` is marked dirty by `mergeChildrenAndKey`, but not unpinned.
		// `leftChildPage` and `rightChildPage` are unpinned by `mergeChildrenAndKey`.

		// The `key` that was originally in `node` (and now in the merged `leftChildNode`)
		// needs to be recursively deleted from the merged child.
		reFetchedLeftChildNode, reFetchedLeftChildPage, reFetchErr := bt.fetchNode(leftChildPageID) // Merged child is pinned
		if reFetchErr != nil {
			bt.bpm.UnpinPage(nodePage.GetPageID(), true) // Parent was dirtied by merge
			return false, fmt.Errorf("failed to re-fetch merged child %d: %w", leftChildPageID, reFetchErr)
		}
		actuallyDeleted, err := bt.deleteRecursive(reFetchedLeftChildNode, reFetchedLeftChildPage, key)
		// `deleteRecursive` will serialize and unpin `reFetchedLeftChildPage`.
		log.Println("Actually Deleted", actuallyDeleted)
		// Finally, unpin the parent node's page.
		if errS := node.serialize(nodePage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); errS != nil {
			bt.bpm.UnpinPage(nodePage.GetPageID(), false)
			return false, fmt.Errorf("failed to serialize parent node %d after merge: %w", node.pageID, errS)
		}
		if e := bt.bpm.UnpinPage(nodePage.GetPageID(), true); e != nil && err == nil {
			err = e // Propagate unpin error if no other error occurred
		}
	}
	return keyActuallyRemoved, err
}

// findPredecessor finds the rightmost key in the subtree rooted at `node`.
// It takes ownership of `node` and `nodePage`, unpinning `nodePage` before returning.
func (bt *BTree[K, V]) findPredecessor(node *Node[K, V], nodePage *Page) (K, V) {
	if node.isLeaf {
		// If it's a leaf, the predecessor is simply the largest key in this node.
		k := node.keys[len(node.keys)-1]
		v := node.values[len(node.values)-1]
		// Unpin the leaf page before returning.
		if e := bt.bpm.UnpinPage(nodePage.GetPageID(), false); e != nil {
			log.Printf("WARNING: Error unpinning leaf page %d during predecessor finding: %v", nodePage.GetPageID(), e)
		}
		return k, v
	}
	// If it's an internal node, recursively go to the rightmost child.
	rightmostChildID := node.childPageIDs[len(node.childPageIDs)-1]
	// Unpin current node's page before fetching the child.
	if e := bt.bpm.UnpinPage(nodePage.GetPageID(), false); e != nil {
		log.Printf("WARNING: Error unpinning internal page %d during predecessor finding: %v", nodePage.GetPageID(), e)
	}

	childNode, childPage, err := bt.fetchNode(rightmostChildID) // Child is pinned
	if err != nil {
		// This is a critical error; predecessor finding failed.
		// Panic or return a specific error that can be handled.
		// For now, panic as finding predecessor is essential for deletion in this path.
		panic(fmt.Errorf("critical error: failed to fetch rightmost child %d during predecessor finding: %w", rightmostChildID, err))
	}
	// Recursive call will handle unpinning of childPage.
	return bt.findPredecessor(childNode, childPage)
}

// findSuccessor finds the leftmost key in the subtree rooted at `node`.
// It takes ownership of `node` and `nodePage`, unpinning `nodePage` before returning.
func (bt *BTree[K, V]) findSuccessor(node *Node[K, V], nodePage *Page) (K, V) {
	if node.isLeaf {
		// If it's a leaf, the successor is simply the smallest key in this node.
		k := node.keys[0]
		v := node.values[0]
		// Unpin the leaf page before returning.
		if e := bt.bpm.UnpinPage(nodePage.GetPageID(), false); e != nil {
			log.Printf("WARNING: Error unpinning leaf page %d during successor finding: %v", nodePage.GetPageID(), e)
		}
		return k, v
	}
	// If it's an internal node, recursively go to the leftmost child.
	leftmostChildID := node.childPageIDs[0]
	// Unpin current node's page before fetching the child.
	if e := bt.bpm.UnpinPage(nodePage.GetPageID(), false); e != nil {
		log.Printf("WARNING: Error unpinning internal page %d during successor finding: %v", nodePage.GetPageID(), e)
	}

	childNode, childPage, err := bt.fetchNode(leftmostChildID) // Child is pinned
	if err != nil {
		// Critical error: successor finding failed.
		panic(fmt.Errorf("critical error: failed to fetch leftmost child %d during successor finding: %w", leftmostChildID, err))
	}
	// Recursive call will handle unpinning of childPage.
	return bt.findSuccessor(childNode, childPage)
}

// ensureChildHasEnoughKeys ensures that the child at `childIdx` has at least `t` keys (if internal) or `t-1` keys (if leaf).
// It does this by attempting to borrow from siblings or merging with a sibling.
// It takes ownership of `parentNode` and `parentPage` but *does not unpin `parentPage`*.
// It fetches, modifies, serializes, and unpins the involved child and sibling pages.
// It marks `parentPage` dirty if `parentNode` is modified.
func (bt *BTree[K, V]) ensureChildHasEnoughKeys(parentNode *Node[K, V], parentPage *Page, childIdx int) error {
	childPageID := parentNode.childPageIDs[childIdx]
	childNode, childPage, err := bt.fetchNode(childPageID) // Child is pinned
	if err != nil {
		return fmt.Errorf("failed to fetch child %d in ensureChildHasEnoughKeys: %w", childPageID, err)
	}

	t := bt.degree
	// Check if child already has enough keys (t-1 for leaf, t for internal is the minimum required after potential borrowing).
	// A safe check is to ensure it has at least `t` keys if it's an internal node, or `t-1` keys if it's a leaf.
	// The problem statement says `len(childNode.keys) < bt.degree` which is `t-1` keys. So it means it has `t-2` or fewer.
	// If `len(childNode.keys) >= t-1`, it might already satisfy the minimum after a previous operation.
	// Let's assume current check `len(childNode.keys) < bt.degree` implies underflow.
	if len(childNode.keys) >= t { // The B-tree minimum key count for internal nodes is t-1, for leaves is t-1.
		// If child has `t` or more keys, it already has enough.
		// In some B-tree implementations, the minimum for internal nodes is `t-1` and for leaves `t-1`.
		// For deletion, we need to ensure child has at least `t` keys for recursive descent if it's an internal node.
		// If it's a leaf, `t-1` keys is the minimum.
		// To be safe for recursive descent, a node should have at least `t` keys if it's not a leaf.
		// If it's a leaf, `t-1` is minimum.
		// Current check `len(childNode.keys) < bt.degree` is for `t-1` keys. So if it has `t-2` or fewer keys, it's underfull.
		// If it has `t-1` keys, it's at minimum, but may need a borrow for recursive descent.
		// Let's keep the existing check for underflow.
		log.Printf("DEBUG: Child node %d already has enough keys (%d >= %d). No action needed.", childNode.pageID, len(childNode.keys), t)
		return bt.bpm.UnpinPage(childPage.GetPageID(), false) // Unpin clean child
	}

	// Try to borrow from left sibling (if one exists)
	if childIdx > 0 {
		leftSiblingPageID := parentNode.childPageIDs[childIdx-1]
		leftSiblingNode, leftSiblingPage, fetchErr := bt.fetchNode(leftSiblingPageID) // Left sibling is pinned
		if fetchErr != nil {
			bt.bpm.UnpinPage(childPage.GetPageID(), false) // Unpin child on error
			return fmt.Errorf("failed to fetch left sibling %d: %w", leftSiblingPageID, fetchErr)
		}

		if len(leftSiblingNode.keys) >= t { // Left sibling has enough to lend
			log.Printf("DEBUG: Borrowing from left sibling %d for child %d.", leftSiblingNode.pageID, childNode.pageID)
			err = bt.borrowFromLeftSibling(parentNode, parentPage, childIdx, childNode, childPage, leftSiblingNode, leftSiblingPage)
			// `borrowFromLeftSibling` handles serialization and unpinning of `childPage` and `leftSiblingPage`.
			// It also marks `parentPage` dirty.
			return err // Propagate error from borrow
		}
		// Left sibling cannot lend, unpin it.
		if e := bt.bpm.UnpinPage(leftSiblingPage.GetPageID(), false); e != nil {
			log.Printf("WARNING: Error unpinning left sibling %d (cannot lend): %v", leftSiblingPage.GetPageID(), e)
			// Continue, as this unpin error is less critical than the main operation
		}
	}

	// Try to borrow from right sibling (if one exists and left sibling couldn't lend)
	if childIdx < len(parentNode.childPageIDs)-1 {
		rightSiblingPageID := parentNode.childPageIDs[childIdx+1]
		rightSiblingNode, rightSiblingPage, fetchErr := bt.fetchNode(rightSiblingPageID) // Right sibling is pinned
		if fetchErr != nil {
			bt.bpm.UnpinPage(childPage.GetPageID(), false) // Unpin child on error
			return fmt.Errorf("failed to fetch right sibling %d: %w", rightSiblingPageID, fetchErr)
		}

		if len(rightSiblingNode.keys) >= t { // Right sibling has enough to lend
			log.Printf("DEBUG: Borrowing from right sibling %d for child %d.", rightSiblingNode.pageID, childNode.pageID)
			err = bt.borrowFromRightSibling(parentNode, parentPage, childIdx, childNode, childPage, rightSiblingNode, rightSiblingPage)
			// `borrowFromRightSibling` handles serialization and unpinning of `childPage` and `rightSiblingPage`.
			// It also marks `parentPage` dirty.
			return err
		}
		// Right sibling cannot lend, unpin it.
		if e := bt.bpm.UnpinPage(rightSiblingPage.GetPageID(), false); e != nil {
			log.Printf("WARNING: Error unpinning right sibling %d (cannot lend): %v", rightSiblingPage.GetPageID(), e)
			// Continue
		}
	}

	// If neither sibling can lend, we must merge.
	// Unpin the child page first, as it will be merged into another or be the target of a merge.
	if errUnpin := bt.bpm.UnpinPage(childPage.GetPageID(), false); errUnpin != nil {
		log.Printf("WARNING: Error unpinning child page %d before merge: %v", childPage.GetPageID(), errUnpin)
		// Continue
	}

	log.Printf("DEBUG: Neither sibling can lend for child %d. Performing merge.", childNode.pageID)
	// Decide which sibling to merge with (prefer left if possible, else right).
	if childIdx > 0 { // Merge child with left sibling
		// `mergeChildrenAndKey` expects `leftMergeChildIdx` to be the index of the LEFT child of the merge pair.
		// So, merge `parentNode.childPageIDs[childIdx-1]` (left sibling) and `parentNode.childPageIDs[childIdx]` (current child).
		// The key `parentNode.keys[childIdx-1]` will come down from the parent.
		return bt.mergeChildrenAndKey(parentNode, parentPage, childIdx-1, nil, nil, nil, nil, parentNode.keys[childIdx-1])
	} else { // Merge child with right sibling (current child is `childIdx=0`)
		// Merge `parentNode.childPageIDs[childIdx]` (current child) and `parentNode.childPageIDs[childIdx+1]` (right sibling).
		// The key `parentNode.keys[childIdx]` will come down from the parent.
		return bt.mergeChildrenAndKey(parentNode, parentPage, childIdx, nil, nil, nil, nil, parentNode.keys[childIdx])
	}
}

// borrowFromLeftSibling takes the largest key/value from the left sibling and moves it to the child,
// promoting a key from the parent to the child, and updating the parent's key.
// It takes ownership of all nodes/pages. It serializes and unpins `childPage` and `leftSiblingPage`.
// It marks `parentPage` dirty but *does not unpin* it.
func (bt *BTree[K, V]) borrowFromLeftSibling(
	parentNode *Node[K, V], parentPage *Page, childIdx int,
	childNode *Node[K, V], childPage *Page,
	leftSiblingNode *Node[K, V], leftSiblingPage *Page) error {

	log.Printf("DEBUG: Borrowing from left sibling: parent %d, child %d, leftSibling %d", parentNode.pageID, childNode.pageID, leftSiblingNode.pageID)

	// Key/value from parent that moves down to the child
	keyFromParent := parentNode.keys[childIdx-1]
	valueFromParent := parentNode.values[childIdx-1]

	// Key/value from left sibling that moves up to parent
	keyFromSibling := leftSiblingNode.keys[len(leftSiblingNode.keys)-1]
	valueFromSibling := leftSiblingNode.values[len(leftSiblingNode.values)-1]

	// Move key/value from parent down to the beginning of the child
	childNode.keys = slices.Insert(childNode.keys, 0, keyFromParent)
	childNode.values = slices.Insert(childNode.values, 0, valueFromParent)

	// Move key/value from left sibling up to replace the old key in parent
	parentNode.keys[childIdx-1] = keyFromSibling
	parentNode.values[childIdx-1] = valueFromSibling

	// If children are not leaves, move the rightmost child pointer from left sibling to leftmost of child
	if !childNode.isLeaf {
		childPointerFromSibling := leftSiblingNode.childPageIDs[len(leftSiblingNode.childPageIDs)-1]
		childNode.childPageIDs = slices.Insert(childNode.childPageIDs, 0, childPointerFromSibling)
		leftSiblingNode.childPageIDs = leftSiblingNode.childPageIDs[:len(leftSiblingNode.childPageIDs)-1]
	}

	// Remove the borrowed key/value from the left sibling
	leftSiblingNode.keys = leftSiblingNode.keys[:len(leftSiblingNode.keys)-1]
	leftSiblingNode.values = leftSiblingNode.values[:len(leftSiblingNode.values)-1]

	parentPage.SetDirty(true) // Parent node modified

	// Serialize modified child and left sibling nodes
	var firstErr error
	if err := childNode.serialize(childPage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
		firstErr = fmt.Errorf("failed to serialize child %d during borrow from left: %w", childNode.pageID, err)
	}
	if err := leftSiblingNode.serialize(leftSiblingPage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
		if firstErr == nil {
			firstErr = fmt.Errorf("failed to serialize left sibling %d during borrow from left: %w", leftSiblingNode.pageID, err)
		}
	}

	// Unpin child and left sibling pages, marking them dirty
	if e := bt.bpm.UnpinPage(childPage.GetPageID(), true); e != nil && firstErr == nil {
		firstErr = fmt.Errorf("failed to unpin child %d during borrow from left: %w", childPage.GetPageID(), e)
	}
	if e := bt.bpm.UnpinPage(leftSiblingPage.GetPageID(), true); e != nil && firstErr == nil {
		firstErr = fmt.Errorf("failed to unpin left sibling %d during borrow from left: %w", leftSiblingPage.GetPageID(), e)
	}

	// Parent page (`parentPage`) is marked dirty. Its serialization and unpinning are
	// the responsibility of the caller (e.g., `ensureChildHasEnoughKeys`, `deleteRecursive`).
	return firstErr
}

// borrowFromRightSibling takes the smallest key/value from the right sibling and moves it to the child,
// promoting a key from the parent to the child, and updating the parent's key.
// It takes ownership of all nodes/pages. It serializes and unpins `childPage` and `rightSiblingPage`.
// It marks `parentPage` dirty but *does not unpin* it.
func (bt *BTree[K, V]) borrowFromRightSibling(
	parentNode *Node[K, V], parentPage *Page, childIdx int,
	childNode *Node[K, V], childPage *Page,
	rightSiblingNode *Node[K, V], rightSiblingPage *Page) error {

	log.Printf("DEBUG: Borrowing from right sibling: parent %d, child %d, rightSibling %d", parentNode.pageID, childNode.pageID, rightSiblingNode.pageID)

	// Key/value from parent that moves down to the end of the child
	keyFromParent := parentNode.keys[childIdx]
	valueFromParent := parentNode.values[childIdx]

	// Key/value from right sibling that moves up to parent
	keyFromSibling := rightSiblingNode.keys[0]
	valueFromSibling := rightSiblingNode.values[0]

	// Move key/value from parent down to the end of the child
	childNode.keys = append(childNode.keys, keyFromParent)
	childNode.values = append(childNode.values, valueFromParent)

	// Move key/value from right sibling up to replace the old key in parent
	parentNode.keys[childIdx] = keyFromSibling
	parentNode.values[childIdx] = valueFromSibling

	// If children are not leaves, move the leftmost child pointer from right sibling to rightmost of child
	if !childNode.isLeaf {
		childPointerFromSibling := rightSiblingNode.childPageIDs[0]
		childNode.childPageIDs = append(childNode.childPageIDs, childPointerFromSibling)
		rightSiblingNode.childPageIDs = slices.Delete(rightSiblingNode.childPageIDs, 0, 1) // Remove first child pointer
	}

	// Remove the borrowed key/value from the right sibling
	rightSiblingNode.keys = slices.Delete(rightSiblingNode.keys, 0, 1)     // Remove first key
	rightSiblingNode.values = slices.Delete(rightSiblingNode.values, 0, 1) // Remove first value

	parentPage.SetDirty(true) // Parent node modified

	// Serialize modified child and right sibling nodes
	var firstErr error
	if err := childNode.serialize(childPage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
		firstErr = fmt.Errorf("failed to serialize child %d during borrow from right: %w", childNode.pageID, err)
	}
	if err := rightSiblingNode.serialize(rightSiblingPage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
		if firstErr == nil {
			firstErr = fmt.Errorf("failed to serialize right sibling %d during borrow from right: %w", rightSiblingNode.pageID, err)
		}
	}

	// Unpin child and right sibling pages, marking them dirty
	if e := bt.bpm.UnpinPage(childPage.GetPageID(), true); e != nil && firstErr == nil {
		firstErr = fmt.Errorf("failed to unpin child %d during borrow from right: %w", childPage.GetPageID(), e)
	}
	if e := bt.bpm.UnpinPage(rightSiblingPage.GetPageID(), true); e != nil && firstErr == nil {
		firstErr = fmt.Errorf("failed to unpin right sibling %d during borrow from right: %w", rightSiblingPage.GetPageID(), e)
	}

	// Parent page (`parentPage`) is marked dirty. Its serialization and unpinning are
	// the responsibility of the caller (e.g., `ensureChildHasEnoughKeys`, `deleteRecursive`).
	return firstErr
}

// mergeChildrenAndKey merges the right child (`parentNode.childPageIDs[leftMergeChildIdx+1]`)
// into the left child (`parentNode.childPageIDs[leftMergeChildIdx]`),
// and also moves the key (`parentNode.keys[leftMergeChildIdx]`) from the parent down.
// It handles fetching all involved pages, performing modifications, serializing, unpinning,
// and deallocating the right child's page. It marks `parentPage` dirty but *does not unpin* it.
// The `originalKeyToDelete` is the key that was found in the parent, now moved down, and needs
// to be recursively deleted from the merged child.
func (bt *BTree[K, V]) mergeChildrenAndKey(
	parentNode *Node[K, V], parentPage *Page, leftMergeChildIdx int,
	// Hints for left/right nodes/pages; they will be fetched if nil.
	// For clarity and directness, this function will always fetch them.
	_leftNodeHint *Node[K, V], _leftPageHint *Page,
	_rightNodeHint *Node[K, V], _rightPageHint *Page,
	originalKeyToDelete K, // The key that was in parentNode and now needs to be deleted from the merged child
) error {
	log.Printf("DEBUG: Initiating merge operation for parent %d, left child index %d, key to delete: %v", parentNode.pageID, leftMergeChildIdx, originalKeyToDelete)

	// 1. Fetch left child
	leftChildPageID := parentNode.childPageIDs[leftMergeChildIdx]
	leftChildNode, leftChildPage, err := bt.fetchNode(leftChildPageID) // Left child is pinned
	if err != nil {
		return fmt.Errorf("failed to fetch left child %d for merge: %w", leftChildPageID, err)
	}

	// 2. Fetch right child
	rightChildPageID := parentNode.childPageIDs[leftMergeChildIdx+1]
	rightChildNode, rightChildPage, err := bt.fetchNode(rightChildPageID) // Right child is pinned
	if err != nil {
		bt.bpm.UnpinPage(leftChildPage.GetPageID(), false) // Unpin left child on error
		return fmt.Errorf("failed to fetch right child %d for merge: %w", rightChildPageID, err)
	}

	// The key and value from parent that will move down to the left child
	keyFromParent := parentNode.keys[leftMergeChildIdx]
	valueFromParent := parentNode.values[leftMergeChildIdx]

	// TODO: WAL Log: merge operation, including deallocation of rightChildPageID

	// 3. Move key from parent down to leftChild.
	leftChildNode.keys = append(leftChildNode.keys, keyFromParent)
	leftChildNode.values = append(leftChildNode.values, valueFromParent)

	// 4. Append keys, values, and children from rightChild to leftChild.
	leftChildNode.keys = append(leftChildNode.keys, rightChildNode.keys...)
	leftChildNode.values = append(leftChildNode.values, rightChildNode.values...)
	if !leftChildNode.isLeaf {
		leftChildNode.childPageIDs = append(leftChildNode.childPageIDs, rightChildNode.childPageIDs...)
	}
	log.Printf("DEBUG: Merged content into left child %d. New keys: %d, New children: %d",
		leftChildNode.pageID, len(leftChildNode.keys), len(leftChildNode.childPageIDs))

	// 5. Remove the merged key/value from parent.
	parentNode.keys = slices.Delete(parentNode.keys, leftMergeChildIdx, leftMergeChildIdx+1)
	parentNode.values = slices.Delete(parentNode.values, leftMergeChildIdx, leftMergeChildIdx+1)
	// 6. Remove the rightChild pointer from parent.
	parentNode.childPageIDs = slices.Delete(parentNode.childPageIDs, leftMergeChildIdx+1, leftMergeChildIdx+2)
	log.Printf("DEBUG: Updated parent %d after merge. New keys: %d, New children: %d",
		parentNode.pageID, len(parentNode.keys), len(parentNode.childPageIDs))

	parentPage.SetDirty(true) // Parent node modified

	// 7. Serialize the modified left child.
	if errS := leftChildNode.serialize(leftChildPage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); errS != nil {
		bt.bpm.UnpinPage(leftChildPage.GetPageID(), false)  // Unpin left child on error
		bt.bpm.UnpinPage(rightChildPage.GetPageID(), false) // Unpin right child on error
		return fmt.Errorf("failed to serialize left child %d after merge: %w", leftChildPageID, errS)
	}

	// 8. Unpin right child's page (its content is now merged into leftChildPage).
	if errU := bt.bpm.UnpinPage(rightChildPage.GetPageID(), false); errU != nil {
		log.Printf("WARNING: Error unpinning right child %d after merge: %v", rightChildPage.GetPageID(), errU)
	}

	// 9. Deallocate the page of the now-merged right child.
	// This is critical to reclaim disk space.
	if errDealloc := bt.diskManager.DeallocatePage(rightChildPageID); errDealloc != nil {
		log.Printf("ERROR: Failed to deallocate merged right child page %d: %v", rightChildPageID, errDealloc)
		// This is a soft error, but indicates a resource leak if not handled.
	}

	// 10. Recursively delete the `originalKeyToDelete` from the now-merged `leftChildNode`.
	// `leftChildPage` is still pinned and dirty. The `deleteRecursive` call will handle its final unpinning.
	log.Printf("DEBUG: Recursively deleting original key %v from merged child %d.", originalKeyToDelete, leftChildNode.pageID)
	_, errDel := bt.deleteRecursive(leftChildNode, leftChildPage, originalKeyToDelete)
	return errDel // This error includes final unpinning of leftChildPage
}

// Close flushes all dirty pages and closes the database file.
func (bt *BTree[K, V]) Close() error {
	if bt.bpm == nil {
		return errors.New("btree buffer pool manager is nil, cannot close")
	}

	log.Println("INFO: Closing B-tree database...")
	// Update persisted tree size in header one last time (if you have an accurate in-memory size tracker)
	// For this implementation, TreeSize is updated on every insert/delete, but a final checkpoint is good.
	// If bt.size was an accurate in-memory field, you'd do:
	// if err := bt.diskManager.UpdateHeaderField(func(h *DBFileHeader){ h.TreeSize = bt.sizeInMemory }); err != nil {
	// 	fmt.Fprintf(os.Stderr, "Error updating tree size in header on close: %v\n", err)
	// }

	// Flush all dirty pages from the buffer pool to disk
	flushErr := bt.bpm.FlushAllPages()
	if flushErr != nil {
		log.Printf("ERROR: Error flushing all pages on close: %v", flushErr)
	}

	// Close the underlying disk manager (which closes the file)
	closeErr := bt.diskManager.Close()
	if closeErr != nil {
		log.Printf("ERROR: Error closing disk manager on close: %v", closeErr)
	}

	if flushErr != nil || closeErr != nil {
		return fmt.Errorf("errors occurred during B-tree close: flush error: %v, close error: %v", flushErr, closeErr)
	}
	log.Println("INFO: B-tree database closed successfully.")
	return nil
}

// String provides a string representation of the B-tree for debugging.
func (bt *BTree[K, V]) String() string {
	if bt.rootPageID == InvalidPageID {
		return "BTree (empty or not initialized)\n"
	}
	str, err := bt.stringRecursive(bt.rootPageID, 0)
	if err != nil {
		return fmt.Sprintf("Error generating string: %v\n", err)
	}
	return str
}

// stringRecursive is a helper for String() to traverse and format the tree.
func (bt *BTree[K, V]) stringRecursive(pageID PageID, level int) (string, error) {
	if pageID == InvalidPageID {
		return "", nil
	}
	node, page, err := bt.fetchNode(pageID) // Node is pinned
	if err != nil {
		return "", fmt.Errorf("stringRecursive failed to fetch page %d: %w", pageID, err)
	}
	// Ensure the page is unpinned once we're done with it for this string representation.
	defer bt.bpm.UnpinPage(page.GetPageID(), false)

	s := ""
	indent := ""
	for i := 0; i < level; i++ {
		indent += "  " // Two spaces per level
	}
	s += fmt.Sprintf("%sPageID: %d (Leaf: %t, Keys: %d)\n", indent, node.pageID, node.isLeaf, len(node.keys))
	s += fmt.Sprintf("%s  Keys: %v\n", indent, node.keys)
	s += fmt.Sprintf("%s  Values: %v\n", indent, node.values)
	if !node.isLeaf {
		s += fmt.Sprintf("%s  ChildPageIDs (%d): %v\n", indent, len(node.childPageIDs), node.childPageIDs)
		for i, childPID := range node.childPageIDs {
			if childPID != InvalidPageID {
				childStr, err := bt.stringRecursive(childPID, level+1)
				if err != nil {
					s += fmt.Sprintf("%s  [%d]: Error fetching child %d: %v\n", indent, i, childPID, err)
				} else {
					s += childStr
				}
			} else {
				s += fmt.Sprintf("%s  [%d]: <InvalidChildPageID>\n", indent, i)
			}
		}
	}
	return s, nil
}

// --- Default Serializers/Deserializers (Generic K, V) ---

// DefaultKeyOrder provides a comparison function for comparable types.
func DefaultKeyOrder[K cmp.Ordered](a, b K) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// SerializeInt64 serializes an int64 to a byte slice.
func SerializeInt64(k int64) ([]byte, error) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(k))
	return buf, nil
}

// DeserializeInt64 deserializes a byte slice to an int64.
func DeserializeInt64(data []byte) (int64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("%w: int64 data must be 8 bytes, got %d", ErrDeserialization, len(data))
	}
	return int64(binary.LittleEndian.Uint64(data)), nil
}

// SerializeString serializes a string to a byte slice.
func SerializeString(s string) ([]byte, error) {
	return []byte(s), nil
}

// DeserializeString deserializes a byte slice to a string.
func DeserializeString(data []byte) (string, error) {
	return string(data), nil
}
