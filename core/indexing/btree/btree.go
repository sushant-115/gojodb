package btree

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"slices"
	"sync"
)

// --- Configuration & Constants ---

const (
	DefaultPageSize          = 4096 // Bytes
	FileHeaderPageID  PageID = 0    // Page ID for the database file header
	InvalidPageID     PageID = 0    // Also used for header, but generally indicates invalid/unallocated
	MaxFilenameLength        = 255  // Example limit
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
)

// --- Page Management ---

// PageID represents a unique identifier for a page on disk.
type PageID uint64

// Page represents an in-memory copy of a disk page.
type Page struct {
	id       PageID
	data     []byte // Actual page content
	pinCount uint32 // How many users are currently using this page
	isDirty  bool   // True if the page has been modified since being read from disk
	// Potentially: lruCounter for page replacement policies
}

// NewPage creates a new Page instance.
func NewPage(id PageID, size int) *Page {
	return &Page{
		id:       id,
		data:     make([]byte, size), // Allocate data based on page size
		pinCount: 0,
		isDirty:  false,
	}
}

// Reset resets the page metadata, useful when reusing a frame in buffer pool.
func (p *Page) Reset() {
	p.id = InvalidPageID
	// p.data might be reused, or re-allocated if page size changes (though typically fixed)
	p.pinCount = 0
	p.isDirty = false
	// Clear data for security/consistency if needed:
	// for i := range p.data { p.data[i] = 0 }
}

// GetData returns the page's data.
func (p *Page) GetData() []byte {
	return p.data
}

// GetPageID returns the page's ID.
func (p *Page) GetPageID() PageID {
	return p.id
}

// IsDirty returns true if the page is dirty.
func (p *Page) IsDirty() bool {
	return p.isDirty
}

// Pin increments the pin count.
func (p *Page) Pin() {
	p.pinCount++
}

// Unpin decrements the pin count.
func (p *Page) Unpin() {
	if p.pinCount > 0 {
		p.pinCount--
	}
}

// GetPinCount returns the pin count.
func (p *Page) GetPinCount() uint32 {
	return p.pinCount
}

// SetDirty marks the page as dirty.
func (p *Page) SetDirty(dirty bool) {
	p.isDirty = dirty
}

// --- DiskManager ---
// DiskManager is responsible for direct I/O operations with the database file.

const dbFileHeaderSize = 64 // Example size for header (magic, version, rootPageID, degree, pageSize, etc.)

type DBFileHeader struct {
	Magic      uint32 // To identify the file type
	Version    uint32 // File format version
	PageSize   uint32
	RootPageID PageID
	Degree     uint32 // BTree degree
	// Potentially: next free page ID for simple free list, total pages, etc.
	// For simplicity, we'll manage free pages implicitly or via a dedicated free list page later.
}

const DBMagic uint32 = 0x6010DB00 // GoJoDB00

type DiskManager struct {
	filePath string
	file     *os.File
	pageSize int
	numPages uint64 // Could track total pages for allocation
	mu       sync.Mutex
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

// OpenOrCreateFile opens an existing database file or creates a new one.
// If creating, it initializes the header.
func (dm *DiskManager) OpenOrCreateFile(create bool, degree int) (*DBFileHeader, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	var file *os.File
	var err error
	var header DBFileHeader

	if _, statErr := os.Stat(dm.filePath); os.IsNotExist(statErr) {
		if !create {
			return nil, fmt.Errorf("%w: %s", ErrDBFileNotFound, dm.filePath)
		}
		file, err = os.OpenFile(dm.filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			return nil, fmt.Errorf("%w: creating file %s: %v", ErrIO, dm.filePath, err)
		}
		dm.file = file
		// Initialize header for a new file
		header = DBFileHeader{
			Magic:      DBMagic,
			Version:    1,
			PageSize:   uint32(dm.pageSize),
			RootPageID: InvalidPageID, // Will be set when root BTree node is created
			Degree:     uint32(degree),
		}
		if err := dm.writeHeader(&header); err != nil {
			dm.Close()
			os.Remove(dm.filePath) // Cleanup
			return nil, err
		}
		// Allocate space for the header page itself
		if _, err := dm.allocateRawPage(); err != FileHeaderPageID { // Should return 0
			dm.Close()
			os.Remove(dm.filePath)
			return nil, fmt.Errorf("failed to allocate header page: %v", err)
		}

	} else if statErr == nil { // File exists
		if create { // If create is true but file exists, it's an error to prevent overwrite
			return nil, fmt.Errorf("%w: %s", ErrDBFileExists, dm.filePath)
		}
		file, err = os.OpenFile(dm.filePath, os.O_RDWR, 0666)
		if err != nil {
			return nil, fmt.Errorf("%w: opening file %s: %v", ErrIO, dm.filePath, err)
		}
		dm.file = file
		if err := dm.readHeader(&header); err != nil {
			dm.Close()
			return nil, err
		}
		if header.Magic != DBMagic {
			dm.Close()
			return nil, fmt.Errorf("invalid database file magic number")
		}
		if header.PageSize != uint32(dm.pageSize) {
			dm.Close()
			return nil, fmt.Errorf("database file page size (%d) does not match configured page size (%d)", header.PageSize, dm.pageSize)
		}
	} else { // Other error stating file
		return nil, fmt.Errorf("%w: stating file %s: %v", ErrIO, dm.filePath, statErr)
	}

	fi, err := dm.file.Stat()
	if err != nil {
		dm.Close()
		return nil, fmt.Errorf("%w: getting file info: %v", ErrIO, err)
	}
	dm.numPages = uint64(fi.Size()) / uint64(dm.pageSize)

	return &header, nil
}

func (dm *DiskManager) writeHeader(header *DBFileHeader) error {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, header); err != nil {
		return fmt.Errorf("%w: serializing header: %v", ErrSerialization, err)
	}
	padding := make([]byte, dbFileHeaderSize-buf.Len()) // Ensure header is fixed size
	buf.Write(padding)

	if _, err := dm.file.WriteAt(buf.Bytes(), 0); err != nil {
		return fmt.Errorf("%w: writing header to disk: %v", ErrIO, err)
	}
	return dm.file.Sync() // Ensure header is flushed
}

func (dm *DiskManager) readHeader(header *DBFileHeader) error {
	data := make([]byte, dbFileHeaderSize)
	if _, err := dm.file.ReadAt(data, 0); err != nil {
		if err == io.EOF && len(data) < dbFileHeaderSize {
			return fmt.Errorf("database file is too small or corrupted (header)")
		}
		return fmt.Errorf("%w: reading header from disk: %v", ErrIO, err)
	}
	buf := bytes.NewReader(data)
	if err := binary.Read(buf, binary.LittleEndian, header); err != nil {
		return fmt.Errorf("%w: deserializing header: %v", ErrDeserialization, err)
	}
	return nil
}

func (dm *DiskManager) UpdateRootPageIDInHeader(rootPageID PageID) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.file == nil {
		return fmt.Errorf("file not open")
	}
	var header DBFileHeader
	if err := dm.readHeader(&header); err != nil {
		return err
	}
	header.RootPageID = rootPageID
	return dm.writeHeader(&header)
}

func (dm *DiskManager) ReadPage(pageID PageID, pageData []byte) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if dm.file == nil {
		return fmt.Errorf("file not open")
	}
	if len(pageData) != dm.pageSize {
		return fmt.Errorf("page data buffer size (%d) does not match disk manager page size (%d)", len(pageData), dm.pageSize)
	}

	offset := int64(pageID) * int64(dm.pageSize)
	bytesRead, err := dm.file.ReadAt(pageData, offset)
	if err != nil {
		if err == io.EOF && bytesRead < dm.pageSize {
			// This could mean trying to read beyond EOF or a corrupted file.
			// For a valid pageID, this shouldn't happen if numPages is correct.
			return fmt.Errorf("%w: partial page read for page %d (EOF), file may be corrupt or pageID out of bounds", ErrIO, pageID)
		}
		return fmt.Errorf("%w: reading page %d: %v", ErrIO, pageID, err)
	}
	if bytesRead != dm.pageSize {
		return fmt.Errorf("%w: short read for page %d, expected %d, got %d", ErrIO, pageID, dm.pageSize, bytesRead)
	}
	return nil
}

func (dm *DiskManager) WritePage(pageID PageID, pageData []byte) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if dm.file == nil {
		return fmt.Errorf("file not open")
	}
	if len(pageData) != dm.pageSize {
		return fmt.Errorf("page data buffer size (%d) does not match disk manager page size (%d)", len(pageData), dm.pageSize)
	}

	offset := int64(pageID) * int64(dm.pageSize)
	_, err := dm.file.WriteAt(pageData, offset)
	if err != nil {
		return fmt.Errorf("%w: writing page %d: %v", ErrIO, pageID, err)
	}
	// For production, fsync is crucial for durability, but expensive.
	// It's often managed by WAL or at transaction commit.
	// For now, let's add it here for simplicity, acknowledging performance impact.
	// return dm.file.Sync()
	return nil // Sync will be handled by buffer pool flush or higher level
}

// allocateRawPage extends the file to accommodate a new page and returns its ID.
// This is a low-level function; buffer pool manager should use this.
func (dm *DiskManager) allocateRawPage() (PageID, error) {
	// This simple version just appends. A real system uses a free list.
	newPageID := PageID(dm.numPages)
	// Extend the file by one page size.
	// Create empty page data to write, effectively extending the file.
	emptyPageData := make([]byte, dm.pageSize)
	offset := int64(newPageID) * int64(dm.pageSize)

	// Seek to the new page offset and write to ensure file is extended
	// Some OS might allow ftruncate then write, others require a write.
	if _, err := dm.file.WriteAt(emptyPageData, offset); err != nil {
		return InvalidPageID, fmt.Errorf("%w: extending file for new page %d: %v", ErrIO, newPageID, err)
	}
	dm.numPages++
	return newPageID, nil
}

func (dm *DiskManager) Sync() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.file != nil {
		return dm.file.Sync()
	}
	return nil
}

func (dm *DiskManager) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.file != nil {
		err := dm.file.Sync() // Ensure all writes are flushed before closing
		if err != nil {
			// Log sync error but still try to close
			fmt.Fprintf(os.Stderr, "Error syncing file on close: %v\n", err)
		}
		closeErr := dm.file.Close()
		dm.file = nil
		return closeErr
	}
	return nil
}

// --- BufferPoolManager ---
// Manages a pool of in-memory pages.

type BufferPoolManager struct {
	diskManager *DiskManager
	poolSize    int
	pages       []*Page        // Actual page frames
	pageTable   map[PageID]int // Maps PageID to index in pages slice
	// For page replacement (simplistic for now: free list, then evict first unpinned)
	freeList []int // List of indices of free frames in `pages`
	mu       sync.Mutex
	pageSize int
}

func NewBufferPoolManager(poolSize int, diskManager *DiskManager) *BufferPoolManager {
	bpm := &BufferPoolManager{
		diskManager: diskManager,
		poolSize:    poolSize,
		pages:       make([]*Page, poolSize),
		pageTable:   make(map[PageID]int),
		freeList:    make([]int, 0, poolSize),
		pageSize:    diskManager.pageSize,
	}
	for i := 0; i < poolSize; i++ {
		bpm.pages[i] = NewPage(InvalidPageID, bpm.pageSize) // Initialize with invalid page ID
		bpm.freeList = append(bpm.freeList, i)
	}
	return bpm
}

// FetchPage retrieves a page from the buffer pool. If not present, reads from disk.
func (bpm *BufferPoolManager) FetchPage(pageID PageID) (*Page, error) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	if frameIdx, ok := bpm.pageTable[pageID]; ok { // Page in pool
		page := bpm.pages[frameIdx]
		page.Pin()
		// TODO: Update LRU/Clock structures here for better replacement
		return page, nil
	}

	// Page not in pool, need to find a frame for it.
	frameIdx, err := bpm.getVictimFrame()
	if err != nil {
		return nil, err // e.g., ErrBufferPoolFull
	}

	victimPage := bpm.pages[frameIdx]

	// If victim page is dirty, write it back to disk first.
	if victimPage.IsDirty() {
		// Unlock temporarily to allow disk manager to lock if needed
		// This is tricky; a finer-grained locking or careful ordering is needed in real systems.
		// For this simplified version, we hold the BPM lock.
		if victimPage.GetPageID() != InvalidPageID { // Ensure it's a valid page to flush
			if err := bpm.diskManager.WritePage(victimPage.GetPageID(), victimPage.GetData()); err != nil {
				return nil, fmt.Errorf("failed to flush dirty victim page %d: %w", victimPage.GetPageID(), err)
			}
		}
	}

	// Remove victim from page table if it was valid
	if victimPage.GetPageID() != InvalidPageID {
		delete(bpm.pageTable, victimPage.GetPageID())
	}

	// Load new page from disk into the victim frame.
	newPageData := make([]byte, bpm.pageSize)
	if err := bpm.diskManager.ReadPage(pageID, newPageData); err != nil {
		// If read fails, put frame back to free list potentially
		bpm.freeList = append(bpm.freeList, frameIdx) // Simplistic, might mess order
		return nil, fmt.Errorf("failed to read page %d from disk: %w", pageID, err)
	}

	// Update the frame with new page's content and metadata.
	victimPage.id = pageID
	copy(victimPage.data, newPageData)
	victimPage.pinCount = 1 // Pin immediately
	victimPage.isDirty = false

	bpm.pageTable[pageID] = frameIdx
	// TODO: Update LRU/Clock structures here

	return victimPage, nil
}

// getVictimFrame finds a frame to use. First from free list, then by evicting.
func (bpm *BufferPoolManager) getVictimFrame() (int, error) {
	if len(bpm.freeList) > 0 {
		frameIdx := bpm.freeList[0]
		bpm.freeList = bpm.freeList[1:]
		return frameIdx, nil
	}

	// No free frames, try to evict. (Very simple eviction: first unpinned)
	// A real system uses LRU, Clock, etc.
	for i, page := range bpm.pages {
		if page.GetPinCount() == 0 {
			return i, nil
		}
	}
	return -1, ErrBufferPoolFull
}

// UnpinPage decreases pin count. If pin count becomes 0, page is candidate for eviction.
func (bpm *BufferPoolManager) UnpinPage(pageID PageID, isDirty bool) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	if frameIdx, ok := bpm.pageTable[pageID]; ok {
		page := bpm.pages[frameIdx]
		if page.GetPinCount() == 0 {
			return fmt.Errorf("cannot unpin page %d with pin count 0", pageID)
		}
		page.Unpin()
		if isDirty {
			page.SetDirty(true)
		}
		// If pin count is 0, it can be added to a list of evictable candidates
		// for LRU/Clock. For freeList, it's implicitly available if not pinned.
		return nil
	}
	return fmt.Errorf("%w: page %d not found to unpin", ErrPageNotFound, pageID)
}

// NewPage allocates a new page on disk and brings it into the buffer pool.
func (bpm *BufferPoolManager) NewPage() (*Page, PageID, error) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	newPageID, err := bpm.diskManager.allocateRawPage()
	if err != nil {
		return nil, InvalidPageID, fmt.Errorf("failed to allocate new page on disk: %w", err)
	}

	frameIdx, err := bpm.getVictimFrame()
	if err != nil {
		// TODO: Could try to deallocate the newly allocated disk page if BPM is full.
		// This is complex (requires free page manager on disk).
		return nil, InvalidPageID, fmt.Errorf("failed to get frame for new page %d: %w", newPageID, err)
	}

	victimPage := bpm.pages[frameIdx]
	if victimPage.IsDirty() && victimPage.GetPageID() != InvalidPageID {
		if err := bpm.diskManager.WritePage(victimPage.GetPageID(), victimPage.GetData()); err != nil {
			// Potentially try to put frame back to free list and error out.
			return nil, InvalidPageID, fmt.Errorf("failed to flush dirty victim page %d for new page: %w", victimPage.GetPageID(), err)
		}
	}
	if victimPage.GetPageID() != InvalidPageID {
		delete(bpm.pageTable, victimPage.GetPageID())
	}

	victimPage.id = newPageID
	victimPage.pinCount = 1   // Pin immediately
	victimPage.isDirty = true // New page is considered dirty until first flush
	// Clear data for the new page
	for i := range victimPage.data {
		victimPage.data[i] = 0
	}

	bpm.pageTable[newPageID] = frameIdx
	return victimPage, newPageID, nil
}

// FlushPage writes a specific page to disk if it's dirty.
func (bpm *BufferPoolManager) FlushPage(pageID PageID) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	if frameIdx, ok := bpm.pageTable[pageID]; ok {
		page := bpm.pages[frameIdx]
		if page.IsDirty() {
			if err := bpm.diskManager.WritePage(page.GetPageID(), page.GetData()); err != nil {
				return err
			}
			page.SetDirty(false)
		}
		return nil
	}
	return fmt.Errorf("%w: page %d not found to flush", ErrPageNotFound, pageID)
}

// FlushAllPages writes all dirty pages to disk.
func (bpm *BufferPoolManager) FlushAllPages() error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	var firstErr error
	for _, page := range bpm.pages { // Iterate over all frames
		if page.GetPageID() != InvalidPageID && page.IsDirty() { // If frame holds a valid, dirty page
			err := bpm.diskManager.WritePage(page.GetPageID(), page.GetData())
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				// Continue trying to flush other pages
				fmt.Fprintf(os.Stderr, "Error flushing page %d: %v\n", page.GetPageID(), err)
			} else {
				page.SetDirty(false)
			}
		}
	}
	if err := bpm.diskManager.Sync(); err != nil { // Sync underlying file after all writes
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// --- BTree Node Serialization/Deserialization ---
// For simplicity, this example assumes K and V are fixed-size or can be easily serialized.
// A real system needs more robust serialization for variable types (e.g. strings, blobs).
// We will use encoding/binary.

const (
	nodeHeaderSizeOffset = 0 // Offset for total header size
	isLeafOffset         = 2 // uint16 for header size, then bool (1 byte)
	numKeysOffset        = 3 // uint16 for numKeys
	// Keys, Values, ChildPageIDs follow.
	// For this example, we'll assume fixed size for K, V, PageID for easier calculation.
	// If K, V are variable, we'd need offsets within the page.
	// Let's define some example fixed sizes. These MUST match actual type sizes.
	// These are placeholders, real serialization is more complex.
	// For generic K, V, we'd need type-specific serializers or constraints.
	// For now, we'll focus on the structure.
	checksumSize = 4 // CRC32 checksum at the end of the page data
)

// Node represents a node in the B-Tree.
// This struct is the in-memory representation. Serialization converts it to/from a Page.
type Node[K any, V any] struct {
	pageID       PageID // ID of the page this node resides on
	isLeaf       bool
	keys         []K
	values       []V
	childPageIDs []PageID     // PageIDs of children (for internal nodes)
	tree         *BTree[K, V] // Pointer back to the tree (for degree, keyOrder, bpm)
	// No direct Page pointer here to avoid circular dependencies with buffer pool frame.
	// Node operates on data from a Page provided by BPM.
}

// serializeNode converts a Node into its byte representation to be stored in a Page.
// This is a simplified example. Real serialization is complex, especially for generics.
// We'll assume K, V are of types that binary.Write can handle (e.g. fixed-size numeric types).
// For strings or slices, it's much more involved (offsets, variable lengths).
func (n *Node[K, V]) serialize(page *Page, keySerializer func(K) ([]byte, error), valueSerializer func(V) ([]byte, error)) error {
	if n.tree == nil || n.tree.bpm == nil {
		return ErrBTreeNotInitializedProperly
	}
	pageSize := n.tree.bpm.pageSize
	buffer := new(bytes.Buffer)

	// 1. isLeaf (1 byte)
	if err := binary.Write(buffer, binary.LittleEndian, n.isLeaf); err != nil {
		return fmt.Errorf("%w: writing isLeaf: %v", ErrSerialization, err)
	}

	// 2. numKeys (uint16)
	numKeys := uint16(len(n.keys))
	if err := binary.Write(buffer, binary.LittleEndian, numKeys); err != nil {
		return fmt.Errorf("%w: writing numKeys: %v", ErrSerialization, err)
	}

	// 3. Keys
	for _, k := range n.keys {
		keyData, err := keySerializer(k)
		if err != nil {
			return fmt.Errorf("%w: serializing key: %v", ErrSerialization, err)
		}
		// Write length of key data (e.g. uint16), then key data
		if err := binary.Write(buffer, binary.LittleEndian, uint16(len(keyData))); err != nil {
			return err
		}
		if _, err := buffer.Write(keyData); err != nil {
			return err
		}
	}

	// 4. Values
	for _, v := range n.values {
		valData, err := valueSerializer(v)
		if err != nil {
			return fmt.Errorf("%w: serializing value: %v", ErrSerialization, err)
		}
		if err := binary.Write(buffer, binary.LittleEndian, uint16(len(valData))); err != nil {
			return err
		}
		if _, err := buffer.Write(valData); err != nil {
			return err
		}
	}

	// 5. Child PageIDs (only for internal nodes)
	if !n.isLeaf {
		numChildren := uint16(len(n.childPageIDs)) // Should be numKeys + 1
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
	if len(serializedData)+checksumSize > pageSize {
		return fmt.Errorf("%w: node data (%d bytes) + checksum (%d) exceeds page size (%d)",
			ErrSerialization, len(serializedData), checksumSize, pageSize)
	}

	// Copy to page data and add checksum
	pageData := page.GetData()
	copy(pageData, serializedData)
	// Clear remaining part of the page
	for i := len(serializedData); i < pageSize-checksumSize; i++ {
		pageData[i] = 0
	}

	checksum := crc32.ChecksumIEEE(pageData[:pageSize-checksumSize])
	binary.LittleEndian.PutUint32(pageData[pageSize-checksumSize:], checksum)

	page.SetDirty(true) // Mark page as dirty after serialization
	return nil
}

// deserializeNode populates a Node from its byte representation in a Page.
func (n *Node[K, V]) deserialize(page *Page, keyDeserializer func([]byte) (K, error), valueDeserializer func([]byte) (V, error)) error {
	if n.tree == nil || n.tree.bpm == nil {
		var zeroK K
		var zeroV V
		n.keys = make([]K, 0)
		n.values = make([]V, 0)
		n.childPageIDs = make([]PageID, 0)
		return ErrBTreeNotInitializedProperly
	}
	pageSize := n.tree.bpm.pageSize
	pageData := page.GetData()

	// Verify checksum
	storedChecksum := binary.LittleEndian.Uint32(pageData[pageSize-checksumSize:])
	calculatedChecksum := crc32.ChecksumIEEE(pageData[:pageSize-checksumSize])
	if storedChecksum != calculatedChecksum {
		var zeroK K
		var zeroV V
		n.keys = make([]K, 0)
		n.values = make([]V, 0)
		n.childPageIDs = make([]PageID, 0)
		return fmt.Errorf("%w: stored=0x%x, calculated=0x%x for page %d",
			ErrChecksumMismatch, storedChecksum, calculatedChecksum, page.GetPageID())
	}

	buffer := bytes.NewReader(pageData[:pageSize-checksumSize]) // Read from data part, excluding checksum

	// 1. isLeaf
	if err := binary.Read(buffer, binary.LittleEndian, &n.isLeaf); err != nil {
		return fmt.Errorf("%w: reading isLeaf: %v", ErrDeserialization, err)
	}

	// 2. numKeys
	var numKeys uint16
	if err := binary.Read(buffer, binary.LittleEndian, &numKeys); err != nil {
		return fmt.Errorf("%w: reading numKeys: %v", ErrDeserialization, err)
	}
	n.keys = make([]K, numKeys)
	n.values = make([]V, numKeys) // Assuming one value per key

	// 3. Keys
	for i := uint16(0); i < numKeys; i++ {
		var keyDataLen uint16
		if err := binary.Read(buffer, binary.LittleEndian, &keyDataLen); err != nil {
			return err
		}
		keyData := make([]byte, keyDataLen)
		if _, err := io.ReadFull(buffer, keyData); err != nil {
			return err
		}
		key, err := keyDeserializer(keyData)
		if err != nil {
			return fmt.Errorf("%w: deserializing key %d: %v", ErrDeserialization, i, err)
		}
		n.keys[i] = key
	}

	// 4. Values
	for i := uint16(0); i < numKeys; i++ {
		var valDataLen uint16
		if err := binary.Read(buffer, binary.LittleEndian, &valDataLen); err != nil {
			return err
		}
		valData := make([]byte, valDataLen)
		if _, err := io.ReadFull(buffer, valData); err != nil {
			return err
		}
		val, err := valueDeserializer(valData)
		if err != nil {
			return fmt.Errorf("%w: deserializing value %d: %v", ErrDeserialization, i, err)
		}
		n.values[i] = val
	}

	// 5. Child PageIDs
	if !n.isLeaf {
		var numChildren uint16
		if err := binary.Read(buffer, binary.LittleEndian, &numChildren); err != nil {
			return fmt.Errorf("%w: reading numChildren: %v", ErrDeserialization, err)
		}
		if numKeys > 0 && numChildren != numKeys+1 && !(numKeys == 0 && numChildren == 0) { // Root can be empty leaf
			// If numKeys is 0 for an internal node, numChildren could be 0 if it's an empty new root before first split.
			// Or 1 if it's a root that had its only key removed and its child became the new root (but this node would be gone).
			// This check needs to be robust. For a non-empty internal node, numChildren == numKeys + 1.
			// If numKeys == 0 for an internal node, it should have 0 children if it's truly empty, or 1 if it's a root pointing to a single child.
			// The B-Tree logic should ensure this.
		}
		n.childPageIDs = make([]PageID, numChildren)
		for i := uint16(0); i < numChildren; i++ {
			if err := binary.Read(buffer, binary.LittleEndian, &n.childPageIDs[i]); err != nil {
				return fmt.Errorf("%w: reading childPageID %d: %v", ErrDeserialization, i, err)
			}
		}
	} else {
		n.childPageIDs = make([]PageID, 0) // Ensure it's empty for leaves
	}
	n.pageID = page.GetPageID()
	return nil
}

// --- BTree Structure & Operations (Refactored for Persistence) ---

// Order defines a function that compares two keys.
type Order[K any] func(a, b K) int

// KeyValueSerializer defines functions to serialize/deserialize keys and values.
type KeyValueSerializer[K any, V any] struct {
	SerializeKey     func(K) ([]byte, error)
	DeserializeKey   func([]byte) (K, error)
	SerializeValue   func(V) ([]byte, error)
	DeserializeValue func([]byte) (V, error)
}

// BTree represents the B-Tree structure.
type BTree[K any, V any] struct {
	rootPageID   PageID // PageID of the root node
	degree       int
	keyOrder     Order[K]
	kvSerializer KeyValueSerializer[K, V]
	bpm          *BufferPoolManager // Buffer Pool Manager
	diskManager  *DiskManager       // For header updates primarily
	size         int                // Number of items, needs to be persisted or recalculated
	// File path stored in diskManager
}

// NewBTreeFile creates a new B-Tree, initializing its disk file.
func NewBTreeFile[K any, V any](filePath string, degree int, keyOrder Order[K], kvSerializer KeyValueSerializer[K, V], poolSize int, pageSize int) (*BTree[K, V], error) {
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
		return nil, err
	}

	header, err := dm.OpenOrCreateFile(true, degree) // Create = true
	if err != nil {
		return nil, err
	}

	bpm := NewBufferPoolManager(poolSize, dm)

	bt := &BTree[K, V]{
		rootPageID:   InvalidPageID, // Will be set after creating the first root node page
		degree:       degree,
		keyOrder:     keyOrder,
		kvSerializer: kvSerializer,
		bpm:          bpm,
		diskManager:  dm,
		size:         0,
	}

	// Create the first root node page (empty leaf)
	rootPage, rootPageID, err := bpm.NewPage()
	if err != nil {
		dm.Close()
		os.Remove(filePath) // Cleanup
		return nil, fmt.Errorf("failed to create initial root page: %w", err)
	}
	defer bpm.UnpinPage(rootPageID, true) // Unpin after use, mark dirty

	bt.rootPageID = rootPageID
	rootNode := &Node[K, V]{
		pageID:       rootPageID,
		isLeaf:       true,
		keys:         make([]K, 0),
		values:       make([]V, 0),
		childPageIDs: make([]PageID, 0),
		tree:         bt,
	}
	if err := rootNode.serialize(rootPage, kvSerializer.SerializeKey, kvSerializer.SerializeValue); err != nil {
		dm.Close()
		os.Remove(filePath) // Cleanup
		return nil, fmt.Errorf("failed to serialize initial root node: %w", err)
	}

	// Update file header with the new root page ID
	if err := dm.UpdateRootPageIDInHeader(rootPageID); err != nil {
		dm.Close()
		os.Remove(filePath) // Cleanup
		return nil, fmt.Errorf("failed to update header with root page ID: %w", err)
	}
	// Persist size if needed (or recalculate on open)
	// For now, size is in-memory. For persistence, it should be in header or a meta-page.

	return bt, nil
}

// OpenBTreeFile opens an existing B-Tree from its disk file.
func OpenBTreeFile[K any, V any](filePath string, keyOrder Order[K], kvSerializer KeyValueSerializer[K, V], poolSize int, defaultPageSize int) (*BTree[K, V], error) {
	if keyOrder == nil {
		return nil, ErrNilKeyOrder
	}
	if kvSerializer.SerializeKey == nil || kvSerializer.DeserializeKey == nil || kvSerializer.SerializeValue == nil || kvSerializer.DeserializeValue == nil {
		return nil, errors.New("all key/value serializers must be provided")
	}

	dm, err := NewDiskManager(filePath, defaultPageSize) // Page size will be read from header
	if err != nil {
		return nil, err
	}

	header, err := dm.OpenOrCreateFile(false, 0) // Create = false, degree will be read
	if err != nil {
		return nil, err
	}

	// Adjust disk manager's page size if it was different from default
	if dm.pageSize != int(header.PageSize) {
		dm.pageSize = int(header.PageSize)
		// Potentially re-init BPM if page size changed, or ensure BPM uses correct page size from start
	}

	bpm := NewBufferPoolManager(poolSize, dm)

	bt := &BTree[K, V]{
		rootPageID:   header.RootPageID,
		degree:       int(header.Degree),
		keyOrder:     keyOrder,
		kvSerializer: kvSerializer,
		bpm:          bpm,
		diskManager:  dm,
		// Size needs to be loaded from header/meta-page or recalculated by traversing.
		// For now, initialize to 0, search/insert/delete will not update it correctly without persistence.
		size: 0,
	}
	// TODO: Load tree size from a persisted location.
	// For now, size will be inaccurate for an opened tree until fully integrated.

	if bt.rootPageID == InvalidPageID { // Should not happen if file was properly initialized
		// This could mean an empty but valid DB file.
		// Or a corrupted header. The OpenOrCreateFile should have caught DBMagic issues.
		// If it's a truly empty DB (e.g. header initialized but no root created yet),
		// this is okay, first insert will create root.
		// However, NewBTreeFile ensures a root is created.
		// This implies a potentially problematic state if rootPageID is InvalidPageID here.
		// For robustness, one might re-initialize root if it's an empty DB.
		fmt.Fprintf(os.Stderr, "Warning: Opened BTree with InvalidPageID as root. Tree might be empty or header inconsistent.\n")
	}

	return bt, nil
}

// Helper to fetch and deserialize a node
func (bt *BTree[K, V]) fetchNode(pageID PageID) (*Node[K, V], *Page, error) {
	if pageID == InvalidPageID {
		return nil, nil, errors.New("attempted to fetch node with InvalidPageID")
	}
	page, err := bt.bpm.FetchPage(pageID)
	if err != nil {
		return nil, nil, err
	}
	node := &Node[K, V]{tree: bt} // pageID will be set during deserialize
	err = node.deserialize(page, bt.kvSerializer.DeserializeKey, bt.kvSerializer.DeserializeValue)
	if err != nil {
		bt.bpm.UnpinPage(pageID, false) // Unpin on error
		return nil, nil, err
	}
	return node, page, nil
}

// Size returns the number of key-value pairs in the B-Tree.
// NOTE: For persisted BTree, size should be loaded from disk or recalculated.
// This current implementation's size field is in-memory only.
func (bt *BTree[K, V]) Size() int {
	// For a persistent tree, this should ideally be read from a persisted metadata page.
	// Recalculating on every call is too expensive.
	// The `bt.size` field is updated by insert/delete but not persisted yet.
	return bt.size
}

func (bt *BTree[K, V]) Search(key K) (V, bool, error) {
	var zeroV V
	if bt.rootPageID == InvalidPageID { // Empty tree
		return zeroV, false, nil
	}

	node, page, err := bt.fetchNode(bt.rootPageID)
	if err != nil {
		return zeroV, false, fmt.Errorf("failed to fetch root node for search: %w", err)
	}
	// page is already pinned by fetchNode

	val, found := bt.searchRecursive(node, page, key) // Pass page to allow unpinning
	// searchRecursive will unpin the page it was working on before returning (if no error) or on error path.
	// The initial page for root is unpinned by the searchRecursive's final step or error.

	return val, found, nil // Error is handled by returning it from searchRecursive if it occurs
}

func (bt *BTree[K, V]) searchRecursive(currNode *Node[K, V], currPage *Page, key K) (V, bool) {
	// Ensure page is unpinned when this function scope exits, unless passed upwards.
	// This is tricky with recursion. Each level should unpin its own fetched page.
	// The caller of searchRecursive (i.e. Search or another recursive call) is responsible for the page it fetched.
	// So, currPage (passed in) will be unpinned by the caller of this instance of searchRecursive.

	idx, foundKeyInCurrNode := slices.BinarySearchFunc(currNode.keys, key, bt.keyOrder)

	if foundKeyInCurrNode {
		val := currNode.values[idx]
		bt.bpm.UnpinPage(currPage.GetPageID(), false) // Unpin current page, not dirty
		return val, true
	}

	if currNode.isLeaf {
		bt.bpm.UnpinPage(currPage.GetPageID(), false) // Unpin leaf, not dirty
		var zeroV V
		return zeroV, false
	}

	// Not found in internal node, descend. Unpin current page first.
	childPageIDToSearch := currNode.childPageIDs[idx]
	bt.bpm.UnpinPage(currPage.GetPageID(), false) // Unpin current internal node's page

	// Fetch and search child
	childNode, childPage, err := bt.fetchNode(childPageIDToSearch)
	if err != nil {
		// This error needs to be propagated. Current signature doesn't allow it.
		// This is a major limitation of not returning error from recursive search.
		// For now, log and return not found. Production code MUST handle this.
		fmt.Fprintf(os.Stderr, "Error fetching child page %d in searchRecursive: %v\n", childPageIDToSearch, err)
		var zeroV V
		return zeroV, false // Error case
	}
	// Recursive call will handle unpinning childPage
	return bt.searchRecursive(childNode, childPage, key)
}

// Insert (Refactored for persistence) - simplified error handling for brevity
func (bt *BTree[K, V]) Insert(key K, value V) error {
	if bt.rootPageID == InvalidPageID { // Tree is completely empty, create first root.
		rootPg, rootPgID, err := bt.bpm.NewPage()
		if err != nil {
			return fmt.Errorf("failed to create first root page for insert: %w", err)
		}

		bt.rootPageID = rootPgID
		if err := bt.diskManager.UpdateRootPageIDInHeader(rootPgID); err != nil {
			// TODO: deallocate page from BPM/Disk if header update fails. Complex.
			bt.bpm.UnpinPage(rootPgID, false) // Try to unpin, mark not dirty as it's a failed setup
			return fmt.Errorf("failed to update header with new root page ID: %w", err)
		}

		rootNode := &Node[K, V]{pageID: rootPgID, isLeaf: true, tree: bt}
		// Insert into this new root node (which is currently empty)
		// This directly becomes an insertNonFull on a new leaf.
		err = bt.insertNonFull(rootNode, rootPg, key, value) // Page is passed to be written
		// insertNonFull will serialize and unpin.
		return err
	}

	rootNode, rootPage, err := bt.fetchNode(bt.rootPageID)
	if err != nil {
		return err
	}
	// rootPage is pinned.

	if len(rootNode.keys) == 2*bt.degree-1 { // Root is full
		// Create new root page
		newRootDiskPage, newRootPageID, err := bt.bpm.NewPage()
		if err != nil {
			bt.bpm.UnpinPage(rootPage.GetPageID(), false) // Unpin old root
			return err
		}
		// newRootDiskPage is pinned by NewPage

		oldRootPageID := bt.rootPageID
		bt.rootPageID = newRootPageID // Update BTree's root pointer

		// Update file header with the new root page ID
		if err := bt.diskManager.UpdateRootPageIDInHeader(newRootPageID); err != nil {
			bt.bpm.UnpinPage(rootPage.GetPageID(), false)
			bt.bpm.UnpinPage(newRootDiskPage.GetPageID(), false) // Attempt to unpin new root too
			// TODO: Deallocate newRootPageID from disk if possible
			return fmt.Errorf("failed to update header with new root page ID during split: %w", err)
		}

		newRootNode := &Node[K, V]{
			pageID:       newRootPageID,
			isLeaf:       false,
			childPageIDs: []PageID{oldRootPageID}, // Old root is first child
			tree:         bt,
		}
		// No keys in newRootNode yet. splitChild will add one.

		// Unpin old root page as splitChild will operate on newRootNode and its child (oldRootNode)
		// splitChild will fetch oldRootNode again if needed, or operate on its PageID.
		// bt.bpm.UnpinPage(oldRootPageID, false) // Old root page is `rootPage`
		// Careful: `rootNode` is in-memory rep of `rootPage`. `rootPage` is pinned.
		// `splitChild` needs to operate on `newRootNode` and `newRootDiskPage`.
		// It will modify `newRootNode` and then serialize it to `newRootDiskPage`.
		// It will also modify `rootNode` (the old root) and serialize it to `rootPage`.

		// `splitChild` expects the parent node (newRootNode) and its page (newRootDiskPage)
		// and the index of the child to split (0, which is oldRootPageID).
		// It will fetch the child (oldRootNode/rootPage) itself.
		err = bt.splitChild(newRootNode, newRootDiskPage, 0, rootNode, rootPage) // Pass old root node and page
		if err != nil {
			// Complex recovery: try to revert rootPageID, unpin pages.
			bt.bpm.UnpinPage(rootPage.GetPageID(), false)
			bt.bpm.UnpinPage(newRootDiskPage.GetPageID(), false)
			return err
		}

		// Now insert into the new root structure
		err = bt.insertNonFull(newRootNode, newRootDiskPage, key, value)
		// newRootDiskPage is unpinned by insertNonFull or its callees
		// rootPage (old root) was modified by splitChild and should have been unpinned by it.
		return err

	} else { // Root is not full
		err = bt.insertNonFull(rootNode, rootPage, key, value)
		// rootPage is unpinned by insertNonFull or its callees
		return err
	}
}

// insertNonFull (Refactored for persistence)
// node and its corresponding page are passed in. page is assumed to be pinned.
// This function is responsible for serializing changes to `page` and unpinning it.
func (bt *BTree[K, V]) insertNonFull(node *Node[K, V], page *Page, key K, value V) error {
	idx := slices.IndexFunc(node.keys, func(k K) bool { return bt.keyOrder(key, k) <= 0 })
	if idx == -1 {
		idx = len(node.keys)
	}

	originalSize := len(node.keys) // To check if an update happened vs insert

	if node.isLeaf {
		updated := false
		if idx > 0 && bt.keyOrder(key, node.keys[idx-1]) == 0 {
			node.values[idx-1] = value
			updated = true
		} else if idx < len(node.keys) && bt.keyOrder(key, node.keys[idx]) == 0 {
			node.values[idx] = value
			updated = true
		}

		if updated {
			// Value updated, serialize and unpin
			if err := node.serialize(page, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
				bt.bpm.UnpinPage(page.GetPageID(), false) // Unpin even on error, don't mark dirty if serialize failed
				return err
			}
			return bt.bpm.UnpinPage(page.GetPageID(), true) // Mark dirty
		}

		// Insert new key/value
		node.keys = slices.Insert(node.keys, idx, key)
		node.values = slices.Insert(node.values, idx, value)
		bt.size++ // Global tree size (needs persistence)

		if err := node.serialize(page, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
			bt.bpm.UnpinPage(page.GetPageID(), false)
			bt.size-- // Revert size increment on error
			return err
		}
		return bt.bpm.UnpinPage(page.GetPageID(), true)

	} else { // Internal node
		updated := false
		if idx > 0 && bt.keyOrder(key, node.keys[idx-1]) == 0 {
			node.values[idx-1] = value
			updated = true
		} else if idx < len(node.keys) && bt.keyOrder(key, node.keys[idx]) == 0 {
			node.values[idx] = value
			updated = true
		}
		if updated {
			if err := node.serialize(page, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
				bt.bpm.UnpinPage(page.GetPageID(), false)
				return err
			}
			return bt.bpm.UnpinPage(page.GetPageID(), true)
		}

		childPageIDToDescend := node.childPageIDs[idx]
		childNode, childPage, err := bt.fetchNode(childPageIDToDescend)
		if err != nil {
			bt.bpm.UnpinPage(page.GetPageID(), false) // Unpin parent
			return err
		}
		// childPage is now pinned

		if len(childNode.keys) == 2*bt.degree-1 { // Child is full
			// Unpin parent page BEFORE splitChild, as splitChild modifies parent.
			// splitChild will re-fetch parent if needed, or we pass parent node & page.
			// Let's pass parent node & page to splitChild.
			// `page` is parent's page. `childPage` is child's page.
			err = bt.splitChild(node, page, idx, childNode, childPage) // Pass childNode and childPage
			if err != nil {
				// childPage was unpinned by splitChild (or its error path)
				// parent page (`page`) also needs to be unpinned here.
				bt.bpm.UnpinPage(page.GetPageID(), false) // Or true if splitChild dirtied it before error
				return err
			}
			// After split, parent `node` (and `page`) is modified.
			// The key to insert might now go into a different child or be the promoted key.
			// Re-evaluate based on the (potentially modified) parent `node`.
			if bt.keyOrder(key, node.keys[idx]) > 0 { // Key > promoted key
				idx++ // Go to new right child of split
			} else if bt.keyOrder(key, node.keys[idx]) == 0 { // Key == promoted key
				node.values[idx] = value // Update value in parent
				// Serialize parent and unpin
				if err := node.serialize(page, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
					bt.bpm.UnpinPage(page.GetPageID(), false)
					return err
				}
				return bt.bpm.UnpinPage(page.GetPageID(), true)
			}
			// else key < promoted key, idx remains same (points to original left part of split child)

			// Fetch the (potentially new) child to descend into
			// The original childPage is no longer valid if it was split.
			// The childPageID at node.childPageIDs[idx] is the correct one.
			bt.bpm.UnpinPage(page.GetPageID(), true) // Parent `page` is dirty from split, unpin it.

			// Re-fetch the correct child to insert into
			// This is inefficient. A better splitChild would return the relevant nodes/pages.
			// For now, re-fetch.
			descendChildPageID := node.childPageIDs[idx]
			descendChildNode, descendChildPage, fetchErr := bt.fetchNode(descendChildPageID)
			if fetchErr != nil {
				return fetchErr
			} // Error fetching, already unpinned parent

			return bt.insertNonFull(descendChildNode, descendChildPage, key, value)
		} else {
			// Child is not full. Unpin parent page first.
			bt.bpm.UnpinPage(page.GetPageID(), false) // Parent not modified yet by this path
			return bt.insertNonFull(childNode, childPage, key, value)
		}
	}
}

// splitChild (Refactored for persistence)
// parentNode and parentPage are the parent. childIdx is the index of the child to split.
// childToSplitNode and childToSplitPage are the actual child node and page to be split.
// All pages involved (parentPage, childToSplitPage, newSiblingPage) must be unpinned by this function.
func (bt *BTree[K, V]) splitChild(
	parentNode *Node[K, V], parentPage *Page, childIdx int,
	childToSplitNode *Node[K, V], childToSplitPage *Page) error {

	t := bt.degree

	// Create new sibling page and node
	newSiblingDiskPage, newSiblingPageID, err := bt.bpm.NewPage()
	if err != nil {
		bt.bpm.UnpinPage(childToSplitPage.GetPageID(), false) // Unpin child
		// parentPage is unpinned by caller if this errors
		return err
	}
	// newSiblingDiskPage is pinned

	newSiblingNode := &Node[K, V]{
		pageID: newSiblingPageID,
		isLeaf: childToSplitNode.isLeaf,
		keys:   make([]K, t-1),
		values: make([]V, t-1),
		tree:   bt,
	}
	if !childToSplitNode.isLeaf {
		newSiblingNode.childPageIDs = make([]PageID, t)
	} else {
		newSiblingNode.childPageIDs = make([]PageID, 0)
	}

	// Middle key/value from childToSplit moves to parent.
	middleKey := childToSplitNode.keys[t-1]
	middleValue := childToSplitNode.values[t-1]

	// Copy data to newSiblingNode
	copy(newSiblingNode.keys, childToSplitNode.keys[t:])
	copy(newSiblingNode.values, childToSplitNode.values[t:])
	if !childToSplitNode.isLeaf {
		copy(newSiblingNode.childPageIDs, childToSplitNode.childPageIDs[t:])
	}

	// Truncate childToSplitNode
	childToSplitNode.keys = childToSplitNode.keys[:t-1]
	childToSplitNode.values = childToSplitNode.values[:t-1]
	if !childToSplitNode.isLeaf {
		childToSplitNode.childPageIDs = childToSplitNode.childPageIDs[:t]
	}

	// Update parentNode
	parentNode.keys = slices.Insert(parentNode.keys, childIdx, middleKey)
	parentNode.values = slices.Insert(parentNode.values, childIdx, middleValue)
	parentNode.childPageIDs = slices.Insert(parentNode.childPageIDs, childIdx+1, newSiblingPageID)

	// Serialize all modified nodes to their pages
	var firstErr error
	if err := parentNode.serialize(parentPage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
		firstErr = fmt.Errorf("serializing parent during split: %w", err)
	}
	if err := childToSplitNode.serialize(childToSplitPage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
		if firstErr == nil {
			firstErr = fmt.Errorf("serializing child during split: %w", err)
		}
	}
	if err := newSiblingNode.serialize(newSiblingDiskPage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
		if firstErr == nil {
			firstErr = fmt.Errorf("serializing new sibling during split: %w", err)
		}
	}

	// Unpin all pages involved, marking them dirty
	// Parent page is unpinned by the caller of splitChild (insertNonFull)
	// because insertNonFull needs to decide if parent was dirty from other ops too.
	// Let's change this: splitChild should manage unpinning of pages it dirties.
	// The caller (insertNonFull) will unpin the parent page it fetched.
	// If splitChild dirties parent, it should mark parentPage.isDirty = true.
	parentPage.SetDirty(true) // Parent was modified

	bt.bpm.UnpinPage(childToSplitPage.GetPageID(), true)
	bt.bpm.UnpinPage(newSiblingDiskPage.GetPageID(), true)

	return firstErr
}

// Delete (Refactored for persistence) - High-level structure. Implementation is complex.
// For brevity, the full persistent delete is not fully implemented here but outlined.
// It would mirror the in-memory logic but with fetch/serialize/unpin page operations.
func (bt *BTree[K, V]) Delete(key K) error {
	if bt.rootPageID == InvalidPageID {
		return ErrKeyNotFound
	}

	rootNode, rootPage, err := bt.fetchNode(bt.rootPageID)
	if err != nil {
		return err
	}

	keyFoundInTree := false
	err = bt.deleteRecursive(rootNode, rootPage, key, &keyFoundInTree)
	// rootPage is unpinned by deleteRecursive or its callees

	if err != nil {
		return err
	}
	if !keyFoundInTree {
		return ErrKeyNotFound
	}

	// After potential modifications, check if root needs to be adjusted
	// Re-fetch root node as its content might have changed due to merges propagating up
	// This is inefficient; better to have deleteRecursive return new rootPageID if it changes.
	// For now, let's assume rootPageID in BTree struct is updated if root changes.
	// The check for empty root and making child the new root:
	finalRootPage, err := bt.bpm.FetchPage(bt.rootPageID) // Fetch current root
	if err != nil {
		return fmt.Errorf("failed to fetch root page post-delete: %w", err)
	}
	finalRootNode := &Node[K, V]{tree: bt}
	if err := finalRootNode.deserialize(finalRootPage, bt.kvSerializer.DeserializeKey, bt.kvSerializer.DeserializeValue); err != nil {
		bt.bpm.UnpinPage(finalRootPage.GetPageID(), false)
		return fmt.Errorf("failed to deserialize root page post-delete: %w", err)
	}

	if len(finalRootNode.keys) == 0 && !finalRootNode.isLeaf {
		if len(finalRootNode.childPageIDs) > 0 { // Should be 1 child if it's an internal node with 0 keys after delete
			oldRootPageID := bt.rootPageID
			bt.rootPageID = finalRootNode.childPageIDs[0] // New root is the only child
			if err := bt.diskManager.UpdateRootPageIDInHeader(bt.rootPageID); err != nil {
				// Revert rootPageID change on error
				bt.rootPageID = oldRootPageID
				bt.bpm.UnpinPage(finalRootPage.GetPageID(), false)
				return fmt.Errorf("failed to update header for new root after delete: %w", err)
			}
			// TODO: Deallocate oldRootPageID from disk (add to free list)
			// This is complex and requires a free page manager.
		} else {
			// This case (internal node with 0 keys and 0 children) should ideally not happen
			// if B-Tree properties are maintained. It implies the tree might be entirely empty.
			// If so, rootPageID might become InvalidPageID or point to a new empty leaf.
		}
	}
	bt.bpm.UnpinPage(finalRootPage.GetPageID(), false) // Unpin, might be dirty if it was the one modified.

	if keyFoundInTree {
		bt.size-- // Global tree size (needs persistence)
	}
	return nil
}

// deleteRecursive (Outline - requires full adaptation like insertNonFull)
// node and page are current. page is pinned. Must be unpinned by this func or its callees.
func (bt *BTree[K, V]) deleteRecursive(node *Node[K, V], page *Page, key K, keyFound *bool) error {
	// This function needs a similar transformation as insertNonFull:
	// 1. Find key or child to descend.
	// 2. If leaf: delete, serialize, unpin.
	// 3. If internal and key found: handle cases 2a, 2b, 2c (predecessor/successor/merge)
	//    - This involves fetching other nodes, modifying them, serializing, unpinning.
	// 4. If internal and key not found: ensure child has enough keys (borrow/merge)
	//    - This also involves fetching/modifying/serializing/unpinning multiple nodes.
	//    - Then recurse.
	// 5. All fetched pages must be unpinned, marked dirty if modified.
	// This is a very complex function to fully implement here with persistence.
	// The logic would follow the in-memory version but with BPM interactions.

	// Placeholder: Unpin the current page and return NotImplemented or a basic error.
	// This indicates the function is not fully implemented for persistence.
	bt.bpm.UnpinPage(page.GetPageID(), false) // Assume no modification for this placeholder
	return errors.New("persistent deleteRecursive not fully implemented")
}

// Close flushes all dirty pages to disk and closes the database file.
func (bt *BTree[K, V]) Close() error {
	if bt.bpm == nil {
		return errors.New("btree buffer pool manager is nil, cannot close")
	}
	// Persist tree size here if it's managed in a header/meta page.
	// e.g. update header with bt.size

	flushErr := bt.bpm.FlushAllPages() // This also syncs the file via diskManager.Sync()
	closeErr := bt.diskManager.Close() // DiskManager close is now simplified as BPM handles sync

	if flushErr != nil && closeErr != nil {
		return fmt.Errorf("error flushing pages: %v; and error closing file: %v", flushErr, closeErr)
	}
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// String representation for debugging (reads from disk, very inefficient for large trees)
func (bt *BTree[K, V]) String() string {
	if bt.rootPageID == InvalidPageID {
		return "BTree (empty or not initialized)\n"
	}
	// For a persistent tree, String() is complex as it requires fetching pages.
	// This is a simplified version that might be slow.
	str, err := bt.stringRecursive(bt.rootPageID, 0)
	if err != nil {
		return fmt.Sprintf("Error generating string: %v\n", err)
	}
	return str
}

func (bt *BTree[K, V]) stringRecursive(pageID PageID, level int) (string, error) {
	if pageID == InvalidPageID {
		return "", nil
	}

	node, page, err := bt.fetchNode(pageID)
	if err != nil {
		return "", fmt.Errorf("stringRecursive failed to fetch page %d: %w", pageID, err)
	}
	defer bt.bpm.UnpinPage(pageID, false) // Unpin after reading for string

	s := ""
	indent := ""
	for i := 0; i < level; i++ {
		indent += "  "
	}

	s += fmt.Sprintf("%sPageID: %d (Leaf: %v, Keys: %d)\n", indent, node.pageID, node.isLeaf, len(node.keys))
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

// DefaultKeyOrder provides a default comparator for standard comparable types.
func DefaultKeyOrder[K cmp.Ordered](a, b K) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// Example Serializers for basic types (e.g., int64 keys, string values)
// These are illustrative. Production code would need robust, efficient ones.

func SerializeInt64(k int64) ([]byte, error) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(k))
	return buf, nil
}
func DeserializeInt64(data []byte) (int64, error) {
	if len(data) != 8 {
		return 0, errors.New("int64 data must be 8 bytes")
	}
	return int64(binary.LittleEndian.Uint64(data)), nil
}
func SerializeString(s string) ([]byte, error) {
	return []byte(s), nil
}
func DeserializeString(data []byte) (string, error) {
	return string(data), nil
}

// --- Next Steps and Production Considerations (Reiterated) ---
// 1. Full Deletion Logic: The `deleteRecursive` needs full implementation with BPM.
// 2. Write-Ahead Logging (WAL): Crucial for atomicity and durability against crashes.
//    - Log records for all changes (insert, delete, split, merge, page alloc/dealloc).
//    - Redo/Undo logic for recovery.
//    - Checkpointing.
// 3. Advanced Buffer Pool Management: LRU, Clock, or other replacement strategies.
//    - Better handling of buffer pool full scenarios.
// 4. Concurrency Control:
//    - Page-level latches (R/W locks on pages in buffer pool).
//    - Latch crabbing/coupling for B-Tree operations.
//    - Transaction management if ACID properties are needed across multiple operations.
// 5. Free Page Management: A proper on-disk free list or bitmap to reuse deallocated pages.
//    - Current `allocateRawPage` just appends, leading to file growth only.
// 6. Iterators/Range Scans: Efficiently scan key ranges.
// 7. Robust Error Handling & Recovery: More comprehensive error checking, and ability
//    to bring the database to a consistent state after errors/crashes (relies on WAL).
// 8. B+Tree Adaptation: Consider if B+Tree (values only in leaves, linked leaves) is more suitable.
// 9. Variable-Length Data: More sophisticated serialization for keys/values that are not
//    fixed size (e.g., using offset pointers within a page, or overflow pages for very large values).
// 10. Comprehensive Testing: Unit, integration, concurrency, fault-injection, performance.
// 11. Metadata Management: Persisting tree size, degree, key/value type information more robustly.
