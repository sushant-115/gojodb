package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"unsafe"
)

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
