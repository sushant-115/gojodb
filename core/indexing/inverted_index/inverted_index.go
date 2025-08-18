package inverted_index

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/sushant-115/gojodb/core/indexing"
	"github.com/sushant-115/gojodb/core/indexing/btree" // Reusing btree's page, disk, buffer managers
	flushmanager "github.com/sushant-115/gojodb/core/write_engine/flush_manager"
	"github.com/sushant-115/gojodb/core/write_engine/memtable"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	"go.uber.org/zap"
)

const (
	// Default settings for the inverted index's internal storage
	invertedIndexPageSize       = 4096 // Same as B-tree for consistency
	invertedIndexBufferPoolSize = 100  // Larger pool for postings lists
	invertedIndexLogBufferSize  = 4096
	invertedIndexLogSegmentSize = 16 * 1024

	// Special PageID for the header of the inverted index file
	invertedIndexHeaderPageID pagemanager.PageID = 0

	// Size of the header within each postings list data page
	// DataLength (uint32) + NextPageID (uint64)
	postingsListPageHeaderSize = 4 + 8
)

// InvertedIndexHeader defines the structure of the inverted index file header.
// This header is stored on invertedIndexHeaderPageID (PageID 0).
type InvertedIndexHeader struct {
	Magic   uint32                  // A magic number to identify the file
	Version uint32                  // Version of the file format
	LastLSN pagemanager.LSN         // Last LSN applied to this index for recovery
	_       [128 - (4 + 4 + 8)]byte // Padding to ensure fixed size (128 bytes)
}

// PostingsListPageHeader is a small header at the beginning of each page
// that stores a segment of a postings list.
type PostingsListPageHeader struct {
	DataLength uint32             // Number of bytes of actual postings list data in this page
	NextPageID pagemanager.PageID // PageID of the next page in the chain, or btree.InvalidPageID if this is the last
}

// PostingsListMetadata stores metadata about a postings list, allowing it to be stored on disk.
// This is what the termDictionary maps to.
type PostingsListMetadata struct {
	StartPageID pagemanager.PageID `json:"start_page_id"` // The PageID where this postings list starts
	TotalLength uint32             `json:"total_length"`  // Total length of the serialized postings list in bytes across all pages
	NumKeys     uint32             `json:"num_keys"`      // Number of document keys in the postings list
}

// InvertedIndex manages the inverted index for text-based search.
// It maps terms (words) to their postings lists, which are stored on disk.
type InvertedIndex struct {
	// termDictionary maps a term (string) to its PostingsListMetadata.
	// This dictionary is kept in-memory and periodically flushed/recovered.
	termDictionary map[string]PostingsListMetadata
	dictMu         sync.RWMutex // Protects access to the termDictionary map

	// Disk and Buffer Pool Managers for the postings list data file
	dm  *flushmanager.DiskManager
	bpm *memtable.BufferPoolManager
	lm  *wal.LogManager // LogManager for inverted index operations

	filePath   string // Path to the main data file for postings lists
	logDir     string // Directory for inverted index logs
	archiveDir string // Directory for inverted index log archives

	header InvertedIndexHeader // In-memory copy of the file header
}

// NewInvertedIndex creates and initializes a new InvertedIndex.
// It sets up disk-backed storage for postings lists and a term dictionary.
func NewInvertedIndex(filePath, logDir, archiveDir string, logger *zap.Logger) (*InvertedIndex, error) {
	idx := &InvertedIndex{
		termDictionary: make(map[string]PostingsListMetadata),
		filePath:       filePath,
		logDir:         logDir,
		archiveDir:     archiveDir,
	}

	// 1. Initialize LogManager for the inverted index
	var err error
	idx.lm, err = wal.NewLogManager(logDir, logger, indexing.InvertedIndexType)
	if err != nil {
		return nil, fmt.Errorf("failed to create LogManager for inverted index: %w", err)
	}

	// 2. Initialize DiskManager for the inverted index data file
	// log.Println("filepath: ", filePath)
	idx.dm, err = flushmanager.NewDiskManager(filePath, invertedIndexPageSize)
	if err != nil {
		idx.lm.Close() // Clean up log manager
		return nil, fmt.Errorf("failed to create DiskManager for inverted index: %w", err)
	}

	// 3. Initialize BufferPoolManager for the inverted index data file
	// THIS MUST BE DONE BEFORE ANY OPERATIONS THAT USE BPM (like readHeader)
	idx.bpm = memtable.NewBufferPoolManager(invertedIndexBufferPoolSize, idx.dm, idx.lm)

	// 4. Open or create the inverted index data file and read/write header
	// Attempt to open the file first. If it doesn't exist, create it.
	_, err = idx.dm.OpenOrCreateFile(false, 0, 0) // Try to open existing file
	if err != nil {
		if os.IsNotExist(err) || strings.Contains(err.Error(), "database file not found") {
			// log.Printf("INFO: Inverted index file %s not found. Creating new file.", filePath)
			// File not found, so create a new one
			_, err = idx.dm.OpenOrCreateFile(true, 0, 0) // Now create it
			if err != nil {
				idx.bpm.FlushAllPages()
				idx.dm.Close()
				idx.lm.Close()
				return nil, fmt.Errorf("failed to create new inverted index file %s: %w", filePath, err)
			}
			// Initialize new header for the new file
			idx.header = InvertedIndexHeader{
				Magic:   btree.DBMagic,
				Version: 1,
				LastLSN: pagemanager.InvalidLSN,
			}
			// Write the new header to a newly allocated page 0
			headerPage, newPageID, writeErr := idx.bpm.NewPage() // This should allocate page 0
			if writeErr != nil {
				idx.bpm.FlushAllPages()
				idx.dm.Close()
				idx.lm.Close()
				return nil, fmt.Errorf("failed to allocate header page (page 0) for new inverted index: %w", writeErr)
			}
			if newPageID != invertedIndexHeaderPageID {
				// log.Printf("WARNING: NewPage returned PageID %d, expected %d for header.", newPageID, invertedIndexHeaderPageID)
			}

			headerPage.Lock() // Acquire X-latch

			buf := new(bytes.Buffer)
			if err := binary.Write(buf, binary.LittleEndian, &idx.header); err != nil {
				headerPage.Unlock()
				idx.bpm.UnpinPage(newPageID, false) // Unpin without marking dirty
				idx.bpm.FlushAllPages()
				idx.dm.Close()
				idx.lm.Close()
				return nil, fmt.Errorf("failed to serialize initial inverted index header: %w", err)
			}
			// Ensure the buffer length matches the header size
			if buf.Len() != 128 { // Use the explicit header size
				headerPage.Unlock()
				idx.bpm.UnpinPage(newPageID, false)
				idx.bpm.FlushAllPages()
				idx.dm.Close()
				idx.lm.Close()
				return nil, fmt.Errorf("inverted index header serialization size mismatch! Expected %d bytes, got %d bytes", 128, buf.Len())
			}
			h := []byte{}
			copy(h, buf.Bytes())
			headerPage.SetData(h)
			headerPage.SetDirty(true)
			headerPage.Unlock()
			if err := idx.bpm.UnpinPage(newPageID, true); err != nil { // Unpin and mark dirty
				// log.Printf("WARNING: Failed to unpin header page %d after initial write: %v", newPageID, err)
			}
			// log.Printf("INFO: Initial inverted index header written to new file %s.", filePath)
		} else {
			// Some other error occurred trying to open the existing file
			idx.bpm.FlushAllPages() // Clean up BPM
			idx.dm.Close()
			idx.lm.Close()
			return nil, fmt.Errorf("failed to open existing inverted index file %s: %w", filePath, err)
		}
	} else {
		// File was successfully opened, now read its header
		if err := idx.readHeader(); err != nil {
			idx.bpm.FlushAllPages() // Clean up BPM
			idx.dm.Close()
			idx.lm.Close()
			return nil, fmt.Errorf("failed to read inverted index header: %w", err)
		}
		if idx.header.Magic != btree.DBMagic {
			idx.bpm.FlushAllPages() // Clean up BPM
			idx.dm.Close()
			idx.lm.Close()
			return nil, fmt.Errorf("invalid inverted index file magic number: 0x%x", idx.header.Magic)
		}
	}

	// 5. Perform recovery for the inverted index (reapply logs)
	// This will replay any changes to postings lists that were not flushed to disk.
	// if err := idx.lm.Recover(idx.dm, wal.LSN(idx.header.LastLSN)); err != nil {
	// 	idx.bpm.FlushAllPages() // This will flush pages, but recovery failed, so state might be inconsistent
	// 	idx.dm.Close()
	// 	idx.lm.Close()
	// 	return nil, fmt.Errorf("inverted index recovery failed: %w", err)
	// }

	// 6. Load the term dictionary (from dedicated JSON file for simplicity)
	if err := idx.loadTermDictionary(); err != nil {
		// log.Printf("WARNING: Failed to load inverted index term dictionary: %v. Starting with empty dictionary.", err)
		idx.termDictionary = make(map[string]PostingsListMetadata) // Start fresh if load fails
	}

	// log.Printf("INFO: Inverted index initialized. Data file: %s, Log dir: %s, Terms in dictionary: %d",
	// filePath, logDir, len(idx.termDictionary))
	return idx, nil
}

func (idx *InvertedIndex) GetLogManager() *wal.LogManager {
	return idx.lm
}

func (idx *InvertedIndex) UpdateTermDicktionary(term string, metadata PostingsListMetadata) {
	// Apply to inverted index's term dictionary
	idx.dictMu.Lock() // Acquire lock for inverted index dictionary
	idx.termDictionary[term] = metadata
	idx.dictMu.Unlock() // Release lock
}

func (idx *InvertedIndex) UpdateHeaderLSN(lsn pagemanager.LSN) error {
	idx.header.LastLSN = lsn
	return idx.writeHeader()
}

// readHeader reads the InvertedIndexHeader from the beginning of the file (PageID 0).
func (idx *InvertedIndex) readHeader() error {
	page, err := idx.bpm.FetchPage(invertedIndexHeaderPageID)
	if err != nil {
		return fmt.Errorf("failed to fetch inverted index header page: %w", err)
	}
	defer idx.bpm.UnpinPage(invertedIndexHeaderPageID, false) // Unpin header page, it's clean

	page.RLock() // Acquire S-latch on the page
	defer page.RUnlock()

	// Deserialize header using binary.Read
	buf := bytes.NewReader(page.GetData()[:128]) // Read exactly 128 bytes
	if err := binary.Read(buf, binary.LittleEndian, &idx.header); err != nil {
		return fmt.Errorf("failed to deserialize inverted index header: %w", err)
	}
	// log.Printf("DEBUG: Inverted index header read. Magic: 0x%x, Version: %d, LastLSN: %d",
	// idx.header.Magic, idx.header.Version, idx.header.LastLSN) // Corrected log line
	return nil
}

// writeHeader writes the InvertedIndexHeader to the beginning of the file (PageID 0).
func (idx *InvertedIndex) writeHeader() error {
	page, err := idx.bpm.FetchPage(invertedIndexHeaderPageID)
	if err != nil {
		return fmt.Errorf("failed to fetch inverted index header page for writing: %w", err)
	}
	defer idx.bpm.UnpinPage(invertedIndexHeaderPageID, true) // Unpin header page, mark dirty

	page.Lock() // Acquire X-latch on the page
	defer page.Unlock()

	// Serialize header using binary.Write
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, &idx.header); err != nil {
		return fmt.Errorf("failed to serialize inverted index header: %w", err)
	}

	// Ensure the buffer length matches the header size
	if buf.Len() != 128 {
		return fmt.Errorf("inverted index header serialization size mismatch! Expected %d bytes, got %d bytes.", 128, buf.Len())
	}
	h := []byte{}
	copy(h, buf.Bytes()) // Copy serialized bytes to page data
	page.SetData(h)
	page.SetDirty(true) // Mark the page dirty

	// log.Printf("DEBUG: Inverted index header written. Magic: 0x%x, Version: %d, LastLSN: %d",
	// idx.header.Magic, idx.header.Version, idx.header.LastLSN)
	return nil
}

// loadTermDictionary loads the in-memory term dictionary from a dedicated file.
// In a real system, this would be a separate persistent index (e.g., a B-tree on terms).
func (idx *InvertedIndex) loadTermDictionary() error {
	dictFilePath := idx.filePath + ".dict" // Separate file for dictionary
	file, err := os.Open(dictFilePath)
	if err != nil {
		return fmt.Errorf("failed to open term dictionary file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&idx.termDictionary); err != nil {
		return fmt.Errorf("failed to decode term dictionary: %w", err)
	}
	return nil
}

// saveTermDictionary saves the in-memory term dictionary to a dedicated file.
func (idx *InvertedIndex) saveTermDictionary() error {
	dictFilePath := idx.filePath + ".dict"
	tmpFilePath := dictFilePath + ".tmp"
	file, err := os.Create(tmpFilePath)
	if err != nil {
		return fmt.Errorf("failed to create temporary term dictionary file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(idx.termDictionary); err != nil {
		_ = os.Remove(tmpFilePath)
		return fmt.Errorf("failed to encode term dictionary to JSON: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = os.Remove(tmpFilePath)
		return fmt.Errorf("failed to sync temporary term dictionary file: %w", err)
	}
	if err := os.Rename(tmpFilePath, dictFilePath); err != nil {
		_ = os.Remove(tmpFilePath)
		return fmt.Errorf("failed to rename temporary term dictionary file: %w", err)
	}
	return nil
}

// Insert processes a text and associates it with a document key in the inverted index.
// It tokenizes, normalizes, and adds terms to the disk-backed postings lists.
func (idx *InvertedIndex) Insert(text string, docKey string) error {
	// log.Println("Recieved insert request", text, docKey)
	if text == "" || docKey == "" {
		return nil // Nothing to index
	}

	terms := idx.tokenizeAndNormalize(text)
	if len(terms) == 0 {
		return nil
	}

	idx.dictMu.Lock()
	defer idx.dictMu.Unlock()

	// For each term, update its postings list
	for _, term := range terms {
		metadata, exists := idx.termDictionary[term]
		var currentPostingsList []string

		if exists {
			// Read existing postings list from disk
			var err error
			currentPostingsList, err = idx.readPostingsList(metadata)
			if err != nil {
				// log.Printf("ERROR: Failed to read postings list for term '%s': %v. Re-creating list.", term, err)
				// If we can't read, treat as if it doesn't exist and create new.
				// In a real system, this might warrant more robust error handling or recovery.
				currentPostingsList = []string{}
			}
		} else {
			currentPostingsList = []string{}
		}

		// Add docKey if not already present, maintaining sorted order
		i := sort.SearchStrings(currentPostingsList, docKey)
		if i < 0 || i >= len(currentPostingsList) {
			currentPostingsList = append(currentPostingsList, "") // Grow slice by one
			copy(currentPostingsList[i+1:], currentPostingsList[i:])
			currentPostingsList[i] = docKey
		} else {
			// DocKey already exists, no need to update postings list on disk
			continue // Move to next term
		}

		// Serialize the updated postings list
		serializedList, err := serializePostingsList(currentPostingsList)
		if err != nil {
			return fmt.Errorf("failed to serialize postings list for term '%s': %w", term, err)
		}

		// Write the updated postings list back to disk
		newMetadata, err := idx.writePostingsList(serializedList)
		if err != nil {
			return fmt.Errorf("failed to write postings list for term '%s': %w", term, err)
		}
		newMetadata.NumKeys = uint32(len(currentPostingsList)) // Update key count

		// Update term dictionary
		idx.termDictionary[term] = newMetadata
		// log.Printf("DEBUG: Inverted index: Updated term '%s'. New metadata: %+v", term, newMetadata)

		// Log the update operation for recovery (conceptual for now)
		// This is a simplified logging. A real system would log the term, old metadata, and new metadata.
		// For now, we'll log the term and the new metadata.
		metadataBytes := []byte(text) // Ignore error, should be fine
		termBytes := []byte(docKey)

		// Combine term and metadata for logging: Term (null-terminated) + Metadata JSON
		logData := new(bytes.Buffer)
		logData.Write(termBytes)
		logData.WriteByte(0) // Null terminator as separator
		logData.Write(metadataBytes)

		lr := &wal.LogRecord{
			TxnID:     0, // Not part of a 2PC transaction for now
			Type:      wal.LogRecordTypeUpdate,
			IndexType: indexing.InvertedIndexType, // Using generic update type
			PageID:    pagemanager.InvalidPageID,  // Not a specific page, but a logical update to the term dictionary
			Data:      logData.Bytes(),
		}
		if _, err := idx.lm.AppendRecord(lr, wal.LogTypeInvertedIndex); err != nil {
			// log.Printf("WARNING: Failed to log inverted index update for term '%s': %v", term, err)
		}
	}
	return nil
}

// Search retrieves a list of document keys that contain any of the query terms.
// It performs a union of results for multiple terms by reading postings lists from disk.
func (idx *InvertedIndex) Search(query string) ([]string, error) {
	if query == "" {
		return nil, nil
	}

	queryTerms := idx.tokenizeAndNormalize(query)
	if len(queryTerms) == 0 {
		return nil, nil
	}

	idx.dictMu.RLock()
	defer idx.dictMu.RUnlock()

	resultSet := make(map[string]struct{}) // Use a map to ensure unique results
	for _, term := range queryTerms {
		if metadata, ok := idx.termDictionary[term]; ok {
			postingsList, err := idx.readPostingsList(metadata)
			if err != nil {
				// log.Printf("ERROR: Failed to read postings list for term '%s' during search: %v", term, err)
				continue // Skip this term if its postings list can't be read
			}
			for _, key := range postingsList {
				resultSet[key] = struct{}{}
			}
		}
	}

	// Convert map keys to a slice
	results := make([]string, 0, len(resultSet))
	for key := range resultSet {
		results = append(results, key)
	}
	sort.Strings(results) // Sort results for consistent output

	// log.Printf("DEBUG: Inverted index: Search for '%s'. Found %d results: %v", query, len(results), results)
	return results, nil
}

// writePostingsList writes a serialized postings list to the data file, potentially spanning multiple pages.
// It returns the metadata for the stored list (pointing to the first page).
func (idx *InvertedIndex) writePostingsList(serializedList []byte) (PostingsListMetadata, error) {
	totalLength := uint32(len(serializedList))
	if totalLength == 0 {
		return PostingsListMetadata{}, nil
	}

	effectivePageSize := uint32(invertedIndexPageSize - postingsListPageHeaderSize)

	var firstPageID pagemanager.PageID
	//var currentPageID pagemanager.PageID = btree.InvalidPageID
	var prevPage *pagemanager.Page // To update NextPageID of the previous page
	var prevPageID pagemanager.PageID

	bytesWritten := uint32(0)

	for bytesWritten < totalLength {
		// Calculate how much data to write to the current page
		chunkSize := effectivePageSize
		if bytesWritten+chunkSize > totalLength {
			chunkSize = totalLength - bytesWritten
		}

		// Allocate a new page for this chunk
		newPage, newPageID, err := idx.bpm.NewPage()
		if err != nil {
			// If allocation fails, attempt to deallocate any pages already allocated for this list
			// (This is a simplification; full rollback would be complex)
			// log.Printf("ERROR: Failed to allocate new page for postings list (chunk size %d): %v", chunkSize, err)
			return PostingsListMetadata{}, fmt.Errorf("failed to allocate page for postings list: %w", err)
		}

		if firstPageID == btree.InvalidPageID {
			firstPageID = newPageID // This is the first page of the list
		}

		// Update NextPageID of the *previous* page in the chain
		if prevPage != nil {
			prevPage.Lock() // Acquire X-latch on prev page
			prevHeader := PostingsListPageHeader{
				DataLength: uint32(len(prevPage.GetData())) - postingsListPageHeaderSize, // Actual data written to prev page
				NextPageID: newPageID,
			}
			if err := serializePostingsListPageHeader(prevPage.GetData(), prevHeader); err != nil {
				prevPage.Unlock()
				idx.bpm.UnpinPage(prevPageID, false) // Unpin without marking dirty on serialization error
				idx.bpm.UnpinPage(newPageID, false)  // Unpin new page
				return PostingsListMetadata{}, fmt.Errorf("failed to update prev page header: %w", err)
			}
			prevPage.SetDirty(true)
			prevPage.Unlock()
			idx.bpm.UnpinPage(prevPageID, true) // Unpin previous page, mark dirty
		}

		// Write data to the new page
		dataOffset := postingsListPageHeaderSize
		copy(newPage.GetData()[dataOffset:], serializedList[bytesWritten:bytesWritten+chunkSize])

		// Prepare header for the current new page (NextPageID will be set in next iteration or to InvalidPageID)
		currentPageHeader := PostingsListPageHeader{
			DataLength: chunkSize,
			NextPageID: btree.InvalidPageID, // Placeholder, will be updated if there's a next page
		}
		if err := serializePostingsListPageHeader(newPage.GetData(), currentPageHeader); err != nil {
			idx.bpm.UnpinPage(newPageID, false)
			return PostingsListMetadata{}, fmt.Errorf("failed to serialize current page header: %w", err)
		}
		newPage.SetDirty(true) // Mark the page dirty

		// Update for next iteration
		prevPage = newPage
		prevPageID = newPageID
		bytesWritten += chunkSize
	}

	// Unpin the last page (which is `prevPage`)
	if prevPage != nil {
		if err := idx.bpm.UnpinPage(prevPageID, true); err != nil {
			// log.Printf("WARNING: Failed to unpin last postings list page %d: %v", prevPageID, err)
		}
	}

	// log.Printf("DEBUG: Wrote postings list of total length %d starting at PageID %d", totalLength, firstPageID)
	return PostingsListMetadata{
		StartPageID: firstPageID,
		TotalLength: totalLength,
		NumKeys:     0, // Will be updated by caller (Insert)
	}, nil
}

// readPostingsList reads a serialized postings list from disk, potentially spanning multiple pages,
// and deserializes it.
func (idx *InvertedIndex) readPostingsList(metadata PostingsListMetadata) ([]string, error) {
	if metadata.TotalLength == 0 || metadata.StartPageID == btree.InvalidPageID {
		return []string{}, nil
	}

	var buffer bytes.Buffer
	currentPageID := metadata.StartPageID

	for currentPageID != btree.InvalidPageID {
		page, err := idx.bpm.FetchPage(currentPageID)
		if err != nil {
			// Unpin any previously fetched pages before returning an error
			// This is simplified: a real system would track all pinned pages in a transaction context.
			// log.Printf("ERROR: Failed to fetch page %d while reading postings list: %v", currentPageID, err)
			return nil, fmt.Errorf("failed to fetch page %d for postings list: %w", currentPageID, err)
		}

		page.RLock() // Acquire S-latch for reading

		header, err := deserializePostingsListPageHeader(page.GetData())
		if err != nil {
			page.RUnlock()
			idx.bpm.UnpinPage(currentPageID, false)
			return nil, fmt.Errorf("failed to deserialize postings list page header for page %d: %w", currentPageID, err)
		}

		// Read data chunk
		dataStart := postingsListPageHeaderSize
		dataEnd := uint32(dataStart) + header.DataLength
		if dataEnd > uint32(len(page.GetData())) {
			page.RUnlock()
			idx.bpm.UnpinPage(currentPageID, false)
			return nil, fmt.Errorf("postings list data extends beyond page boundary for page %d", currentPageID)
		}
		buffer.Write(page.GetData()[dataStart:dataEnd])

		nextPageID := header.NextPageID

		page.RUnlock()                                              // Release S-latch
		if e := idx.bpm.UnpinPage(currentPageID, false); e != nil { // Unpin clean page
			// log.Printf("WARNING: Failed to unpin page %d after reading postings list chunk: %v", currentPageID, e)
		}
		currentPageID = nextPageID
	}

	// Deserialize the complete postings list
	postingsList, err := deserializePostingsList(buffer.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize complete postings list: %w", err)
	}
	return postingsList, nil
}

// Close ensures the inverted index is saved before shutdown.
func (idx *InvertedIndex) Close() error {
	// log.Println("INFO: Closing InvertedIndex...")
	var firstErr error

	// Flush all dirty pages from the buffer pool
	if err := idx.bpm.FlushAllPages(); err != nil {
		// log.Printf("ERROR: Failed to flush all pages from inverted index BPM: %v", err)
		firstErr = err
	}

	// Save the term dictionary
	if err := idx.saveTermDictionary(); err != nil {
		// log.Printf("ERROR: Failed to save inverted index term dictionary: %v", err)
		if firstErr == nil {
			firstErr = err
		}
	}

	// Update header with latest LSN (after all flushes)
	idx.header.LastLSN = pagemanager.LSN(idx.lm.GetCurrentLSN())
	if err := idx.writeHeader(); err != nil {
		// log.Printf("ERROR: Failed to write final inverted index header: %v", err)
		if firstErr == nil {
			firstErr = err
		}
	}

	// Close the underlying buffer pool manager (which also closes disk manager)
	if err := idx.bpm.FlushAllPages(); err != nil {
		// log.Printf("ERROR: Failed to close inverted index BPM: %v", err)
		if firstErr == nil {
			firstErr = err
		}
	}
	// Close the log manager
	if err := idx.lm.Close(); err != nil {
		// log.Printf("ERROR: Failed to close inverted index LogManager: %v", err)
		if firstErr == nil {
			firstErr = err
		}
	}

	// log.Println("INFO: InvertedIndex closed successfully.")
	return firstErr
}

// tokenizeAndNormalize breaks text into terms (words) and normalizes them.
// This is a basic implementation; a production system would use more advanced NLP techniques.
func (idx *InvertedIndex) tokenizeAndNormalize(text string) []string {
	// Convert to lowercase
	text = strings.ToLower(text)

	// Remove punctuation and split by whitespace
	// Using a more robust regex for word characters (alphanumeric + underscore)
	reg := regexp.MustCompile(`\b[a-z0-9]+\b`) // Matches sequences of alphanumeric characters
	words := reg.FindAllString(text, -1)

	// Simple stop word removal (example list)
	stopWords := map[string]struct{}{
		"a": {}, "an": {}, "the": {}, "is": {}, "are": {}, "and": {}, "or": {},
		"to": {}, "in": {}, "of": {}, "for": {}, "with": {}, "on": {}, "at": {},
		"this": {}, "that": {}, "it": {}, "be": {}, "have": {}, "has": {},
	}

	var terms []string
	for _, word := range words {
		// Basic stemming (very simple, just remove common suffixes)
		if strings.HasSuffix(word, "ing") && len(word) > 3 {
			word = strings.TrimSuffix(word, "ing")
		} else if strings.HasSuffix(word, "s") && len(word) > 1 {
			word = strings.TrimSuffix(word, "s")
		} else if strings.HasSuffix(word, "ed") && len(word) > 2 {
			word = strings.TrimSuffix(word, "ed")
		}

		if _, isStopWord := stopWords[word]; !isStopWord && word != "" {
			terms = append(terms, word)
		}
	}
	return terms
}

// --- Postings List Serialization/Deserialization ---
// This is a simple JSON-based serialization. For extreme efficiency,
// binary encodings like VarByte or Frame of Reference (FOR) would be used.

// serializePostingsList converts a slice of strings (document keys) into a byte slice.
func serializePostingsList(keys []string) ([]byte, error) {
	return json.Marshal(keys)
}

// deserializePostingsList converts a byte slice back into a slice of strings.
func deserializePostingsList(data []byte) ([]string, error) {
	var keys []string
	err := json.Unmarshal(data, &keys)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal postings list: %w", err)
	}
	return keys, nil
}

// --- PostingsListPageHeader Serialization/Deserialization ---

// serializePostingsListPageHeader writes the header to the beginning of a page's data.
func serializePostingsListPageHeader(pageData []byte, header PostingsListPageHeader) error {
	if len(pageData) < postingsListPageHeaderSize {
		return fmt.Errorf("page data buffer too small for postings list page header")
	}
	binary.LittleEndian.PutUint32(pageData[0:4], header.DataLength)
	binary.LittleEndian.PutUint64(pageData[4:12], uint64(header.NextPageID))
	return nil
}

// deserializePostingsListPageHeader reads the header from the beginning of a page's data.
func deserializePostingsListPageHeader(pageData []byte) (PostingsListPageHeader, error) {
	if len(pageData) < postingsListPageHeaderSize {
		return PostingsListPageHeader{}, fmt.Errorf("page data buffer too small to read postings list page header")
	}
	header := PostingsListPageHeader{
		DataLength: binary.LittleEndian.Uint32(pageData[0:4]),
		NextPageID: pagemanager.PageID(binary.LittleEndian.Uint64(pageData[4:12])),
	}
	return header, nil
}
