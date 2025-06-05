package btree

import (
	"cmp"
	"encoding/binary" // For serializing pagemanager.PageID to bytes
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"sync" // For sync.RWMutex and sync.Map

	flushmanager "github.com/sushant-115/gojodb/core/write_engine/flush_manager"
	"github.com/sushant-115/gojodb/core/write_engine/memtable"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
)

// --- Configuration & Constants ---

const (
	DefaultPageSize                      = 4096       // Bytes
	FileHeaderPageID  pagemanager.PageID = 0          // Page ID for the database file header
	InvalidPageID     pagemanager.PageID = 0          // Also used for header, but generally indicates invalid/unallocated
	MaxFilenameLength                    = 255        // Example limit
	DBMagic           uint32             = 0x6010DB00 // GoJoDB00

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
	// --- NEW: 2PC Specific Errors ---
	ErrTxnNotFound      = errors.New("transaction not found")
	ErrTxnAlreadyExists = errors.New("transaction already exists in table")
	ErrTxnInvalidState  = errors.New("transaction is in an invalid state for this operation")
	ErrKeyLocked        = errors.New("key is currently locked by another transaction")
	ErrPrepareFailed    = errors.New("prepare phase failed for transaction")
	// --- END NEW ---
	// --- NEW: Iterator Specific Errors ---
	ErrIteratorInvalid = errors.New("iterator is invalid or exhausted")
	// --- END NEW ---
)

// --- NEW: TransactionState and Transaction types ---

// TransactionState represents the in-memory state of a transaction on a participant.
type TransactionState int

const (
	TxnStateRunning   TransactionState = iota // Transaction is active, operations are being applied
	TxnStatePrepared                          // Participant has voted COMMIT and is waiting for global decision
	TxnStateCommitted                         // Participant has received COMMIT decision
	TxnStateAborted                           // Participant has received ABORT decision or decided to abort locally
)

// TransactionOperation represents a single operation within a distributed transaction on a participant.
type TransactionOperation struct {
	Command string `json:"command"` // PUT or DELETE
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
}

// Transaction represents an in-memory record of an active or prepared transaction.
type Transaction struct {
	ID        uint64
	State     TransactionState
	Operation []TransactionOperation
	// Operations: For undo, you might need to store the operations performed by this txn
	// on this shard, or at least the pages/keys it modified.
	// For V1, we'll rely on WAL for recovery.
	// LocksHeld: Keep track of locks held by this transaction.
	locksHeld map[string]struct{} // Set of keys locked by this transaction
}

// --- END NEW ---

// --- BTree Structure & Operations (Refactored for Persistence) ---
type Order[K any] func(a, b K) int

type KeyValueSerializer[K any, V any] struct {
	SerializeKey     func(K) ([]byte, error)
	DeserializeKey   func([]byte) (K, error)
	SerializeValue   func(V) ([]byte, error)
	DeserializeValue func([]byte) (V, error)
}

type BTree[K any, V any] struct {
	rootPageID   pagemanager.PageID
	degree       int
	keyOrder     Order[K]
	kvSerializer KeyValueSerializer[K, V]
	bpm          *memtable.BufferPoolManager
	diskManager  *flushmanager.DiskManager
	logManager   *wal.LogManager // Placeholder for *LogManager

	// --- NEW: 2PC Participant State ---
	transactionTable       map[uint64]*Transaction // TxnID -> Transaction (in-memory state of active txns)
	keyLocks               map[string]uint64       // Key (string representation) -> TxnID (ID of txn holding lock)
	keyLocksMu             sync.RWMutex
	transactionTableLockMu sync.RWMutex // Protects keyLocks map
	// --- END NEW ---
}

// NewBTreeFile creates a new database file and initializes a new B-tree within it.
func NewBTreeFile[K any, V any](filePath string, degree int, keyOrder Order[K], kvSerializer KeyValueSerializer[K, V], poolSize int, pageSize int, logManager *wal.LogManager) (*BTree[K, V], error) {
	if degree < 2 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidDegree, degree)
	}
	if keyOrder == nil {
		return nil, ErrNilKeyOrder
	}
	if kvSerializer.SerializeKey == nil || kvSerializer.DeserializeKey == nil || kvSerializer.SerializeValue == nil || kvSerializer.DeserializeValue == nil {
		return nil, errors.New("all key/value serializers must be provided")
	}

	dm, err := flushmanager.NewDiskManager(filePath, pageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk manager: %w", err)
	}

	// Open or create the file. 'create=true' means it will create if not exists, error if exists.
	_, err = dm.OpenOrCreateFile(true, degree, 0)
	if err != nil {
		dm.Close() // Ensure disk manager is closed on failure
		// If the file already exists, it's an error for NewBTreeFile
		if errors.Is(err, ErrDBFileExists) {
			return nil, fmt.Errorf("%w: file %s already exists. Use OpenBTreeFile to open an existing database", err, filePath)
		}
		return nil, fmt.Errorf("failed to open/create database file: %w", err)
	}

	bpm := memtable.NewBufferPoolManager(poolSize, dm, logManager)

	bt := &BTree[K, V]{
		rootPageID: InvalidPageID, degree: degree, keyOrder: keyOrder,
		kvSerializer: kvSerializer, bpm: bpm, diskManager: dm, logManager: logManager,
		// --- NEW: Initialize 2PC Participant State ---
		transactionTable: make(map[uint64]*Transaction),
		keyLocks:         make(map[string]uint64),
		// --- END NEW ---
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
		childPageIDs: make([]pagemanager.PageID, 0),
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
	if err := dm.UpdateHeaderField(func(h *flushmanager.DBFileHeader) {
		h.RootPageID = rootPageIDForNew
		h.TreeSize = 0 // Initially empty tree
		h.Degree = uint32(degree)
	}); err != nil {
		dm.Close()
		_ = os.Remove(filePath)
		return nil, fmt.Errorf("failed to update header with root page ID: %w", err)
	}

	log.Printf("INFO: New B-tree database created at %s with root pagemanager.PageID %d, Degree %d", filePath, rootPageIDForNew, degree)
	return bt, nil
}

// OpenBTreeFile opens an existing database file and initializes the B-tree from its header.
func OpenBTreeFile[K any, V any](filePath string, keyOrder Order[K], kvSerializer KeyValueSerializer[K, V], poolSize int, defaultPageSize int, logManager *wal.LogManager) (*BTree[K, V], error) {
	if keyOrder == nil {
		return nil, ErrNilKeyOrder
	}
	if kvSerializer.SerializeKey == nil || kvSerializer.DeserializeKey == nil || kvSerializer.SerializeValue == nil || kvSerializer.DeserializeValue == nil {
		return nil, errors.New("all key/value serializers must be provided")
	}

	dm, err := flushmanager.NewDiskManager(filePath, defaultPageSize)
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
	if dm.GetPageSize() != int(header.PageSize) {
		log.Printf("WARNING: DiskManager initialized with page size %d, but file header specifies %d. Adjusting DiskManager's pageSize.", dm.GetPageSize(), header.PageSize)
		dm.SetPageSize(int(header.PageSize)) // Adjust DM's page size to match the file's
	}

	bpm := memtable.NewBufferPoolManager(poolSize, dm, logManager)

	bt := &BTree[K, V]{
		rootPageID:   header.RootPageID,
		degree:       int(header.Degree),
		keyOrder:     keyOrder,
		kvSerializer: kvSerializer,
		bpm:          bpm,
		diskManager:  dm,
		logManager:   logManager,
		// --- NEW: Initialize 2PC Participant State ---
		transactionTable: make(map[uint64]*Transaction),
		keyLocks:         make(map[string]uint64),
		// --- END NEW ---
	}

	// --- CRITICAL FIX: Recovery Process using LogManager and BPM ---
	// This is where you would implement database recovery.
	// 1. Analysis Pass (scan WAL to find last checkpoint and active transactions)
	// 2. Redo Pass (replay WAL records for committed changes not on disk, ensuring pageLSN consistency)
	// 3. Undo Pass (rollback uncommitted transactions if any)
	if logManager != nil {
		log.Println("INFO: Starting database recovery process...")
		// Pass the BTree instance to recovery so it can interact with its locking and transaction table
		// if err := logManager.Recover(dm, wal.LSN(header.LastLSN)); err != nil {
		// 	dm.Close()
		// 	return nil, fmt.Errorf("database recovery failed: %w", err)
		// }
		log.Println("INFO: Database recovery complete.")
		// After recovery, the header's LastLSN might need to be updated if the recovery process
		// wrote new log records (e.g., compensation logs for undo).
		// For now, we assume recovery just applies changes and doesn't write new logs to header.
		// A more robust recovery would read the latest LSN after recovery and update header.
		// For simplicity, we re-read header after recovery.
		if err := dm.ReadHeader(header); err != nil { // Re-read header after potential recovery changes
			dm.Close()
			return nil, fmt.Errorf("failed to re-read header after recovery: %w", err)
		}
		// CRITICAL FIX: Update BTree's in-memory state from the re-read header
		bt.rootPageID = header.RootPageID
		bt.degree = int(header.Degree)
		// If TreeSize was a field in BTree, you'd update it here too: bt.treeSize = header.TreeSize
	}
	// --- END CRITICAL FIX ---

	log.Printf("INFO: Existing B-tree database opened from %s. Root pagemanager.PageID: %d, Degree: %d, TreeSize: %d",
		filePath, bt.rootPageID, bt.degree, header.TreeSize)
	return bt, nil
}

func (bt *BTree[K, V]) GetRootPageID() pagemanager.PageID {
	return bt.rootPageID
}

// SetRootPageID updates the B-tree's in-memory rootPageID and logs the change.
// It also updates the disk header immediately to ensure durability of root changes.
func (bt *BTree[K, V]) SetRootPageID(newRootPageID pagemanager.PageID, txnID uint64) error {
	oldRootPageID := bt.rootPageID
	bt.rootPageID = newRootPageID

	// Log the root page ID change
	newRootPageIDBytes := make([]byte, 8) // pagemanager.PageID is uint64
	binary.LittleEndian.PutUint64(newRootPageIDBytes, uint64(newRootPageID))

	logRecord := &wal.LogRecord{
		TxnID:  txnID,
		Type:   wal.LogRecordTypeRootChange,
		PageID: newRootPageID, // New root page ID
		Data:   newRootPageIDBytes,
	}
	// binary.LittleEndian.PutUint64(logRecord.Data, uint64(oldRootPageID))

	lsn, err := bt.logManager.AppendRecord(logRecord, wal.LogTypeBtree)
	if err != nil {
		log.Printf("ERROR: Failed to log root page ID change from %d to %d: %v", oldRootPageID, newRootPageID, err)
		// This is a critical error, as root change might not be recoverable.
		// Revert in-memory change for safety, though disk might be inconsistent.
		bt.rootPageID = oldRootPageID
		return fmt.Errorf("failed to log root page ID change: %w", err)
	}

	// Flush the log immediately to ensure durability of the root change record
	if err := bt.logManager.Sync(); err != nil {
		log.Printf("ERROR: Failed to flush log for root page ID change LSN %d: %v", lsn, err)
		// This is also critical.
		bt.rootPageID = oldRootPageID
		return fmt.Errorf("failed to flush root page ID change log: %w", err)
	}

	// Update the file header with the new root page ID
	if err := bt.diskManager.UpdateHeaderField(func(h *flushmanager.DBFileHeader) {
		h.RootPageID = newRootPageID
	}); err != nil {
		log.Printf("ERROR: Failed to update disk header with new root page ID %d: %v", newRootPageID, err)
		// This is critical. The log record is there, but disk header is not.
		// Recovery will fix this, but current state is inconsistent.
		bt.rootPageID = oldRootPageID // Revert in-memory for consistency with disk
		return fmt.Errorf("failed to update disk header for root change: %w", err)
	}

	log.Printf("INFO: B-tree root page ID changed from %d to %d (Txn %d). Logged and flushed.", oldRootPageID, newRootPageID, txnID)
	return nil
}

func (bt *BTree[K, V]) GetDiskManager() *flushmanager.DiskManager {
	return bt.diskManager
}

func (bt *BTree[K, V]) FetchPage(pageID pagemanager.PageID) (*pagemanager.Page, error) {
	return bt.bpm.FetchPage(pageID)
}

func (bt *BTree[K, V]) FlushPage(pageID pagemanager.PageID) error {
	return bt.bpm.FlushPage(pageID)
}

func (bt *BTree[K, V]) UnpinPage(pageID pagemanager.PageID, isDirty bool) error {
	return bt.bpm.UnpinPage(pageID, isDirty)
}

// fetchNode retrieves a node from the buffer pool or disk.
func (bt *BTree[K, V]) fetchNode(pageID pagemanager.PageID) (*Node[K, V], *pagemanager.Page, error) {
	if pageID == InvalidPageID {
		return nil, nil, errors.New("attempted to fetch node with InvalidPageID")
	}
	page, err := bt.bpm.FetchPage(pageID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch page %d from buffer pool: %w", pageID, err)
	}
	// --- NEW: Acquire S-latch on page before deserializing ---
	page.RLock()
	defer page.RUnlock() // Ensure latch is released
	// --- END NEW ---
	node := &Node[K, V]{tree: bt}
	err = node.deserialize(page, bt.kvSerializer.DeserializeKey, bt.kvSerializer.DeserializeValue)
	if err != nil {
		bt.bpm.UnpinPage(pageID, false) // Unpin page if deserialization fails
		return nil, nil, fmt.Errorf("failed to deserialize node from page %d: %w", pageID, err)
	}
	return node, page, nil
}

func (bt *BTree[K, V]) DeallocatePage(pageID pagemanager.PageID) error {
	return bt.diskManager.DeallocatePage(pageID)
}
func (bt *BTree[K, V]) GetPageSize() int {
	return DefaultPageSize
}

func (bt *BTree[K, V]) ReadPage(pageID pagemanager.PageID, pageData []byte) error {
	return bt.diskManager.ReadPage(pageID, pageData)
}

func (bt *BTree[K, V]) GetNumPages() uint64 {
	return bt.diskManager.GetNumPages()
}

func (bt *BTree[K, V]) InvalidatePage(pageID pagemanager.PageID) {
	bt.bpm.InvalidatePage(pageID)
}

func (bt *BTree[K, V]) SetNumPages(totalPages uint64) {
	bt.diskManager.SetNumPages(totalPages)
}

func (bt *BTree[K, V]) WritePage(pageID pagemanager.PageID, pageData []byte) error {
	return bt.diskManager.WritePage(pageID, pageData)
}

// GetSize reads the persisted tree size from the file header.
func (bt *BTree[K, V]) GetSize() (uint64, error) {
	var header flushmanager.DBFileHeader
	// Read header directly, ensures latest size from disk
	if err := bt.diskManager.ReadHeader(&header); err != nil {
		return 0, fmt.Errorf("failed to read tree size from header: %w", err)
	}
	return header.TreeSize, nil
}

// updatePersistedSize updates the TreeSize field in the file header.
// NOTE: This is inefficient for frequent updates. In a real DB, size is often
// updated in memory and flushed periodically or at shutdown/checkpoint.
func (bt *BTree[K, V]) updatePersistedSize(delta int64) error {
	return bt.diskManager.UpdateHeaderField(func(h *flushmanager.DBFileHeader) {
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
func (bt *BTree[K, V]) searchRecursive(currNode *Node[K, V], currPage *pagemanager.Page, key K) (V, bool, error) {
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

// --- NEW: Transactional Insert/Delete Operations ---

// Insert inserts a key-value pair into the B-tree. If txnID is 0, it's an auto-commit operation.
func (bt *BTree[K, V]) Insert(key K, value V, txnID uint64) error {
	// Acquire lock for the key
	if err := bt.acquireLock(key, txnID); err != nil {
		return fmt.Errorf("failed to acquire lock for key %v: %w", key, err)
	}
	defer func() {
		// For auto-commit, release immediately. For 2PC, locks are released by Commit/Abort.
		if txnID == 0 {
			bt.releaseLock(key, txnID)
		}
	}()

	// Check if key exists for update or if it's a new insert for size update
	_, exists, searchErr := bt.Search(key)
	if searchErr != nil {
		log.Printf("WARNING: Search error during pre-insert check for key %v: %v", key, searchErr)
		// We proceed, but the actual insert path will handle if the key is found or not.
		// If search failed due to corruption (e.g. checksum), the insert might also fail.
	}

	// If the tree is empty, create the first root node
	if bt.rootPageID == InvalidPageID {
		log.Printf("DEBUG: Txn %d: Inserting first key %v. Creating initial root page.", txnID, key)
		rootPg, rootPgID, err := bt.bpm.NewPage() // Allocates a new page and pins it
		if err != nil {
			return fmt.Errorf("failed to create first root page for insert: %w", err)
		}
		// Use SetRootPageID to update and log the change
		if err := bt.SetRootPageID(rootPgID, txnID); err != nil {
			bt.bpm.UnpinPage(rootPgID, false)           // Unpin even on error
			_ = bt.diskManager.DeallocatePage(rootPgID) // Try to free the orphaned page
			return fmt.Errorf("failed to set initial root page ID: %w", err)
		}
		rootNode := &Node[K, V]{pageID: rootPgID, isLeaf: true, tree: bt, keys: make([]K, 0), values: make([]V, 0), childPageIDs: make([]pagemanager.PageID, 0)}
		// TODO: WAL Log creation of new root page with TxnID
		return bt.insertNonFull(rootNode, rootPg, key, value, !exists, txnID) // insertNonFull unpins rootPg
	}

	// Fetch the current root node
	rootNode, rootPage, err := bt.fetchNode(bt.rootPageID)
	if err != nil {
		return fmt.Errorf("failed to fetch root node during insert: %w", err)
	}

	// If the root node is full, the tree must grow in height by splitting the root
	if len(rootNode.keys) == 2*bt.degree-1 {
		log.Printf("DEBUG: Txn %d: Root node %d is full. Splitting root.", txnID, bt.rootPageID)
		newRootDiskPage, newRootPageID, err := bt.bpm.NewPage() // Allocates a new page for the new root
		if err != nil {
			bt.bpm.UnpinPage(rootPage.GetPageID(), false) // Unpin old root on error
			return fmt.Errorf("failed to create new root page during split: %w", err)
		}

		oldRootPageID := bt.rootPageID // Store old root ID
		// Use SetRootPageID to update and log the change
		if err := bt.SetRootPageID(newRootPageID, txnID); err != nil {
			bt.bpm.UnpinPage(rootPage.GetPageID(), false)        // Unpin old root
			bt.bpm.UnpinPage(newRootDiskPage.GetPageID(), false) // Unpin new root
			_ = bt.diskManager.DeallocatePage(newRootPageID)     // Try to free orphaned new root page
			return fmt.Errorf("failed to set new root page ID during split: %w", err)
		}

		// Create the new in-memory root node
		newRootNode := &Node[K, V]{
			pageID:       newRootPageID,
			isLeaf:       false,                               // New root is always an internal node
			childPageIDs: []pagemanager.PageID{oldRootPageID}, // Old root becomes the first child
			tree:         bt,
			keys:         make([]K, 0),
			values:       make([]V, 0),
		}
		// TODO: WAL Log new root page creation and the subsequent split operation with TxnID

		// Split the old root node. `splitChild` promotes a key to `newRootNode` and unpins `rootPage` and the new sibling page.
		err = bt.splitChild(newRootNode, newRootDiskPage, 0, rootNode, rootPage, txnID)
		if err != nil {
			// If split fails, newRootDiskPage and rootPage (old root) are already unpinned (or handled by splitChild)
			// The B-tree state might be inconsistent if split fails before full completion.
			// This is a critical error path.
			return fmt.Errorf("failed to split root node: %w", err)
		}
		log.Printf("DEBUG: Txn %d: Root split complete. New root %d, Old root %d", txnID, newRootPageID, oldRootPageID)

		// --- CRITICAL FIX: Serialize the newRootNode after it's modified by splitChild ---
		// The splitChild method modifies newRootNode (the parent) in memory and marks newRootDiskPage dirty.
		// But it does NOT serialize newRootNode to newRootDiskPage.
		// We must do that here before attempting to re-fetch it.
		if errS := newRootNode.serialize(newRootDiskPage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); errS != nil {
			// If serialization fails, unpin the page and return error.
			bt.bpm.UnpinPage(newRootDiskPage.GetPageID(), false) // Unpin even if serialize failed
			return fmt.Errorf("failed to serialize new root node %d after split: %w", newRootPageID, errS)
		}
		// Unpin newRootDiskPage here, as it's now serialized and dirty.
		// The subsequent fetchNode will re-pin it.
		if errU := bt.bpm.UnpinPage(newRootDiskPage.GetPageID(), true); errU != nil {
			return fmt.Errorf("failed to unpin new root page %d after serialization: %w", newRootPageID, errU)
		}
		// --- END CRITICAL FIX ---

		// Re-fetch newRoot to insert into it, as splitChild only modified it in memory before serializing
		// This is inefficient. splitChild should ideally return the modified parent node.
		// For now, fetch it again.
		reloadedNewRootNode, reloadedNewRootPage, fetchErr := bt.fetchNode(newRootPageID)
		if fetchErr != nil {
			return fetchErr
		}
		return bt.insertNonFull(reloadedNewRootNode, reloadedNewRootPage, key, value, !exists, txnID) // Recursive call unpins
	} else {
		// Root node is not full, just insert into it directly.
		return bt.insertNonFull(rootNode, rootPage, key, value, !exists, txnID) // insertNonFull unpins rootPage
	}
}

// insertNonFull inserts a key-value pair into a node that is guaranteed not to be full.
// It takes ownership of `node` and `page`, unpinning `page` before returning.
func (bt *BTree[K, V]) insertNonFull(node *Node[K, V], page *pagemanager.Page, key K, value V, incrementSize bool, txnID uint64) error {
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
			log.Printf("DEBUG: Txn %d: Updated existing key %v in leaf node %d", txnID, key, node.pageID)
			// TODO: WAL Log value update with TxnID
			if err := node.serialize(page, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); err != nil {
				bt.bpm.UnpinPage(page.GetPageID(), false)
				return fmt.Errorf("failed to serialize leaf node %d after update: %w", node.pageID, err)
			}
			return bt.bpm.UnpinPage(page.GetPageID(), true) // Unpin, mark dirty
		}

		// Key not found, insert new key-value pair
		node.keys = slices.Insert(node.keys, idx, key)
		node.values = slices.Insert(node.values, idx, value)
		log.Printf("DEBUG: Txn %d: Inserted new key %v into leaf node %d at index %d. New numKeys: %d", txnID, key, node.pageID, idx, len(node.keys))

		// TODO: WAL Log key/value insert with TxnID
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
			log.Printf("DEBUG: Txn %d: Updated existing key %v in internal node %d", txnID, key, node.pageID)
			// TODO: WAL Log value update in internal node with TxnID
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
			log.Printf("DEBUG: Txn %d: Child node %d of parent %d is full. Splitting child.", txnID, childNode.pageID, node.pageID)
			// TODO: WAL Log upcoming split operation (parent and child involved) with TxnID
			err = bt.splitChild(node, page, idx, childNode, childPage, txnID)
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
				log.Printf("DEBUG: Txn %d: Key %v matches promoted key from split. Updating value in parent %d.", txnID, key, node.pageID)
				// TODO: WAL Log this update in parent node with TxnID
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
				return fmt.Errorf("failed to unpin parent node %d after serialization: %w", node.pageID, errU)
			}

			// Fetch the correct child to descend into (it's now `node.childPageIDs[idx]`)
			descendChildPageID := node.childPageIDs[idx]
			descendChildNode, descendChildPage, fetchErr := bt.fetchNode(descendChildPageID) // New child is pinned
			if fetchErr != nil {
				return fmt.Errorf("failed to fetch correct child %d after split: %w", descendChildPageID, fetchErr)
			}
			return bt.insertNonFull(descendChildNode, descendChildPage, key, value, incrementSize, txnID) // Recursive call unpins
		} else {
			// Child has space. Unpin current parent node before descending.
			unpinErr := bt.bpm.UnpinPage(page.GetPageID(), false)
			if unpinErr != nil {
				bt.bpm.UnpinPage(childPage.GetPageID(), false) // Unpin child if parent unpin fails
				return fmt.Errorf("failed to unpin parent page %d before descending: %w", page.GetPageID(), unpinErr)
			}
			// Recurse into the child
			return bt.insertNonFull(childNode, childPage, key, value, incrementSize, txnID) // Recursive call unpins
		}
	}
}

// splitChild splits a full childNode, promoting a key to the parentNode.
// It takes ownership of `parentNode` and `parentPage`, `childToSplitNode`, `childToSplitPage`.
// It serializes and unpins `childToSplitPage` and the new sibling page.
// It modifies `parentNode` and marks `parentPage` dirty, but *does not unpin* `parentPage`.
// The caller (`insertNonFull`) is responsible for serializing and unpinning `parentPage`.
func (bt *BTree[K, V]) splitChild(parentNode *Node[K, V], parentPage *pagemanager.Page, childIdx int, childToSplitNode *Node[K, V], childToSplitPage *pagemanager.Page, txnID uint64) error {
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
		newSiblingNode.childPageIDs = make([]pagemanager.PageID, t) // t children for the right half
	} else {
		newSiblingNode.childPageIDs = make([]pagemanager.PageID, 0)
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

	// TODO: WAL - Log modifications to parentNode, childToSplitNode, newSiblingNode with TxnID
	log.Printf("DEBUG: Txn %d: Split child %d into new sibling %d. Promoted key %v to parent %d.",
		txnID, childToSplitNode.pageID, newSiblingPageID, middleKey, parentNode.pageID)

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

// Delete removes a key-value pair from the B-tree. If txnID is 0, it's an auto-commit operation.
func (bt *BTree[K, V]) Delete(key K, txnID uint64) error {
	// Acquire lock for the key
	if err := bt.acquireLock(key, txnID); err != nil {
		return fmt.Errorf("failed to acquire lock for key %v: %w", key, err)
	}
	defer func() {
		// For auto-commit, release immediately. For 2PC, locks are released by Commit/Abort.
		if txnID == 0 {
			bt.releaseLock(key, txnID)
		}
	}()

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
	wasDeleted, err := bt.deleteRecursive(rootNode, rootPage, key, txnID)
	if err != nil {
		return fmt.Errorf("recursive deletion failed for key %v: %w", key, err)
	}
	if !wasDeleted {
		// This should theoretically not happen if pre-search found the key
		log.Printf("WARNING: Txn %d: Key %v found in pre-search but not deleted by recursive delete.", txnID, key)
		return ErrKeyNotFound
	}

	// After deletion, the root might have changed (e.g., if it became empty and its child became the new root).
	// We need to re-fetch the root to check its current state and update the B-tree's rootPageID if needed.
	// This re-fetch is somewhat inefficient but safer given complex deletion paths.
	currentRootNode, currentRootPage, fetchErr := bt.fetchNode(bt.rootPageID) // Current root is pinned
	if fetchErr != nil {
		return fmt.Errorf("failed to fetch root page post-delete for root check: %w", fetchErr)
	}

	// Case: Root has 0 keys and is not a leaf (meaning it has only one child left after merges/deletions)
	if len(currentRootNode.keys) == 0 && !currentRootNode.isLeaf {
		if len(currentRootNode.childPageIDs) == 1 {
			oldRootPageID := bt.rootPageID
			newRootPageID := currentRootNode.childPageIDs[0] // The only child becomes the new root
			log.Printf("DEBUG: Txn %d: Root %d has 0 keys and 1 child. New root is child %d.", txnID, oldRootPageID, newRootPageID)

			// Use SetRootPageID to update and log the change
			if err := bt.SetRootPageID(newRootPageID, txnID); err != nil {
				bt.bpm.UnpinPage(currentRootPage.GetPageID(), false) // Unpin old root
				return fmt.Errorf("failed to set new root page ID after deletion: %w", err)
			}
			// TODO: Deallocate the oldRootPageID from disk using the free list manager
			// This is critical to reclaim space.
			if err := bt.diskManager.DeallocatePage(oldRootPageID); err != nil {
				log.Printf("ERROR: Failed to deallocate old root page %d: %v", oldRootPageID, err)
				// This is a soft error, but indicates a resource leak.
			}
		} else if len(currentRootNode.childPageIDs) == 0 {
			// This state usually means the tree became completely empty (last key deleted from a root that was also a leaf).
			// The root should remain a single empty leaf page. No change to bt.rootPageID needed if it's already pointing to this empty leaf.
			// The `deleteRecursive` should have already made `currentRootNode` an empty leaf.
			log.Printf("DEBUG: Txn %d: B-tree root %d became empty after deletion.", txnID, bt.rootPageID)
		}
	}

	// Unpin the current (potentially new) root page. It's only dirty if its ID changed in the header.
	// The `SetRootPageID` already handles logging and updating the header.
	// The `currentRootPage` might be the old root or the new one.
	// If the root actually changed, `SetRootPageID` already flushed the log and updated the header.
	// We just need to unpin this page if it was pinned by this function.
	unpinErr := bt.bpm.UnpinPage(currentRootPage.GetPageID(), currentRootPage.IsDirty()) // Check if it's dirty before unpinning
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
func (bt *BTree[K, V]) deleteRecursive(node *Node[K, V], nodePage *pagemanager.Page, key K, txnID uint64) (bool, error) {
	idx, found := slices.BinarySearchFunc(node.keys, key, bt.keyOrder)
	var err error
	var actuallyDeleted bool = false

	if node.isLeaf {
		// Case 1: Key is in a leaf node.
		if found {
			// Remove the key and its value from the leaf node.
			node.keys = slices.Delete(node.keys, idx, idx+1)
			node.values = slices.Delete(node.values, idx, idx+1)
			log.Printf("DEBUG: Txn %d: Deleted key %v from leaf node %d. New numKeys: %d", txnID, key, node.pageID, len(node.keys))

			// TODO: WAL Log key deletion from leaf with TxnID
			// Serialize the modified leaf node and mark it dirty.
			if errS := node.serialize(nodePage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); errS != nil {
				bt.bpm.UnpinPage(nodePage.GetPageID(), false) // Unpin even on serialize failure
				return false, fmt.Errorf("failed to serialize leaf node %d after deletion: %w", node.pageID, errS)
			}
			actuallyDeleted = true                             // Key was successfully deleted.
			err = bt.bpm.UnpinPage(nodePage.GetPageID(), true) // Unpin, mark dirty
		} else {
			// Key not found in this leaf node.
			log.Printf("DEBUG: Txn %d: Key %v not found in leaf node %d during recursive delete.", txnID, key, node.pageID)
			err = bt.bpm.UnpinPage(nodePage.GetPageID(), false) // Unpin, page is clean
		}
		return actuallyDeleted, err
	}

	// Key is not in a leaf node (this is an internal node).
	if found {
		// Case 2: Key `k` is found in this internal node `node` at `node.keys[idx]`.
		// We need to replace `k` with its predecessor or successor, and then recursively delete that predecessor/successor.
		log.Printf("DEBUG: Txn %d: Key %v found in internal node %d at index %d.", txnID, key, node.pageID, idx)
		actuallyDeleted, err = bt.deleteFromInternalNode(node, nodePage, key, idx, txnID)
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
			log.Printf("DEBUG: Txn %d: Child node %d is under-full. Calling ensureChildHasEnoughKeys from parent %d.", txnID, childNode.pageID, node.pageID)

			// This function will ensure `node.childPageIDs[idx]` now points to a child with enough keys.
			// It modifies `node` (parent) and `nodePage` (marks dirty).
			errEnsure := bt.ensureChildHasEnoughKeys(node, nodePage, idx, txnID)
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
				log.Printf("DEBUG: Txn %d: Key %v found in parent %d after ensureChildHasEnoughKeys. Recursing into deleteFromInternalNode.", txnID, key, node.pageID)
				actuallyDeleted, err = bt.deleteFromInternalNode(node, nodePage, key, idx, txnID)
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
			actuallyDeleted, err = bt.deleteRecursive(reFetchedChildNode, reFetchedChildPage, key, txnID) // Recursive call unpins child
		} else {
			// Child has space. Unpin current parent node before descending.
			unpinErr := bt.bpm.UnpinPage(nodePage.GetPageID(), false)
			if unpinErr != nil {
				bt.bpm.UnpinPage(childPage.GetPageID(), false) // Unpin child if parent unpin fails
				return false, fmt.Errorf("failed to unpin parent page %d before descending: %w", nodePage.GetPageID(), unpinErr)
			}
			// Recurse into the child
			return bt.deleteRecursive(childNode, childPage, key, txnID) // Recursive call unpins child
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
func (bt *BTree[K, V]) deleteFromInternalNode(node *Node[K, V], nodePage *pagemanager.Page, key K, idxInNode int, txnID uint64) (bool, error) {
	t := bt.degree
	var err error
	keyActuallyRemoved := false // This will be true if the replacement key is successfully deleted from child
	actuallyDeleted := false
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
		log.Printf("DEBUG: Txn %d: Borrowing predecessor from left child %d for key %v in parent %d.", txnID, leftChildNode.pageID, key, node.pageID)
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
		keyActuallyRemoved, err = bt.deleteRecursive(reFetchedLeftChildNode, reFetchedLeftChildPage, predKey, txnID)
		// `deleteRecursive` will serialize and unpin `reFetchedLeftChildPage`.
		if err != nil {
			bt.bpm.UnpinPage(nodePage.GetPageID(), false) // Unpin parent on error
			return false, fmt.Errorf("failed to delete recursive. isKeyDelete: %v. Error: %w", keyActuallyRemoved, err)
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
		log.Printf("DEBUG: Txn %d: Borrowing successor from right child %d for key %v in parent %d.", txnID, rightChildNode.pageID, key, node.pageID)
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
		_, err = bt.deleteRecursive(reFetchedRightChildNode, reFetchedRightChildPage, succKey, txnID)
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
		log.Printf("DEBUG: Txn %d: Merging child %d (left) and %d (right) for key %v in parent %d.",
			txnID, leftChildNode.pageID, rightChildNode.pageID, key, node.pageID)

		// `mergeChildrenAndKey` is a high-level function that handles:
		// 1. Moving `key` and `value` from `node` to `leftChildNode`.
		// 2. Moving all contents from `rightChildNode` to `leftChildNode`.
		// 3. Updating `node` (removing `key` and `rightChild` pointer).
		// 4. Serializing `leftChildNode` and marking `nodePage` dirty.
		// 5. Unpinning `leftChildPage` and `rightChildPage`.
		// 6. Deallocating `rightChildPageID`.
		err = bt.mergeChildrenAndKey(node, nodePage, idxInNode, nil, nil, nil, nil, key, txnID)
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
		actuallyDeleted, err := bt.deleteRecursive(reFetchedLeftChildNode, reFetchedLeftChildPage, key, txnID)
		// `deleteRecursive` will serialize and unpin `reFetchedLeftChildPage`.
		log.Println("Actually Deleted", actuallyDeleted)
		// Finally, unpin the parent node's page.
		if errS := node.serialize(nodePage, bt.kvSerializer.SerializeKey, bt.kvSerializer.SerializeValue); errS != nil {
			bt.bpm.UnpinPage(nodePage.GetPageID(), false)
			return false, fmt.Errorf("failed to serialize parent node %d after merge: %w", node.pageID, errS)
		}
		if e := bt.bpm.UnpinPage(nodePage.GetPageID(), true); e != nil && err == nil {
			err = e // Propagate unpin error if no other error occurred
			log.Println("DiskManager: Failed to unpinpage. Error: ", err)

		}
	}
	return actuallyDeleted, err
}

// findPredecessor finds the rightmost key in the subtree rooted at `node`.
// It takes ownership of `node` and `nodePage`, unpinning `nodePage` before returning.
func (bt *BTree[K, V]) findPredecessor(node *Node[K, V], nodePage *pagemanager.Page) (K, V) {
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
func (bt *BTree[K, V]) findSuccessor(node *Node[K, V], nodePage *pagemanager.Page) (K, V) {
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
func (bt *BTree[K, V]) ensureChildHasEnoughKeys(parentNode *Node[K, V], parentPage *pagemanager.Page, childIdx int, txnID uint64) error {
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
		// For deletion, we need to ensure child has at least `t` keys if it's an internal node.
		// If it's a leaf, `t-1` keys is the minimum.
		// To be safe for recursive descent, a node should have at least `t` keys if it's not a leaf.
		// If it's a leaf, `t-1` is minimum.
		// Current check `len(childNode.keys) < bt.degree` is for `t-1` keys. So if it has `t-2` or fewer keys, it's underfull.
		// If it has `t-1` keys, it's at minimum, but may need a borrow for recursive descent.
		// Let's keep the existing check for underflow.
		log.Printf("DEBUG: Txn %d: Child node %d already has enough keys (%d >= %d). No action needed.", txnID, childNode.pageID, len(childNode.keys), t)
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
			log.Printf("DEBUG: Txn %d: Borrowing from left sibling %d for child %d.", txnID, leftSiblingNode.pageID, childNode.pageID)
			err = bt.borrowFromLeftSibling(parentNode, parentPage, childIdx, childNode, childPage, leftSiblingNode, leftSiblingPage, txnID)
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
			log.Printf("DEBUG: Txn %d: Borrowing from right sibling %d for child %d.", txnID, rightSiblingNode.pageID, childNode.pageID)
			err = bt.borrowFromRightSibling(parentNode, parentPage, childIdx, childNode, childPage, rightSiblingNode, rightSiblingPage, txnID)
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

	log.Printf("DEBUG: Txn %d: Neither sibling can lend for child %d. Performing merge.", txnID, childNode.pageID)
	// Decide which sibling to merge with (prefer left if possible, else right).
	if childIdx > 0 { // Merge child with left sibling
		// `mergeChildrenAndKey` expects `leftMergeChildIdx` to be the index of the LEFT child of the merge pair.
		// So, merge `parentNode.childPageIDs[childIdx-1]` (left sibling) and `parentNode.childPageIDs[childIdx]` (current child).
		// The key `parentNode.keys[childIdx-1]` will come down from the parent.
		return bt.mergeChildrenAndKey(parentNode, parentPage, childIdx-1, nil, nil, nil, nil, parentNode.keys[childIdx-1], txnID)
	} else { // Merge child with right sibling (current child is `childIdx=0`)
		// Merge `parentNode.childPageIDs[childIdx]` (current child) and `parentNode.childPageIDs[childIdx+1]` (right sibling).
		// The key `parentNode.keys[childIdx]` will come down from the parent.
		return bt.mergeChildrenAndKey(parentNode, parentPage, childIdx, nil, nil, nil, nil, parentNode.keys[childIdx], txnID)
	}
}

// borrowFromLeftSibling takes the largest key/value from the left sibling and moves it to the child,
// promoting a key from the parent to the child, and updating the parent's key.
// It takes ownership of all nodes/pages. It serializes and unpins `childPage` and `leftSiblingPage`.
// It marks `parentPage` dirty but *does not unpin* it.
func (bt *BTree[K, V]) borrowFromLeftSibling(
	parentNode *Node[K, V], parentPage *pagemanager.Page, childIdx int,
	childNode *Node[K, V], childPage *pagemanager.Page,
	leftSiblingNode *Node[K, V], leftSiblingPage *pagemanager.Page, txnID uint64) error {

	log.Printf("DEBUG: Txn %d: Borrowing from left sibling: parent %d, child %d, leftSibling %d", txnID, parentNode.pageID, childNode.pageID, leftSiblingNode.pageID)

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
	parentNode *Node[K, V], parentPage *pagemanager.Page, childIdx int,
	childNode *Node[K, V], childPage *pagemanager.Page,
	rightSiblingNode *Node[K, V], rightSiblingPage *pagemanager.Page, txnID uint64) error {

	log.Printf("DEBUG: Txn %d: Borrowing from right sibling: parent %d, child %d, rightSibling %d", txnID, parentNode.pageID, childNode.pageID, rightSiblingNode.pageID)

	// Key/value from parent that moves down to the end of the child
	keyFromParent := parentNode.keys[childIdx]
	valueFromParent := parentNode.values[childIdx]

	// Key/value from right sibling that moves up to parent
	keyFromSibling := rightSiblingNode.keys[0]
	valueFromSibling := rightSiblingNode.values[0]

	// Move key/value from parent down to the end of the child
	childNode.keys = append(childNode.keys, keyFromParent)
	childNode.values = append(childNode.values, valueFromParent)

	// If children are not leaves, move the leftmost child pointer from right sibling to rightmost of child
	if !childNode.isLeaf {
		childPointerFromSibling := rightSiblingNode.childPageIDs[0]
		childNode.childPageIDs = slices.Insert(childNode.childPageIDs, len(childNode.childPageIDs), childPointerFromSibling) // Append to end
		rightSiblingNode.childPageIDs = slices.Delete(rightSiblingNode.childPageIDs, 0, 1)                                   // Remove first child pointer
	}

	// Move key/value from right sibling up to replace the old key in parent
	parentNode.keys[childIdx] = keyFromSibling
	parentNode.values[childIdx] = valueFromSibling

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
	parentNode *Node[K, V], parentPage *pagemanager.Page, leftMergeChildIdx int,
	// Hints for left/right nodes/pages; they will be fetched if nil.
	// For clarity and directness, this function will always fetch them.
	_leftNodeHint *Node[K, V], _leftPageHint *pagemanager.Page,
	_rightNodeHint *Node[K, V], _rightPageHint *pagemanager.Page,
	originalKeyToDelete K, // The key that was in parentNode and now needs to be deleted from the merged child
	txnID uint64) error {

	log.Printf("DEBUG: Txn %d: Initiating merge operation for parent %d, left child index %d, key to delete: %v", txnID, parentNode.pageID, leftMergeChildIdx, originalKeyToDelete)

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

	// TODO: WAL Log: merge operation, including deallocation of rightChildPageID with TxnID

	// 3. Move key from parent down to leftChild.
	leftChildNode.keys = append(leftChildNode.keys, keyFromParent)
	leftChildNode.values = append(leftChildNode.values, valueFromParent)

	// 4. Append keys, values, and children from rightChild to leftChild.
	leftChildNode.keys = append(leftChildNode.keys, rightChildNode.keys...)
	leftChildNode.values = append(leftChildNode.values, rightChildNode.values...)
	if !leftChildNode.isLeaf {
		leftChildNode.childPageIDs = append(leftChildNode.childPageIDs, rightChildNode.childPageIDs...)
	}
	log.Printf("DEBUG: Txn %d: Merged content into left child %d. New keys: %d, New children: %d",
		txnID, leftChildNode.pageID, len(leftChildNode.keys), len(leftChildNode.childPageIDs))

	// 5. Remove the merged key/value from parent.
	parentNode.keys = slices.Delete(parentNode.keys, leftMergeChildIdx, leftMergeChildIdx+1)
	parentNode.values = slices.Delete(parentNode.values, leftMergeChildIdx, leftMergeChildIdx+1)
	// 6. Remove the rightChild pointer from parent.
	parentNode.childPageIDs = slices.Delete(parentNode.childPageIDs, leftMergeChildIdx+1, leftMergeChildIdx+2)
	log.Printf("DEBUG: Txn %d: Updated parent %d after merge. New keys: %d, New children: %d",
		txnID, parentNode.pageID, len(parentNode.keys), len(parentNode.childPageIDs))

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
	log.Printf("DEBUG: Txn %d: Recursively deleting original key %v from merged child %d.", txnID, originalKeyToDelete, leftChildNode.pageID)
	_, errDel := bt.deleteRecursive(leftChildNode, leftChildPage, originalKeyToDelete, txnID)
	return errDel // This error includes final unpinning of leftChildPage
}

// --- NEW: 2PC Participant Methods ---

// Prepare handles the PREPARE phase of a 2PC transaction.
// It attempts to acquire locks for all keys involved in the operations and logs a PREPARE record.
func (bt *BTree[K, V]) Prepare(txnID uint64, operations []TransactionOperation) error {
	bt.transactionTableLockMu.Lock()
	defer bt.transactionTableLockMu.Unlock()
	// bt.keyLocksMu.Lock()
	// defer bt.keyLocksMu.Unlock()

	if _, ok := bt.transactionTable[txnID]; ok {
		return fmt.Errorf("%w: transaction %d already exists", ErrTxnAlreadyExists, txnID)
	}

	txn := &Transaction{
		ID:        txnID,
		State:     TxnStateRunning, // Initially running
		locksHeld: make(map[string]struct{}),
		Operation: operations,
	}
	bt.transactionTable[txnID] = txn

	log.Printf("INFO: Txn %d: Received PREPARE request. Attempting to acquire locks.", txnID)
	log.Println("DEBUG Operations: ", operations)
	// Acquire locks for all keys involved in the transaction on this shard
	for _, op := range operations {
		keyStr := fmt.Sprintf("%v", op.Key) // Convert K to string for map key
		log.Println("KeyStr: ", keyStr, txnID)
		if err := bt.acquireLockInternal(keyStr, txnID); err != nil {
			// If lock acquisition fails, abort the transaction on this participant.
			log.Printf("ERROR: Txn %d: Failed to acquire lock for key %v during PREPARE: %v. Aborting locally.", txnID, op.Key, err)
			bt.releaseAllLocksForTxn(txnID)    // Release any locks already held
			delete(bt.transactionTable, txnID) // Remove from table
			return fmt.Errorf("%w: failed to acquire lock for key %v", ErrPrepareFailed, op.Key)
		}
		txn.locksHeld[keyStr] = struct{}{} // Track locks held by this transaction
		log.Printf("DEBUG: Txn %d: Acquired lock for key %v.", txnID, op.Key)
	}

	err := bt.ProcessOperations(txnID, operations)
	if err != nil {
		log.Println("PROCESS OPERATION ERROR: ", err)
		return fmt.Errorf("%w: failed to process transaction %v", ErrPrepareFailed, operations)

	}

	// Log the PREPARE record
	// This record needs to contain enough info to redo/undo the operations if needed.
	// For V1, we'll log a generic PREPARE record. Full operations would be serialized into NewData.
	// For now, we assume the Coordinator will re-send operations on COMMIT/ABORT.
	_, err = bt.logManager.AppendRecord(&wal.LogRecord{
		TxnID:  txnID,
		Type:   wal.LogRecordTypePrepare,
		PageID: InvalidPageID, // Not specific to a page, but to the transaction
	}, wal.LogTypeBtree)
	if err != nil {
		bt.releaseAllLocksForTxn(txnID)
		delete(bt.transactionTable, txnID)
		return fmt.Errorf("%w: failed to log PREPARE record for txn %d: %v", ErrPrepareFailed, txnID, err)
	}
	// Flush the log to ensure the PREPARE record is durable before voting COMMIT
	if err := bt.logManager.Sync(); err != nil {
		bt.releaseAllLocksForTxn(txnID)
		delete(bt.transactionTable, txnID)
		return fmt.Errorf("%w: failed to flush log for PREPARE record for txn %d: %v", ErrPrepareFailed, txnID, err)
	}

	txn.State = TxnStatePrepared // Transition state to PREPARED
	log.Printf("INFO: Txn %d: PREPARED. All locks acquired and PREPARE record logged.", txnID)
	return nil // Vote COMMIT
}

// Commit handles the COMMIT phase of a 2PC transaction.
// It logs a COMMIT record and releases locks.
func (bt *BTree[K, V]) Commit(txnID uint64) error {
	log.Println("COMMIT DEBUG: unlock tx: ", txnID)
	bt.keyLocksMu.Lock()
	defer bt.keyLocksMu.Unlock()
	log.Println("COMMIT DEBUG: lock tx: ", txnID)
	txn, ok := bt.transactionTable[txnID]
	if !ok {
		return fmt.Errorf("%w: transaction %d not found in table", ErrTxnNotFound, txnID)
	}
	if txn.State != TxnStatePrepared && txn.State != TxnStateRunning { // Can commit from Running if coordinator crashed before prepare log
		log.Printf("WARNING: Txn %d: Attempted to COMMIT from state %v. Expected PREPARED or RUNNING. Proceeding.", txnID, txn.State)
	}

	log.Printf("INFO: Txn %d: Received COMMIT request. Logging COMMIT record.", txnID)

	// Log the COMMIT record
	_, err := bt.logManager.AppendRecord(&wal.LogRecord{
		TxnID:  txnID,
		Type:   wal.LogRecordTypeCommitTxn,
		PageID: InvalidPageID,
	}, wal.LogTypeBtree)
	if err != nil {
		return fmt.Errorf("failed to log COMMIT record for txn %d: %w", txnID, err)
	}
	// Flush the log to ensure COMMIT record is durable
	if err := bt.logManager.Sync(); err != nil {
		return fmt.Errorf("failed to flush log for COMMIT record for txn %d: %w", txnID, err)
	}

	txn.State = TxnStateCommitted      // Transition state to COMMITTED
	bt.releaseAllLocksForTxn(txnID)    // Release all locks held by this transaction
	delete(bt.transactionTable, txnID) // Remove from in-memory table

	log.Printf("INFO: Txn %d: COMMITTED. Locks released.", txnID)
	return nil
}

// Abort handles the ABORT phase of a 2PC transaction.
// It logs an ABORT record, releases locks, and rolls back changes (V1: conceptual).
func (bt *BTree[K, V]) Abort(txnID uint64) error {
	bt.keyLocksMu.Lock()
	defer bt.keyLocksMu.Unlock()

	txn, ok := bt.transactionTable[txnID]
	if !ok {
		return fmt.Errorf("%w: transaction %d not found in table", ErrTxnNotFound, txnID)
	}
	if txn.State != TxnStatePrepared && txn.State != TxnStateRunning {
		log.Printf("WARNING: Txn %d: Attempted to ABORT from state %v. Expected PREPARED or RUNNING. Proceeding.", txnID, txn.State)
	}

	log.Printf("INFO: Txn %d: Received ABORT request. Logging ABORT record.", txnID)

	// Log the ABORT record
	_, err := bt.logManager.AppendRecord(&wal.LogRecord{
		TxnID:  txnID,
		Type:   wal.LogRecordTypeAbortTxn,
		PageID: InvalidPageID,
	}, wal.LogTypeBtree)
	if err != nil {
		return fmt.Errorf("failed to log ABORT record for txn %d: %w", txnID, err)
	}
	// Flush the log to ensure ABORT record is durable
	if err := bt.logManager.Sync(); err != nil {
		return fmt.Errorf("failed to flush log for ABORT record for txn %d: %w", txnID, err)
	}

	// TODO: Rollback changes made by this transaction.
	// This would involve reading the WAL backwards from the PREPARE record
	// and applying OldData to reverse changes. For V1, this is conceptual.
	log.Printf("WARNING: Txn %d: Rollback logic is conceptual for V1. Changes are not actively undone.", txnID)

	txn.State = TxnStateAborted        // Transition state to ABORTED
	bt.releaseAllLocksForTxn(txnID)    // Release all locks held by this transaction
	delete(bt.transactionTable, txnID) // Remove from in-memory table

	log.Printf("INFO: Txn %d: ABORTED. Locks released.", txnID)
	return nil
}

// ProcessOperations processes a list of operations within a transaction context.
// This is called by Prepare.
func (bt *BTree[K, V]) ProcessOperations(txnID uint64, operations []TransactionOperation) error {
	log.Printf("DEBUG: Txn %d: Processing %d operations.", txnID, len(operations))
	for i, op := range operations {
		log.Printf("DEBUG: Txn %d: Processing operation %d: %s Key: %s", txnID, i, op.Command, op.Key)
		var err error
		switch op.Command {
		case "PUT":
			var k K
			var v V
			// Need to convert string key/value from TransactionOperation back to K/V types
			// This requires a reverse lookup or a generic conversion function.
			// For simplicity, assuming K is string and V is string for now.
			k = (any)(op.Key).(K)
			v = (any)(op.Value).(V)
			err = bt.Insert(k, v, txnID) // Call transactional insert
		case "DELETE":
			var k K
			k = (any)(op.Key).(K)
			err = bt.Delete(k, txnID) // Call transactional delete
		default:
			err = fmt.Errorf("unsupported operation command: %s", op.Command)
		}
		if err != nil {
			return fmt.Errorf("txn %d: failed to process operation %d (%s %s): %w", txnID, i, op.Command, op.Key, err)
		}
	}
	log.Printf("DEBUG: Txn %d: All operations processed successfully.", txnID)
	return nil
}

// acquireLock attempts to acquire a lock for a key for a given transaction.
// It blocks until the lock is acquired or an error occurs.
func (bt *BTree[K, V]) acquireLock(key K, txnID uint64) error {
	keyStr := fmt.Sprintf("%v", key) // Convert K to string for map key
	return bt.acquireLockInternal(keyStr, txnID)
}

// acquireLockInternal is the internal, string-key based lock acquisition.
func (bt *BTree[K, V]) acquireLockInternal(keyStr string, txnID uint64) error {
	// bt.keyLocksMu.Lock()
	// defer bt.keyLocksMu.Unlock()

	for {
		bt.keyLocksMu.Lock()
		log.Printf("DEBUG: Txn %d: Key '%s' is locked by Txn %d. Waiting...", txnID, keyStr, bt.keyLocks[keyStr])
		if ownerTxnID, ok := bt.keyLocks[keyStr]; ok {
			if ownerTxnID == txnID {
				// Already locked by this transaction, idempotent.
				bt.keyLocksMu.Unlock()
				return nil
			}
			// Locked by another transaction, wait.
			log.Printf("DEBUG: Txn %d: Key '%s' is locked by Txn %d. Waiting...", txnID, keyStr, ownerTxnID)
			// For V1, simple blocking wait. In production, use condition variables or timeout.
			// This is a busy-wait loop, which is inefficient.
			// A production system would use a condition variable or a lock manager with queues.
			// For now, we'll just return ErrKeyLocked to avoid indefinite blocking in a simple test.
			// If this is called from Prepare, the Prepare will fail.
			bt.keyLocksMu.Unlock()
			return ErrKeyLocked
		} else {
			// Lock available, acquire it.
			bt.keyLocks[keyStr] = txnID
			if txn, ok := bt.transactionTable[txnID]; ok {
				txn.locksHeld[keyStr] = struct{}{}
			}
			log.Printf("DEBUG: Txn %d: Acquired lock for key '%s'.", txnID, keyStr)
			bt.keyLocksMu.Unlock()
			return nil
		}
	}
}

// releaseLock releases the lock for a specific key held by a specific transaction.
func (bt *BTree[K, V]) releaseLock(key K, txnID uint64) {
	keyStr := fmt.Sprintf("%v", key) // Convert K to string for map key
	bt.releaseLockInternal(keyStr, txnID)
}

// releaseLockInternal is the internal, string-key based lock release.
func (bt *BTree[K, V]) releaseLockInternal(keyStr string, txnID uint64) {
	bt.keyLocksMu.Lock()
	defer bt.keyLocksMu.Unlock()

	if ownerTxnID, ok := bt.keyLocks[keyStr]; ok {
		if ownerTxnID == txnID {
			delete(bt.keyLocks, keyStr)
			if txn, ok := bt.transactionTable[txnID]; ok {
				delete(txn.locksHeld, keyStr)
			}
			log.Printf("DEBUG: Txn %d: Released lock for key '%s'.", txnID, keyStr)
		} else {
			log.Printf("WARNING: Txn %d: Attempted to release lock for key '%s' owned by Txn %d.", txnID, keyStr, ownerTxnID)
		}
	} else {
		log.Printf("WARNING: Txn %d: Attempted to release unheld lock for key '%s'.", txnID, keyStr)
	}
}

// releaseAllLocksForTxn releases all locks held by a given transaction.
func (bt *BTree[K, V]) releaseAllLocksForTxn(txnID uint64) {
	log.Println("DEBUG: Release all locks for txn unlock")
	bt.transactionTableLockMu.Lock()
	defer bt.transactionTableLockMu.Unlock()
	log.Println("DEBUG: Release all locks for txn lock")

	txn, ok := bt.transactionTable[txnID]
	if !ok {
		log.Printf("WARNING: Attempted to release locks for non-existent Txn %d.", txnID)
		return
	}

	for keyStr := range txn.locksHeld {
		if ownerTxnID, held := bt.keyLocks[keyStr]; held && ownerTxnID == txnID {
			delete(bt.keyLocks, keyStr)
			log.Printf("DEBUG: Txn %d: Released lock for key '%s' during bulk release.", txnID, keyStr)
		} else {
			log.Printf("WARNING: Txn %d: Expected to hold lock for key '%s' but it was not held or owned by another txn %d.", txnID, keyStr, ownerTxnID)
		}
	}
	txn.locksHeld = make(map[string]struct{}) // Clear the transaction's record of held locks
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

// SerializeString serializes a string to a byte slice.
func SerializeString(s string) ([]byte, error) {
	return []byte(s), nil
}

// DeserializeString deserializes a byte slice to a string.
func DeserializeString(data []byte) (string, error) {
	return string(data), nil
}

// --- NEW: BTree Iterator Interface and Implementation ---

// BTreeIterator defines the interface for iterating over key-value pairs in a B-tree.
type BTreeIterator[K any, V any] interface {
	Next() (K, V, bool, error) // Returns next key, value, true if valid, or error
	Close() error              // Releases any resources held by the iterator
}

// pathEntry represents a node and the index of the child taken to reach the next level.
type pathEntry[K any, V any] struct {
	node *Node[K, V]
	page *pagemanager.Page // Pinned and latched page for this node
	idx  int               // Index of the child that was followed to get to the next level
}

// bTreeIterator is the concrete implementation of BTreeIterator.
type bTreeIterator[K, V any] struct {
	tree              *BTree[K, V]
	currentNode       *Node[K, V]       // The current leaf node being iterated
	currentPage       *pagemanager.Page // The page object for currentNode (pinned and latched)
	currentKeyIdx     int               // Index of the current key in currentNode.keys
	startKey          K
	endKey            K
	isExhausted       bool
	initialSearchDone bool              // Flag to indicate if initial search to startKey is complete
	pathStack         []pathEntry[K, V] // Stack to keep track of the path from root to current leaf
	isFullScan        bool              // NEW: Flag to indicate if this is a full scan (unbounded)
}

// Iterator returns a new BTreeIterator for the specified key range [startKey, endKey).
// It acquires an S-latch on the root page and pins it.
func (bt *BTree[K, V]) Iterator(startKey K, endKey K) (BTreeIterator[K, V], error) {
	// var zeroK K
	if bt.rootPageID == InvalidPageID {
		return nil, errors.New("cannot create iterator on an empty B-tree")
	}

	iter := &bTreeIterator[K, V]{
		tree:       bt,
		startKey:   startKey,
		endKey:     endKey,
		pathStack:  make([]pathEntry[K, V], 0), // Initialize empty stack
		isFullScan: false,                      // Not a full scan
	}

	// Find the first leaf page containing or preceding startKey and populate the path stack
	node, page, err := bt.findLeafForIterator(bt.rootPageID, startKey, iter.pathStack)
	if err != nil {
		iter.Close() // Ensure any pinned pages from failed search are unpinned
		return nil, fmt.Errorf("failed to find starting leaf for iterator: %w", err)
	}
	log.Println("Iterator Node: ", node.keys, node.values)
	iter.currentNode = node
	iter.currentPage = page

	// Find the starting key index within the leaf node
	idx, _ := slices.BinarySearchFunc(node.keys, startKey, bt.keyOrder)
	iter.currentKeyIdx = idx

	// If startKey is greater than all keys in the leaf, or if the initial key is outside the range
	// (e.g., startKey is greater than the largest key in the tree), move to the next leaf.
	// This ensures we start at the first valid key in the range.
	if iter.currentKeyIdx == len(node.keys) || (len(node.keys) > 0 && bt.keyOrder(node.keys[iter.currentKeyIdx], startKey) < 0) {
		// If the binary search result `idx` is `len(node.keys)`, it means `startKey` is greater than all keys in this leaf.
		// Or, if `startKey` is smaller than the key at `idx` but we need to ensure we start from `startKey` or greater.
		// The `slices.BinarySearchFunc` returns the index to insert `startKey` to maintain sorted order.
		// If `startKey` is greater than all keys, `idx` will be `len(node.keys)`.
		// If `startKey` is exactly equal to `node.keys[idx]`, then `idx` is correct.
		// If `startKey` is between `node.keys[idx-1]` and `node.keys[idx]`, then `idx` is correct.
		// We need to ensure `currentKeyIdx` points to the first key >= `startKey`.
		// The `slices.BinarySearchFunc` does this.
		// If `idx` is `len(node.keys)`, it means `startKey` is larger than all keys in this leaf. We need to go to the next leaf.
		if err := iter.moveToNextLeaf(); err != nil {
			iter.Close() // Clean up if moving to next leaf fails
			return nil, fmt.Errorf("failed to move to next leaf after initial search: %w", err)
		}
	}

	iter.initialSearchDone = true
	return iter, nil
}

// FullScan returns a new BTreeIterator configured to scan the entire B-tree.
func (bt *BTree[K, V]) FullScan() (BTreeIterator[K, V], error) {
	if bt.rootPageID == InvalidPageID {
		return nil, errors.New("cannot create full scan iterator on an empty B-tree")
	}

	iter := &bTreeIterator[K, V]{
		tree:       bt,
		pathStack:  make([]pathEntry[K, V], 0),
		isFullScan: true, // Mark as full scan
	}

	// Find the absolute leftmost leaf to start the full scan
	node, page, err := bt.findLeftmostLeaf(bt.rootPageID, iter.pathStack)
	if err != nil {
		iter.Close() // Ensure any pinned pages from failed search are unpinned
		return nil, fmt.Errorf("failed to find leftmost leaf for full scan: %w", err)
	}

	iter.currentNode = node
	iter.currentPage = page
	iter.currentKeyIdx = 0        // Start from the very first key in the leftmost leaf
	iter.initialSearchDone = true // Initial search is done, we are at the beginning

	return iter, nil
}

// findLeafForIterator recursively finds the leaf node where the iteration should start.
// It uses read crabbing (S-latches) and populates the pathStack.
func (bt *BTree[K, V]) findLeafForIterator(pageID pagemanager.PageID, key K, pathStack []pathEntry[K, V]) (*Node[K, V], *pagemanager.Page, error) {
	currNode, currPage, err := bt.fetchNode(pageID) // Fetches and S-latches currPage
	if err != nil {
		return nil, nil, err
	}

	// Add current node and its page to the path stack
	// The index `idx` for this level will be determined in the recursive call.
	// For now, push with a placeholder index. It will be updated by the caller.
	pathStack = append(pathStack, pathEntry[K, V]{node: currNode, page: currPage, idx: -1})

	if currNode.isLeaf {
		return currNode, currPage, nil // Found the leaf
	}

	// Find the child to descend into
	idx, _ := slices.BinarySearchFunc(currNode.keys, key, bt.keyOrder)
	childPageID := currNode.childPageIDs[idx]

	// Update the index in the last path entry for the current node
	pathStack[len(pathStack)-1].idx = idx

	// Read crabbing: Acquire S-latch on child before releasing S-latch on parent
	// The fetchNode for child will acquire S-latch on child.
	// We then release the S-latch on currPage.
	unpinErr := bt.bpm.UnpinPage(currPage.GetPageID(), false) // Unpin parent page
	if unpinErr != nil {
		log.Printf("WARNING: Error unpinning page %d during iterator findLeaf: %v", currPage.GetPageID(), unpinErr)
		// Proceed, but log the warning.
	}

	// Pass the updated pathStack to the recursive call
	return bt.findLeafForIterator(childPageID, key, pathStack) // Recursive call
}

// Next returns the next key-value pair in the iteration.
// It manages latch coupling as it moves between pages.
func (iter *bTreeIterator[K, V]) Next() (K, V, bool, error) {
	var zeroK K
	var zeroV V

	if iter.isExhausted {
		return zeroK, zeroV, false, ErrIteratorInvalid
	}

	for {
		// Check if current node is valid and has more keys to process
		if iter.currentNode == nil {
			// This can happen if moveToNextLeaf failed or was called when no more leaves exist
			iter.isExhausted = true
			iter.Close()
			return zeroK, zeroV, false, nil
		}

		// Process keys in the current leaf node
		for iter.currentKeyIdx < len(iter.currentNode.keys) {
			key := iter.currentNode.keys[iter.currentKeyIdx]
			value := iter.currentNode.values[iter.currentKeyIdx]

			// If it's a full scan, just return the key/value
			if iter.isFullScan {
				iter.currentKeyIdx++ // Advance for next call
				return key, value, true, nil
			}

			// For bounded scans, check if the current key is within the desired endKey range
			// The condition is `key >= startKey` and `key < endKey`.
			// `iter.tree.keyOrder(key, iter.endKey) < 0` means `key < endKey`.
			// `iter.tree.keyOrder(key, iter.startKey) >= 0` means `key >= startKey`.
			if iter.tree.keyOrder(key, iter.endKey) < 0 { // key < endKey
				// If initial search is done, we only need to check against endKey.
				// Otherwise, we need to ensure key is >= startKey.
				if iter.initialSearchDone && iter.tree.keyOrder(key, iter.startKey) >= 0 {
					iter.currentKeyIdx++ // Advance for next call
					return key, value, true, nil
				} else if !iter.initialSearchDone { // This branch should ideally not be hit if initial search is done correctly
					// This means we are still in the process of finding the first valid key.
					// If key is within range, advance and return.
					if iter.tree.keyOrder(key, iter.startKey) >= 0 {
						iter.currentKeyIdx++
						return key, value, true, nil
					}
				}
			} else {
				// Reached or exceeded endKey. Exhaust the iterator.
				iter.isExhausted = true
				iter.Close()
				return zeroK, zeroV, false, nil
			}
			iter.currentKeyIdx++ // Move to the next key in the current node if the current one was skipped (e.g., < startKey)
		}

		// Current node exhausted, move to the next leaf node
		if err := iter.moveToNextLeaf(); err != nil {
			iter.isExhausted = true
			iter.Close()
			return zeroK, zeroV, false, fmt.Errorf("failed to move to next leaf: %w", err)
		}

		// After moving to the next leaf, reset currentKeyIdx to 0 for the new leaf
		iter.currentKeyIdx = 0
	}
}

// moveToNextLeaf moves the iterator to the next leaf node in the B-tree.
// It uses the path stack to traverse up and then down to the next leaf.
// It handles releasing the latch on the current page and acquiring on the next.
func (iter *bTreeIterator[K, V]) moveToNextLeaf() error {
	// Unpin the current leaf page
	if iter.currentPage != nil {
		if err := iter.tree.bpm.UnpinPage(iter.currentPage.GetPageID(), false); err != nil {
			log.Printf("WARNING: Error unpinning current page %d during moveToNextLeaf: %v", iter.currentPage.GetPageID(), err)
			// Continue, but log the error
		}
		iter.currentPage = nil
		iter.currentNode = nil
	}

	// Traverse up the path stack to find the next available child
	for len(iter.pathStack) > 0 {
		// Pop the current entry from the stack
		lastEntryIdx := len(iter.pathStack) - 1
		currentParentEntry := iter.pathStack[lastEntryIdx]
		iter.pathStack = iter.pathStack[:lastEntryIdx] // Pop

		// Get the parent node and its page (which should still be pinned from findLeafForIterator or previous moveToNextLeaf)
		parentNode := currentParentEntry.node
		parentPage := currentParentEntry.page
		childIdx := currentParentEntry.idx

		// Check if there's a next sibling child
		if childIdx+1 < len(parentNode.childPageIDs) {
			// Found a next sibling. Descend into it.
			nextChildPageID := parentNode.childPageIDs[childIdx+1]

			// Unpin the parent page (read crabbing) before fetching the next child
			if err := iter.tree.bpm.UnpinPage(parentPage.GetPageID(), false); err != nil {
				log.Printf("WARNING: Error unpinning parent page %d before descending to next child: %v", parentPage.GetPageID(), err)
				// Continue, but log
			}

			// Find the leftmost leaf in the subtree rooted at nextChildPageID
			newNode, newPage, err := iter.tree.findLeftmostLeaf(nextChildPageID, iter.pathStack)
			if err != nil {
				return fmt.Errorf("failed to find leftmost leaf from child %d: %w", nextChildPageID, err)
			}
			iter.currentNode = newNode
			iter.currentPage = newPage
			iter.currentKeyIdx = 0 // Start from the beginning of the new leaf
			return nil             // Successfully moved to next leaf
		} else {
			// No more children at this level for the current parent.
			// Unpin the parent page as we are done with it.
			if err := iter.tree.bpm.UnpinPage(parentPage.GetPageID(), false); err != nil {
				log.Printf("WARNING: Error unpinning parent page %d after exhausting children: %v", parentPage.GetPageID(), err)
			}
			// Continue loop to go up to the next parent
		}
	}

	// Stack is empty, no more leaves to visit.
	iter.isExhausted = true
	return nil
}

// findLeftmostLeaf recursively finds the leftmost leaf node in the subtree rooted at pageID.
// It populates the pathStack as it descends.
func (bt *BTree[K, V]) findLeftmostLeaf(pageID pagemanager.PageID, pathStack []pathEntry[K, V]) (*Node[K, V], *pagemanager.Page, error) {
	currNode, currPage, err := bt.fetchNode(pageID) // Fetches and S-latches currPage
	if err != nil {
		return nil, nil, err
	}

	// Add current node and its page to the path stack. Index is always 0 for leftmost descent.
	pathStack = append(pathStack, pathEntry[K, V]{node: currNode, page: currPage, idx: 0})

	if currNode.isLeaf {
		return currNode, currPage, nil // Found the leftmost leaf
	}

	childPageID := currNode.childPageIDs[0] // Always take the leftmost child

	// Read crabbing: Acquire S-latch on child before releasing S-latch on parent
	if err := bt.bpm.UnpinPage(currPage.GetPageID(), false); err != nil {
		log.Printf("WARNING: Error unpinning page %d during findLeftmostLeaf: %v", currPage.GetPageID(), err)
	}

	// Recursive call
	return bt.findLeftmostLeaf(childPageID, pathStack)
}

// Close releases any resources held by the iterator, specifically unpinning all pages in the path stack and the current page.
func (iter *bTreeIterator[K, V]) Close() error {
	var firstErr error

	// Unpin the current page if it's still pinned
	if iter.currentPage != nil {
		if err := iter.tree.bpm.UnpinPage(iter.currentPage.GetPageID(), false); err != nil {
			log.Printf("WARNING: Error closing iterator, failed to unpin current page %d: %v", iter.currentPage.GetPageID(), err)
			if firstErr == nil {
				firstErr = err
			}
		}
		iter.currentPage = nil
		iter.currentNode = nil
	}

	// Unpin all pages in the path stack
	for i := len(iter.pathStack) - 1; i >= 0; i-- {
		entry := iter.pathStack[i]
		if entry.page != nil {
			if err := iter.tree.bpm.UnpinPage(entry.page.GetPageID(), false); err != nil {
				log.Printf("WARNING: Error closing iterator, failed to unpin page %d from stack: %v", entry.page.GetPageID(), err)
				if firstErr == nil {
					firstErr = err
				}
			}
			iter.pathStack[i].page = nil // Clear reference
			iter.pathStack[i].node = nil // Clear reference
		}
	}
	iter.pathStack = nil // Clear the stack

	iter.isExhausted = true
	log.Println("DEBUG: BTreeIterator closed.")
	return firstErr
}
