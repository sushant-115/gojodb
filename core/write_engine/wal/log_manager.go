package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time" // Added for time.Ticker

	"github.com/sushant-115/gojodb/core/transaction"
	flushmanager "github.com/sushant-115/gojodb/core/write_engine/flush_manager"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
)

// --- Write-Ahead Logging (WAL) Constants and Types ---

type LSN pagemanager.LSN // Log Sequence Number
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
	LogRecordTypeCheckpointStart
	LogRecordTypeCheckpointEnd
	// --- NEW: 2PC Specific Log Record Types ---
	LogRecordTypePrepare   // Transaction Prepare record
	LogRecordTypeCommitTxn // Transaction Commit record (final phase 2)
	LogRecordTypeAbortTxn  // Transaction Abort record (final phase 2)
	// --- END NEW ---
	LogRecordTypeRootChange // NEW: Log record for B-tree root page ID change
)

// LogRecord represents a single entry in the Write-Ahead Log.
type LogRecord struct {
	LSN     LSN
	PrevLSN LSN    // LSN of the previous log record by the same transaction (for undo)
	TxnID   uint64 // Transaction ID (0 if not part of a transaction or single op)
	Type    LogRecordType
	PageID  pagemanager.PageID // Page affected (if applicable)
	Offset  uint16             // Offset within the page (if applicable for UPDATE)
	OldData []byte             // For UNDO (and REDO if needed for physiological logging)
	NewData []byte             // For REDO
	// Other type-specific fields for specific log record types would go here.
}

// LogManager manages the Write-Ahead Log file(s).
// It is responsible for appending log records, managing log segments,
// ensuring durability, and providing a basic archiving mechanism.
type LogManager struct {
	logDir                   string         // Directory where active log segments reside
	archiveDir               string         // Directory for archived log segments
	logFile                  *os.File       // Current active log segment file handle
	currentSegmentID         uint64         // ID of the current active log segment
	currentLSN               LSN            // The next LSN to be assigned (global, monotonically increasing)
	currentSegmentFileOffset int64          // Current byte offset within the active log segment file
	buffer                   *bytes.Buffer  // In-memory buffer for log records before flushing
	mu                       sync.Mutex     // Protects access to LogManager state (currentLSN, buffer, logFile, segmentID)
	flushCond                *sync.Cond     // For signaling when buffer needs flushing (e.g., buffer is full)
	bufferSize               int            // Maximum size of the in-memory buffer
	segmentSizeLimit         int64          // Maximum size of a single log segment file before rotation
	stopChan                 chan struct{}  // Channel to signal stopping the flusher goroutine
	wg                       sync.WaitGroup // WaitGroup for flusher goroutine

	// --- NEW: Recovery State (for Analysis Pass) ---
	// This would typically be part of a dedicated RecoveryManager,
	// but for V1, we'll keep it here for simplicity.
	// Maps to track transaction states during recovery
	recoveryTxnStates map[uint64]transaction.TransactionState // TxnID -> state (e.g., PREPARED, COMMITTED, ABORTED)
	// --- END NEW ---
	// --- NEW: For Log Streaming (Replication) ---
	// Channel to signal new log records are available (for streaming readers)
	newLogReady chan struct{}
	// --- END NEW ---
}

// NewLogManager creates and initializes a new LogManager.
// It sets up log and archive directories, finds the latest log segment,
// and starts a background flusher goroutine.
func NewLogManager(logDir string, archiveDir string, bufferSize int, segmentSizeLimit int64) (*LogManager, error) {
	if bufferSize <= 0 {
		return nil, fmt.Errorf("log buffer size must be positive")
	}
	if segmentSizeLimit <= 0 {
		return nil, fmt.Errorf("log segment size limit must be positive")
	}
	if segmentSizeLimit < int64(bufferSize) {
		return nil, fmt.Errorf("log segment size limit (%d) must be greater than or equal to buffer size (%d)", segmentSizeLimit, bufferSize)
	}

	// Ensure log and archive directories exist
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory %s: %w", logDir, err)
	}
	if err := os.MkdirAll(archiveDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create archive directory %s: %w", archiveDir, err)
	}

	lm := &LogManager{
		logDir:           logDir,
		archiveDir:       archiveDir,
		currentSegmentID: 0,                                            // Will be determined by findOrCreateLatestLogSegment
		currentLSN:       0,                                            // Will be determined by findOrCreateLatestLogSegment
		buffer:           bytes.NewBuffer(make([]byte, 0, bufferSize)), // Pre-allocate buffer capacity
		bufferSize:       bufferSize,
		segmentSizeLimit: segmentSizeLimit,
		stopChan:         make(chan struct{}),
		// --- NEW: Initialize Recovery State ---
		recoveryTxnStates: make(map[uint64]transaction.TransactionState),
		// --- END NEW ---
		// --- NEW: Initialize newLogReady channel ---
		newLogReady: make(chan struct{}, 1), // Buffered channel to avoid blocking appends
		// --- END NEW ---
	}
	lm.flushCond = sync.NewCond(&lm.mu)

	// Find or create the latest log segment and set initial LSN
	if err := lm.findOrCreateLatestLogSegment(); err != nil {
		return nil, fmt.Errorf("failed to initialize log segment: %w", err)
	}

	// Start a background goroutine to periodically flush the buffer
	lm.wg.Add(1)
	go lm.flusher()

	log.Printf("INFO: LogManager initialized. Log directory: %s, Archive directory: %s, Current Segment ID: %d, Initial Global LSN: %d",
		logDir, archiveDir, lm.currentSegmentID, lm.currentLSN)
	return lm, nil
}

// GetCurrentLSN returns the current global LSN of the log manager.
func (lm *LogManager) GetCurrentLSN() LSN {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.currentLSN
}

// findOrCreateLatestLogSegment scans the log directory to find the latest segment,
// or creates the first one if none exist. It sets lm.logFile, lm.currentSegmentID, and lm.currentLSN.
// This method MUST be called with lm.mu locked.
func (lm *LogManager) findOrCreateLatestLogSegment() error {
	var segmentFiles []struct {
		path string
		id   uint64
		size int64
	}

	// 1. Collect all log segments (archived and active)
	dirs := []string{lm.logDir, lm.archiveDir}
	for _, dir := range dirs {
		files, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("failed to read directory %s: %w", dir, err)
		}
		for _, file := range files {
			if file.IsDir() {
				continue
			}
			if strings.HasPrefix(file.Name(), "log_") && strings.HasSuffix(file.Name(), ".log") {
				parts := strings.Split(strings.TrimSuffix(file.Name(), ".log"), "_")
				if len(parts) == 2 {
					id, parseErr := strconv.ParseUint(parts[1], 10, 64)
					if parseErr == nil {
						info, _ := file.Info() // Get file info to read size
						segmentFiles = append(segmentFiles, struct {
							path string
							id   uint64
							size int64
						}{filepath.Join(dir, file.Name()), id, info.Size()})
					}
				}
			}
		}
	}

	// Sort segments by their ID
	sort.Slice(segmentFiles, func(i, j int) bool {
		return segmentFiles[i].id < segmentFiles[j].id
	})

	var currentGlobalLSN LSN = 0
	var latestActiveSegmentID uint64 = 0
	var latestActiveSegmentSize int64 = 0

	// 2. Calculate global LSN by summing sizes of all segments.
	// Identify the latest segment that's still in the active log directory.
	for _, seg := range segmentFiles {
		// Sum up all segment sizes to determine the true current LSN
		currentGlobalLSN += LSN(seg.size)
		if filepath.Dir(seg.path) == lm.logDir {
			latestActiveSegmentID = seg.id
			latestActiveSegmentSize = seg.size
		}
	}

	if latestActiveSegmentID == 0 {
		// No existing log segments in logDir, start with segment 1
		lm.currentSegmentID = 1
		lm.currentLSN = 0 // Global LSN starts at 0 for the very first log record
		lm.currentSegmentFileOffset = 0
	} else {
		lm.currentSegmentID = latestActiveSegmentID
		lm.currentLSN = currentGlobalLSN                      // Set current LSN to the end of all existing segments
		lm.currentSegmentFileOffset = latestActiveSegmentSize // Set initial offset in current segment
	}

	latestLogFilePath := lm.getLogSegmentPath(lm.currentSegmentID)
	logFile, err := os.OpenFile(latestLogFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to open/create log segment %s: %w", latestLogFilePath, err)
	}
	lm.logFile = logFile

	log.Printf("DEBUG: LogManager: Initialized currentSegmentID: %d, Initial Global LSN: %d, Initial Segment File Offset: %d",
		lm.currentSegmentID, lm.currentLSN, lm.currentSegmentFileOffset)

	return nil
}

// getLogSegmentPath returns the full path for a log segment file.
func (lm *LogManager) getLogSegmentPath(segmentID uint64) string {
	return filepath.Join(lm.logDir, fmt.Sprintf("log_%05d.log", segmentID))
}

// Append adds a LogRecord to the in-memory buffer and assigns it an LSN.
// It returns the assigned LSN. The record is not guaranteed to be on disk immediately.
func (lm *LogManager) Append(record *LogRecord) (LSN, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Assign LSN and update current LSN
	record.LSN = lm.currentLSN

	// Serialize the log record
	serializedRecord, err := record.Serialize()
	if err != nil {
		return InvalidLSN, fmt.Errorf("failed to serialize log record: %w", err)
	}

	recordSize := int64(len(serializedRecord))

	// Check if record fits in buffer, if not, flush first
	if lm.buffer.Len()+int(recordSize) > lm.bufferSize {
		log.Printf("DEBUG: Log buffer full (%d bytes). Flushing before appending Global LSN %d.", lm.buffer.Len(), record.LSN)
		if err := lm.flushInternal(); err != nil {
			return InvalidLSN, fmt.Errorf("failed to flush log buffer before append: %w", err)
		}
	}

	// Check if appending this record would exceed the segment size limit
	// This check should happen *after* flushInternal to ensure the buffer is as empty as possible.
	// lm.currentSegmentFileOffset reflects bytes written to the *file* plus bytes in the *buffer*.
	if lm.currentSegmentFileOffset+recordSize > lm.segmentSizeLimit {
		log.Printf("INFO: Log segment %d reaching limit (%d bytes). Rolling to new segment.", lm.currentSegmentID, lm.segmentSizeLimit)
		if err := lm.rollLogSegment(); err != nil {
			return InvalidLSN, fmt.Errorf("failed to roll log segment before append: %w", err)
		}
		// After rolling, currentSegmentFileOffset is reset to 0 for the new segment.
	}

	// Append to buffer
	if _, err := lm.buffer.Write(serializedRecord); err != nil {
		return InvalidLSN, fmt.Errorf("failed to write record to log buffer: %w", err)
	}

	// Update global LSN and current segment file offset after successful append to buffer
	lm.currentLSN += LSN(recordSize)
	lm.currentSegmentFileOffset += recordSize

	// Signal the flusher goroutine if the buffer is now full or close to full
	if lm.buffer.Len() >= lm.bufferSize/2 { // Signal at half full to trigger proactive flushing
		lm.flushCond.Signal()
	}
	if err := lm.flushInternal(); err != nil {
		return InvalidLSN, fmt.Errorf("failed to flush log buffer before append: %w", err)
	}

	// Signal log stream readers that new log is ready
	select {
	case lm.newLogReady <- struct{}{}:
		// Signal sent
	default:
		// Channel is full, reader is not ready, skip signal
	}

	log.Printf("DEBUG: Appended log record Global LSN %d (Type: %v, PageID: %d, Size: %d) to segment %d, new segment offset %d",
		record.LSN, record.Type, record.PageID, recordSize, lm.currentSegmentID, lm.currentSegmentFileOffset)
	return record.LSN, nil
}

// Recover performs the recovery process (Redo Pass + basic Analysis/Undo) on database startup.
// It scans log records from archived and active log segments and reapplies
// committed changes to data pages, and resolves prepared transactions.
// dm: The DiskManager to interact with data pages.
// bpm: The BufferPoolManager to fetch/flush pages (not used for recovery reads/writes directly, but for context).
// lastLSN: The last LSN recorded in the DBFileHeader, indicating the state of the data file.
func (lm *LogManager) Recover(dm *flushmanager.DiskManager, lastLSN LSN) error {
	log.Println("INFO: Starting LogManager recovery process (Redo Pass + basic Analysis/Undo)...")

	// --- Analysis Pass (V1: Identify committed/aborted transactions) ---
	// Clear previous recovery state
	lm.recoveryTxnStates = make(map[uint64]transaction.TransactionState)

	// Collect all log segments (archived and active) in order.
	lm.mu.Lock()
	segmentInfos, err := lm.getOrderedLogSegments()
	lm.mu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to get ordered log segments for recovery: %w", err)
	}

	// --- Redo Pass (V1: Reapply changes from log) ---
	for _, segInfo := range segmentInfos {
		log.Printf("INFO: Analyzing and Replaying from log segment: %s (ID: %d, Global LSN Range: %d-%d)", segInfo.path, segInfo.id, segInfo.startGlobalLSN, segInfo.endGlobalLSN)
		segmentFile, err := os.Open(segInfo.path)
		if err != nil {
			return fmt.Errorf("failed to open log segment %s for recovery: %w", segInfo.path, err)
		}
		// No defer segmentFile.Close() here, close explicitly at end of loop iteration.
		// For robustness, seek to where this segment starts if the file was opened for global LSN.
		// Since we open each segment file individually, we read from its beginning.

		reader := bufio.NewReader(segmentFile) // Use bufio.NewReader for efficient byte-by-byte reading
		currentLocalOffset := LSN(0)           // Offset within the current segment file

		for {
			var lr LogRecord
			// Read and deserialize a single log record
			err := lm.readLogRecord(reader, &lr)
			if err == io.EOF {
				break // End of segment file
			}
			if err != nil {
				log.Printf("ERROR: Failed to read log record from %s at offset %d: %v. Stopping recovery for this segment.", segInfo.path, currentLocalOffset, err)
				// In a real system, this might indicate a corrupted log or truncated record.
				// We might try to skip to the next segment or require manual intervention.
				break
			}

			// Ensure the LSN in the record matches what we expect from segment order.
			// This check is mainly for debugging if LSNs are inconsistent.
			expectedGlobalLSN := segInfo.startGlobalLSN + currentLocalOffset
			if lr.LSN != expectedGlobalLSN {
				log.Printf("WARNING: LSN mismatch during recovery! Record LSN: %d, Expected Global LSN: %d. Proceeding but investigate.", lr.LSN, expectedGlobalLSN)
			}

			// --- Analysis Pass Logic ---
			// Update transaction states based on log records
			switch lr.Type {
			case LogRecordTypePrepare:
				lm.recoveryTxnStates[lr.TxnID] = transaction.TxnStatePrepared
				log.Printf("DEBUG: Recovery Analysis: Txn %d is PREPARED (LSN %d)", lr.TxnID, lr.LSN)
			case LogRecordTypeCommitTxn:
				lm.recoveryTxnStates[lr.TxnID] = transaction.TxnStateCommitted
				log.Printf("DEBUG: Recovery Analysis: Txn %d is COMMITTED (LSN %d)", lr.TxnID, lr.LSN)
			case LogRecordTypeAbortTxn:
				lm.recoveryTxnStates[lr.TxnID] = transaction.TxnStateAborted
				log.Printf("DEBUG: Recovery Analysis: Txn %d is ABORTED (LSN %d)", lr.TxnID, lr.LSN)
			}
			// For data modification records, track dirty pages if needed for Redo/Undo
			if lr.Type == LogRecordTypeUpdate || lr.Type == LogRecordTypeNewPage {
				// We don't track dirty pages in LogManager itself for Redo,
				// as Redo applies all committed changes regardless.
				// Dirty page table is typically built by scanning data pages
				// and comparing their LSNs to the log.
			}

			// --- Redo Pass Logic ---
			// Reapply changes for committed transactions, or all if no transaction context.
			// This is a simplified Redo-all approach for operations that are not part of a 2PC txn.
			// For 2PC transactions, we only Redo if the transaction is known to be COMMITTED.
			applyRecord := false
			if lr.TxnID == 0 { // Non-transactional operation (auto-commit)
				applyRecord = true
			} else { // Transactional operation
				if state, ok := lm.recoveryTxnStates[lr.TxnID]; ok && state == transaction.TxnStateCommitted {
					applyRecord = true
				} else {
					log.Printf("DEBUG: Skipping Redo for Txn %d (LSN %d, Type %v): Not committed or state unknown.", lr.TxnID, lr.LSN, lr.Type)
				}
			}

			if applyRecord && lr.LSN >= lastLSN { // Only apply if LSN is newer than last checkpoint
				log.Printf("DEBUG: Replaying log record LSN %d (Type: %v, PageID: %d) to disk.", lr.LSN, lr.Type, lr.PageID)

				pageData := make([]byte, dm.GetPageSize())

				// Read page from disk (it might not exist if it's a new page log record)
				readErr := dm.ReadPage(lr.PageID, pageData)
				if readErr != nil && lr.Type != LogRecordTypeNewPage {
					log.Printf("WARNING: Failed to read page %d for recovery replay: %v. Skipping record LSN %d.", lr.PageID, readErr, lr.LSN)
					// A real system might panic or require manual intervention here.
					currentLocalOffset += LSN(lr.Size()) // Advance LSN even on error
					continue
				} else if readErr != nil && lr.Type == LogRecordTypeNewPage {
					log.Printf("DEBUG: Page %d not found on disk, but it's a new page record. Will allocate if needed.", lr.PageID)
				}

				// Apply the change based on log record type
				switch lr.Type {
				case LogRecordTypeNewPage:
					// Ensure the page exists on disk. If it was truncated, re-allocate.
					if lr.PageID.GetID() >= dm.GetNumPages() {
						log.Printf("INFO: Re-allocating page %d during recovery (was truncated or never allocated).", lr.PageID)
						emptyPage := make([]byte, dm.GetPageSize())
						if writeErr := dm.WritePage(lr.PageID, emptyPage); writeErr != nil {
							return fmt.Errorf("failed to re-allocate new page %d during recovery: %w", lr.PageID, writeErr)
						}
						// // CRITICAL FIX: Invalidate the page in BufferPoolManager after writing it to disk
						// bpm.InvalidatePage(lr.PageID)
						// if lr.PageID >= PageID(dm.numPages) {
						// 	dm.numPages = uint64(lr.PageID) + 1
						// }
					}
					if len(lr.NewData) > 0 { // Apply initial data if logged
						if writeErr := dm.WritePage(lr.PageID, lr.NewData); writeErr != nil {
							return fmt.Errorf("failed to write new page data for %d during recovery: %w", lr.PageID, writeErr)
						}
						// CRITICAL FIX: Invalidate the page in BufferPoolManager after writing it to disk
						// bpm.InvalidatePage(lr.PageID)
					}

				case LogRecordTypeUpdate:
					copy(pageData, lr.NewData) // Overwrite page data with new data
					if writeErr := dm.WritePage(lr.PageID, pageData); writeErr != nil {
						return fmt.Errorf("failed to write updated page %d during recovery: %w", lr.PageID, writeErr)
					}
					// TODO: Add cases for other LogRecordTypes (InsertKey, DeleteKey, NodeSplit, NodeMerge)
					// These would require understanding the byte format within the page.
					// For now, LogRecordTypeUpdate is a generic page overwrite.

				case LogRecordTypeRootChange: // NEW: Handle root page ID changes during recovery
					newRootPageID := pagemanager.PageID(binary.LittleEndian.Uint64(lr.NewData))
					log.Printf("INFO: Recovery: Applying root page ID change to %d (from LSN %d).", newRootPageID, lr.LSN)
					if err := dm.UpdateHeaderField(func(h *flushmanager.DBFileHeader) {
						h.RootPageID = newRootPageID
					}); err != nil {
						return fmt.Errorf("failed to update root page ID in header during recovery: %w", err)
					}
					// Also update the in-memory rootPageID of the BTree if it's directly accessible here.
					// The BTree instance is passed to Recover, so we can set its rootPageID.
					// This requires a setter on BTree or direct access.
					// Assuming `dm` has a reference to the BTree or can update its root.
					// For now, we'll rely on the BTree re-reading the header after recovery.
				}
			}
			currentLocalOffset += LSN(lr.Size()) // Advance local offset
		}
		segmentFile.Close() // Explicitly close after processing
	}

	// --- Undo Pass (V1: Rollback uncommitted transactions) ---
	log.Println("INFO: Starting LogManager recovery Undo Pass (V1: aborting prepared/unknown transactions)...")
	for txnID, state := range lm.recoveryTxnStates {
		if state == transaction.TxnStatePrepared { // Or any other state that is not Committed/Aborted
			log.Printf("WARNING: Txn %d was PREPARED but not COMMITTED/ABORTED. Forcing ABORT.", txnID)
			// In a real system, you would scan the log backwards from the end
			// to find all operations for this transaction and undo them using OldData.
			// For V1, we just acknowledge the abort and rely on future writes to fix state.
			// A simple approach is to write an ABORT_TXN log record and then rely on
			// application-level consistency checks or manual intervention.
			// This is a placeholder for actual undo logic.
		}
	}

	// 3. After replaying all necessary logs, update the DBFileHeader's LastLSN.
	// This marks the point up to which recovery has completed.
	// We use the current LSN of the LogManager as the new LastLSN.
	lm.mu.Lock() // Acquire lock as we're reading lm.currentLSN
	finalLSN := lm.currentLSN
	lm.mu.Unlock()

	if err := dm.UpdateHeaderField(func(h *flushmanager.DBFileHeader) {
		h.LastLSN = pagemanager.LSN(finalLSN)
	}); err != nil {
		return fmt.Errorf("failed to update DBFileHeader LastLSN after recovery: %w", err)
	}

	log.Println("INFO: LogManager recovery process complete.")
	return nil
}

// readLogRecord reads a single log record from the provided io.Reader.
// It handles variable-length fields by reading lengths first.
func (lm *LogManager) ReadLogRecord(reader *bufio.Reader, lr *LogRecord) error {
	return lm.readLogRecord(reader, lr)
}

// readLogRecord reads a single log record from the provided io.Reader.
// It handles variable-length fields by reading lengths first.
func (lm *LogManager) readLogRecord(reader *bufio.Reader, lr *LogRecord) error {
	// Read fixed-size fields into a buffer first
	fixedHeaderBuf := make([]byte, 8+8+8+1+8+2) // LSN, PrevLSN, TxnID, Type, PageID, Offset
	n, err := io.ReadFull(reader, fixedHeaderBuf)
	if err == io.EOF {
		return io.EOF // Propagate EOF
	}
	if err != nil {
		return fmt.Errorf("failed to read fixed log record header: %w", err)
	}
	if n != len(fixedHeaderBuf) {
		return fmt.Errorf("short read for fixed log record header: expected %d, got %d", len(fixedHeaderBuf), n)
	}

	// Deserialize fixed fields
	tempReader := bytes.NewReader(fixedHeaderBuf)
	if err := binary.Read(tempReader, binary.LittleEndian, &lr.LSN); err != nil {
		return fmt.Errorf("failed to deserialize LSN: %w", err)
	}
	if err := binary.Read(tempReader, binary.LittleEndian, &lr.PrevLSN); err != nil {
		return fmt.Errorf("failed to deserialize PrevLSN: %w", err)
	}
	if err := binary.Read(tempReader, binary.LittleEndian, &lr.TxnID); err != nil {
		return fmt.Errorf("failed to deserialize TxnID: %w", err)
	}
	if err := binary.Read(tempReader, binary.LittleEndian, &lr.Type); err != nil {
		return fmt.Errorf("failed to deserialize Type: %w", err)
	}
	if err := binary.Read(tempReader, binary.LittleEndian, &lr.PageID); err != nil {
		return fmt.Errorf("failed to deserialize PageID: %w", err)
	}
	if err := binary.Read(tempReader, binary.LittleEndian, &lr.Offset); err != nil {
		return fmt.Errorf("failed to deserialize Offset: %w", err)
	}

	// Read variable-length OldData
	var oldDataLen uint16
	if err := binary.Read(reader, binary.LittleEndian, &oldDataLen); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		} // Truncated record
		return fmt.Errorf("failed to deserialize OldData length: %w", err)
	}
	lr.OldData = make([]byte, oldDataLen)
	if _, err := io.ReadFull(reader, lr.OldData); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		} // Truncated record
		return fmt.Errorf("failed to read OldData: %w", err)
	}

	// Read variable-length NewData
	var newDataLen uint16
	if err := binary.Read(reader, binary.LittleEndian, &newDataLen); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		} // Truncated record
		return fmt.Errorf("failed to deserialize NewData length: %w", err)
	}
	lr.NewData = make([]byte, newDataLen)
	if _, err := io.ReadFull(reader, lr.NewData); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		} // Truncated record
		return fmt.Errorf("failed to read NewData: %w", err)
	}

	return nil
}

// Flush ensures all log records up to a certain LSN (or all if targetLSN is InvalidLSN) are written to disk.
// This is a blocking call that ensures durability.
func (lm *LogManager) Flush(targetLSN LSN) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Flush any buffered data immediately.
	if err := lm.flushInternal(); err != nil {
		return fmt.Errorf("failed to flush log buffer: %w", err)
	}

	// Ensure data is synced to disk.
	if lm.logFile != nil {
		if err := lm.logFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync log file: %w", err)
		}
	}

	log.Printf("DEBUG: LogManager flushed and synced all buffered data up to Global LSN %d (in segment %d).", lm.currentLSN, lm.currentSegmentID)
	return nil
}

// getOrderedLogSegments returns a sorted list of all log segments (active and archived)
// along with their global LSN ranges.
// This function should be called with lm.mu locked, or consider its own locking.
func (lm *LogManager) getOrderedLogSegments() ([]struct {
	path           string
	id             uint64
	size           int64
	startGlobalLSN LSN
	endGlobalLSN   LSN
}, error) {
	var segmentFiles []struct {
		path string
		id   uint64
		size int64
	}

	dirs := []string{lm.logDir, lm.archiveDir}
	for _, dir := range dirs {
		files, err := os.ReadDir(dir)
		if err != nil {
			return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
		}
		for _, file := range files {
			if !file.IsDir() && strings.HasPrefix(file.Name(), "log_") && strings.HasSuffix(file.Name(), ".log") {
				parts := strings.Split(strings.TrimSuffix(file.Name(), ".log"), "_")
				if len(parts) == 2 {
					id, parseErr := strconv.ParseUint(parts[1], 10, 64)
					if parseErr == nil {
						info, _ := file.Info()
						segmentFiles = append(segmentFiles, struct {
							path string
							id   uint64
							size int64
						}{filepath.Join(dir, file.Name()), id, info.Size()})
					}
				}
			}
		}
	}

	sort.Slice(segmentFiles, func(i, j int) bool {
		return segmentFiles[i].id < segmentFiles[j].id
	})

	var orderedSegments []struct {
		path           string
		id             uint64
		size           int64
		startGlobalLSN LSN
		endGlobalLSN   LSN
	}
	currentGlobalLSN := LSN(0)
	for _, seg := range segmentFiles {
		orderedSegments = append(orderedSegments, struct {
			path           string
			id             uint64
			size           int64
			startGlobalLSN LSN
			endGlobalLSN   LSN
		}{
			path:           seg.path,
			id:             seg.id,
			size:           seg.size,
			startGlobalLSN: currentGlobalLSN,
			endGlobalLSN:   currentGlobalLSN + LSN(seg.size),
		})
		currentGlobalLSN += LSN(seg.size)
	}
	return orderedSegments, nil
}

// StartLogStream provides a channel to stream log records from a given LSN.
// This is used by replication followers to catch up and stay in sync.
func (lm *LogManager) StartLogStream(fromLSN LSN) (<-chan LogRecord, error) {
	logStream := make(chan LogRecord)

	go func() {
		defer close(logStream)

		currentReaderGlobalLSN := fromLSN
		var currentSegmentFile *os.File
		var currentSegmentReader *bufio.Reader
		var currentSegmentInfo struct { // Tracks the segment currently being read
			path           string
			id             uint64
			size           int64
			startGlobalLSN LSN
			endGlobalLSN   LSN
		}
		segmentIndex := -1 // Index into the orderedSegments slice

		// Helper to open/re-open and seek the correct log segment
		openAndSeekCorrectSegment := func() error {
			if currentSegmentFile != nil {
				currentSegmentFile.Close()
				currentSegmentFile = nil
			}

			lm.mu.Lock()
			orderedSegments, err := lm.getOrderedLogSegments() // This re-scans. For production, consider caching.
			lm.mu.Unlock()
			if err != nil {
				log.Printf("ERROR: Failed to get ordered log segments for streaming: %v", err)
				return err
			}

			// Find the segment that contains currentReaderGlobalLSN
			found := false
			for i, seg := range orderedSegments {
				if currentReaderGlobalLSN >= seg.startGlobalLSN && currentReaderGlobalLSN < seg.endGlobalLSN {
					currentSegmentInfo = seg
					segmentIndex = i
					found = true
					break
				}
				// Handle case where fromLSN is exactly at the end of a segment.
				// This means we should start at the beginning of the *next* segment.
				if currentReaderGlobalLSN == seg.endGlobalLSN && i+1 < len(orderedSegments) {
					currentSegmentInfo = orderedSegments[i+1]
					segmentIndex = i + 1
					currentReaderGlobalLSN = currentSegmentInfo.startGlobalLSN // Adjust LSN to start of next segment
					found = true
					break
				}
			}

			if !found {
				// This can happen if fromLSN is past the end of all existing logs.
				// In this case, we start waiting for new logs.
				log.Printf("INFO: FromLSN %d is beyond current known log segments. Will wait for new logs.", fromLSN)
				currentSegmentInfo.id = 0 // Mark as 'no active segment'
				return nil
			}

			file, err := os.Open(currentSegmentInfo.path)
			if err != nil {
				log.Printf("ERROR: Failed to open log segment %s for streaming: %v", currentSegmentInfo.path, err)
				return err
			}
			currentSegmentFile = file

			offsetInSegment := int64(currentReaderGlobalLSN - currentSegmentInfo.startGlobalLSN)
			_, err = currentSegmentFile.Seek(offsetInSegment, io.SeekStart)
			if err != nil {
				log.Printf("ERROR: Failed to seek log file %s to offset %d (Global LSN %d): %v", currentSegmentInfo.path, offsetInSegment, currentReaderGlobalLSN, err)
				currentSegmentFile.Close()
				currentSegmentFile = nil
				return err
			}
			currentSegmentReader = bufio.NewReader(currentSegmentFile)
			log.Printf("DEBUG: Log stream opened segment %d (%s) and sought to offset %d (Global LSN %d).",
				currentSegmentInfo.id, filepath.Base(currentSegmentInfo.path), offsetInSegment, currentReaderGlobalLSN)
			return nil
		}

		// Initial open and seek
		if err := openAndSeekCorrectSegment(); err != nil {
			return // Exit goroutine on initial failure
		}

		for {
			select {
			case <-lm.stopChan:
				log.Println("INFO: Log stream goroutine stopping due to stop signal.")
				return
			default:
				// If no active segment, or already past the end of the last known segment, just wait.
				if currentSegmentInfo.id == 0 {
					log.Printf("DEBUG: Log stream waiting for new logs (no active segment or past known logs).")
					select {
					case <-lm.newLogReady:
						// Try to open and seek again, as new logs might have appeared or segments rolled.
						if err := openAndSeekCorrectSegment(); err != nil {
							return
						}
						continue // Try reading again
					case <-lm.stopChan:
						log.Println("INFO: Log stream goroutine stopping due to stop signal while waiting for new logs.")
						return
					}
				}

				var lr LogRecord
				readErr := lm.readLogRecord(currentSegmentReader, &lr)

				if readErr == io.EOF {
					log.Printf("DEBUG: Reached EOF for segment %d (Global LSN %d).", currentSegmentInfo.id, currentReaderGlobalLSN)

					lm.mu.Lock()
					orderedSegments, err := lm.getOrderedLogSegments()
					lm.mu.Unlock()
					if err != nil {
						log.Printf("ERROR: Failed to get ordered log segments during EOF handling: %v", err)
						return
					}

					// Check if there's a next segment to transition to
					if segmentIndex+1 < len(orderedSegments) {
						// Move to the next segment
						currentSegmentInfo = orderedSegments[segmentIndex+1]
						segmentIndex++
						currentReaderGlobalLSN = currentSegmentInfo.startGlobalLSN // Reset LSN to start of new segment
						log.Printf("INFO: Transitioning to next log segment %d (Global LSN from %d).", currentSegmentInfo.id, currentReaderGlobalLSN)
						if err := openAndSeekCorrectSegment(); err != nil {
							return
						}
						continue // Try reading from the new segment
					} else {
						// This was the last segment. Now wait for new appends to *this* segment or for a segment roll.
						// Check if any new data has been written since last read in this segment.
						lm.mu.Lock()
						fileInfo, statErr := currentSegmentFile.Stat() // Stat the current segment file
						lm.mu.Unlock()

						if statErr == nil && LSN(fileInfo.Size()) > (currentReaderGlobalLSN-currentSegmentInfo.startGlobalLSN) {
							// New data physically written to the *current* segment. Re-open/re-seek.
							log.Printf("DEBUG: New data detected in current segment %d. Re-opening and re-seeking.", currentSegmentInfo.id)
							if err := openAndSeekCorrectSegment(); err != nil {
								return
							}
							continue // Try reading again immediately
						} else if LSN(fileInfo.Size()) < (currentReaderGlobalLSN - currentSegmentInfo.startGlobalLSN) {
							// This indicates a potential truncation or rollback, or the LSN is somehow invalid.
							log.Printf("WARNING: Current reader LSN %d is past actual file size %d in segment %d. Re-seeking to end of file.",
								currentReaderGlobalLSN, fileInfo.Size(), currentSegmentInfo.id)
							// Reset currentReaderGlobalLSN to end of file and try to read from there.
							currentReaderGlobalLSN = currentSegmentInfo.startGlobalLSN + LSN(fileInfo.Size())
							if err := openAndSeekCorrectSegment(); err != nil {
								return
							}
							continue
						}

						// No new data in current file, wait for signal.
						log.Printf("DEBUG: Log stream reached end of last segment %d. Waiting for new appends.", currentSegmentInfo.id)
						select {
						case <-lm.newLogReady:
							// New log records are ready, attempt to re-open and re-seek.
							// It's possible a segment roll happened, so re-evaluate segments.
							if err := openAndSeekCorrectSegment(); err != nil {
								return
							}
							continue // Try reading again
						case <-lm.stopChan:
							log.Println("INFO: Log stream goroutine stopping due to stop signal while waiting for new logs at end of stream.")
							return
						}
					}
				}

				if readErr != nil {
					log.Printf("ERROR: Failed to read log record during streaming: %v", readErr)
					return // Exit goroutine on unrecoverable error
				}

				// Successfully read a record, send it to the channel and update currentReaderGlobalLSN
				select {
				case logStream <- lr:
					currentReaderGlobalLSN = lr.LSN + LSN(lr.Size())
				case <-lm.stopChan:
					log.Println("INFO: Log stream goroutine stopping because logStream send was blocked or stop signal received.")
					return
				}
			}
		}
	}()

	return logStream, nil
}

// flushInternal writes the buffered log records to the log file.
// This method MUST be called with lm.mu locked. It does NOT call Sync().
func (lm *LogManager) flushInternal() error {
	if lm.buffer.Len() == 0 {
		return nil // Nothing to flush
	}
	if lm.logFile == nil {
		return fmt.Errorf("log file is not open, cannot flush")
	}

	n, err := lm.logFile.Write(lm.buffer.Bytes())
	if err != nil {
		return fmt.Errorf("failed to write log buffer to file: %w", err)
	}
	if n != lm.buffer.Len() {
		return fmt.Errorf("short write to log file: expected %d, wrote %d", lm.buffer.Len(), n)
	}

	// Clear the buffer after successful write
	lm.buffer.Reset()

	log.Printf("DEBUG: Log buffer written to OS buffer. %d bytes.", n)
	lm.flushCond.Broadcast() // Signal any waiting goroutines that buffer has been flushed/reset
	return nil
}

// rollLogSegment closes the current log file, archives it, and opens a new log file.
// This method MUST be called with lm.mu locked.
func (lm *LogManager) rollLogSegment() error {
	log.Printf("INFO: Rolling log segment %d...", lm.currentSegmentID)

	// 1. Flush any remaining buffer to the current log file
	if err := lm.flushInternal(); err != nil {
		return fmt.Errorf("failed to flush buffer before rolling segment: %w", err)
	}

	// 2. Sync the current log file to ensure all data is on disk
	if lm.logFile != nil {
		if err := lm.logFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync log file before rolling segment: %w", err)
		}
		// 3. Close the current log file
		if err := lm.logFile.Close(); err != nil {
			return fmt.Errorf("failed to close log file %s: %w", lm.getLogSegmentPath(lm.currentSegmentID), err)
		}
		lm.logFile = nil // Clear file handle
	}

	// 4. Archive the just-closed log segment
	oldSegmentPath := lm.getLogSegmentPath(lm.currentSegmentID)
	archivePath := filepath.Join(lm.archiveDir, filepath.Base(oldSegmentPath))

	if err := os.Rename(oldSegmentPath, archivePath); err != nil {
		return fmt.Errorf("failed to archive log segment %s to %s: %w", oldSegmentPath, archivePath, err)
	}
	log.Printf("INFO: Archived log segment %d from %s to %s", lm.currentSegmentID, oldSegmentPath, archivePath)

	// 5. Increment segment ID and open a new log file
	lm.currentSegmentID++
	newSegmentPath := lm.getLogSegmentPath(lm.currentSegmentID)
	newLogFile, err := os.OpenFile(newSegmentPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to open new log segment %s: %w", newSegmentPath, err)
	}
	lm.logFile = newLogFile
	lm.currentSegmentFileOffset = 0 // Reset offset for the new segment

	log.Printf("INFO: Rolled to new log segment %d: %s", lm.currentSegmentID, newSegmentPath)
	return nil
}

// flusher is a goroutine that periodically flushes the log buffer.
func (lm *LogManager) flusher() {
	defer lm.wg.Done()
	// Create a ticker for periodic flushing (e.g., every 100ms)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-lm.stopChan:
			log.Println("INFO: Log flusher goroutine stopping.")
			lm.mu.Lock()
			// Final flush and sync before exiting
			if err := lm.flushInternal(); err != nil {
				log.Printf("ERROR: Final flushInternal failed on flusher stop: %v", err)
			}
			if lm.logFile != nil { // Ensure logFile is not nil before syncing
				if err := lm.logFile.Sync(); err != nil {
					log.Printf("ERROR: Final logFile.Sync failed on flusher stop: %v", err)
				}
			}
			lm.mu.Unlock()
			return
		case <-ticker.C:
			// Time to perform a periodic flush
			lm.mu.Lock()
			if lm.buffer.Len() > 0 { // Only flush if there's data
				if err := lm.flushInternal(); err != nil {
					log.Printf("ERROR: Periodic flushInternal failed: %v", err)
				}
				if lm.logFile != nil { // Ensure logFile is not nil before syncing
					if err := lm.logFile.Sync(); err != nil {
						log.Printf("ERROR: Periodic logFile.Sync failed: %v", err)
					}
				}
			}
			lm.mu.Unlock()
		}
	}
}

// Close stops the LogManager, flushes any remaining records, and closes the log file.
func (lm *LogManager) Close() error {
	log.Println("INFO: Closing LogManager...")
	close(lm.stopChan) // Signal the flusher goroutine to stop
	lm.wg.Wait()       // Wait for the flusher to finish its final flush and exit

	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Perform a final log segment roll to ensure all data is archived.
	// This also handles the final flush and sync of the current segment.
	// Only roll if there's data in the buffer or the current segment file is not empty.
	if lm.buffer.Len() > 0 || (lm.logFile != nil && lm.currentSegmentFileOffset > 0) {
		if err := lm.rollLogSegment(); err != nil {
			log.Printf("ERROR: Failed to perform final log segment roll on close: %v", err)
			// Don't return here, try to close the file handle anyway.
		}
	} else if lm.logFile != nil {
		// If no data was written to the current segment, just close it if it's open.
		// Avoid archiving an empty segment unless explicitly desired.
		if err := lm.logFile.Close(); err != nil {
			log.Printf("ERROR: Failed to close potentially empty log file on close: %v", err)
		}
		lm.logFile = nil
	}

	// The logFile should be nil after rollLogSegment or direct close.
	// If it's not nil (e.g., if rollLogSegment failed to close it), try to close it.
	if lm.logFile != nil {
		log.Printf("WARNING: Log file %s was not closed by rollLogSegment on close. Attempting to close now.", lm.getLogSegmentPath(lm.currentSegmentID))
		if err := lm.logFile.Close(); err != nil {
			return fmt.Errorf("failed to close log file during final cleanup: %w", err)
		}
		lm.logFile = nil
	}

	log.Println("INFO: LogManager closed successfully.")
	return nil
}

// --- LogRecord Serialization/Deserialization ---

// Serialize converts a LogRecord into a byte slice.
// This format must be stable for recovery.
func (lr *LogRecord) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write fixed-size fields
	if err := binary.Write(buf, binary.LittleEndian, lr.LSN); err != nil {
		return nil, fmt.Errorf("failed to serialize LSN: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, lr.PrevLSN); err != nil {
		return nil, fmt.Errorf("failed to serialize PrevLSN: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, lr.TxnID); err != nil {
		return nil, fmt.Errorf("failed to serialize TxnID: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, lr.Type); err != nil {
		return nil, fmt.Errorf("failed to serialize Type: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, lr.PageID); err != nil {
		return nil, fmt.Errorf("failed to serialize PageID: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, lr.Offset); err != nil {
		return nil, fmt.Errorf("failed to serialize Offset: %w", err)
	}

	// Write variable-length OldData
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(lr.OldData))); err != nil {
		return nil, fmt.Errorf("failed to serialize OldData length: %w", err)
	}
	if _, err := buf.Write(lr.OldData); err != nil {
		return nil, fmt.Errorf("failed to write OldData: %w", err)
	}

	// Write variable-length NewData
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(lr.NewData))); err != nil {
		return nil, fmt.Errorf("failed to serialize NewData length: %w", err)
	}
	if _, err := buf.Write(lr.NewData); err != nil {
		return nil, fmt.Errorf("failed to write NewData: %w", err)
	}

	return buf.Bytes(), nil
}

// Deserialize reads a byte slice into a LogRecord.
// This is crucial for recovery.
func (lr *LogRecord) Deserialize(data []byte) error {
	buf := bytes.NewReader(data)

	// Read fixed-size fields
	if err := binary.Read(buf, binary.LittleEndian, &lr.LSN); err != nil {
		return fmt.Errorf("failed to deserialize LSN: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &lr.PrevLSN); err != nil {
		return fmt.Errorf("failed to deserialize PrevLSN: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &lr.TxnID); err != nil {
		return fmt.Errorf("failed to deserialize TxnID: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &lr.Type); err != nil {
		return fmt.Errorf("failed to deserialize Type: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &lr.PageID); err != nil {
		return fmt.Errorf("failed to deserialize PageID: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &lr.Offset); err != nil {
		return fmt.Errorf("failed to deserialize Offset: %w", err)
	}

	// Read variable-length OldData
	var oldDataLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &oldDataLen); err != nil {
		return fmt.Errorf("failed to deserialize OldData length: %w", err)
	}
	lr.OldData = make([]byte, oldDataLen)
	if _, err := io.ReadFull(buf, lr.OldData); err != nil {
		return fmt.Errorf("failed to read OldData: %w", err)
	}

	// Read variable-length NewData
	var newDataLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &newDataLen); err != nil {
		return fmt.Errorf("failed to deserialize NewData length: %w", err)
	}
	lr.NewData = make([]byte, newDataLen)
	if _, err := io.ReadFull(buf, lr.NewData); err != nil {
		return fmt.Errorf("failed to read NewData: %w", err)
	}

	return nil
}

// Size returns the approximate serialized size of the LogRecord.
// Useful for LSN calculation and buffer management.
func (lr *LogRecord) Size() int {
	// Fixed size fields: LSN, PrevLSN, TxnID (3 * 8 bytes = 24)
	// Type (1 byte)
	// PageID (8 bytes)
	// Offset (2 bytes)
	// OldDataLen, NewDataLen (2 * 2 bytes = 4)
	fixedSize := 24 + 1 + 8 + 2 + 4
	return fixedSize + len(lr.OldData) + len(lr.NewData)
}
