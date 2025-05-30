package btree

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
)

// --- Write-Ahead Logging (WAL) Constants and Types ---

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
	LogRecordTypeCheckpointStart
	LogRecordTypeCheckpointEnd
	// --- NEW: 2PC Specific Log Record Types ---
	LogRecordTypePrepare   // Transaction Prepare record
	LogRecordTypeCommitTxn // Transaction Commit record (final phase 2)
	LogRecordTypeAbortTxn  // Transaction Abort record (final phase 2)
	// --- END NEW ---
)

// LogRecord represents a single entry in the Write-Ahead Log.
type LogRecord struct {
	LSN     LSN
	PrevLSN LSN    // LSN of the previous log record by the same transaction (for undo)
	TxnID   uint64 // Transaction ID (0 if not part of a transaction or single op)
	Type    LogRecordType
	PageID  PageID // Page affected (if applicable)
	Offset  uint16 // Offset within the page (if applicable for UPDATE)
	OldData []byte // For UNDO (and REDO if needed for physiological logging)
	NewData []byte // For REDO
	// Other type-specific fields for specific log record types would go here.
}

// LogManager manages the Write-Ahead Log file(s).
// It is responsible for appending log records, managing log segments,
// ensuring durability, and providing a basic archiving mechanism.
type LogManager struct {
	logDir           string         // Directory where active log segments reside
	archiveDir       string         // Directory for archived log segments
	logFile          *os.File       // Current active log segment file handle
	currentSegmentID uint64         // ID of the current active log segment
	currentLSN       LSN            // The next LSN to be assigned (global, monotonically increasing)
	buffer           *bytes.Buffer  // In-memory buffer for log records before flushing
	mu               sync.Mutex     // Protects access to LogManager state (currentLSN, buffer, logFile, segmentID)
	flushCond        *sync.Cond     // For signaling when buffer needs flushing (e.g., buffer is full)
	bufferSize       int            // Maximum size of the in-memory buffer
	segmentSizeLimit int64          // Maximum size of a single log segment file before rotation
	stopChan         chan struct{}  // Channel to signal stopping the flusher goroutine
	wg               sync.WaitGroup // WaitGroup for flusher goroutine

	// --- NEW: Recovery State (for Analysis Pass) ---
	// This would typically be part of a dedicated RecoveryManager,
	// but for V1, we'll keep it here for simplicity.
	// Maps to track transaction states during recovery
	recoveryTxnStates map[uint64]TransactionState // TxnID -> state (e.g., PREPARED, COMMITTED, ABORTED)
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
		recoveryTxnStates: make(map[uint64]TransactionState),
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

	log.Printf("INFO: LogManager initialized. Log directory: %s, Archive directory: %s, Current Segment ID: %d, Initial LSN: %d",
		logDir, archiveDir, lm.currentSegmentID, lm.currentLSN)
	return lm, nil
}

// findOrCreateLatestLogSegment scans the log directory to find the latest segment,
// or creates the first one if none exist. It sets lm.logFile, lm.currentSegmentID, and lm.currentLSN.
// This method MUST be called with lm.mu locked.
func (lm *LogManager) findOrCreateLatestLogSegment() error {
	files, err := os.ReadDir(lm.logDir)
	if err != nil {
		return fmt.Errorf("failed to read log directory: %w", err)
	}

	var latestSegmentID uint64 = 0
	var latestLogFilePath string

	// Find the highest existing segment ID
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if strings.HasPrefix(file.Name(), "log_") && strings.HasSuffix(file.Name(), ".log") {
			parts := strings.Split(strings.TrimSuffix(file.Name(), ".log"), "_")
			if len(parts) == 2 {
				id, parseErr := strconv.ParseUint(parts[1], 10, 64)
				if parseErr == nil && id > latestSegmentID {
					latestSegmentID = id
				}
			}
		}
	}

	if latestSegmentID == 0 {
		// No existing log segments, start with segment 1
		lm.currentSegmentID = 1
	} else {
		// Continue with the latest found segment
		lm.currentSegmentID = latestSegmentID
	}

	latestLogFilePath = lm.getLogSegmentPath(lm.currentSegmentID)

	// Open the latest log segment for appending.
	// If it's a new segment (currentSegmentID was incremented), it will be created.
	// If it's an existing segment, it will be opened in append mode.
	logFile, err := os.OpenFile(latestLogFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to open/create log segment %s: %w", latestLogFilePath, err)
	}
	lm.logFile = logFile

	// Set current LSN based on the current size of the active log file.
	// In a real system, LSNs would be recovered from a checkpoint or by scanning the log.
	fileInfo, err := lm.logFile.Stat()
	if err != nil {
		lm.logFile.Close()
		return fmt.Errorf("failed to stat active log file: %w", err)
	}
	// For simplicity, LSN is global byte offset.
	// A more robust LSN would be (segment_id, offset_in_segment) or a global counter
	// persisted in a superblock/checkpoint. Here, we assume LSN is a global counter
	// and its value is the total bytes written across all segments.
	// This simple LSN scheme is problematic for recovery if segments are deleted.
	// A better LSN is a monotonically increasing counter persisted in a metadata file.
	// For now, we'll derive it from the size of the *current* log file,
	// implying LSNs are relative to the start of the current segment.
	// This needs careful consideration for recovery.
	lm.currentLSN = LSN(fileInfo.Size())
	log.Printf("DEBUG: Active log segment: %s, Initial LSN for segment: %d", latestLogFilePath, lm.currentLSN)

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
	record.LSN = lm.currentLSN // LSN is relative to the start of the current segment for now

	// Serialize the log record
	serializedRecord, err := record.Serialize()
	if err != nil {
		return InvalidLSN, fmt.Errorf("failed to serialize log record: %w", err)
	}

	// Check if record fits in buffer, if not, flush first
	if lm.buffer.Len()+len(serializedRecord) > lm.bufferSize {
		log.Printf("DEBUG: Log buffer full (%d bytes). Flushing before appending LSN %d.", lm.buffer.Len(), record.LSN)
		if err := lm.flushInternal(); err != nil {
			return InvalidLSN, fmt.Errorf("failed to flush log buffer before append: %w", err)
		}
	}

	// Check if appending this record would exceed the segment size limit
	// This check should happen *after* flushInternal to ensure the buffer is as empty as possible.
	currentLogFileSize := int64(lm.buffer.Len()) // Data currently in buffer
	if lm.logFile != nil {
		fileInfo, statErr := lm.logFile.Stat()
		if statErr == nil {
			currentLogFileSize += fileInfo.Size() // Data already written to file
		} else {
			log.Printf("WARNING: Failed to stat log file for size check: %v", statErr)
		}
	}

	if currentLogFileSize+int64(len(serializedRecord)) > lm.segmentSizeLimit {
		log.Printf("INFO: Log segment %d reaching limit (%d bytes). Rolling to new segment.", lm.currentSegmentID, lm.segmentSizeLimit)
		if err := lm.rollLogSegment(); err != nil {
			return InvalidLSN, fmt.Errorf("failed to roll log segment before append: %w", err)
		}
		// After rolling, currentLSN is reset to 0 for the new segment.
		// Re-assign LSN based on the new segment's start.
		record.LSN = lm.currentLSN
	}

	// Append to buffer
	if _, err := lm.buffer.Write(serializedRecord); err != nil {
		return InvalidLSN, fmt.Errorf("failed to write record to log buffer: %w", err)
	}

	// Update current LSN by record size after successful append to buffer
	lm.currentLSN += LSN(len(serializedRecord))

	// Signal the flusher goroutine if the buffer is now full or close to full
	if lm.buffer.Len() >= lm.bufferSize/2 { // Signal at half full to trigger proactive flushing
		lm.flushCond.Signal()
	}

	log.Printf("DEBUG: Appended log record LSN %d (Type: %v, PageID: %d, Size: %d) to segment %d",
		record.LSN, record.Type, record.PageID, len(serializedRecord), lm.currentSegmentID)
	return record.LSN, nil
}

// Recover performs the recovery process (Redo Pass + basic Analysis/Undo) on database startup.
// It scans log records from archived and active log segments and reapplies
// committed changes to data pages, and resolves prepared transactions.
// dm: The DiskManager to interact with data pages.
// bpm: The BufferPoolManager to fetch/flush pages (not used for recovery reads/writes directly, but for context).
// lastLSN: The last LSN recorded in the DBFileHeader, indicating the state of the data file.
func (lm *LogManager) Recover(dm *DiskManager, bpm *BufferPoolManager, lastLSN LSN) error {
	log.Println("INFO: Starting LogManager recovery process (Redo Pass + basic Analysis/Undo)...")

	// --- Analysis Pass (V1: Identify committed/aborted transactions) ---
	// Clear previous recovery state
	lm.recoveryTxnStates = make(map[uint64]TransactionState)

	// Collect all log segments (archived and active) in order.
	var segmentPaths []string

	// Add archived segments
	archiveFiles, err := os.ReadDir(lm.archiveDir)
	if err != nil {
		return fmt.Errorf("failed to read archive directory: %w", err)
	}
	for _, file := range archiveFiles {
		if !file.IsDir() && strings.HasPrefix(file.Name(), "log_") && strings.HasSuffix(file.Name(), ".log") {
			segmentPaths = append(segmentPaths, filepath.Join(lm.archiveDir, file.Name()))
		}
	}

	// Add active segments (from logDir)
	logFiles, err := os.ReadDir(lm.logDir)
	if err != nil {
		return fmt.Errorf("failed to read log directory: %w", err)
	}
	for _, file := range logFiles {
		if !file.IsDir() && strings.HasPrefix(file.Name(), "log_") && strings.HasSuffix(file.Name(), ".log") {
			segmentPaths = append(segmentPaths, filepath.Join(lm.logDir, file.Name()))
		}
	}

	// Sort segments by their ID to ensure correct replay order
	sort.Slice(segmentPaths, func(i, j int) bool {
		id1, _ := strconv.ParseUint(strings.TrimSuffix(strings.Split(filepath.Base(segmentPaths[i]), "_")[1], ".log"), 10, 64)
		id2, _ := strconv.ParseUint(strings.TrimSuffix(strings.Split(filepath.Base(segmentPaths[j]), "_")[1], ".log"), 10, 64)
		return id1 < id2
	})

	// --- Redo Pass (V1: Reapply changes from log) ---
	var currentGlobalLSN LSN = 0 // Track global LSN for recovery progress (relative to start of logs being scanned)
	for _, segmentPath := range segmentPaths {
		log.Printf("INFO: Analyzing and Replaying from log segment: %s", segmentPath)
		segmentFile, err := os.Open(segmentPath)
		if err != nil {
			return fmt.Errorf("failed to open log segment %s for recovery: %w", segmentPath, err)
		}
		defer segmentFile.Close() // Close each segment file after processing

		reader := bufio.NewReader(segmentFile) // Use bufio.NewReader for efficient byte-by-byte reading
		for {
			var lr LogRecord
			// recordStartOffset := currentGlobalLSN // LSN of this record

			// Read and deserialize a single log record
			err := lm.readLogRecord(reader, &lr)
			if err == io.EOF {
				break // End of segment file
			}
			if err != nil {
				log.Printf("ERROR: Failed to read log record from %s at offset %d: %v. Stopping recovery for this segment.", segmentPath, currentGlobalLSN, err)
				// In a real system, this might indicate a corrupted log or truncated record.
				// We might try to skip to the next segment or require manual intervention.
				break
			}

			// --- Analysis Pass Logic ---
			// Update transaction states based on log records
			switch lr.Type {
			case LogRecordTypePrepare:
				lm.recoveryTxnStates[lr.TxnID] = TxnStatePrepared
				log.Printf("DEBUG: Recovery Analysis: Txn %d is PREPARED (LSN %d)", lr.TxnID, lr.LSN)
			case LogRecordTypeCommitTxn:
				lm.recoveryTxnStates[lr.TxnID] = TxnStateCommitted
				log.Printf("DEBUG: Recovery Analysis: Txn %d is COMMITTED (LSN %d)", lr.TxnID, lr.LSN)
			case LogRecordTypeAbortTxn:
				lm.recoveryTxnStates[lr.TxnID] = TxnStateAborted
				log.Printf("DEBUG: Recovery Analysis: Txn %d is ABORTED (LSN %d)", lr.LSN)
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
				if state, ok := lm.recoveryTxnStates[lr.TxnID]; ok && state == TxnStateCommitted {
					applyRecord = true
				} else {
					log.Printf("DEBUG: Skipping Redo for Txn %d (LSN %d, Type %v): Not committed or state unknown.", lr.TxnID, lr.LSN, lr.Type)
				}
			}

			if applyRecord && lr.LSN >= lastLSN { // Only apply if LSN is newer than last checkpoint
				log.Printf("DEBUG: Replaying log record LSN %d (Type: %v, PageID: %d) to disk.", lr.LSN, lr.Type, lr.PageID)

				pageData := make([]byte, dm.pageSize)

				// Read page from disk (it might not exist if it's a new page log record)
				readErr := dm.ReadPage(lr.PageID, pageData)
				if readErr != nil && lr.Type != LogRecordTypeNewPage {
					log.Printf("WARNING: Failed to read page %d for recovery replay: %v. Skipping record LSN %d.", lr.PageID, readErr, lr.LSN)
					// A real system might panic or require manual intervention here.
					currentGlobalLSN += LSN(lr.Size()) // Advance LSN even on error
					continue
				} else if readErr != nil && lr.Type == LogRecordTypeNewPage {
					log.Printf("DEBUG: Page %d not found on disk, but it's a new page record. Will allocate if needed.", lr.PageID)
				}

				// Apply the change based on log record type
				switch lr.Type {
				case LogRecordTypeNewPage:
					// Ensure the page exists on disk. If it was truncated, re-allocate.
					if lr.PageID.GetID() >= dm.numPages {
						log.Printf("INFO: Re-allocating page %d during recovery (was truncated or never allocated).", lr.PageID)
						emptyPage := make([]byte, dm.pageSize)
						if writeErr := dm.WritePage(lr.PageID, emptyPage); writeErr != nil {
							return fmt.Errorf("failed to re-allocate new page %d during recovery: %w", lr.PageID, writeErr)
						}
						if lr.PageID >= PageID(dm.numPages) {
							dm.numPages = uint64(lr.PageID) + 1
						}
					}
					if len(lr.NewData) > 0 { // Apply initial data if logged
						if writeErr := dm.WritePage(lr.PageID, lr.NewData); writeErr != nil {
							return fmt.Errorf("failed to write new page data for %d during recovery: %w", lr.PageID, writeErr)
						}
					}
				case LogRecordTypeUpdate:
					copy(pageData, lr.NewData) // Overwrite page data with new data
					if writeErr := dm.WritePage(lr.PageID, pageData); writeErr != nil {
						return fmt.Errorf("failed to write updated page %d during recovery: %w", lr.PageID, writeErr)
					}
					// TODO: Add cases for other LogRecordTypes (InsertKey, DeleteKey, NodeSplit, NodeMerge)
					// These would require understanding the byte format within the page.
					// For now, LogRecordTypeUpdate is a generic page overwrite.
				}
			}
			currentGlobalLSN += LSN(lr.Size()) // Advance global LSN
		}
	}

	// --- Undo Pass (V1: Rollback uncommitted transactions) ---
	log.Println("INFO: Starting LogManager recovery Undo Pass (V1: aborting prepared/unknown transactions)...")
	for txnID, state := range lm.recoveryTxnStates {
		if state == TxnStatePrepared { // Or any other state that is not Committed/Aborted
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

	if err := dm.UpdateHeaderField(func(h *DBFileHeader) {
		h.LastLSN = finalLSN
	}); err != nil {
		return fmt.Errorf("failed to update DBFileHeader LastLSN after recovery: %w", err)
	}

	log.Println("INFO: LogManager recovery process complete.")
	return nil
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

	log.Printf("DEBUG: LogManager flushed and synced all buffered data up to LSN %d (in segment %d).", lm.currentLSN, lm.currentSegmentID)
	return nil
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
	archivePath := filepath.Join(lm.archiveDir, filepath.Base(oldSegmentPath)) // Copy to archive dir

	// In a real system, this would be a robust copy operation to a separate,
	// potentially remote, durable storage. For simplicity, we'll use os.Rename
	// to simulate moving it, implying it's "archived" and no longer in the active log directory.
	// Note: os.Rename will fail across different filesystems. A real archiver would copy then delete.
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
	lm.currentLSN = 0 // Reset LSN for the new segment (LSN is now offset within segment)

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
	if err := lm.rollLogSegment(); err != nil {
		log.Printf("ERROR: Failed to perform final log segment roll on close: %v", err)
		// Don't return here, try to close the file handle anyway.
	}

	// The logFile should be nil after rollLogSegment.
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
