package btree_core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time" // Added for time.Ticker
)

// --- Write-Ahead Logging (WAL) Constants and Types (Copied for logmanager.go context) ---

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
	// For this generic implementation, we'll only serialize the common fields.
}

// LogManager manages the Write-Ahead Log file.
// It is responsible for appending log records and ensuring they are durable on disk.
type LogManager struct {
	logFilePath string
	logFile     *os.File
	currentLSN  LSN            // The next LSN to be assigned
	buffer      *bytes.Buffer  // In-memory buffer for log records before flushing
	mu          sync.Mutex     // Protects access to currentLSN and buffer
	flushCond   *sync.Cond     // For signaling when buffer needs flushing (e.g., buffer is full)
	bufferSize  int            // Maximum size of the in-memory buffer
	stopChan    chan struct{}  // Channel to signal stopping the flusher goroutine
	wg          sync.WaitGroup // WaitGroup for flusher goroutine
}

// NewLogManager creates and initializes a new LogManager.
// It opens or creates the log file and starts a background flusher goroutine.
func NewLogManager(logFilePath string, bufferSize int) (*LogManager, error) {
	if bufferSize <= 0 {
		return nil, fmt.Errorf("log buffer size must be positive")
	}

	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create log file %s: %w", logFilePath, err)
	}

	// Get current file size to determine the starting LSN.
	// In a real system, LSNs would be recovered from a checkpoint or the last log record.
	fileInfo, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to stat log file: %w", err)
	}
	initialLSN := LSN(fileInfo.Size()) // Simple LSN based on file offset

	lm := &LogManager{
		logFilePath: logFilePath,
		logFile:     logFile,
		currentLSN:  initialLSN,
		buffer:      bytes.NewBuffer(make([]byte, 0, bufferSize)), // Pre-allocate buffer capacity
		bufferSize:  bufferSize,
		stopChan:    make(chan struct{}),
	}
	lm.flushCond = sync.NewCond(&lm.mu)

	// Start a background goroutine to periodically flush the buffer
	lm.wg.Add(1)
	go lm.flusher()

	log.Printf("INFO: LogManager initialized. Log file: %s, Initial LSN: %d", logFilePath, initialLSN)
	return lm, nil
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

	// Increment LSN by record size after successful serialization
	lm.currentLSN += LSN(len(serializedRecord))

	// Check if record fits in buffer, if not, flush first
	if lm.buffer.Len()+len(serializedRecord) > lm.bufferSize {
		log.Printf("DEBUG: Log buffer full (%d bytes). Flushing before appending LSN %d.", lm.buffer.Len(), record.LSN)
		// It's important to flush synchronously here to make space.
		// If flushInternal fails, we return the error.
		if err := lm.flushInternal(); err != nil {
			return InvalidLSN, fmt.Errorf("failed to flush log buffer before append: %w", err)
		}
	}

	// Append to buffer
	if _, err := lm.buffer.Write(serializedRecord); err != nil {
		return InvalidLSN, fmt.Errorf("failed to write record to log buffer: %w", err)
	}

	// Signal the flusher goroutine if the buffer is now full or close to full
	// The flusher will pick this up on its next tick or if it's explicitly waiting.
	if lm.buffer.Len() >= lm.bufferSize/2 { // Signal at half full to trigger proactive flushing
		lm.flushCond.Signal()
	}

	log.Printf("DEBUG: Appended log record LSN %d (Type: %v, PageID: %d, Size: %d)", record.LSN, record.Type, record.PageID, len(serializedRecord))
	return record.LSN, nil
}

// Flush ensures all log records up to a certain LSN (or all if targetLSN is InvalidLSN) are written to disk.
// This is a blocking call that ensures durability.
func (lm *LogManager) Flush(targetLSN LSN) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Flush any buffered data immediately.
	// This ensures that any log records currently in memory are written to the OS buffer.
	if err := lm.flushInternal(); err != nil {
		return fmt.Errorf("failed to flush log buffer: %w", err)
	}

	// Ensure data is synced to disk.
	// This is the critical step for durability.
	if err := lm.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync log file: %w", err)
	}

	// In a more complex system, if targetLSN is specific, you might wait
	// for a background flusher to confirm it's on disk.
	// For this setup, `flushInternal` + `Sync` guarantees durability.
	// The `targetLSN` parameter is less critical here since we force sync all.
	// It's mainly for recovery or specific commit point flushing.

	log.Printf("DEBUG: LogManager flushed and synced all buffered data up to LSN %d.", lm.currentLSN)
	return nil
}

// flushInternal writes the buffered log records to the log file.
// This method MUST be called with lm.mu locked. It does NOT call Sync().
func (lm *LogManager) flushInternal() error {
	if lm.buffer.Len() == 0 {
		return nil // Nothing to flush
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
	// Do NOT call lm.logFile.Sync() here. Syncing is explicitly called by Flush().
	// Signaling happens after buffer cleared.
	lm.flushCond.Broadcast() // Signal any waiting goroutines that buffer has been flushed/reset
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
			if err := lm.logFile.Sync(); err != nil { // Ensure final sync
				log.Printf("ERROR: Final logFile.Sync failed on flusher stop: %v", err)
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
				if err := lm.logFile.Sync(); err != nil { // Ensure periodic sync
					log.Printf("ERROR: Periodic logFile.Sync failed: %v", err)
				}
			}
			lm.mu.Unlock()
			// Removed: case <-lm.flushCond.C: as sync.Cond does not expose a channel.
			// The flusher's periodic tick is sufficient to handle buffer fullness.
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

	// One last flush and sync to ensure everything is written
	if err := lm.flushInternal(); err != nil {
		log.Printf("ERROR: Failed to perform final log flush on close: %v", err)
		// Don't return here, try to close the file anyway.
	}
	if err := lm.logFile.Sync(); err != nil { // Ensure final sync before closing
		log.Printf("ERROR: Final logFile.Sync failed on close: %v", err)
	}

	if err := lm.logFile.Close(); err != nil {
		return fmt.Errorf("failed to close log file: %w", err)
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
