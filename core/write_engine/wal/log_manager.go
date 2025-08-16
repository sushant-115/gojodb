package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"

	// "encoding/gob" // No longer needed for WALStreamReader
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time" // Added for LastHeartbeat

	"github.com/sushant-115/gojodb/core/indexing"
	"github.com/sushant-115/gojodb/core/security/encryption"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"go.uber.org/zap"
)

const (
	walFilePrefix            = "wal-"
	walFileSuffix            = ".log"
	defaultMaxWalSegmentSize = 16 * 1024 * 1024 // 16MB per WAL segment
	defaultArchiveDir        = "archive"
	slotsFileName            = "replication_slots.json"
	defaultMinLSNToKeep      = LSN(0) // Default LSN to keep, can be updated by replication slots
)

var (
	encryptionKey = []byte("SOMEKEY1SOMEKEY1")
)

type LSN uint64

// LogRecordType defines the type of operation logged.
type LogRecordType uint8

// ... (existing LogRecordType constants)
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
	LogTypeRTreeInsert
	LogTypeRTreeSplit
	LogTypeRTreeNewRoot
	LogTypeRTreeUpdate
	// --- NEW: 2PC Specific Log Record Types ---
	LogRecordTypePrepare   // Transaction Prepare record
	LogRecordTypeCommitTxn // Transaction Commit record (final phase 2)
	LogRecordTypeAbortTxn  // Transaction Abort record (final phase 2)
	// --- END NEW ---
	LogRecordTypeRootChange // NEW: Log record for B-tree root page ID change
	LogTypeRTreeDelete
	// Add other types as needed
	LogRecordTypeNoOp
)

type LogType uint8

const (
	LogTypeBtree LogType = iota + 1
	LogTypeInvertedIndex
	LogTypeSpatial
)

// LogRecord represents a single log entry in the WAL.
type LogRecord struct {
	LSN        LSN
	Type       LogRecordType
	IndexType  indexing.IndexType
	Timestamp  int64              // Unix Nano
	TxnID      uint64             // Transaction ID, 0 if not part of a transaction
	PageID     pagemanager.PageID // Page ID, relevant for page-level operations
	Data       []byte             // Actual log data (e.g., page diff, transaction info)
	PrevLSN    LSN                // LSN of the previous log record for this transaction/page (optional, for recovery)
	CRC        uint32             // Checksum for integrity
	LogType    LogType            // General type for routing to index specific apply logic
	SegmentID  uint64             // ID of the WAL segment this record belongs to
	RecordSize uint32             // Size of this record on disk
}

// Encode serializes the LogRecord into a byte slice for network transmission or disk storage.
func (lr *LogRecord) Encode() ([]byte, error) {
	// Use a buffer for efficient byte concatenation.
	buf := new(bytes.Buffer)

	// Write fixed-size fields using binary.BigEndian.
	// The order of writing must be strictly maintained for decoding.
	if err := binary.Write(buf, binary.BigEndian, lr.LSN); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, lr.Type); err != nil {
		return nil, err
	}

	indexTypeLen := uint32(len(lr.IndexType))
	if err := binary.Write(buf, binary.BigEndian, indexTypeLen); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(string(lr.IndexType)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, lr.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, lr.TxnID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, lr.PageID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, lr.PrevLSN); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, lr.LogType); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, lr.SegmentID); err != nil {
		return nil, err
	}

	// For the variable-size Data field, first write its length, then the data itself.
	// This is crucial for the decoder to know how many bytes to read.
	dataLen := uint32(len(lr.Data))
	if err := binary.Write(buf, binary.BigEndian, dataLen); err != nil {
		return nil, err
	}
	if _, err := buf.Write(lr.Data); err != nil {
		return nil, err
	}

	// The CRC should be calculated on the encoded data *before* the CRC field itself is appended.
	// We'll calculate it now and then append it.
	encodedBytesWithoutCRC := buf.Bytes()
	crc := crc32.ChecksumIEEE(encodedBytesWithoutCRC)
	if err := binary.Write(buf, binary.BigEndian, crc); err != nil {
		return nil, err
	}

	// Finally, we can set the total record size and return the full byte slice.
	lr.RecordSize = uint32(buf.Len())

	return buf.Bytes(), nil
}

// DecodeLogRecord deserializes a byte slice back into a LogRecord struct.
func DecodeLogRecord(data []byte) (*LogRecord, error) {
	buf := bytes.NewReader(data)
	lr := &LogRecord{}

	// Read fixed-size fields in the exact same order they were written.
	if err := binary.Read(buf, binary.BigEndian, &lr.LSN); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &lr.Type); err != nil {
		return nil, err
	}

	var indexTypeLen uint32
	if err := binary.Read(buf, binary.BigEndian, &indexTypeLen); err != nil {
		return nil, err
	}
	indexTypeBytes := make([]byte, indexTypeLen)
	if _, err := io.ReadFull(buf, indexTypeBytes); err != nil {
		return nil, err
	}
	lr.IndexType = indexing.IndexType(indexTypeBytes)

	if err := binary.Read(buf, binary.BigEndian, &lr.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &lr.TxnID); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &lr.PageID); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &lr.PrevLSN); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &lr.LogType); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &lr.SegmentID); err != nil {
		return nil, err
	}

	// Read the length of the Data field, then read that many bytes.
	var dataLen uint32
	if err := binary.Read(buf, binary.BigEndian, &dataLen); err != nil {
		return nil, err
	}
	lr.Data = make([]byte, dataLen)
	if _, err := io.ReadFull(buf, lr.Data); err != nil {
		return nil, err
	}

	// Read the CRC.
	if err := binary.Read(buf, binary.BigEndian, &lr.CRC); err != nil {
		return nil, err
	}

	// Verify the CRC for data integrity.
	// The CRC was calculated on the data *before* the CRC itself was appended.
	bytesToVerify := data[:len(data)-4]
	if crc32.ChecksumIEEE(bytesToVerify) != lr.CRC {
		return nil, errors.New("log record checksum mismatch")
	}

	lr.RecordSize = uint32(len(data))
	return lr, nil
}

// ReplicationSlot tracks a consumer of WAL records.
type ReplicationSlot struct {
	SlotName       string    `json:"slot_name"`        // Unique name for the slot (e.g., replica_node_id + "_" + index_type)
	ConsumerNodeID string    `json:"consumer_node_id"` // ID of the node consuming the WALs
	IndexType      string    `json:"index_type"`       // Type of index this slot is for (e.g., "btree", "inverted_index")
	RequiredLSN    LSN       `json:"required_lsn"`     // The oldest LSN this consumer still needs. WALs up to this LSN cannot be deleted.
	SnapshotLSN    LSN       `json:"snapshot_lsn"`     // The LSN at which the consumer started (e.g., from a snapshot).
	RestartLSN     LSN       `json:"restart_lsn"`
	IsActive       bool      `json:"is_active"`      // Whether the slot is currently considered active for WAL retention.
	LastHeartbeat  time.Time `json:"last_heartbeat"` // Last time the consumer confirmed activity.
	CreationTime   time.Time `json:"creation_time"`
}

type LogManager struct {
	walDir             string
	currentSegmentFile *os.File
	currentSegmentID   uint64
	currentLSN         LSN
	maxSegmentSize     int64
	archiveDir         string
	logger             *zap.Logger
	mu                 sync.RWMutex // Protects currentLSN, segment switching, and replicationSlots
	mtx                sync.Mutex
	cond               *sync.Cond

	slotsMtx         sync.RWMutex
	slotsFilePath    string
	segmentStartLSNs map[int]LSN // Maps segment ID to its starting LSN
	segmentMetaMtx   sync.RWMutex

	// WAL encryption fields
	encryptionKey []byte
	cryptoUtil    *encryption.CryptoUtils

	// Replication Slots Management
	replicationSlots map[string]*ReplicationSlot // Key: SlotName
	shutdownCh       chan struct{}
}

// WALStreamReader provides an interface for reading from the WAL across multiple segments.
type WALStreamReader struct {
	logManager *LogManager
	slotName   string

	currentSegmentID int
	currentFile      *os.File
	// The gob decoder is removed as we will parse the binary format manually.
	// decoder          *gob.Decoder

	// Channel to signal the reader to stop
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewLogManager creates or opens a LogManager for the given directory.
func NewLogManager(walDir string, logger *zap.Logger) (*LogManager, error) {
	if err := os.MkdirAll(walDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory %s: %w", walDir, err)
	}

	archivePath := filepath.Join(walDir, defaultArchiveDir)
	if err := os.MkdirAll(archivePath, 0750); err != nil {
		return nil, fmt.Errorf("failed to create WAL archive directory %s: %w", archivePath, err)
	}

	var crypto *encryption.CryptoUtils
	if len(encryptionKey) > 0 {
		var err error
		crypto, err = encryption.NewCryptoUtils(encryptionKey)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize crypto utils: %w", err)
		}
	}

	// Initialize logger (ideally passed in, but creating a default one for now)
	//logger, _ := zap.NewDevelopment() // TODO: Pass logger as argument

	lm := &LogManager{
		walDir:           walDir,
		maxSegmentSize:   defaultMaxWalSegmentSize,
		archiveDir:       archivePath,
		slotsFilePath:    filepath.Join(walDir, slotsFileName),
		segmentStartLSNs: make(map[int]LSN),
		logger:           logger.Named("log_manager"),
		replicationSlots: make(map[string]*ReplicationSlot),
		cryptoUtil:       crypto,
		shutdownCh:       make(chan struct{}),
	}

	lm.cond = sync.NewCond(&lm.mtx)
	if err := lm.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover log manager: %w", err)
	}
	if err := lm.loadReplicationSlots(); err != nil {
		lm.logger.Warn("Could not load replication slots, starting fresh", zap.Error(err))
	}

	// Start a background goroutine for periodic pruning
	go lm.runPruningLoop()

	return lm, nil
}

func (l *LogManager) GetCurrentLSN() LSN {
	return l.currentLSN
}

func (lm *LogManager) runPruningLoop() {
	// Prune shortly after startup and then periodically
	time.Sleep(1 * time.Minute)
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	lm.pruneOldSegments()
	for {
		select {
		case <-ticker.C:
			lm.pruneOldSegments()
		case <-lm.shutdownCh:
			return
		}
	}
}

func (lm *LogManager) pruneOldSegments() {
	lm.mtx.Lock()
	checkpointLSN := lm.currentLSN
	lm.mtx.Unlock()

	if checkpointLSN == 0 {
		lm.logger.Debug("Skipping WAL pruning, no checkpoint has been set.")
		return
	}

	lm.slotsMtx.RLock()
	minSlotLSN := LSN(0)
	for _, slot := range lm.replicationSlots {
		if slot.IsActive {
			if minSlotLSN == 0 || slot.RestartLSN < minSlotLSN {
				minSlotLSN = slot.RestartLSN
			}
		}
	}
	lm.slotsMtx.RUnlock()

	// The safe LSN is the minimum of the checkpoint LSN and all active slot LSNs.
	safeLSN := checkpointLSN
	if minSlotLSN > 0 && minSlotLSN < safeLSN {
		safeLSN = minSlotLSN
	}

	lm.logger.Info("Running WAL pruning",
		zap.Uint64("safeLSN", uint64(safeLSN)),
		zap.Uint64("checkpointLSN", uint64(checkpointLSN)),
		zap.Uint64("minSlotLSN", uint64(minSlotLSN)))

	lm.segmentMetaMtx.Lock()
	defer lm.segmentMetaMtx.Unlock()

	// Collect segment IDs to check for pruning
	var segmentIDs []int
	for id := range lm.segmentStartLSNs {
		segmentIDs = append(segmentIDs, id)
	}
	sort.Ints(segmentIDs)

	// We can prune a segment if the *next* segment starts before the safe LSN.
	// We cannot prune the current active segment for writing.
	for i := 0; i < len(segmentIDs)-1; i++ {
		segmentID := segmentIDs[i]
		nextSegmentID := segmentIDs[i+1]
		nextSegmentStartLSN := lm.segmentStartLSNs[nextSegmentID]

		if segmentID < int(lm.currentSegmentID) && nextSegmentStartLSN < safeLSN {
			filePath := filepath.Join(lm.walDir, fmt.Sprintf("%s%d%s", walFilePrefix, segmentID, walFileSuffix))
			lm.logger.Info("Pruning old WAL segment", zap.String("file", filePath), zap.Int("segmentID", segmentID))
			err := os.Remove(filePath)
			if err != nil {
				lm.logger.Error("Failed to prune WAL segment", zap.String("file", filePath), zap.Error(err))
			} else {
				delete(lm.segmentStartLSNs, segmentID)
			}
		}
	}
}

// GetWALReaderForStreaming creates a new WALStreamReader for a given replication slot.
// It creates the slot if it doesn't exist.
func (lm *LogManager) GetWALReaderForStreaming(fromLSN LSN, slotName string) (*WALStreamReader, error) {
	lm.slotsMtx.Lock()
	slot, exists := lm.replicationSlots[slotName]

	// LSNs are 1-based. A request for LSN 0 means start from the very beginning.
	// We handle this by translating it to a request for LSN 1.
	effectiveFromLSN := fromLSN
	if effectiveFromLSN == 0 {
		lm.logger.Debug("Received request for LSN 0, treating as LSN 1.", zap.String("slotName", slotName))
		effectiveFromLSN = 1
	}

	if !exists {
		lm.logger.Info("Creating new replication slot", zap.String("slotName", slotName), zap.Uint64("fromLSN", uint64(effectiveFromLSN)))
		slot = &ReplicationSlot{
			SlotName:      slotName,
			RestartLSN:    effectiveFromLSN,
			IsActive:      true,
			LastHeartbeat: time.Now(),
		}
		lm.replicationSlots[slotName] = slot
	} else {
		slot.IsActive = true
		slot.LastHeartbeat = time.Now()
		// If the replica requests an LSN that is older than what we have, we respect it.
		// A more advanced implementation might reject this.
		if effectiveFromLSN < slot.RestartLSN {
			lm.logger.Warn("Replica requested an older LSN than tracked.",
				zap.String("slot", slotName),
				zap.Uint64("requested", uint64(effectiveFromLSN)),
				zap.Uint64("tracked", uint64(slot.RestartLSN)))
		}
		// Always update the RestartLSN to what was requested (after validation)
		slot.RestartLSN = effectiveFromLSN
	}
	lm.slotsMtx.Unlock()

	// Persist slot changes
	if err := lm.persistReplicationSlots(); err != nil {
		return nil, fmt.Errorf("failed to persist replication slot: %w", err)
	}

	reader := &WALStreamReader{
		logManager: lm,
		slotName:   slotName,
		stopChan:   make(chan struct{}),
	}
	reader.wg.Add(1)

	// Open the initial segment using the effective LSN
	if err := reader.openSegmentForLSN(slot.RestartLSN); err != nil {
		lm.logger.Warn("Failed to open segment from LSN.",
			zap.String("slot", slotName),
			zap.Uint64("requested", uint64(effectiveFromLSN)),
			zap.Uint64("tracked", uint64(slot.RestartLSN)))
		return nil, err
	}

	return reader, nil
}

// Next reads the next log record from the stream. It blocks until a new record is available
// or the reader is closed. This function is now rewritten to manually parse the binary format.
func (r *WALStreamReader) Next(logRecord *LogRecord) ([]byte, error) {
	for {
		r.logManager.logger.Debug("Calling Next", zap.Any("Currentfile", r.currentFile.Name()))
		select {
		case <-r.stopChan:
			return nil, io.EOF // Or a more specific error like ErrReaderClosed
		default:
			// Step 1: Read the size of the record first (4 bytes).
			var recordSize uint32
			if err := binary.Read(r.currentFile, binary.BigEndian, &recordSize); err != nil {
				if err == io.EOF {
					r.logManager.logger.Debug("End of the file", zap.Any("Currentfile", r.currentFile))
					// Reached the end of the current file. We need to check if a new segment exists.
					nextSegmentID := r.currentSegmentID + 1
					r.logManager.segmentMetaMtx.RLock()
					_, nextSegmentExists := r.logManager.segmentStartLSNs[nextSegmentID]
					r.logManager.segmentMetaMtx.RUnlock()

					if nextSegmentExists {
						// Switch to the next segment
						r.logManager.logger.Debug("Switching to next WAL segment for streaming", zap.Int("nextSegmentID", nextSegmentID))
						r.currentFile.Close()
						if openErr := r.openSegment(nextSegmentID); openErr != nil {
							return nil, openErr
						}
						continue // Retry reading from the new segment
					}

					// We've hit the end of the latest segment, wait for more data.
					r.logManager.mtx.Lock()
					select {
					case <-r.stopChan: // Check again in case of race
						r.logManager.mtx.Unlock()
						return nil, io.EOF
					default:
						r.logManager.cond.Wait()
					}
					r.logManager.mtx.Unlock()
					continue // Retry reading after being woken up
				}
				// A real error occurred while reading the size
				r.logManager.logger.Error("Error reading log record size", zap.Error(err))
				return nil, err
			}

			// Step 2: Read the full record data into a buffer.
			recordData := make([]byte, recordSize)
			if _, err := io.ReadFull(r.currentFile, recordData); err != nil {
				// This is a critical error, might indicate a truncated/corrupt WAL file.
				r.logManager.logger.Error("Failed to read full log record from WAL stream", zap.Error(err), zap.Uint32("expectedSize", recordSize))
				return nil, err
			}

			logRecord, err := DecodeLogRecord(recordData)
			if err != nil {
				return nil, err
			}

			// Step 4: Decrypt payload if encryption is enabled.
			// if r.logManager.cryptoUtil != nil {
			// 	decryptedPayload, decryptErr := r.logManager.cryptoUtil.Decrypt(logRecord.Data)
			// 	if decryptErr != nil {
			// 		r.logManager.logger.Error("Failed to decrypt log payload", zap.Error(decryptErr), zap.Uint64("lsn", uint64(logRecord.LSN)))
			// 		return fmt.Errorf("failed to decrypt log record at LSN %d: %w", logRecord.LSN, decryptErr)
			// 	}
			// 	logRecord.Data = decryptedPayload
			// }
			// Step 5: Update the replication slot's progress.
			r.logManager.UpdateSlotLSN(r.slotName, logRecord.LSN+1)
			return recordData, nil
		}
	}
}

// Close stops the reader and marks the replication slot as inactive.
func (r *WALStreamReader) Close() error {
	close(r.stopChan)
	r.logManager.cond.Broadcast() // Wake up the reader if it's waiting, so it can exit
	r.wg.Wait()

	r.logManager.slotsMtx.Lock()
	if slot, exists := r.logManager.replicationSlots[r.slotName]; exists {
		slot.IsActive = false
		slot.LastHeartbeat = time.Now()
	}
	r.logManager.slotsMtx.Unlock()

	if err := r.logManager.persistReplicationSlots(); err != nil {
		r.logManager.logger.Error("Failed to persist slot state on close", zap.Error(err))
	}

	if r.currentFile != nil {
		return r.currentFile.Close()
	}
	return nil
}

func (r *WALStreamReader) openSegmentForLSN(lsn LSN) error {
	r.logManager.segmentMetaMtx.RLock()
	defer r.logManager.segmentMetaMtx.RUnlock()

	var targetSegmentID = 1
	var latestSegmentID = 1
	var latestSegmentStartLSN LSN = 0

	// Find the segment containing the LSN
	for id, startLSN := range r.logManager.segmentStartLSNs {
		if id > latestSegmentID {
			latestSegmentID = id
			latestSegmentStartLSN = startLSN
		}
		if lsn >= startLSN {
			if targetSegmentID == -1 || startLSN > r.logManager.segmentStartLSNs[targetSegmentID] {
				targetSegmentID = id
			}
		}
	}

	if targetSegmentID == -1 {
		if lsn >= latestSegmentStartLSN && latestSegmentID != -1 {
			targetSegmentID = latestSegmentID
		} else {
			return fmt.Errorf("could not find WAL segment for LSN %d. It may have been pruned", lsn)
		}
	}
	r.logManager.logger.Debug("Found the target segment ID", zap.Any("segmentId", targetSegmentID))
	if err := r.openSegment(targetSegmentID); err != nil {
		return err
	}
	r.logManager.logger.Debug("Current wal file ", zap.Any("filename", r.currentFile))

	// Scan from the beginning of the segment to find the exact record
	for {
		// Store the current position before reading, so we can seek back if we overshoot.
		startPos, err := r.currentFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to get current position in WAL segment %d: %w", r.currentSegmentID, err)
		}

		var recordSize uint32
		err = binary.Read(r.currentFile, binary.BigEndian, &recordSize)
		if err == io.EOF {
			// We've reached the end of the file while searching for the starting LSN.
			// This is not an error; it just means the log we're looking for hasn't been written yet.
			// The reader is correctly positioned at the end, and the Next() method will wait.
			r.logManager.logger.Info("Reached end of segment while seeking for start LSN. Reader will wait for new records.",
				zap.Uint64("requestedLSN", uint64(lsn)),
				zap.Int("segmentID", r.currentSegmentID))
			return nil
		}
		if err != nil {
			return fmt.Errorf("error while seeking to LSN %d in segment %d (reading size): %w", lsn, targetSegmentID, err)
		}

		recordData := make([]byte, recordSize)
		if _, err := io.ReadFull(r.currentFile, recordData); err != nil {
			// This could be an unexpected EOF if the file is truncated.
			r.logManager.logger.Warn("Could not read full record while seeking; file may be truncated. Reader will wait.",
				zap.Uint64("requestedLSN", uint64(lsn)), zap.Error(err))
			// Seek back to before this failed read and let Next() handle waiting.
			r.currentFile.Seek(startPos, io.SeekStart)
			return nil
		}

		currentLSN := LSN(binary.BigEndian.Uint64(recordData[0:8]))
		if currentLSN >= lsn {
			// We found the record we need to start at (or the one just after it).
			// Seek back to the beginning of this record so Next() can read it.
			_, err := r.currentFile.Seek(startPos, io.SeekStart)
			if err != nil {
				return fmt.Errorf("failed to seek back to start of record for LSN %d: %w", lsn, err)
			}
			r.logManager.logger.Info("Starting stream",
				zap.String("slot", r.slotName),
				zap.Uint64("requestedLSN", uint64(lsn)),
				zap.Uint64("actualStartLSN", uint64(currentLSN)),
				zap.Int64("seekPosition", startPos))
			return nil
		}
	}
}

func (r *WALStreamReader) openSegment(segmentID int) error {
	// FIX: Removed the incorrect negation of segmentID.
	// segmentID = -1 * segmentID
	filePath := filepath.Join(r.logManager.walDir, fmt.Sprintf("%s%020d%s", walFilePrefix, segmentID, walFileSuffix))
	r.logManager.logger.Debug("Segment filePath: ", zap.Any("filepath", filePath))
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("could not open WAL segment %d for streaming: %w", segmentID, err)
	}
	r.logManager.logger.Debug("Segment filePath: ", zap.Any("filepath", filePath))
	r.currentFile = file
	r.logManager.logger.Debug("Segment file: ", zap.Any("file", r.currentFile))
	r.currentSegmentID = segmentID
	r.logManager.logger.Debug("Segment ID: ", zap.Any("ID", r.currentSegmentID))
	// FIX: No longer need a gob decoder.
	// r.decoder = gob.NewDecoder(file)
	return nil
}

// recover attempts to restore the LogManager state from existing WAL files.
func (lm *LogManager) recover() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	files, err := os.ReadDir(lm.walDir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory %s: %w", lm.walDir, err)
	}

	var lastSegmentID uint64 = 0
	var lastLSN LSN = 0
	foundSegments := false

	for _, file := range files {
		if file.IsDir() || !strings.HasPrefix(file.Name(), walFilePrefix) || !strings.HasSuffix(file.Name(), walFileSuffix) {
			continue
		}
		foundSegments = true
		segmentIDStr := strings.TrimSuffix(strings.TrimPrefix(file.Name(), walFilePrefix), walFileSuffix)
		segmentID, err := strconv.ParseUint(segmentIDStr, 10, 64)
		if err != nil {
			lm.logger.Warn("Skipping invalid WAL segment file name", zap.String("filename", file.Name()), zap.Error(err))
			continue
		}

		if segmentID >= lastSegmentID { // Find the highest segment ID
			segmentPath := filepath.Join(lm.walDir, file.Name())
			tempLastLSN, err := lm.findLastLSNInSegment(segmentPath)
			if err != nil {
				lm.logger.Error("Failed to find last LSN in segment, segment might be corrupt or empty", zap.String("segmentPath", segmentPath), zap.Error(err))
				// If the highest segment is problematic, this could be an issue.
				// For now, we'll just take the segment ID and assume LSN starts from 0 if it's a new segment or this fails.
				// A more robust recovery would try to repair or truncate.
			}

			// If this segment is definitely later or findLastLSNInSegment succeeded for it
			if segmentID > lastSegmentID || (segmentID == lastSegmentID && tempLastLSN > lastLSN) {
				lastSegmentID = segmentID
				if tempLastLSN > 0 { // Only update if a valid LSN was found
					lastLSN = tempLastLSN
				}
			}
		}
	}

	if !foundSegments {
		// No WAL segments found, start fresh
		lm.currentSegmentID = 1
		lm.currentLSN = 0 // LSNs are 1-based, so first LSN will be 1
		lm.logger.Info("No existing WAL segments found, starting with new segment.", zap.Uint64("segmentID", lm.currentSegmentID))
		return lm.openNewSegment(lm.currentSegmentID)
	}

	// Found existing segments, open the last one
	lm.currentSegmentID = lastSegmentID
	lm.currentLSN = lastLSN // The next LSN will be lastLSN + 1
	segmentPath := filepath.Join(lm.walDir, fmt.Sprintf("%s%020d%s", walFilePrefix, lm.currentSegmentID, walFileSuffix))

	lm.logger.Info("Recovered from existing WAL segments",
		zap.Uint64("lastSegmentID", lm.currentSegmentID),
		zap.Uint64("lastLSN", uint64(lm.currentLSN)))

	f, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_RDWR, 0640)
	if err != nil {
		// If append fails, it might be due to corruption or incomplete write.
		// A more complex recovery might try to truncate the last entry or start a new segment.
		// For now, try opening a new segment if the last one is problematic.
		lm.logger.Error("Failed to open last WAL segment for append, attempting to start new segment", zap.String("path", segmentPath), zap.Error(err))
		lm.currentSegmentID++
		lm.currentLSN = 0 // Reset LSN if starting a new segment due to unrecoverable old one.
		return lm.openNewSegment(lm.currentSegmentID)
	}
	lm.currentSegmentFile = f

	// Verify file size and potentially roll over if it's already too large (e.g. due to crash mid-write)
	stat, err := lm.currentSegmentFile.Stat()
	if err != nil {
		lm.logger.Error("Failed to stat current WAL segment after opening", zap.Error(err))
		// Potentially close and try new segment
		lm.currentSegmentFile.Close()
		lm.currentSegmentID++
		lm.currentLSN = 0
		return lm.openNewSegment(lm.currentSegmentID)
	}
	if stat.Size() >= lm.maxSegmentSize {
		if err := lm.rollOverSegment(); err != nil {
			return fmt.Errorf("failed to roll over segment during recovery: %w", err)
		}
	}

	return nil
}

// findLastLSNInSegment reads a WAL segment file and returns the LSN of the last valid record.
func (lm *LogManager) findLastLSNInSegment(segmentPath string) (LSN, error) {
	file, err := os.Open(segmentPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open segment %s: %w", segmentPath, err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var lastReadLSN LSN = 0
	var record LogRecord
	var recordSize uint32

	for {
		// Read record size
		err = binary.Read(reader, binary.BigEndian, &recordSize)
		if err == io.EOF {
			break // End of file, successfully read all records
		}
		if err != nil {
			return lastReadLSN, fmt.Errorf("error reading record size in %s at LSN approx %d: %w", segmentPath, lastReadLSN, err)
		}
		if recordSize == 0 { // Should not happen with valid records
			return lastReadLSN, fmt.Errorf("encountered zero record size in %s", segmentPath)
		}

		recordData := make([]byte, recordSize)
		_, err = io.ReadFull(reader, recordData)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Truncated record, the last valid LSN is the one before this attempt
			lm.logger.Warn("Found truncated record at end of segment", zap.String("segmentPath", segmentPath), zap.Uint64("approx_lsn", uint64(lastReadLSN)))
			return lastReadLSN, nil // Return the LSN of the last successfully read record
		}
		if err != nil {
			return lastReadLSN, fmt.Errorf("error reading record data in %s at LSN approx %d: %w", segmentPath, lastReadLSN, err)
		}

		if len(recordData) < 8 { // Size of LSN (uint64)
			lm.logger.Warn("Record data too short to contain LSN", zap.String("segmentPath", segmentPath), zap.Uint32("recordSize", recordSize))
			return lastReadLSN, fmt.Errorf("record data too short")
		}
		currentLSN := LSN(binary.BigEndian.Uint64(recordData[:8]))

		if currentLSN == 0 { // LSNs should be > 0
			return lastReadLSN, fmt.Errorf("found LSN 0 in segment %s", segmentPath)
		}

		record.LSN = currentLSN
		lastReadLSN = record.LSN
	}
	return lastReadLSN, nil
}

func (lm *LogManager) openNewSegment(segmentID uint64) error {
	segmentPath := filepath.Join(lm.walDir, fmt.Sprintf("%s%020d%s", walFilePrefix, segmentID, walFileSuffix))
	f, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0640) // Use 0640 permissions
	if err != nil {
		return fmt.Errorf("failed to create new WAL segment %s: %w", segmentPath, err)
	}
	lm.currentSegmentFile = f
	lm.currentSegmentID = segmentID
	lm.logger.Info("Opened new WAL segment", zap.String("path", segmentPath), zap.Uint64("segmentID", lm.currentSegmentID))
	return nil
}

func (lm *LogManager) persistReplicationSlots() error {
	lm.slotsMtx.RLock()
	defer lm.slotsMtx.RUnlock()

	data, err := json.MarshalIndent(lm.replicationSlots, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(lm.slotsFilePath, data, 0644)
}

// AppendRecord appends a log record to the WAL.
func (lm *LogManager) AppendRecord(lr *LogRecord, logType LogType) (LSN, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.currentLSN++
	lsn := lm.currentLSN

	encoded, err := lr.Encode()
	if err != nil {
		return 0, fmt.Errorf("failed to encode log record: %w", err)
	}

	currentFileSize, err := lm.currentSegmentFile.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("failed to seek current WAL segment: %w", err)
	}

	if currentFileSize+int64(lr.RecordSize) > lm.maxSegmentSize {
		if err := lm.rollOverSegment(); err != nil {
			return 0, fmt.Errorf("failed to roll over WAL segment: %w", err)
		}
	}

	if err := binary.Write(lm.currentSegmentFile, binary.BigEndian, lr.RecordSize); err != nil {
		return 0, fmt.Errorf("failed to write record size to WAL: %w", err)
	}
	if _, err := lm.currentSegmentFile.Write(encoded); err != nil {
		lm.logger.Fatal("FATAL: Failed to write record data to WAL. WAL may be corrupt.", zap.Error(err))
		return 0, fmt.Errorf("failed to write record data to WAL: %w", err)
	}

	return lsn, nil
}

func (lm *LogManager) rollOverSegment() error {
	if lm.currentSegmentFile != nil {
		if err := lm.currentSegmentFile.Sync(); err != nil {
			lm.logger.Error("Failed to sync current WAL segment before roll over", zap.Error(err))
		}
		if err := lm.currentSegmentFile.Close(); err != nil {
			lm.logger.Error("Failed to close current WAL segment before roll over", zap.Error(err))
		}
		lm.logger.Info("Closed WAL segment for rollover", zap.Uint64("segmentID", lm.currentSegmentID))
	}
	lm.currentSegmentID++
	return lm.openNewSegment(lm.currentSegmentID)
}

// Sync forces all buffered data to be written to disk.
func (lm *LogManager) Sync() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if lm.currentSegmentFile != nil {
		return lm.currentSegmentFile.Sync()
	}
	return nil
}

// Close closes the LogManager and the current WAL file.
func (lm *LogManager) Close() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if lm.currentSegmentFile != nil {
		err := lm.currentSegmentFile.Sync()
		if err != nil {
			lm.logger.Error("Failed to sync WAL on close", zap.Error(err))
		}
		closeErr := lm.currentSegmentFile.Close()
		lm.currentSegmentFile = nil
		lm.logger.Info("LogManager closed.", zap.Uint64("lastLSN", uint64(lm.currentLSN)))
		return closeErr
	}
	return nil
}

// --- Replication Slot Management ---

// CreateReplicationSlot creates a new replication slot.
func (lm *LogManager) CreateReplicationSlot(slotName, consumerNodeID, indexType string, startLSN LSN) (*ReplicationSlot, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if _, exists := lm.replicationSlots[slotName]; exists {
		return nil, fmt.Errorf("replication slot '%s' already exists", slotName)
	}
	slot := &ReplicationSlot{
		SlotName:       slotName,
		ConsumerNodeID: consumerNodeID,
		IndexType:      indexType,
		RequiredLSN:    startLSN,
		SnapshotLSN:    startLSN,
		IsActive:       true,
		LastHeartbeat:  time.Now(),
		CreationTime:   time.Now(),
	}
	lm.replicationSlots[slotName] = slot
	lm.logger.Info("Created replication slot", zap.String("slotName", slotName), zap.String("consumer", consumerNodeID), zap.Uint64("startLSN", uint64(startLSN)))
	return slot, nil
}

// UpdateReplicationSlot advances the RequiredLSN for a slot or updates its heartbeat.
func (lm *LogManager) UpdateReplicationSlot(slotName string, newRequiredLSN LSN, isActive bool) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	slot, exists := lm.replicationSlots[slotName]
	if !exists {
		return fmt.Errorf("replication slot '%s' not found", slotName)
	}
	if newRequiredLSN > lm.currentLSN {
		lm.logger.Warn("Attempt to update slot with LSN greater than current LSN",
			zap.String("slotName", slotName),
			zap.Uint64("newRequiredLSN", uint64(newRequiredLSN)),
			zap.Uint64("currentLSN", uint64(lm.currentLSN)))
	} else if newRequiredLSN < slot.RequiredLSN {
		lm.logger.Warn("Attempt to rewind RequiredLSN for slot (not allowed)",
			zap.String("slotName", slotName),
			zap.Uint64("currentRequiredLSN", uint64(slot.RequiredLSN)),
			zap.Uint64("attemptedLSN", uint64(newRequiredLSN)))
	} else {
		slot.RequiredLSN = newRequiredLSN
	}
	slot.IsActive = isActive
	slot.LastHeartbeat = time.Now()
	lm.logger.Debug("Updated replication slot", zap.String("slotName", slotName), zap.Uint64("newRequiredLSN", uint64(slot.RequiredLSN)), zap.Bool("isActive", slot.IsActive))
	return nil
}

// DropReplicationSlot removes a replication slot.
func (lm *LogManager) DropReplicationSlot(slotName string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if _, exists := lm.replicationSlots[slotName]; !exists {
		return fmt.Errorf("replication slot '%s' not found for dropping", slotName)
	}
	delete(lm.replicationSlots, slotName)
	lm.logger.Info("Dropped replication slot", zap.String("slotName", slotName))
	return nil
}

// GetMinRequiredLSNForAllSlots calculates the minimum LSN required by any active slot.
func (lm *LogManager) GetMinRequiredLSNForAllSlots() LSN {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	minLSN := lm.currentLSN
	if len(lm.replicationSlots) == 0 {
		return defaultMinLSNToKeep
	}

	firstActiveSlot := true
	for _, slot := range lm.replicationSlots {
		if slot.IsActive {
			if firstActiveSlot {
				minLSN = slot.RequiredLSN
				firstActiveSlot = false
			} else if slot.RequiredLSN < minLSN {
				minLSN = slot.RequiredLSN
			}
		}
	}
	if firstActiveSlot && len(lm.replicationSlots) > 0 {
		return lm.currentLSN
	}

	return minLSN
}

// PruneWALSegments archives or deletes WAL segments no longer needed.
func (lm *LogManager) PruneWALSegments(checkpointLSN LSN) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	minSlotLSN := lm.GetMinRequiredLSNForAllSlots()

	effectiveMinLSNToKeep := minSlotLSN
	if checkpointLSN > 0 && checkpointLSN < effectiveMinLSNToKeep {
	}

	lm.logger.Info("Attempting to prune WAL segments", zap.Uint64("minSlotRequiredLSN", uint64(effectiveMinLSNToKeep)), zap.Uint64("currentMaxLSN", uint64(lm.currentLSN)))

	files, err := os.ReadDir(lm.walDir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory for pruning: %w", err)
	}

	var segmentFiles []string
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), walFilePrefix) && strings.HasSuffix(file.Name(), walFileSuffix) {
			segmentFiles = append(segmentFiles, file.Name())
		}
	}
	sort.Strings(segmentFiles)

	for _, fileName := range segmentFiles {
		segmentIDStr := strings.TrimSuffix(strings.TrimPrefix(fileName, walFilePrefix), walFileSuffix)
		segmentID, _ := strconv.ParseUint(segmentIDStr, 10, 64)

		if segmentID >= lm.currentSegmentID {
			continue
		}

		lm.logger.Debug("Considering segment for pruning (actual pruning logic TBD)", zap.String("segmentFile", fileName), zap.Uint64("segmentID", segmentID))
	}

	return nil
}

// UpdateSlotLSN updates the restart LSN for a given slot.
func (lm *LogManager) UpdateSlotLSN(slotName string, lsn LSN) {
	lm.slotsMtx.Lock()
	defer lm.slotsMtx.Unlock()
	if slot, exists := lm.replicationSlots[slotName]; exists {
		if lsn > slot.RestartLSN {
			slot.RestartLSN = lsn
			slot.LastHeartbeat = time.Now()
		}
	}
}

// archiveSegment moves a WAL segment to the archive directory.
func (lm *LogManager) archiveSegment(segmentPath string) error {
	if _, err := os.Stat(segmentPath); os.IsNotExist(err) {
		lm.logger.Warn("WAL segment to archive does not exist", zap.String("path", segmentPath))
		return nil
	}

	fileName := filepath.Base(segmentPath)
	archivePath := filepath.Join(lm.archiveDir, fileName)

	err := os.Rename(segmentPath, archivePath)
	if err != nil {
		return fmt.Errorf("failed to move WAL segment %s to archive: %w", fileName, err)
	}
	lm.logger.Info("Archived WAL segment", zap.String("segment", fileName), zap.String("archivePath", archivePath))
	return nil
}

func (lm *LogManager) loadReplicationSlots() error {
	lm.slotsMtx.Lock()
	defer lm.slotsMtx.Unlock()

	data, err := os.ReadFile(lm.slotsFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var slots map[string]*ReplicationSlot
	if err := json.Unmarshal(data, &slots); err != nil {
		return err
	}

	lm.replicationSlots = slots
	for _, slot := range lm.replicationSlots {
		slot.IsActive = false
	}
	lm.logger.Info("Successfully loaded replication slots", zap.Int("count", len(lm.replicationSlots)))
	return nil
}
