package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time" // Added for LastHeartbeat

	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"go.uber.org/zap"
)

const (
	walFilePrefix            = "wal-"
	walFileSuffix            = ".log"
	defaultMaxWalSegmentSize = 16 * 1024 * 1024 // 16MB per WAL segment
	defaultArchiveDir        = "archive"
	defaultMinLSNToKeep      = LSN(0) // Default LSN to keep, can be updated by replication slots
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

type LogType uint

const (
	LogTypeBtree LogType = iota + 1
	LogTypeInvertedIndex
	LogTypeSpatial
)

// LogRecord represents a single log entry in the WAL.
type LogRecord struct {
	LSN        LSN
	Type       LogRecordType
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

// ReplicationSlot tracks a consumer of WAL records.
type ReplicationSlot struct {
	SlotName       string    `json:"slot_name"`        // Unique name for the slot (e.g., replica_node_id + "_" + index_type)
	ConsumerNodeID string    `json:"consumer_node_id"` // ID of the node consuming the WALs
	IndexType      string    `json:"index_type"`       // Type of index this slot is for (e.g., "btree", "inverted_index")
	RequiredLSN    LSN       `json:"required_lsn"`     // The oldest LSN this consumer still needs. WALs up to this LSN cannot be deleted.
	SnapshotLSN    LSN       `json:"snapshot_lsn"`     // The LSN at which the consumer started (e.g., from a snapshot).
	IsActive       bool      `json:"is_active"`        // Whether the slot is currently considered active for WAL retention.
	LastHeartbeat  time.Time `json:"last_heartbeat"`   // Last time the consumer confirmed activity.
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

	// Replication Slots Management
	replicationSlots map[string]*ReplicationSlot // Key: SlotName
}

// NewLogManager creates or opens a LogManager for the given directory.
func NewLogManager(walDir string) (*LogManager, error) {
	if err := os.MkdirAll(walDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory %s: %w", walDir, err)
	}

	archivePath := filepath.Join(walDir, defaultArchiveDir)
	if err := os.MkdirAll(archivePath, 0750); err != nil {
		return nil, fmt.Errorf("failed to create WAL archive directory %s: %w", archivePath, err)
	}

	// Initialize logger (ideally passed in, but creating a default one for now)
	logger, _ := zap.NewDevelopment() // TODO: Pass logger as argument

	lm := &LogManager{
		walDir:           walDir,
		maxSegmentSize:   defaultMaxWalSegmentSize,
		archiveDir:       archivePath,
		logger:           logger.Named("log_manager"),
		replicationSlots: make(map[string]*ReplicationSlot),
	}

	if err := lm.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover log manager: %w", err)
	}
	// TODO: Load replication slots from a persistent store (e.g., a metadata file in walDir or via FSM)
	// For now, they are in-memory and lost on restart.
	// lm.loadReplicationSlots()

	return lm, nil
}

func (l *LogManager) GetCurrentLSN() LSN {
	return l.currentLSN
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
		err = binary.Read(reader, binary.LittleEndian, &recordSize)
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

		// Deserialize record (simplified, assuming gob or similar)
		// For recovery, we only strictly need the LSN if the record is valid.
		// A full deserialization and CRC check is better.
		// For now, let's assume we can extract LSN without full gob. Let's refine record structure for this.
		// For this example, assume LSN is at the beginning of recordData.
		if len(recordData) < 8 { // Size of LSN (uint64)
			lm.logger.Warn("Record data too short to contain LSN", zap.String("segmentPath", segmentPath), zap.Uint32("recordSize", recordSize))
			return lastReadLSN, fmt.Errorf("record data too short")
		}
		currentLSN := LSN(binary.LittleEndian.Uint64(recordData[:8]))
		// TODO: Validate CRC of the recordData here
		// crcFound := binary.LittleEndian.Uint32(recordData[len(recordData)-4:])
		// calculatedCRC := crc32.ChecksumIEEE(recordData[:len(recordData)-4])
		// if crcFound != calculatedCRC {
		//    lm.logger.Warn("CRC mismatch for record", zap.Uint64("lsn", uint64(currentLSN)))
		//    return lastReadLSN, fmt.Errorf("CRC mismatch at LSN %d", currentLSN)
		// }
		if currentLSN == 0 { // LSNs should be > 0
			return lastReadLSN, fmt.Errorf("found LSN 0 in segment %s", segmentPath)
		}

		record.LSN = currentLSN // For this simplified example
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
	// Note: currentLSN is NOT reset here. It's carried over from the previous segment's last LSN.
	// If this is the very first segment (during initial recovery with no files), currentLSN is 0.
	lm.logger.Info("Opened new WAL segment", zap.String("path", segmentPath), zap.Uint64("segmentID", lm.currentSegmentID))
	return nil
}

// AppendRecord appends a log record to the WAL.
func (lm *LogManager) AppendRecord(lr *LogRecord, logType LogType) (LSN, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.currentLSN++
	lsn := lm.currentLSN

	// TODO: Populate PrevLSN if needed by recovery logic for transactions or pages
	record := LogRecord{
		LSN:       lsn,
		Type:      lr.Type,
		Timestamp: time.Now().UnixNano(),
		TxnID:     lr.TxnID,
		PageID:    lr.PageID,
		Data:      lr.Data,
		LogType:   logType, // Store the general log type
		SegmentID: lm.currentSegmentID,
	}

	// Serialize record (e.g., using gob or custom binary format)
	// For simplicity, let's assume a binary format:
	// LSN (8B) | Type (1B) | Timestamp (8B) | TxnID (8B) | PageID (4B) | DataLen (4B) | Data (variable) | CRC (4B)

	// Calculate size first
	// Size = 8(LSN) + 1(Type) + 8(Timestamp) + 8(TxnID) + 4(PageID) + 4(DataLen) + len(Data) + 4(CRC)
	headerSize := 8 + 1 + 8 + 8 + 4 + 4
	serializedRecordSize := uint32(headerSize + len(record.Data) + 4)
	record.RecordSize = serializedRecordSize

	buffer := make([]byte, serializedRecordSize)
	offset := 0
	binary.LittleEndian.PutUint64(buffer[offset:], uint64(record.LSN))
	offset += 8
	buffer[offset] = byte(record.Type)
	offset += 1
	binary.LittleEndian.PutUint64(buffer[offset:], uint64(record.Timestamp))
	offset += 8
	binary.LittleEndian.PutUint64(buffer[offset:], record.TxnID)
	offset += 8
	binary.LittleEndian.PutUint64(buffer[offset:], uint64(record.PageID))
	offset += 4
	binary.LittleEndian.PutUint32(buffer[offset:], uint32(len(record.Data)))
	offset += 4
	copy(buffer[offset:], record.Data)
	offset += len(record.Data)

	// TODO: Calculate CRC on buffer[:offset] and put it at buffer[offset:]
	// For now, placeholder CRC
	binary.LittleEndian.PutUint32(buffer[offset:], 0xDEADBEEF) // Placeholder CRC

	// Check if segment needs to be rolled over BEFORE writing the record + its size
	// The size of the on-disk entry is 4 (for recordSize) + serializedRecordSize itself
	onDiskEntrySize := int64(4 + serializedRecordSize)
	currentFileSize, err := lm.currentSegmentFile.Seek(0, io.SeekEnd) // Get current file size by seeking to end
	if err != nil {
		return 0, fmt.Errorf("failed to seek current WAL segment: %w", err)
	}

	if currentFileSize+onDiskEntrySize > lm.maxSegmentSize {
		if err := lm.rollOverSegment(); err != nil {
			return 0, fmt.Errorf("failed to roll over WAL segment: %w", err)
		}
		// After roll-over, currentFileSize is effectively 0 for the new segment.
	}

	// Write record size, then the record itself
	if err := binary.Write(lm.currentSegmentFile, binary.LittleEndian, serializedRecordSize); err != nil {
		return 0, fmt.Errorf("failed to write record size to WAL: %w", err)
	}
	if _, err := lm.currentSegmentFile.Write(buffer); err != nil {
		// This is a critical error. The WAL is now in an inconsistent state if part of the record was written.
		// A robust system might try to truncate the partial write or mark the segment as corrupt.
		lm.logger.Fatal("FATAL: Failed to write record data to WAL. WAL may be corrupt.", zap.Error(err))
		return 0, fmt.Errorf("failed to write record data to WAL: %w", err)
	}

	// Optionally sync to disk
	// lm.Sync()

	return lsn, nil
}

func (lm *LogManager) rollOverSegment() error {
	if lm.currentSegmentFile != nil {
		if err := lm.currentSegmentFile.Sync(); err != nil {
			lm.logger.Error("Failed to sync current WAL segment before roll over", zap.Error(err))
			// Continue with rollover, but log the error
		}
		if err := lm.currentSegmentFile.Close(); err != nil {
			lm.logger.Error("Failed to close current WAL segment before roll over", zap.Error(err))
			// Continue, but this is problematic
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
		err := lm.currentSegmentFile.Sync() // Ensure data is flushed before closing
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
// The FSM should persist this information. This LogManager method updates its in-memory view.
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
		RequiredLSN:    startLSN, // Initially, the consumer needs from here
		SnapshotLSN:    startLSN,
		IsActive:       true,
		LastHeartbeat:  time.Now(),
		CreationTime:   time.Now(),
	}
	lm.replicationSlots[slotName] = slot
	lm.logger.Info("Created replication slot", zap.String("slotName", slotName), zap.String("consumer", consumerNodeID), zap.Uint64("startLSN", uint64(startLSN)))
	// TODO: Persist this change if LogManager is responsible for slot persistence.
	// Typically, Controller/FSM manages this state, LogManager just uses it.
	return slot, nil
}

// UpdateReplicationSlot advances the RequiredLSN for a slot or updates its heartbeat.
// Called by a replica when it has successfully processed logs up to a certain LSN.
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
		// Don't update LSN beyond current, but can update activity/heartbeat
	} else if newRequiredLSN < slot.RequiredLSN {
		// This shouldn't happen if LSNs are only advanced.
		// Could happen if a replica tries to "rewind", which should be handled carefully.
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
	// TODO: Persist change
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
	// TODO: Persist change
	return nil
}

// GetMinRequiredLSNForAllSlots calculates the minimum LSN required by any active slot.
// This is used to determine which WAL segments can be safely archived/deleted.
func (lm *LogManager) GetMinRequiredLSNForAllSlots() LSN {
	lm.mu.RLock() // Use RLock for read-only access
	defer lm.mu.RUnlock()

	minLSN := lm.currentLSN // Start with current LSN, no slot can require more than this
	if len(lm.replicationSlots) == 0 {
		return defaultMinLSNToKeep // Or a system-wide configured minimum retention LSN
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
	// If no active slots, what should be the min LSN?
	// It should be based on checkpoint LSN or a configured retention period.
	// For now, if no active slots, it means WALs up to current can potentially be candidates for cleanup
	// based on other criteria (like checkpointing).
	// Let's assume if no active slots, the effective min required LSN is high, allowing cleanup.
	// This needs to be coordinated with checkpointing.
	if firstActiveSlot && len(lm.replicationSlots) > 0 { // No active slots found, but slots exist
		return lm.currentLSN // effectively no WAL retention needed by slots
	}

	return minLSN
}

// PruneWALSegments archives or deletes WAL segments no longer needed by any replication slot
// or by the checkpointing mechanism.
func (lm *LogManager) PruneWALSegments(checkpointLSN LSN) error {
	lm.mu.Lock() // Need full lock as we might be modifying files
	defer lm.mu.Unlock()

	minSlotLSN := lm.GetMinRequiredLSNForAllSlots() // RLock was inside, so this is fine.

	// The actual LSN to retain up to is the minimum of what slots need and what checkpoints need.
	// Checkpoints ensure recovery up to checkpointLSN, so WALs before that (that are part of the checkpoint)
	// might be prunable if no slots need them. This logic is complex.
	// For simplicity now: retain up to min(minSlotLSN, checkpointLSN if checkpointing is considered)
	// More simply, retain WALs needed by the OLDEST of (any active slot's required LSN) OR (the last checkpoint's LSN).
	// If checkpointLSN is 0 or very old, minSlotLSN dominates.
	// If minSlotLSN is very old (e.g. inactive replica), checkpointLSN might allow pruning some.

	// Simplified: We need WALs at least up to the oldest active slot's RequiredLSN.
	// And we need WALs since the last successful checkpoint for local crash recovery.
	// So, effectiveMinLSN = min(oldest_slot_required_LSN, last_checkpoint_start_LSN)
	// For now, let's use minSlotLSN as the primary driver for slot-based retention.
	// Checkpointing mechanism would have its own pruning logic which should be coordinated.

	effectiveMinLSNToKeep := minSlotLSN
	if checkpointLSN > 0 && checkpointLSN < effectiveMinLSNToKeep {
		// This case is tricky. If a checkpoint is *newer* than what a slot needs,
		// it implies the slot is lagging significantly. We still honor the slot.
		// If a checkpoint is *older* than what slots need, slots dictate retention.
		// If checkpointLSN is what we need for local recovery, and it's newer than minSlotLSN,
		// then we must keep up to checkpointLSN.
		// Let's be conservative: keep WALs needed for the older of the two.
		// Actually, we need WALs from the oldest of (what any slot requires) OR (what local recovery from last checkpoint requires)
		// So, if minSlotLSN is 100, and checkpoint LSN is 50, we need to keep from 50.
		// If minSlotLSN is 50, and checkpoint LSN is 100, we need to keep from 50.
		// So, targetLSNToKeep = min(minSlotLSN, checkpointLSNThatEnsuresRecovery)
		// This needs more thought with checkpointing. For now, just use minSlotLSN for slot-based pruning.
	}

	lm.logger.Info("Attempting to prune WAL segments", zap.Uint64("minSlotRequiredLSN", uint64(effectiveMinLSNToKeep)), zap.Uint64("currentMaxLSN", uint64(lm.currentLSN)))

	files, err := os.ReadDir(lm.walDir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory for pruning: %w", err)
	}

	// Sort files by segment ID to process them in order
	var segmentFiles []string
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), walFilePrefix) && strings.HasSuffix(file.Name(), walFileSuffix) {
			segmentFiles = append(segmentFiles, file.Name())
		}
	}
	sort.Strings(segmentFiles) // Sorts them lexicographically, which works for zero-padded numbers

	for _, fileName := range segmentFiles {
		//segmentPath := filepath.Join(lm.walDir, fileName)
		segmentIDStr := strings.TrimSuffix(strings.TrimPrefix(fileName, walFilePrefix), walFileSuffix)
		segmentID, _ := strconv.ParseUint(segmentIDStr, 10, 64)

		if segmentID >= lm.currentSegmentID {
			continue // Never prune the current segment or future ones (should not exist)
		}

		// To decide if a segment can be pruned, we need to know the LSN range it covers.
		// This is hard without reading the segment or storing LSN ranges per segment.
		// Approximation: if the *next* segment's first LSN is > effectiveMinLSNToKeep,
		// then this current segment *might* be prunable.
		// Better: The FSM or metadata should store first/last LSN for each segment file.

		// Simplified Pruning Logic (Placeholder):
		// Assumes segments are pruned if their ID is much lower than the segment ID of effectiveMinLSNToKeep.
		// This is NOT robust. A robust method needs to know LSNs within segments.
		// For now, we will not implement actual pruning here, just the framework.
		// Actual pruning would move files to lm.archiveDir and then potentially delete from archiveDir.

		// To implement robustly:
		// 1. Iterate through segment files.
		// 2. For each segment, determine the LSN of its *last* record.
		// 3. If segment_last_lsn < effectiveMinLSNToKeep, then this segment can be archived/deleted.
		// This requires reading each segment, which can be slow.
		// Optimization: Store first/last LSN per segment in a metadata file.

		lm.logger.Debug("Considering segment for pruning (actual pruning logic TBD)", zap.String("segmentFile", fileName), zap.Uint64("segmentID", segmentID))
		// Example: if segmentID < segmentIDContaining(effectiveMinLSNToKeep)
		//   archiveSegment(segmentPath)
	}

	return nil
}

// archiveSegment moves a WAL segment to the archive directory.
func (lm *LogManager) archiveSegment(segmentPath string) error {
	if _, err := os.Stat(segmentPath); os.IsNotExist(err) {
		lm.logger.Warn("WAL segment to archive does not exist", zap.String("path", segmentPath))
		return nil // Already gone
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

// GetWALReaderForStreaming returns an io.ReadCloser for streaming WAL records starting from a given LSN.
// The caller is responsible for closing the reader.
// This implementation needs to be ables to read across multiple segment files.
func (lm *LogManager) GetWALReaderForStreaming(startLSN LSN, forSlotName string) (io.ReadCloser, error) {
	lm.mu.RLock() // RLock as we are reading segments
	defer lm.mu.RUnlock()

	slot, exists := lm.replicationSlots[forSlotName]
	if !exists {
		return nil, fmt.Errorf("replication slot %s not found for streaming", forSlotName)
	}
	if !slot.IsActive {
		return nil, fmt.Errorf("replication slot %s is not active", forSlotName)
	}
	// Ensure startLSN is not older than what the slot itself requires (or its snapshot LSN)
	// This check might be refined based on slot semantics.
	if startLSN < slot.SnapshotLSN {
		lm.logger.Warn("Requested stream LSN is older than slot's snapshot LSN",
			zap.String("slot", forSlotName),
			zap.Uint64("request_lsn", uint64(startLSN)),
			zap.Uint64("slot_snapshot_lsn", uint64(slot.SnapshotLSN)))
		// Depending on policy, either error out or start from slot.SnapshotLSN
		// For now, let's allow it but log, assuming the caller knows what they're doing
		// (e.g. a full resync after a problem).
	}

	// Find the segment containing startLSN.
	// This requires metadata about LSN ranges per segment or iterating.
	// For now, placeholder. A real implementation needs to locate the correct starting segment and offset.
	lm.logger.Info("WAL streaming request", zap.Uint64("startLSN", uint64(startLSN)), zap.String("slot", forSlotName))
	// This would return a custom reader that can seamlessly transition across WAL segment files.
	// return NewWALStreamReader(lm.walDir, startLSN, lm.logger) // Hypothetical constructor
	return nil, fmt.Errorf("GetWALReaderForStreaming not fully implemented - requires multi-segment reading logic")
}

// Placeholder for ApplyLogRecord if LogManager itself is involved in applying replicated logs (unlikely)
// Typically, consumers of WAL (like index managers) have their own ApplyLogRecord methods.
// type LogType string // Already defined in your existing code, ensure it's used
// const (
// 	LogTypeBTree        LogType = "btree"
// 	LogTypeInvertedIndex LogType = "inverted_index"
// 	LogTypeSpatialIndex LogType = "spatial_index"
// 	// ... other log types
// )
// func (lm *LogManager) ApplyLogRecord(lr LogRecord, logType LogType) error {
//    return fmt.Errorf("ApplyLogRecord should be handled by specific index managers based on logType %s", logType)
// }

// TODO:
// - Persist replication slot information.
// - Implement robust WAL segment LSN tracking for efficient pruning and streaming.
// - Implement WALStreamReader for multi-segment reading.
// - Coordinate pruning with checkpointing mechanism (e.g., FSM tells LogManager about checkpoint LSNs).
// - Add methods to query slot status for monitoring.
// - Consider security if WAL files are directly exposed for streaming (encryption, access control).
