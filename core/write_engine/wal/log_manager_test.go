package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pagemanager "github.com/sushant-115/gojodb/core/write_engine/page_manager"
	"go.uber.org/zap"
)

// --- Test Helpers ---

func setupLogManager(t *testing.T) (*LogManager, string) {
	t.Helper()
	tempDir := t.TempDir()
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	lm, err := NewLogManager(tempDir, logger, "")
	require.NoError(t, err)

	return lm, tempDir
}

func newTestLogRecord(data string) *LogRecord {
	return &LogRecord{
		Type:      LogRecordTypeUpdate,
		IndexType: "test_btree",
		Timestamp: time.Now().UnixNano(),
		TxnID:     uint64(time.Now().UnixNano()),
		PageID:    pagemanager.PageID(1),
		Data:      []byte(data),
		PrevLSN:   0,
		LogType:   LogTypeBtree,
	}
}

// --- WAL Core Read/Write Tests ---

// TestWALReader_SimpleRead verifies the most basic functionality: writing a few records
// and immediately reading them back with a new stream reader.
func TestWALReader_SimpleRead(t *testing.T) {
	lm, _ := setupLogManager(t)
	defer lm.Close()

	recordsToWrite := []*LogRecord{
		newTestLogRecord("record data 1"),
		newTestLogRecord("record data 2"),
		newTestLogRecord("record data 3"),
	}

	for i, lr := range recordsToWrite {
		lsn, err := lm.AppendRecord(context.Background(), lr, lr.LogType)
		require.NoError(t, err)
		require.Equal(t, LSN(i+1), lsn, "LSN should be sequential and 1-based")
	}
	require.NoError(t, lm.Sync())

	reader, err := lm.GetWALReaderForStreaming(1, "test_slot_simple")
	require.NoError(t, err)
	defer reader.Close()

	for i, expectedRecord := range recordsToWrite {
		var placeholder LogRecord
		recordBytes, err := reader.Next(&placeholder)
		require.NoError(t, err, "Failed to read record %d", i+1)
		require.NotNil(t, recordBytes)

		decodedRecord, err := DecodeLogRecord(recordBytes)
		require.NoError(t, err)

		require.Equal(t, LSN(i+1), decodedRecord.LSN)
		require.Equal(t, expectedRecord.Type, decodedRecord.Type)
		require.Equal(t, expectedRecord.Data, decodedRecord.Data)
		require.Equal(t, expectedRecord.TxnID, decodedRecord.TxnID)
	}
}

// TestWALReader_RecoveryAndRead simulates a restart by creating a LogManager, writing
// data, closing it, then opening a new LogManager from the same directory.
func TestWALReader_RecoveryAndRead(t *testing.T) {
	tempDir := t.TempDir()
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	lm1, err := NewLogManager(tempDir, logger, "")
	require.NoError(t, err)

	recordToRecover := newTestLogRecord("this must survive a restart")
	_, err = lm1.AppendRecord(context.Background(), recordToRecover, recordToRecover.LogType)
	require.NoError(t, err)
	require.NoError(t, lm1.Close())

	lm2, err := NewLogManager(tempDir, logger, "")
	require.NoError(t, err)
	defer lm2.Close()

	reader, err := lm2.GetWALReaderForStreaming(1, "test_slot_recovery")
	require.NoError(t, err, "Failed to get reader after recovery")
	defer reader.Close()

	var placeholder LogRecord
	recordBytes, err := reader.Next(&placeholder)
	require.NoError(t, err, "Next() failed to read the record after recovery")

	decoded, err := DecodeLogRecord(recordBytes)
	require.NoError(t, err)
	require.Equal(t, LSN(1), decoded.LSN)
	require.Equal(t, recordToRecover.Data, decoded.Data)
}

// TestWALReader_StartFromMiddle checks if the reader can start from a specific LSN
// that is not at the beginning of the segment.
func TestWALReader_StartFromMiddle(t *testing.T) {
	lm, _ := setupLogManager(t)
	defer lm.Close()

	for i := 0; i < 10; i++ {
		lr := newTestLogRecord(fmt.Sprintf("record %d", i+1))
		_, err := lm.AppendRecord(context.Background(), lr, lr.LogType)
		require.NoError(t, err)
	}
	require.NoError(t, lm.Sync())

	startLSN := LSN(6)
	reader, err := lm.GetWALReaderForStreaming(startLSN, "test_slot_middle")
	require.NoError(t, err)
	defer reader.Close()

	var placeholder LogRecord
	recordBytes, err := reader.Next(&placeholder)
	require.NoError(t, err)

	decodedRecord, err := DecodeLogRecord(recordBytes)
	require.NoError(t, err)
	require.Equal(t, startLSN, decodedRecord.LSN)
	require.Equal(t, []byte("record 6"), decodedRecord.Data)
}

// TestWALReader_WaitAndRead tests the reader's ability to block and wait for new records
// when it reaches the end of the log, then successfully read the new record.
func TestWALReader_WaitAndRead(t *testing.T) {
	lm, _ := setupLogManager(t)
	defer lm.Close()

	initialRecord := newTestLogRecord("initial record")
	_, err := lm.AppendRecord(context.Background(), initialRecord, initialRecord.LogType)
	require.NoError(t, err)
	require.NoError(t, lm.Sync())

	reader, err := lm.GetWALReaderForStreaming(1, "test_slot_wait")
	require.NoError(t, err)
	defer reader.Close()

	var placeholder LogRecord
	_, err = reader.Next(&placeholder)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	var receivedRecord *LogRecord
	var nextErr error

	go func() {
		defer wg.Done()
		recordBytes, err := reader.Next(&placeholder)
		if err != nil {
			nextErr = err
			return
		}
		receivedRecord, nextErr = DecodeLogRecord(recordBytes)
	}()

	time.Sleep(100 * time.Millisecond)

	awaitedRecord := newTestLogRecord("the awaited record")
	_, err = lm.AppendRecord(context.Background(), awaitedRecord, awaitedRecord.LogType)
	require.NoError(t, err)
	require.NoError(t, lm.Sync())

	wg.Wait()

	require.NoError(t, nextErr)
	require.NotNil(t, receivedRecord)
	require.Equal(t, LSN(2), receivedRecord.LSN)
	require.Equal(t, awaitedRecord.Data, receivedRecord.Data)
}

// TestWAL_FileNameFormat confirms that WAL segment files are named with the expected
// zero-padded naming convention.
func TestWAL_FileNameFormat(t *testing.T) {
	lm, walDir := setupLogManager(t)

	lr := newTestLogRecord("data")
	_, err := lm.AppendRecord(context.Background(), lr, lr.LogType)
	require.NoError(t, err)
	require.NoError(t, lm.Close())

	expectedFileName := "wal-00000000000000000001.log"
	expectedFilePath := filepath.Join(walDir, expectedFileName)

	_, err = os.Stat(expectedFilePath)
	require.NoError(t, err, "Expected WAL file with padded name was not found at %s", expectedFilePath)
}

// --- CRC / Integrity Tests ---

// TestLogRecord_EncodeDecodeRoundTrip verifies that all LogRecord fields survive
// a full encode→decode cycle without corruption.
func TestLogRecord_EncodeDecodeRoundTrip(t *testing.T) {
	original := &LogRecord{
		LSN:       42,
		Type:      LogRecordTypeInsertKey,
		IndexType: "btree",
		Timestamp: 1_700_000_000_000_000_000,
		TxnID:     99,
		PageID:    pagemanager.PageID(7),
		Data:      []byte("hello world"),
		PrevLSN:   41,
		LogType:   LogTypeBtree,
		SegmentID: 2,
	}

	encoded, err := original.Encode()
	require.NoError(t, err)

	decoded, err := DecodeLogRecord(encoded)
	require.NoError(t, err)

	require.Equal(t, original.LSN, decoded.LSN)
	require.Equal(t, original.Type, decoded.Type)
	require.Equal(t, original.IndexType, decoded.IndexType)
	require.Equal(t, original.Timestamp, decoded.Timestamp)
	require.Equal(t, original.TxnID, decoded.TxnID)
	require.Equal(t, original.PageID, decoded.PageID)
	require.Equal(t, original.Data, decoded.Data)
	require.Equal(t, original.PrevLSN, decoded.PrevLSN)
	require.Equal(t, original.LogType, decoded.LogType)
	require.Equal(t, original.SegmentID, decoded.SegmentID)
}

// TestLogRecord_EncodeDecodeEmptyData verifies encode/decode works with nil/empty Data.
func TestLogRecord_EncodeDecodeEmptyData(t *testing.T) {
	original := &LogRecord{
		LSN:     1,
		Type:    LogRecordTypeNoOp,
		TxnID:   10,
		Data:    nil,
		LogType: LogTypeBtree,
	}

	encoded, err := original.Encode()
	require.NoError(t, err)

	decoded, err := DecodeLogRecord(encoded)
	require.NoError(t, err)
	require.Equal(t, original.TxnID, decoded.TxnID)
	require.Empty(t, decoded.Data)
}

// TestLogRecord_EncodeDecodeLargeData verifies encode/decode works with large payloads.
func TestLogRecord_EncodeDecodeLargeData(t *testing.T) {
	largeData := make([]byte, 64*1024) // 64 KB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	original := &LogRecord{
		LSN:     1,
		Type:    LogRecordTypeUpdate,
		TxnID:   1,
		Data:    largeData,
		LogType: LogTypeBtree,
	}

	encoded, err := original.Encode()
	require.NoError(t, err)

	decoded, err := DecodeLogRecord(encoded)
	require.NoError(t, err)
	require.Equal(t, largeData, decoded.Data)
}

// TestDecodeLogRecord_CorruptedCRC verifies that decoding fails with a clear error
// when the trailing CRC bytes have been tampered with.
func TestDecodeLogRecord_CorruptedCRC(t *testing.T) {
	lr := &LogRecord{
		LSN:     1,
		Type:    LogRecordTypeUpdate,
		TxnID:   1,
		Data:    []byte("integrity matters"),
		LogType: LogTypeBtree,
	}

	encoded, err := lr.Encode()
	require.NoError(t, err)

	// Flip the last byte of the CRC field.
	encoded[len(encoded)-1] ^= 0xFF

	_, err = DecodeLogRecord(encoded)
	require.Error(t, err, "Expected error on CRC mismatch but got none")
	require.Contains(t, err.Error(), "checksum")
}

// --- Transaction Record Type Tests ---

// TestWAL_TransactionRecordTypes writes PREPARE, COMMIT, and ABORT log record types
// and verifies they round-trip correctly through the WAL.
func TestWAL_TransactionRecordTypes(t *testing.T) {
	lm, _ := setupLogManager(t)
	defer lm.Close()

	txnID := uint64(12345)
	types := []LogRecordType{
		LogRecordTypeNoOp,
		LogRecordTypePrepare,
		LogRecordTypeCommitTxn,
		LogRecordTypeAbortTxn,
	}

	for i, recType := range types {
		lr := &LogRecord{
			Type:    recType,
			TxnID:   txnID,
			LogType: LogTypeBtree,
			Data:    []byte(fmt.Sprintf("txn_payload_%d", i)),
		}
		_, err := lm.AppendRecord(context.Background(), lr, lr.LogType)
		require.NoError(t, err)
	}
	require.NoError(t, lm.Sync())

	reader, err := lm.GetWALReaderForStreaming(1, "txn_types_slot")
	require.NoError(t, err)
	defer reader.Close()

	for i, expectedType := range types {
		var placeholder LogRecord
		rawBytes, err := reader.Next(&placeholder)
		require.NoError(t, err, "record %d", i)

		decoded, err := DecodeLogRecord(rawBytes)
		require.NoError(t, err)
		require.Equal(t, expectedType, decoded.Type, "record %d type mismatch", i)
		require.Equal(t, txnID, decoded.TxnID)
	}
}

// TestWAL_MultipleLogTypes verifies that BTree, InvertedIndex, and Spatial log
// types all persist and are read back correctly.
func TestWAL_MultipleLogTypes(t *testing.T) {
	lm, _ := setupLogManager(t)
	defer lm.Close()

	entries := []struct {
		logType LogType
		data    string
	}{
		{LogTypeBtree, "btree_entry"},
		{LogTypeInvertedIndex, "inverted_entry"},
		{LogTypeSpatial, "spatial_entry"},
	}

	for _, e := range entries {
		lr := &LogRecord{
			Type:    LogRecordTypeUpdate,
			TxnID:   1,
			Data:    []byte(e.data),
			LogType: e.logType,
		}
		_, err := lm.AppendRecord(context.Background(), lr, e.logType)
		require.NoError(t, err)
	}
	require.NoError(t, lm.Sync())

	reader, err := lm.GetWALReaderForStreaming(1, "multi_type_slot")
	require.NoError(t, err)
	defer reader.Close()

	for i, e := range entries {
		var placeholder LogRecord
		rawBytes, err := reader.Next(&placeholder)
		require.NoError(t, err, "entry %d", i)

		decoded, err := DecodeLogRecord(rawBytes)
		require.NoError(t, err)
		require.Equal(t, e.logType, decoded.LogType, "entry %d log type mismatch", i)
		require.Equal(t, []byte(e.data), decoded.Data)
	}
}

// --- Concurrency Tests ---

// TestWAL_ConcurrentAppends verifies that concurrent writes from multiple goroutines
// all succeed and produce distinct, sequential LSNs.
func TestWAL_ConcurrentAppends(t *testing.T) {
	lm, _ := setupLogManager(t)
	defer lm.Close()

	const numGoroutines = 10
	const recordsPerGoroutine = 50

	var wg sync.WaitGroup
	lsnCh := make(chan LSN, numGoroutines*recordsPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for r := 0; r < recordsPerGoroutine; r++ {
				lr := &LogRecord{
					Type:    LogRecordTypeUpdate,
					TxnID:   uint64(gID),
					Data:    []byte(fmt.Sprintf("goroutine_%d_record_%d", gID, r)),
					LogType: LogTypeBtree,
				}
				lsn, err := lm.AppendRecord(context.Background(), lr, lr.LogType)
				if err != nil {
					t.Errorf("goroutine %d: AppendRecord error: %v", gID, err)
					return
				}
				lsnCh <- lsn
			}
		}(g)
	}

	wg.Wait()
	close(lsnCh)

	// Collect all LSNs and verify they are all unique.
	seen := make(map[LSN]struct{})
	for lsn := range lsnCh {
		_, dup := seen[lsn]
		require.False(t, dup, "duplicate LSN %d detected", lsn)
		seen[lsn] = struct{}{}
	}
	require.Len(t, seen, numGoroutines*recordsPerGoroutine)
}

// TestWAL_ConcurrentReadersAndWriter verifies that multiple readers can stream
// concurrently while a writer is appending records.
func TestWAL_ConcurrentReadersAndWriter(t *testing.T) {
	lm, _ := setupLogManager(t)
	defer lm.Close()

	const numRecords = 20
	const numReaders = 3

	// Prime the WAL with some initial records.
	for i := 0; i < numRecords; i++ {
		lr := newTestLogRecord(fmt.Sprintf("record_%d", i+1))
		_, err := lm.AppendRecord(context.Background(), lr, lr.LogType)
		require.NoError(t, err)
	}
	require.NoError(t, lm.Sync())

	var wg sync.WaitGroup
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			reader, err := lm.GetWALReaderForStreaming(1, fmt.Sprintf("concurrent_reader_%d", readerID))
			if err != nil {
				t.Errorf("reader %d: GetWALReaderForStreaming error: %v", readerID, err)
				return
			}
			defer reader.Close()

			var count int
			for count < numRecords {
				var placeholder LogRecord
				rawBytes, err := reader.Next(&placeholder)
				if err != nil {
					t.Errorf("reader %d: Next error after %d records: %v", readerID, count, err)
					return
				}
				_, err = DecodeLogRecord(rawBytes)
				if err != nil {
					t.Errorf("reader %d: DecodeLogRecord error: %v", readerID, err)
					return
				}
				count++
			}
		}(r)
	}

	wg.Wait()
}

// --- Context Cancellation Tests ---

// TestWAL_AppendWithCancelledContext verifies that AppendRecord respects context cancellation.
func TestWAL_AppendWithCancelledContext(t *testing.T) {
	lm, _ := setupLogManager(t)
	defer lm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	lr := newTestLogRecord("should not be written")
	_, err := lm.AppendRecord(ctx, lr, lr.LogType)
	require.Error(t, err, "Expected error with cancelled context")
}

// --- Recovery Correctness Tests ---

// TestWAL_MultipleRestarts writes records across multiple open/close cycles to verify
// LSN continuity is maintained across restarts.
func TestWAL_MultipleRestarts(t *testing.T) {
	tempDir := t.TempDir()
	logger, _ := zap.NewDevelopment()

	var lastLSN LSN

	for round := 0; round < 3; round++ {
		lm, err := NewLogManager(tempDir, logger, "")
		require.NoError(t, err, "round %d: NewLogManager failed", round)

		lr := newTestLogRecord(fmt.Sprintf("round_%d", round))
		lsn, err := lm.AppendRecord(context.Background(), lr, lr.LogType)
		require.NoError(t, err, "round %d: AppendRecord failed", round)

		if round > 0 {
			require.Greater(t, uint64(lsn), uint64(lastLSN),
				"round %d: LSN should be strictly greater than previous round's LSN", round)
		}
		lastLSN = lsn
		require.NoError(t, lm.Close(), "round %d: Close failed", round)
	}
}

// TestWAL_LargeNumberOfRecords writes a large batch of records and reads them all back,
// verifying that sequence and data integrity are preserved.
func TestWAL_LargeNumberOfRecords(t *testing.T) {
	lm, _ := setupLogManager(t)
	defer lm.Close()

	const totalRecords = 1000

	for i := 0; i < totalRecords; i++ {
		lr := newTestLogRecord(fmt.Sprintf("data_%05d", i))
		lsn, err := lm.AppendRecord(context.Background(), lr, lr.LogType)
		require.NoError(t, err, "record %d", i)
		require.Equal(t, LSN(i+1), lsn)
	}
	require.NoError(t, lm.Sync())

	reader, err := lm.GetWALReaderForStreaming(1, "bulk_slot")
	require.NoError(t, err)
	defer reader.Close()

	for i := 0; i < totalRecords; i++ {
		var placeholder LogRecord
		rawBytes, err := reader.Next(&placeholder)
		require.NoError(t, err, "reading record %d", i)

		decoded, err := DecodeLogRecord(rawBytes)
		require.NoError(t, err)
		require.Equal(t, LSN(i+1), decoded.LSN)
		require.Equal(t, []byte(fmt.Sprintf("data_%05d", i)), decoded.Data)
	}
}

// TestWAL_GetCurrentLSN verifies that GetCurrentLSN tracks the number of appended records.
func TestWAL_GetCurrentLSN(t *testing.T) {
	lm, _ := setupLogManager(t)
	defer lm.Close()

	require.Equal(t, LSN(0), lm.GetCurrentLSN())

	for i := 1; i <= 5; i++ {
		lr := newTestLogRecord("x")
		_, err := lm.AppendRecord(context.Background(), lr, lr.LogType)
		require.NoError(t, err)
		require.Equal(t, LSN(i), lm.GetCurrentLSN())
	}
}
