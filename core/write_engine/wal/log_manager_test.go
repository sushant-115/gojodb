package wal

// To use this file, place it in the same 'wal' package directory as your source code
// and run `go test -v`.
// For a more isolated "black box" test, you can change the package to `wal_test`
// and adjust the imports accordingly (e.g., import "your_module_path/wal").

import (
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

// setupLogManager creates a LogManager in a temporary directory for isolated testing.
func setupLogManager(t *testing.T) (*LogManager, string) {
	t.Helper()
	tempDir := t.TempDir()
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	lm, err := NewLogManager(tempDir, logger, "")
	require.NoError(t, err)

	return lm, tempDir
}

// newTestLogRecord creates a sample LogRecord for writing to the WAL.
func newTestLogRecord(data string) *LogRecord {
	return &LogRecord{
		Type:      LogRecordTypeUpdate,
		IndexType: "test_btree",
		Timestamp: time.Now().UnixNano(),
		TxnID:     uint64(time.Now().UnixNano()), // Use unique TxnID
		PageID:    pagemanager.PageID(1),
		Data:      []byte(data),
		PrevLSN:   0,
		LogType:   LogTypeBtree,
	}
}

// --- Test Cases ---

// TestWALReader_SimpleRead verifies the most basic functionality: writing a few records
// and immediately reading them back with a new stream reader. This confirms that the
// core Append and Next logic works correctly in the simplest case.
func TestWALReader_SimpleRead(t *testing.T) {
	lm, _ := setupLogManager(t)
	defer lm.Close()

	// 1. Write some records to the WAL
	recordsToWrite := []*LogRecord{
		newTestLogRecord("record data 1"),
		newTestLogRecord("record data 2"),
		newTestLogRecord("record data 3"),
	}

	for i, lr := range recordsToWrite {
		lsn, err := lm.AppendRecord(lr, lr.LogType)
		require.NoError(t, err)
		require.Equal(t, LSN(i+1), lsn, "LSN should be sequential and 1-based")
	}
	require.NoError(t, lm.Sync()) // Ensure data is flushed to disk

	// 2. Get a reader to stream from the very beginning (LSN 1)
	reader, err := lm.GetWALReaderForStreaming(1, "test_slot_simple")
	require.NoError(t, err)
	defer reader.Close()

	// 3. Read the records back and verify their content
	for i, expectedRecord := range recordsToWrite {
		var placeholder LogRecord
		recordBytes, err := reader.Next(&placeholder)
		require.NoError(t, err, "Failed to read record %d", i+1)
		require.NotNil(t, recordBytes)

		decodedRecord, err := DecodeLogRecord(recordBytes)
		require.NoError(t, err)

		// Verify the content matches what was written
		require.Equal(t, LSN(i+1), decodedRecord.LSN)
		require.Equal(t, expectedRecord.Type, decodedRecord.Type)
		require.Equal(t, expectedRecord.Data, decodedRecord.Data)
		require.Equal(t, expectedRecord.TxnID, decodedRecord.TxnID)
	}
}

// TestWALReader_RecoveryAndRead is the most critical test for your problem.
// It simulates a service restart by creating a LogManager, writing data, closing it,
// and then creating a *new* LogManager instance from the same directory. The new instance
// must successfully recover the state and allow a reader to stream the old data.
// This test will fail if the `recover` function does not correctly populate the `segmentStartLSNs` map.
func TestWALReader_RecoveryAndRead(t *testing.T) {
	tempDir := t.TempDir()
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	// --- Phase 1: Write data and shutdown ---
	lm1, err := NewLogManager(tempDir, logger, "")
	require.NoError(t, err)

	recordToRecover := newTestLogRecord("this must survive a restart")
	_, err = lm1.AppendRecord(recordToRecover, recordToRecover.LogType)
	require.NoError(t, err)
	require.NoError(t, lm1.Close()) // Close the manager to finalize the file

	// --- Phase 2: Recover and Read ---
	// Create a NEW LogManager instance to trigger the recovery process
	lm2, err := NewLogManager(tempDir, logger, "")
	require.NoError(t, err)
	defer lm2.Close()

	// Try to get a reader. This calls openSegmentForLSN, which depends on the
	// state populated by the recover() function.
	reader, err := lm2.GetWALReaderForStreaming(1, "test_slot_recovery")
	require.NoError(t, err, "Failed to get reader after recovery. This implies recover() did not set up the necessary state for the reader to find WAL segment files.")
	defer reader.Close()

	// Read the record back and verify
	var placeholder LogRecord
	recordBytes, err := reader.Next(&placeholder)
	require.NoError(t, err, "Next() failed to read the record after recovery.")

	decoded, err := DecodeLogRecord(recordBytes)
	require.NoError(t, err)
	require.Equal(t, LSN(1), decoded.LSN)
	require.Equal(t, recordToRecover.Data, decoded.Data)
}

// TestWALReader_StartFromMiddle checks if the reader can correctly find and
// start streaming from a specific LSN that is not at the beginning of the segment.
// This tests the seeking logic within `openSegmentForLSN`.
func TestWALReader_StartFromMiddle(t *testing.T) {
	lm, _ := setupLogManager(t)
	defer lm.Close()

	// 1. Write 10 records
	for i := 0; i < 10; i++ {
		lr := newTestLogRecord(fmt.Sprintf("record %d", i+1))
		_, err := lm.AppendRecord(lr, lr.LogType)
		require.NoError(t, err)
	}
	require.NoError(t, lm.Sync())

	// 2. Get a reader starting from LSN 6
	startLSN := LSN(6)
	reader, err := lm.GetWALReaderForStreaming(startLSN, "test_slot_middle")
	require.NoError(t, err)
	defer reader.Close()

	// 3. Read the first record and confirm it's the correct one (LSN 6)
	var placeholder LogRecord
	recordBytes, err := reader.Next(&placeholder)
	require.NoError(t, err)

	decodedRecord, err := DecodeLogRecord(recordBytes)
	require.NoError(t, err)
	require.Equal(t, startLSN, decodedRecord.LSN)
	require.Equal(t, []byte("record 6"), decodedRecord.Data)
}

// TestWALReader_WaitAndRead tests the reader's ability to block and wait for new
// records when it reaches the end of the log, then successfully read the new
// record once it's written. This is crucial for real-time streaming replication.
func TestWALReader_WaitAndRead(t *testing.T) {
	lm, _ := setupLogManager(t)
	defer lm.Close()

	// 1. Write an initial record
	initialRecord := newTestLogRecord("initial record")
	_, err := lm.AppendRecord(initialRecord, initialRecord.LogType)
	require.NoError(t, err)
	require.NoError(t, lm.Sync())

	// 2. Start a reader and read the first record to catch up
	reader, err := lm.GetWALReaderForStreaming(1, "test_slot_wait")
	require.NoError(t, err)
	defer reader.Close()

	var placeholder LogRecord
	_, err = reader.Next(&placeholder) // Read record 1
	require.NoError(t, err)

	// 3. Run the next call in a goroutine, as it should now block
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

	// Give the goroutine a moment to block inside Next()
	time.Sleep(100 * time.Millisecond)

	// 4. Write a new record, which should unblock the waiting reader
	awaitedRecord := newTestLogRecord("the awaited record")
	_, err = lm.AppendRecord(awaitedRecord, awaitedRecord.LogType)
	require.NoError(t, err)
	require.NoError(t, lm.Sync())

	// 5. Wait for the goroutine to finish and verify the result
	wg.Wait()

	require.NoError(t, nextErr)
	require.NotNil(t, receivedRecord)
	require.Equal(t, LSN(2), receivedRecord.LSN)
	require.Equal(t, awaitedRecord.Data, receivedRecord.Data)
}

// TestWAL_FileNameFormat confirms that WAL segment files are created with the
// expected zero-padded naming convention.
func TestWAL_FileNameFormat(t *testing.T) {
	lm, walDir := setupLogManager(t)

	// Write one record to create the first segment file
	lr := newTestLogRecord("data")
	_, err := lm.AppendRecord(lr, lr.LogType)
	require.NoError(t, err)
	require.NoError(t, lm.Close())

	// Check that the file exists with the correct name
	expectedFileName := "wal-00000000000000000001.log"
	expectedFilePath := filepath.Join(walDir, expectedFileName)

	_, err = os.Stat(expectedFilePath)
	require.NoError(t, err, "Expected WAL file with padded name was not found at %s", expectedFilePath)
}
