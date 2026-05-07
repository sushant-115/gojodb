package transaction

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	"go.uber.org/zap"
)

// --- Helpers ---

func setupTxnManager(t *testing.T, participants []Participant) (*Manager, func()) {
	t.Helper()
	tempDir := t.TempDir()
	logger, _ := zap.NewDevelopment()
	lm, err := wal.NewLogManager(tempDir, logger, "")
	require.NoError(t, err)

	mgr, err := NewManager(logger, lm, participants)
	require.NoError(t, err)

	return mgr, func() { lm.Close() }
}

// mockParticipant is a test double for the Participant interface.
type mockParticipant struct {
	mu         sync.Mutex
	name       string
	latestLSN  uint64
	applied    []*wal.LogRecord
	applyErr   error
}

func (m *mockParticipant) ApplyLogRecord(_ context.Context, r *wal.LogRecord) error {
	if m.applyErr != nil {
		return m.applyErr
	}
	m.mu.Lock()
	m.applied = append(m.applied, r)
	m.mu.Unlock()
	return nil
}

func (m *mockParticipant) GetLatestLSN() uint64 { return m.latestLSN }
func (m *mockParticipant) Name() string          { return m.name }

// --- Transaction Manager Tests ---

// TestTxnManager_BeginCommit verifies the happy path: Begin followed by Commit
// results in a committed transaction that is removed from the active table.
func TestTxnManager_BeginCommit(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	txnID, err := mgr.Begin(context.Background())
	require.NoError(t, err)
	require.NotZero(t, txnID)

	err = mgr.Commit(context.Background(), txnID)
	require.NoError(t, err)

	// After commit the txn should be gone from the active table.
	mgr.mu.RLock()
	_, stillActive := mgr.txnTable[txnID]
	mgr.mu.RUnlock()
	require.False(t, stillActive, "committed transaction should be removed from table")
}

// TestTxnManager_BeginAbort verifies that aborting a transaction removes it from the
// active table and releases its locks.
func TestTxnManager_BeginAbort(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	txnID, err := mgr.Begin(context.Background())
	require.NoError(t, err)

	err = mgr.Abort(context.Background(), txnID)
	require.NoError(t, err)

	mgr.mu.RLock()
	_, stillActive := mgr.txnTable[txnID]
	mgr.mu.RUnlock()
	require.False(t, stillActive, "aborted transaction should be removed from table")
}

// TestTxnManager_BeginPrepareCommit verifies the explicit 2PC path:
// Begin → Prepare → Commit all succeed.
func TestTxnManager_BeginPrepareCommit(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	txnID, err := mgr.Begin(context.Background())
	require.NoError(t, err)

	require.NoError(t, mgr.Prepare(context.Background(), txnID))
	require.NoError(t, mgr.Commit(context.Background(), txnID))
}

// TestTxnManager_PrepareUnknownTxn verifies that Prepare on a non-existent transaction
// returns an error.
func TestTxnManager_PrepareUnknownTxn(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	err := mgr.Prepare(context.Background(), 999999)
	require.Error(t, err)
}

// TestTxnManager_CommitUnknownTxn verifies that Commit on a non-existent transaction
// returns an error.
func TestTxnManager_CommitUnknownTxn(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	err := mgr.Commit(context.Background(), 999999)
	require.Error(t, err)
}

// TestTxnManager_AbortIdempotent verifies that aborting an already-aborted transaction
// is a no-op (returns nil).
func TestTxnManager_AbortIdempotent(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	txnID, err := mgr.Begin(context.Background())
	require.NoError(t, err)
	require.NoError(t, mgr.Abort(context.Background(), txnID))
	require.NoError(t, mgr.Abort(context.Background(), txnID))
}

// TestTxnManager_CommitAlreadyAbortedErrors verifies that committing an aborted
// transaction returns an error.
func TestTxnManager_CommitAlreadyAbortedErrors(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	txnID, err := mgr.Begin(context.Background())
	require.NoError(t, err)
	require.NoError(t, mgr.Abort(context.Background(), txnID))

	// Abort removes it from txnTable; Commit on a missing txn errors.
	err = mgr.Commit(context.Background(), txnID)
	require.Error(t, err)
}

// TestTxnManager_AcquireLockInsideTxn verifies that a transaction can acquire a lock
// and that the lock is reflected in the transaction's locksHeld set.
func TestTxnManager_AcquireLockInsideTxn(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	txnID, err := mgr.Begin(context.Background())
	require.NoError(t, err)

	err = mgr.AcquireLock(context.Background(), txnID, "row:42", LockModeShared)
	require.NoError(t, err)

	mgr.mu.RLock()
	entry := mgr.txnTable[txnID]
	_, held := entry.txn.locksHeld["row:42"]
	mgr.mu.RUnlock()
	require.True(t, held, "lock should be tracked in transaction's locksHeld")
}

// TestTxnManager_LocksReleasedOnCommit verifies that locks held by a transaction are
// released when the transaction commits.
func TestTxnManager_LocksReleasedOnCommit(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	txnID, err := mgr.Begin(context.Background())
	require.NoError(t, err)
	require.NoError(t, mgr.AcquireLock(context.Background(), txnID, "row:1", LockModeExclusive))
	require.NoError(t, mgr.Commit(context.Background(), txnID))

	// After commit, another transaction should immediately get the exclusive lock.
	txn2, err := mgr.Begin(context.Background())
	require.NoError(t, err)
	err = mgr.AcquireLock(context.Background(), txn2, "row:1", LockModeExclusive)
	require.NoError(t, err, "lock should be available after first txn committed")
}

// TestTxnManager_LocksReleasedOnAbort verifies that locks held by a transaction are
// released when the transaction aborts.
func TestTxnManager_LocksReleasedOnAbort(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	txnID, err := mgr.Begin(context.Background())
	require.NoError(t, err)
	require.NoError(t, mgr.AcquireLock(context.Background(), txnID, "row:1", LockModeExclusive))
	require.NoError(t, mgr.Abort(context.Background(), txnID))

	txn2, err := mgr.Begin(context.Background())
	require.NoError(t, err)
	err = mgr.AcquireLock(context.Background(), txn2, "row:1", LockModeExclusive)
	require.NoError(t, err, "lock should be available after first txn aborted")
}

// TestTxnManager_ConcurrentTransactions verifies that many transactions can begin,
// acquire their own locks, and commit concurrently without data races.
func TestTxnManager_ConcurrentTransactions(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	const numTxns = 20
	var wg sync.WaitGroup

	for i := 0; i < numTxns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			txnID, err := mgr.Begin(context.Background())
			if err != nil {
				t.Errorf("Begin error: %v", err)
				return
			}
			// Each transaction locks its own unique key.
			key := string([]byte{byte('a' + id)})
			if err := mgr.AcquireLock(context.Background(), txnID, key, LockModeExclusive); err != nil {
				t.Errorf("AcquireLock error: %v", err)
				return
			}
			mgr.RecordOperation(txnID, "PUT", key, "value")
			if err := mgr.Commit(context.Background(), txnID); err != nil {
				t.Errorf("Commit error: %v", err)
			}
		}(i)
	}
	wg.Wait()
}

// TestTxnManager_RecordOperation verifies that RecordOperation appends operations to
// the transaction's in-memory log.
func TestTxnManager_RecordOperation(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	txnID, err := mgr.Begin(context.Background())
	require.NoError(t, err)

	mgr.RecordOperation(txnID, "PUT", "key1", "val1")
	mgr.RecordOperation(txnID, "DELETE", "key2", "")

	mgr.mu.RLock()
	ops := mgr.txnTable[txnID].txn.Operation
	mgr.mu.RUnlock()

	require.Len(t, ops, 2)
	require.Equal(t, "PUT", ops[0].Command)
	require.Equal(t, "key1", ops[0].Key)
	require.Equal(t, "DELETE", ops[1].Command)
}

// TestTxnManager_RecoveryFreshStart verifies that creating a Manager against an empty
// WAL directory succeeds and starts cleanly.
func TestTxnManager_RecoveryFreshStart(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	// With an empty WAL there's nothing to recover — txnTable should be empty.
	mgr.mu.RLock()
	tableLen := len(mgr.txnTable)
	mgr.mu.RUnlock()
	require.Equal(t, 0, tableLen)
}

// TestTxnManager_AcquireLockUnknownTxn verifies that AcquireLock returns an error when
// the transaction ID is not known to the manager.
func TestTxnManager_AcquireLockUnknownTxn(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	err := mgr.AcquireLock(context.Background(), 999, "key", LockModeShared)
	require.Error(t, err)
}

// TestTxnManager_UniqueTransactionIDs verifies that successive Begin calls return
// strictly increasing, unique transaction IDs.
func TestTxnManager_UniqueTransactionIDs(t *testing.T) {
	mgr, cleanup := setupTxnManager(t, nil)
	defer cleanup()

	seen := make(map[uint64]struct{})
	for i := 0; i < 10; i++ {
		id, err := mgr.Begin(context.Background())
		require.NoError(t, err)
		_, dup := seen[id]
		require.False(t, dup, "duplicate transaction ID %d", id)
		seen[id] = struct{}{}
		require.NoError(t, mgr.Abort(context.Background(), id))
	}
}
