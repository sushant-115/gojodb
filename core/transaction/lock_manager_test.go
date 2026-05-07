package transaction

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- Lock Manager Tests ---

// TestLockManager_SharedGrantedImmediately verifies that a shared lock is granted
// immediately when no other lock is held.
func TestLockManager_SharedGrantedImmediately(t *testing.T) {
	lm := NewLockManager()
	err := lm.Acquire(context.Background(), 1, "key1", LockModeShared)
	require.NoError(t, err)
}

// TestLockManager_ExclusiveGrantedImmediately verifies that an exclusive lock is granted
// immediately when no other lock is held.
func TestLockManager_ExclusiveGrantedImmediately(t *testing.T) {
	lm := NewLockManager()
	err := lm.Acquire(context.Background(), 1, "key1", LockModeExclusive)
	require.NoError(t, err)
}

// TestLockManager_MultipleSharedCompatible verifies that multiple transactions can hold
// shared locks on the same key simultaneously.
func TestLockManager_MultipleSharedCompatible(t *testing.T) {
	lm := NewLockManager()
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeShared))
	require.NoError(t, lm.Acquire(context.Background(), 2, "key1", LockModeShared))
	require.NoError(t, lm.Acquire(context.Background(), 3, "key1", LockModeShared))
}

// TestLockManager_ExclusiveBlocksShared verifies that a shared lock request blocks
// when another transaction holds an exclusive lock on the same key.
func TestLockManager_ExclusiveBlocksShared(t *testing.T) {
	lm := NewLockManager()
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeExclusive))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := lm.Acquire(ctx, 2, "key1", LockModeShared)
	require.Error(t, err, "shared lock should be blocked by existing exclusive lock")
}

// TestLockManager_SharedBlocksExclusive verifies that an exclusive lock request blocks
// when another transaction holds a shared lock on the same key.
func TestLockManager_SharedBlocksExclusive(t *testing.T) {
	lm := NewLockManager()
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeShared))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := lm.Acquire(ctx, 2, "key1", LockModeExclusive)
	require.Error(t, err, "exclusive lock should be blocked by existing shared lock")
}

// TestLockManager_ReleaseUnblocksWaiter verifies that releasing a lock allows a blocked
// transaction to proceed and acquire it.
func TestLockManager_ReleaseUnblocksWaiter(t *testing.T) {
	lm := NewLockManager()
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeExclusive))

	acquired := make(chan struct{})
	go func() {
		err := lm.Acquire(context.Background(), 2, "key1", LockModeShared)
		if err == nil {
			close(acquired)
		}
	}()

	time.Sleep(20 * time.Millisecond)
	lm.Release(1, "key1")

	select {
	case <-acquired:
		// success
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waiter was not unblocked after lock release")
	}
}

// TestLockManager_SharedUpgradeToExclusive verifies that a transaction holding a shared
// lock can upgrade to exclusive when it is the sole shared holder.
func TestLockManager_SharedUpgradeToExclusive(t *testing.T) {
	lm := NewLockManager()
	// Acquire shared first.
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeShared))
	// Upgrade to exclusive — allowed since we're the only holder.
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeExclusive))
}

// TestLockManager_SharedUpgradeBlockedByOtherShared verifies that a shared→exclusive
// upgrade is NOT immediately granted when another transaction also holds shared.
func TestLockManager_SharedUpgradeBlockedByOtherShared(t *testing.T) {
	lm := NewLockManager()
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeShared))
	require.NoError(t, lm.Acquire(context.Background(), 2, "key1", LockModeShared))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// txn 1 tries to upgrade; should block because txn 2 still holds shared.
	err := lm.Acquire(ctx, 1, "key1", LockModeExclusive)
	require.Error(t, err, "upgrade should be blocked while another shared holder exists")
}

// TestLockManager_IdempotentSharedReacquire verifies that reacquiring a shared lock
// the same transaction already holds is a no-op.
func TestLockManager_IdempotentSharedReacquire(t *testing.T) {
	lm := NewLockManager()
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeShared))
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeShared))
}

// TestLockManager_IdempotentExclusiveReacquire verifies that reacquiring an exclusive
// lock the same transaction already holds is a no-op.
func TestLockManager_IdempotentExclusiveReacquire(t *testing.T) {
	lm := NewLockManager()
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeExclusive))
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeExclusive))
}

// TestLockManager_ReleaseAll releases all locks for a transaction at once and verifies
// that previously blocked transactions can now proceed.
func TestLockManager_ReleaseAll(t *testing.T) {
	lm := NewLockManager()
	// txn 1 holds exclusive on both keys.
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeExclusive))
	require.NoError(t, lm.Acquire(context.Background(), 1, "key2", LockModeExclusive))

	key1Acquired := make(chan struct{})
	key2Acquired := make(chan struct{})

	go func() {
		if lm.Acquire(context.Background(), 2, "key1", LockModeShared) == nil {
			close(key1Acquired)
		}
	}()
	go func() {
		if lm.Acquire(context.Background(), 3, "key2", LockModeShared) == nil {
			close(key2Acquired)
		}
	}()

	time.Sleep(20 * time.Millisecond)
	lm.ReleaseAll(1)

	for _, ch := range []chan struct{}{key1Acquired, key2Acquired} {
		select {
		case <-ch:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("ReleaseAll did not unblock a waiter")
		}
	}
}

// TestLockManager_DeadlockDetection verifies that deadlock between two transactions
// is detected and ErrDeadlock is returned.
func TestLockManager_DeadlockDetection(t *testing.T) {
	lm := NewLockManager()

	// txn 1 holds key1, txn 2 holds key2.
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeExclusive))
	require.NoError(t, lm.Acquire(context.Background(), 2, "key2", LockModeExclusive))

	// txn 1 waits for key2 in a goroutine (will block).
	txn1Done := make(chan error, 1)
	go func() {
		txn1Done <- lm.Acquire(context.Background(), 1, "key2", LockModeExclusive)
	}()

	time.Sleep(30 * time.Millisecond)

	// txn 2 tries to acquire key1 — this should form a cycle and return ErrDeadlock.
	err := lm.Acquire(context.Background(), 2, "key1", LockModeExclusive)
	require.ErrorIs(t, err, ErrDeadlock)

	// Clean up: release txn1's lock on key2 so its goroutine can finish.
	lm.Release(2, "key2")
	select {
	case <-txn1Done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("txn1 goroutine did not finish after release")
	}
}

// TestLockManager_ContextCancellationWhileWaiting verifies that a transaction waiting
// for a lock returns ctx.Err() when its context is cancelled.
func TestLockManager_ContextCancellationWhileWaiting(t *testing.T) {
	lm := NewLockManager()
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeExclusive))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- lm.Acquire(ctx, 2, "key1", LockModeShared)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err, "expected context cancellation error")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("goroutine did not unblock after context cancellation")
	}
}

// TestLockManager_LocksDifferentKeysIndependently verifies that locks on different keys
// do not interfere with each other.
func TestLockManager_LocksDifferentKeysIndependently(t *testing.T) {
	lm := NewLockManager()
	// txn 1 holds exclusive on key1.
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeExclusive))
	// txn 2 should be able to get exclusive on key2 immediately.
	require.NoError(t, lm.Acquire(context.Background(), 2, "key2", LockModeExclusive))
}

// TestLockManager_ConcurrentLockingOnDifferentKeys verifies that many goroutines can
// each lock their own distinct key without contention, acquire it, then release it.
func TestLockManager_ConcurrentLockingOnDifferentKeys(t *testing.T) {
	lm := NewLockManager()
	const numGoroutines = 50

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := string([]byte{byte('a' + id)}) // each goroutine owns a unique key
			txnID := uint64(id + 1)
			err := lm.Acquire(context.Background(), txnID, key, LockModeExclusive)
			if err != nil {
				t.Errorf("goroutine %d: unexpected error: %v", id, err)
				return
			}
			lm.Release(txnID, key)
		}(i)
	}
	wg.Wait()
}

// TestLockManager_ReleaseCleanup verifies that releasing all locks for a transaction
// cleans up internal state (no lingering entries in the lock table).
func TestLockManager_ReleaseCleanup(t *testing.T) {
	lm := NewLockManager()
	require.NoError(t, lm.Acquire(context.Background(), 1, "key1", LockModeExclusive))
	lm.Release(1, "key1")

	// After release, txn 2 should be able to lock immediately (no leftover state).
	err := lm.Acquire(context.Background(), 2, "key1", LockModeExclusive)
	require.NoError(t, err)
}
