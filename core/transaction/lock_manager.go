package transaction

import (
	"context"
	"fmt"
	"sync"
)

// LockMode represents the type of lock requested.
type LockMode int

const (
	LockModeShared    LockMode = iota // Multiple readers allowed
	LockModeExclusive                 // Only one writer, no readers
)

// lockRequest represents a pending or granted lock request.
type lockRequest struct {
	txnID   uint64
	mode    LockMode
	granted bool
	waitCh  chan struct{} // Closed when lock is granted
}

// lockEntry tracks all lock requests for a specific key.
type lockEntry struct {
	requests    []*lockRequest
	exclusiveID uint64 // TxnID holding the exclusive lock; 0 means none
	sharedCount int    // Number of granted shared locks
}

func (le *lockEntry) sharedHolders() []uint64 {
	var holders []uint64
	for _, r := range le.requests {
		if r.granted && r.mode == LockModeShared {
			holders = append(holders, r.txnID)
		}
	}
	return holders
}

// LockManager manages row-level locks with deadlock detection.
type LockManager struct {
	mu       sync.Mutex
	table    map[string]*lockEntry      // key → lock state
	txnLocks map[uint64][]string        // txnID → keys locked (for ReleaseAll)
	waitFor  map[uint64]map[uint64]bool // txnID → set of txnIDs it is waiting for
}

// NewLockManager creates a new LockManager.
func NewLockManager() *LockManager {
	return &LockManager{
		table:    make(map[string]*lockEntry),
		txnLocks: make(map[uint64][]string),
		waitFor:  make(map[uint64]map[uint64]bool),
	}
}

// Acquire attempts to acquire a lock on key for txnID.
// It blocks until the lock is granted or ctx is cancelled.
// Returns ErrDeadlock if a deadlock is detected.
func (lm *LockManager) Acquire(ctx context.Context, txnID uint64, key string, mode LockMode) error {
	lm.mu.Lock()

	entry, ok := lm.table[key]
	if !ok {
		entry = &lockEntry{}
		lm.table[key] = entry
	}

	// Check for lock upgrade (shared → exclusive by same txn).
	for _, r := range entry.requests {
		if r.txnID == txnID && r.granted {
			if mode == LockModeShared {
				// Already holds at least shared – idempotent.
				lm.mu.Unlock()
				return nil
			}
			if r.mode == LockModeExclusive {
				// Already holds exclusive – idempotent.
				lm.mu.Unlock()
				return nil
			}
			// Upgrade shared → exclusive; only allowed if we're the sole shared holder.
			if entry.sharedCount == 1 {
				r.mode = LockModeExclusive
				entry.exclusiveID = txnID
				entry.sharedCount = 0
				lm.mu.Unlock()
				return nil
			}
		}
	}

	// Check if lock can be granted immediately.
	canGrant := lm.canGrant(entry, txnID, mode)
	req := &lockRequest{txnID: txnID, mode: mode, granted: canGrant, waitCh: make(chan struct{})}
	entry.requests = append(entry.requests, req)
	lm.txnLocks[txnID] = append(lm.txnLocks[txnID], key)

	if canGrant {
		lm.markGranted(entry, req)
		lm.mu.Unlock()
		return nil
	}

	// Lock is not yet available – register wait-for edges for deadlock detection.
	blockers := lm.getBlockers(entry, txnID, mode)
	if lm.waitFor[txnID] == nil {
		lm.waitFor[txnID] = make(map[uint64]bool)
	}
	for _, b := range blockers {
		lm.waitFor[txnID][b] = true
	}

	// Deadlock check (cycle detection in wait-for graph).
	if lm.detectCycle(txnID, make(map[uint64]bool)) {
		// Clean up the pending request.
		lm.removePendingRequest(entry, req)
		lm.removeWaitEdges(txnID, blockers)
		lm.mu.Unlock()
		return ErrDeadlock
	}

	lm.mu.Unlock()

	// Wait for the lock to be granted or context cancellation.
	select {
	case <-req.waitCh:
		lm.mu.Lock()
		lm.removeWaitEdges(txnID, blockers)
		lm.mu.Unlock()
		return nil
	case <-ctx.Done():
		lm.mu.Lock()
		lm.removePendingRequest(entry, req)
		lm.removeWaitEdges(txnID, blockers)
		lm.tryGrantWaiters(entry)
		lm.mu.Unlock()
		return ctx.Err()
	}
}

// Release releases the lock on key held by txnID.
func (lm *LockManager) Release(txnID uint64, key string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, ok := lm.table[key]
	if !ok {
		return
	}
	lm.removeGrantedRequest(entry, txnID)
	lm.tryGrantWaiters(entry)
	if len(entry.requests) == 0 {
		delete(lm.table, key)
	}

	// Remove from txnLocks index.
	keys := lm.txnLocks[txnID]
	for i, k := range keys {
		if k == key {
			lm.txnLocks[txnID] = append(keys[:i], keys[i+1:]...)
			break
		}
	}
}

// ReleaseAll releases every lock held by txnID and wakes up waiting transactions.
func (lm *LockManager) ReleaseAll(txnID uint64) {
	lm.mu.Lock()
	keys := lm.txnLocks[txnID]
	delete(lm.txnLocks, txnID)
	delete(lm.waitFor, txnID)
	lm.mu.Unlock()

	for _, key := range keys {
		lm.mu.Lock()
		entry, ok := lm.table[key]
		if ok {
			lm.removeGrantedRequest(entry, txnID)
			lm.tryGrantWaiters(entry)
			if len(entry.requests) == 0 {
				delete(lm.table, key)
			}
		}
		lm.mu.Unlock()
	}
}

// --- internal helpers (caller must hold lm.mu) ---

func (lm *LockManager) canGrant(entry *lockEntry, txnID uint64, mode LockMode) bool {
	if mode == LockModeShared {
		// Shared is compatible unless an exclusive lock is held by another txn.
		return entry.exclusiveID == 0 || entry.exclusiveID == txnID
	}
	// Exclusive: no other shared or exclusive holders.
	if entry.exclusiveID != 0 && entry.exclusiveID != txnID {
		return false
	}
	for _, r := range entry.requests {
		if r.granted && r.txnID != txnID {
			return false
		}
	}
	return true
}

func (lm *LockManager) markGranted(entry *lockEntry, req *lockRequest) {
	req.granted = true
	if req.mode == LockModeExclusive {
		entry.exclusiveID = req.txnID
	} else {
		entry.sharedCount++
	}
	close(req.waitCh)
}

func (lm *LockManager) getBlockers(entry *lockEntry, txnID uint64, mode LockMode) []uint64 {
	seen := make(map[uint64]bool)
	var blockers []uint64
	for _, r := range entry.requests {
		if !r.granted || r.txnID == txnID {
			continue
		}
		if mode == LockModeExclusive || r.mode == LockModeExclusive {
			if !seen[r.txnID] {
				seen[r.txnID] = true
				blockers = append(blockers, r.txnID)
			}
		}
	}
	return blockers
}

func (lm *LockManager) detectCycle(start uint64, visited map[uint64]bool) bool {
	if visited[start] {
		return true
	}
	visited[start] = true
	for dep := range lm.waitFor[start] {
		if lm.detectCycle(dep, visited) {
			return true
		}
	}
	delete(visited, start)
	return false
}

func (lm *LockManager) removePendingRequest(entry *lockEntry, req *lockRequest) {
	for i, r := range entry.requests {
		if r == req {
			entry.requests = append(entry.requests[:i], entry.requests[i+1:]...)
			return
		}
	}
}

func (lm *LockManager) removeGrantedRequest(entry *lockEntry, txnID uint64) {
	for i, r := range entry.requests {
		if r.txnID == txnID && r.granted {
			if r.mode == LockModeExclusive {
				entry.exclusiveID = 0
			} else {
				entry.sharedCount--
			}
			entry.requests = append(entry.requests[:i], entry.requests[i+1:]...)
			return
		}
	}
}

func (lm *LockManager) removeWaitEdges(txnID uint64, blockers []uint64) {
	if lm.waitFor[txnID] == nil {
		return
	}
	for _, b := range blockers {
		delete(lm.waitFor[txnID], b)
	}
}

func (lm *LockManager) tryGrantWaiters(entry *lockEntry) {
	for _, r := range entry.requests {
		if r.granted {
			continue
		}
		if lm.canGrant(entry, r.txnID, r.mode) {
			lm.markGranted(entry, r)
		} else {
			// FIFO ordering: stop at the first waiter we can't satisfy.
			break
		}
	}
}

// ErrDeadlock is returned when acquiring a lock would create a cycle.
var ErrDeadlock = fmt.Errorf("deadlock detected")
