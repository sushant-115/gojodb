// Package transaction provides ACID transaction management with 2PC coordination,
// ARIES-style crash recovery, and integration with the WAL log manager.
package transaction

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sushant-115/gojodb/core/write_engine/wal"
	"go.uber.org/zap"
)

// Participant is any index manager that can participate in a 2PC transaction.
type Participant interface {
	// ApplyLogRecord applies a redo/undo log record to the underlying index.
	ApplyLogRecord(ctx context.Context, record *wal.LogRecord) error
	// GetLatestLSN returns the latest durable LSN for this participant.
	GetLatestLSN() uint64
	// Name returns the participant name (used for WAL routing).
	Name() string
}

// txnEntry is the in-memory transaction table entry.
type txnEntry struct {
	txn          *Transaction
	participants []*participantState
	firstLSN     wal.LSN // LSN of the BEGIN log record
	lastLSN      wal.LSN // LSN of the most recent log record for this txn
	createdAt    time.Time
}

type participantState struct {
	p        Participant
	prepared bool // phase-1 vote received
}

// Manager is the central transaction coordinator. It manages the active
// transaction table, drives 2PC, and runs ARIES recovery on startup.
type Manager struct {
	logger      *zap.Logger
	logManager  *wal.LogManager
	lockManager *LockManager

	nextTxnID uint64 // atomically incremented

	mu       sync.RWMutex
	txnTable map[uint64]*txnEntry

	// participants registered at server startup (all index managers on this node).
	allParticipants []Participant
}

// NewManager creates a new transaction Manager and runs ARIES crash recovery.
func NewManager(logger *zap.Logger, lm *wal.LogManager, participants []Participant) (*Manager, error) {
	m := &Manager{
		logger:          logger.Named("txn_manager"),
		logManager:      lm,
		lockManager:     NewLockManager(),
		txnTable:        make(map[uint64]*txnEntry),
		allParticipants: participants,
		nextTxnID:       uint64(time.Now().UnixNano()), // seed with time so IDs survive restarts
	}

	if err := m.recover(context.Background()); err != nil {
		return nil, fmt.Errorf("ARIES recovery failed: %w", err)
	}
	return m, nil
}

// Begin starts a new transaction and returns its ID.
func (m *Manager) Begin(ctx context.Context) (uint64, error) {
	txnID := atomic.AddUint64(&m.nextTxnID, 1)

	txn := &Transaction{
		ID:        txnID,
		State:     TxnStateRunning,
		Operation: nil,
		locksHeld: make(map[string]struct{}),
	}

	// Write a BEGIN record to the WAL so recovery can see it.
	lsn, err := m.appendTxnRecord(txnID, wal.LogRecordTypeNoOp, nil)
	if err != nil {
		return 0, fmt.Errorf("begin: WAL write failed: %w", err)
	}

	m.mu.Lock()
	m.txnTable[txnID] = &txnEntry{
		txn:       txn,
		firstLSN:  lsn,
		lastLSN:   lsn,
		createdAt: time.Now(),
	}
	m.mu.Unlock()

	m.logger.Debug("transaction begun", zap.Uint64("txnID", txnID))
	return txnID, nil
}

// AcquireLock acquires a lock on key for the given transaction.
func (m *Manager) AcquireLock(ctx context.Context, txnID uint64, key string, mode LockMode) error {
	m.mu.RLock()
	entry, ok := m.txnTable[txnID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown transaction %d", txnID)
	}
	if entry.txn.State != TxnStateRunning {
		return fmt.Errorf("transaction %d is not running (state=%d)", txnID, entry.txn.State)
	}

	if err := m.lockManager.Acquire(ctx, txnID, key, mode); err != nil {
		return err
	}

	m.mu.Lock()
	entry.txn.locksHeld[key] = struct{}{}
	m.mu.Unlock()
	return nil
}

// Prepare drives Phase 1 of 2PC: writes a PREPARE WAL record and asks all
// participants to acknowledge they can commit.
func (m *Manager) Prepare(ctx context.Context, txnID uint64) error {
	m.mu.Lock()
	entry, ok := m.txnTable[txnID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("prepare: unknown transaction %d", txnID)
	}
	if entry.txn.State != TxnStateRunning {
		m.mu.Unlock()
		return fmt.Errorf("prepare: txn %d not in running state", txnID)
	}
	entry.txn.State = TxnStatePrepared
	m.mu.Unlock()

	// Write PREPARE to WAL – this is the durability boundary for phase 1.
	lsn, err := m.appendTxnRecord(txnID, wal.LogRecordTypePrepare, nil)
	if err != nil {
		_ = m.abort(ctx, txnID)
		return fmt.Errorf("prepare: WAL write failed: %w", err)
	}

	m.mu.Lock()
	entry.lastLSN = lsn
	m.mu.Unlock()

	m.logger.Info("transaction prepared", zap.Uint64("txnID", txnID), zap.Uint64("lsn", uint64(lsn)))
	return nil
}

// Commit finalises a transaction: writes a COMMIT WAL record then releases locks.
// For a single-node (non-distributed) transaction, Prepare is optional; Commit
// writes the PREPARE+COMMIT atomically when called without a prior Prepare.
func (m *Manager) Commit(ctx context.Context, txnID uint64) error {
	m.mu.Lock()
	entry, ok := m.txnTable[txnID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("commit: unknown transaction %d", txnID)
	}
	if entry.txn.State == TxnStateCommitted {
		m.mu.Unlock()
		return nil
	}
	if entry.txn.State == TxnStateAborted {
		m.mu.Unlock()
		return fmt.Errorf("commit: txn %d already aborted", txnID)
	}
	m.mu.Unlock()

	// Single-node path: auto-prepare if not yet prepared.
	if entry.txn.State == TxnStateRunning {
		if err := m.Prepare(ctx, txnID); err != nil {
			return err
		}
	}

	// Write durable COMMIT record.
	lsn, err := m.appendTxnRecord(txnID, wal.LogRecordTypeCommitTxn, nil)
	if err != nil {
		_ = m.abort(ctx, txnID)
		return fmt.Errorf("commit: WAL write failed: %w", err)
	}

	// Force WAL flush so the commit is durable before we release locks.
	if err := m.logManager.Sync(); err != nil {
		return fmt.Errorf("commit: WAL sync failed: %w", err)
	}

	m.mu.Lock()
	entry.txn.State = TxnStateCommitted
	entry.lastLSN = lsn
	delete(m.txnTable, txnID)
	m.mu.Unlock()

	m.lockManager.ReleaseAll(txnID)
	m.logger.Info("transaction committed", zap.Uint64("txnID", txnID), zap.Uint64("lsn", uint64(lsn)))
	return nil
}

// Abort rolls back a transaction: writes an ABORT record and releases locks.
func (m *Manager) Abort(ctx context.Context, txnID uint64) error {
	return m.abort(ctx, txnID)
}

func (m *Manager) abort(ctx context.Context, txnID uint64) error {
	m.mu.Lock()
	entry, ok := m.txnTable[txnID]
	if !ok {
		m.mu.Unlock()
		return nil // Already gone – idempotent.
	}
	if entry.txn.State == TxnStateAborted {
		m.mu.Unlock()
		return nil
	}
	entry.txn.State = TxnStateAborted
	m.mu.Unlock()

	lsn, err := m.appendTxnRecord(txnID, wal.LogRecordTypeAbortTxn, nil)
	if err != nil {
		m.logger.Error("abort: WAL write failed", zap.Uint64("txnID", txnID), zap.Error(err))
	}

	m.mu.Lock()
	entry.lastLSN = lsn
	delete(m.txnTable, txnID)
	m.mu.Unlock()

	m.lockManager.ReleaseAll(txnID)
	m.logger.Info("transaction aborted", zap.Uint64("txnID", txnID))
	return nil
}

// RecordOperation appends an operation to the transaction's in-memory log.
func (m *Manager) RecordOperation(txnID uint64, command, key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.txnTable[txnID]
	if !ok {
		return
	}
	entry.txn.Operation = append(entry.txn.Operation, TransactionOperation{
		Command: command,
		Key:     key,
		Value:   value,
	})
}

// --- ARIES Recovery ---

// undoRecord is the payload written into ABORT/UNDO WAL records.
type undoRecord struct {
	Command string `json:"cmd"`
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
}

// txnRecoveryInfo is built during the analysis phase of ARIES recovery.
type txnRecoveryInfo struct {
	state    TransactionState
	ops      []undoRecord
	firstLSN wal.LSN
	lastLSN  wal.LSN
}

// recover implements ARIES-style crash recovery:
//  1. Analysis  – scan WAL forward, rebuild the transaction table.
//  2. Redo      – re-apply all changes (even those from transactions that later aborted).
//  3. Undo      – roll back any transactions that never committed.
//
// The WALStreamReader blocks at EOF waiting for new records (it is designed for
// streaming replication).  For one-shot recovery we drain exactly the records
// that existed at startup (up to targetLSN) and then close the reader.
func (m *Manager) recover(ctx context.Context) error {
	m.logger.Info("ARIES recovery: starting analysis pass")

	// Capture the LSN that was current when the log manager initialised.
	// Any record with LSN > targetLSN belongs to the current run, not a crash.
	targetLSN := wal.LSN(m.logManager.GetCurrentLSN())
	if targetLSN == 0 {
		m.logger.Info("ARIES recovery: WAL is empty, fresh start")
		return nil
	}

	reader, err := m.logManager.GetWALReaderForStreaming(0, "aries_recovery")
	if err != nil {
		m.logger.Info("ARIES recovery: no WAL found, fresh start")
		return nil
	}

	// --- Analysis + Redo pass (single forward scan) ---
	table := make(map[uint64]*txnRecoveryInfo) // txnID → info

	// recordsCh receives decoded records from a goroutine that drives the reader.
	// doneCh is closed when the goroutine has read up to targetLSN.
	recordsCh := make(chan *wal.LogRecord, 256)
	doneCh := make(chan struct{})

	go func() {
		defer close(recordsCh)
		defer close(doneCh)
		var dummy wal.LogRecord
		for {
			rawBytes, nextErr := reader.Next(&dummy)
			if nextErr != nil {
				return
			}
			decoded, decErr := wal.DecodeLogRecord(rawBytes)
			if decErr != nil {
				m.logger.Warn("ARIES recovery: skipping corrupt record", zap.Error(decErr))
				continue
			}
			recordsCh <- decoded
			if decoded.LSN >= targetLSN {
				return
			}
		}
	}()

	// Drain the channel with a safety timeout so we never hang forever.
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

drain:
	for {
		select {
		case record, ok := <-recordsCh:
			if !ok {
				break drain
			}
			txnID := record.TxnID
			if txnID == 0 {
				m.redoRecord(ctx, record)
				continue
			}

			info, exists := table[txnID]
			if !exists {
				info = &txnRecoveryInfo{
					state:    TxnStateRunning,
					firstLSN: record.LSN,
				}
				table[txnID] = info
			}
			info.lastLSN = record.LSN

			switch record.Type {
			case wal.LogRecordTypeNoOp:
				// BEGIN placeholder – state already set to Running.
			case wal.LogRecordTypePrepare:
				info.state = TxnStatePrepared
			case wal.LogRecordTypeCommitTxn:
				info.state = TxnStateCommitted
			case wal.LogRecordTypeAbortTxn:
				info.state = TxnStateAborted
			default:
				m.redoRecord(ctx, record)
				if len(record.Data) > 0 {
					var op undoRecord
					if jsonErr := json.Unmarshal(record.Data, &op); jsonErr == nil {
						info.ops = append(info.ops, op)
					}
				}
			}

		case <-timeout.C:
			m.logger.Warn("ARIES recovery: timed out waiting for WAL records")
			break drain
		}
	}

	// Stop the streaming reader.
	_ = reader.Close()

	// --- Undo pass – roll back uncommitted transactions in reverse order ---
	uncommitted := countUncommitted(table)
	m.logger.Info("ARIES recovery: starting undo pass", zap.Int("uncommittedTxns", uncommitted))

	for txnID, info := range table {
		if info.state == TxnStateCommitted || info.state == TxnStateAborted {
			continue
		}
		m.logger.Info("ARIES recovery: undoing transaction", zap.Uint64("txnID", txnID))
		for i := len(info.ops) - 1; i >= 0; i-- {
			m.undoOperation(ctx, txnID, info.ops[i])
		}
		_, _ = m.appendTxnRecord(txnID, wal.LogRecordTypeAbortTxn, nil)
	}

	m.logger.Info("ARIES recovery: complete")
	return nil
}

func (m *Manager) redoRecord(ctx context.Context, record *wal.LogRecord) {
	for _, p := range m.allParticipants {
		_ = p.ApplyLogRecord(ctx, record)
	}
}

func (m *Manager) undoOperation(ctx context.Context, txnID uint64, op undoRecord) {
	// Build a compensating (CLR) log record and apply it.
	payload, _ := json.Marshal(op)
	clr := &wal.LogRecord{
		Type:  wal.LogRecordTypeAbortTxn,
		TxnID: txnID,
		Data:  payload,
	}
	for _, p := range m.allParticipants {
		_ = p.ApplyLogRecord(ctx, clr)
	}
}

func countUncommitted(table map[uint64]*txnRecoveryInfo) int {
	n := 0
	for _, info := range table {
		if info.state != TxnStateCommitted && info.state != TxnStateAborted {
			n++
		}
	}
	return n
}

// --- WAL helpers ---

func (m *Manager) appendTxnRecord(txnID uint64, recType wal.LogRecordType, data []byte) (wal.LSN, error) {
	record := &wal.LogRecord{
		Type:      recType,
		TxnID:     txnID,
		Timestamp: time.Now().UnixNano(),
		Data:      data,
	}
	return m.logManager.AppendRecord(record, wal.LogTypeBtree)
}
