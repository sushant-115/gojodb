package transaction

// TransactionState represents the in-memory state of a transaction on a participant.
type TransactionState int

const (
	TxnStateRunning   TransactionState = iota // Transaction is active, operations are being applied
	TxnStatePrepared                          // Participant has voted COMMIT and is waiting for global decision
	TxnStateCommitted                         // Participant has received COMMIT decision
	TxnStateAborted                           // Participant has received ABORT decision or decided to abort locally
)

// TransactionOperation represents a single operation within a distributed transaction on a participant.
type TransactionOperation struct {
	Command string `json:"command"` // PUT or DELETE
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
}

// Transaction represents an in-memory record of an active or prepared transaction.
type Transaction struct {
	ID        uint64
	State     TransactionState
	Operation []TransactionOperation
	// Operations: For undo, you might need to store the operations performed by this txn
	// on this shard, or at least the pages/keys it modified.
	// For V1, we'll rely on WAL for recovery.
	// LocksHeld: Keep track of locks held by this transaction.
	locksHeld map[string]struct{} // Set of keys locked by this transaction
}
