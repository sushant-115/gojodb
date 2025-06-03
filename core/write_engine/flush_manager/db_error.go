package flushmanager

import "errors"

// --- Error Definitions ---

var (
	ErrKeyNotFound                 = errors.New("key not found")
	ErrKeyAlreadyExists            = errors.New("key already exists (for strict insert)")
	ErrInvalidDegree               = errors.New("btree degree must be at least 2")
	ErrNilKeyOrder                 = errors.New("keyOrder function must be provided")
	ErrPageNotFound                = errors.New("page not found in buffer pool")
	ErrBufferPoolFull              = errors.New("buffer pool is full and no pages can be evicted")
	ErrPagePinned                  = errors.New("page is pinned and cannot be evicted")
	ErrSerialization               = errors.New("error during serialization")
	ErrDeserialization             = errors.New("error during deserialization")
	ErrIO                          = errors.New("i/o error")
	ErrChecksumMismatch            = errors.New("page checksum mismatch, data corruption suspected")
	ErrInvalidPageData             = errors.New("invalid page data")
	ErrDBFileExists                = errors.New("database file already exists")
	ErrDBFileNotFound              = errors.New("database file not found")
	ErrUnsupportedKeyType          = errors.New("key type not supported for default serialization")
	ErrUnsupportedValueType        = errors.New("value type not supported for default serialization")
	ErrValueTooLargeForPage        = errors.New("value too large to fit in page with metadata")
	ErrMaxKeysOrChildrenExceeded   = errors.New("maximum number of keys or children per node exceeded for page size")
	ErrBTreeNotInitializedProperly = errors.New("btree not initialized properly (e.g. missing disk manager or buffer pool)")
	ErrLogRecordTooLarge           = errors.New("log record too large for log buffer")
	ErrLogFileError                = errors.New("log file operation error")
	// --- NEW: 2PC Specific Errors ---
	ErrTxnNotFound      = errors.New("transaction not found")
	ErrTxnAlreadyExists = errors.New("transaction already exists in table")
	ErrTxnInvalidState  = errors.New("transaction is in an invalid state for this operation")
	ErrKeyLocked        = errors.New("key is currently locked by another transaction")
	ErrPrepareFailed    = errors.New("prepare phase failed for transaction")
	// --- END NEW ---
	// --- NEW: Iterator Specific Errors ---
	ErrIteratorInvalid = errors.New("iterator is invalid or exhausted")
	// --- END NEW ---
)
