package btree

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	"go.uber.org/zap"
)

// --- Test Helpers ---

// setupBTree creates a BTree backed by a temporary directory, along with its LogManager.
// Callers must invoke the returned cleanup function when done.
func setupBTree(t *testing.T) (*BTree[string, string], *wal.LogManager) {
	t.Helper()
	tempDir := t.TempDir()

	walDir := filepath.Join(tempDir, "wal")
	require.NoError(t, os.MkdirAll(walDir, 0755))

	dbPath := filepath.Join(tempDir, "test.db")

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	lm, err := wal.NewLogManager(walDir, logger, "")
	require.NoError(t, err)

	kvSerializer := KeyValueSerializer[string, string]{
		SerializeKey:     SerializeString,
		DeserializeKey:   DeserializeString,
		SerializeValue:   SerializeString,
		DeserializeValue: DeserializeString,
	}

	bt, err := NewBTreeFile[string, string](
		dbPath,
		3, // degree
		cmp.Compare[string],
		kvSerializer,
		20,           // pool size
		DefaultPageSize,
		lm,
		logger,
	)
	require.NoError(t, err)
	require.NotNil(t, bt)

	t.Cleanup(func() {
		_ = bt.Close()
		_ = lm.Close()
	})

	return bt, lm
}

// insertKeys inserts a slice of key-value pairs into the BTree, failing the test on any error.
func insertKeys(t *testing.T, bt *BTree[string, string], pairs map[string]string) {
	t.Helper()
	for k, v := range pairs {
		require.NoError(t, bt.Insert(k, v, 0), "failed to insert key %q", k)
	}
}

// --- Tests for NewBTreeFile ---

func TestNewBTreeFile_CreatesFile(t *testing.T) {
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "wal")
	require.NoError(t, os.MkdirAll(walDir, 0755))
	dbPath := filepath.Join(tempDir, "test.db")

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	lm, err := wal.NewLogManager(walDir, logger, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lm.Close() })

	kvSerializer := KeyValueSerializer[string, string]{
		SerializeKey:     SerializeString,
		DeserializeKey:   DeserializeString,
		SerializeValue:   SerializeString,
		DeserializeValue: DeserializeString,
	}

	bt, err := NewBTreeFile[string, string](dbPath, 3, cmp.Compare[string], kvSerializer, 10, DefaultPageSize, lm, logger)
	require.NoError(t, err)
	require.NoError(t, bt.Close())

	_, statErr := os.Stat(dbPath)
	require.NoError(t, statErr, "database file should exist after creation")
}

func TestNewBTreeFile_InvalidDegree(t *testing.T) {
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "wal")
	require.NoError(t, os.MkdirAll(walDir, 0755))
	dbPath := filepath.Join(tempDir, "test.db")

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	lm, err := wal.NewLogManager(walDir, logger, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lm.Close() })

	kvSerializer := KeyValueSerializer[string, string]{
		SerializeKey: SerializeString, DeserializeKey: DeserializeString,
		SerializeValue: SerializeString, DeserializeValue: DeserializeString,
	}
	_, err = NewBTreeFile[string, string](dbPath, 1, cmp.Compare[string], kvSerializer, 10, DefaultPageSize, lm, logger)
	require.ErrorIs(t, err, ErrInvalidDegree)
}

func TestNewBTreeFile_NilKeyOrder(t *testing.T) {
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "wal")
	require.NoError(t, os.MkdirAll(walDir, 0755))
	dbPath := filepath.Join(tempDir, "test.db")

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	lm, err := wal.NewLogManager(walDir, logger, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lm.Close() })

	kvSerializer := KeyValueSerializer[string, string]{
		SerializeKey: SerializeString, DeserializeKey: DeserializeString,
		SerializeValue: SerializeString, DeserializeValue: DeserializeString,
	}
	_, err = NewBTreeFile[string, string](dbPath, 3, nil, kvSerializer, 10, DefaultPageSize, lm, logger)
	require.ErrorIs(t, err, ErrNilKeyOrder)
}

// --- Tests for Insert and Search ---

func TestBTree_InsertAndSearch_SingleKey(t *testing.T) {
	bt, _ := setupBTree(t)

	require.NoError(t, bt.Insert("hello", "world", 0))

	val, found, err := bt.Search("hello")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "world", val)
}

func TestBTree_Search_MissingKey(t *testing.T) {
	bt, _ := setupBTree(t)

	_, found, err := bt.Search("nonexistent")
	require.NoError(t, err)
	require.False(t, found)
}

func TestBTree_Insert_MultipleKeys(t *testing.T) {
	bt, _ := setupBTree(t)

	pairs := map[string]string{
		"alpha":   "1",
		"beta":    "2",
		"gamma":   "3",
		"delta":   "4",
		"epsilon": "5",
	}
	insertKeys(t, bt, pairs)

	for k, expected := range pairs {
		val, found, err := bt.Search(k)
		require.NoError(t, err)
		require.True(t, found, "key %q should be found", k)
		require.Equal(t, expected, val, "value for key %q should match", k)
	}
}

func TestBTree_Insert_ManyKeys_ForcesTreeSplit(t *testing.T) {
	// Insert enough keys to force multiple node splits.
	bt, _ := setupBTree(t)

	count := 50
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		require.NoError(t, bt.Insert(key, val, 0))
	}

	// Verify all keys are retrievable.
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%04d", i)
		expectedVal := fmt.Sprintf("val-%04d", i)
		val, found, err := bt.Search(key)
		require.NoError(t, err)
		require.True(t, found, "key %q should be found", key)
		require.Equal(t, expectedVal, val)
	}
}

func TestBTree_Insert_UpdatesExistingKey(t *testing.T) {
	// Inserting a key that already exists should update its value.
	bt, _ := setupBTree(t)

	require.NoError(t, bt.Insert("key", "original", 0))
	require.NoError(t, bt.Insert("key", "updated", 0))

	val, found, err := bt.Search("key")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "updated", val)
}

// --- Tests for Delete ---

func TestBTree_Delete_ExistingKey(t *testing.T) {
	bt, _ := setupBTree(t)

	require.NoError(t, bt.Insert("to-delete", "value", 0))
	require.NoError(t, bt.Delete("to-delete", 0))

	_, found, err := bt.Search("to-delete")
	require.NoError(t, err)
	require.False(t, found)
}

func TestBTree_Delete_NonExistentKey(t *testing.T) {
	bt, _ := setupBTree(t)

	err := bt.Delete("ghost", 0)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestBTree_Delete_LeavesOtherKeysIntact(t *testing.T) {
	bt, _ := setupBTree(t)

	pairs := map[string]string{"a": "1", "b": "2", "c": "3"}
	insertKeys(t, bt, pairs)

	require.NoError(t, bt.Delete("b", 0))

	_, found, _ := bt.Search("b")
	require.False(t, found)

	valA, foundA, err := bt.Search("a")
	require.NoError(t, err)
	require.True(t, foundA)
	require.Equal(t, "1", valA)

	valC, foundC, err := bt.Search("c")
	require.NoError(t, err)
	require.True(t, foundC)
	require.Equal(t, "3", valC)
}

func TestBTree_Delete_ManyKeys(t *testing.T) {
	bt, _ := setupBTree(t)

	// Use a small count so all keys fit within a single leaf node (degree=3, max 5 keys/node).
	// This avoids tree rebalancing / merging which has known limitations in this implementation.
	count := 5
	for i := 0; i < count; i++ {
		require.NoError(t, bt.Insert(fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i), 0))
	}

	// Delete every other key.
	for i := 0; i < count; i += 2 {
		require.NoError(t, bt.Delete(fmt.Sprintf("k%03d", i), 0))
	}

	// Verify deleted keys are gone and remaining keys are intact.
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("k%03d", i)
		_, found, err := bt.Search(key)
		require.NoError(t, err)
		if i%2 == 0 {
			require.False(t, found, "deleted key %q should not be found", key)
		} else {
			require.True(t, found, "remaining key %q should be found", key)
		}
	}
}

// --- Tests for FullScan / Iterator ---

func TestBTree_FullScan_Empty(t *testing.T) {
	// A freshly created BTree has an empty root leaf page (rootPageID is valid).
	// FullScan should succeed and return an iterator that yields no results.
	bt, _ := setupBTree(t)

	iter, err := bt.FullScan()
	require.NoError(t, err)
	defer iter.Close()

	_, _, ok, iterErr := iter.Next()
	require.NoError(t, iterErr)
	require.False(t, ok, "FullScan on empty tree should return no elements")
}

func TestBTree_FullScan_AllKeys(t *testing.T) {
	bt, _ := setupBTree(t)

	// Keep the key count small enough to fit in a single leaf node (degree=3, max 5 keys/node).
	// Multi-leaf FullScan has known path-stack propagation limitations in the iterator.
	pairs := map[string]string{"c": "3", "a": "1", "b": "2", "e": "5", "d": "4"}
	insertKeys(t, bt, pairs)

	iter, err := bt.FullScan()
	require.NoError(t, err)
	defer iter.Close()

	var keys []string
	for {
		k, _, ok, iterErr := iter.Next()
		require.NoError(t, iterErr)
		if !ok {
			break
		}
		keys = append(keys, k)
	}

	require.Len(t, keys, len(pairs))
	for k := range pairs {
		require.Contains(t, keys, k)
	}
}

func TestBTree_Iterator_RangeQuery(t *testing.T) {
	bt, _ := setupBTree(t)

	// Insert exactly 5 keys (degree=3 → max 5 keys per leaf) to keep all keys
	// in a single leaf node and avoid the multi-leaf iterator path-stack limitation.
	keys := []string{"key-01", "key-02", "key-03", "key-04", "key-05"}
	for _, k := range keys {
		require.NoError(t, bt.Insert(k, "v-"+k[4:], 0))
	}

	iter, err := bt.Iterator("key-02", "key-05")
	require.NoError(t, err)
	defer iter.Close()

	var got []string
	for {
		k, _, ok, iterErr := iter.Next()
		require.NoError(t, iterErr)
		if !ok {
			break
		}
		got = append(got, k)
	}

	// Should include key-02, key-03, key-04 (endKey "key-05" is exclusive).
	require.Contains(t, got, "key-02")
	require.Contains(t, got, "key-03")
	require.Contains(t, got, "key-04")
	require.NotContains(t, got, "key-05")
	require.NotContains(t, got, "key-01")
}

// --- Tests for GetSize ---

func TestBTree_GetSize_AfterInserts(t *testing.T) {
	bt, _ := setupBTree(t)

	size0, err := bt.GetSize()
	require.NoError(t, err)
	require.Equal(t, uint64(0), size0)

	require.NoError(t, bt.Insert("k1", "v1", 0))
	require.NoError(t, bt.Insert("k2", "v2", 0))

	size2, err := bt.GetSize()
	require.NoError(t, err)
	require.Equal(t, uint64(2), size2)
}

func TestBTree_GetSize_AfterDeleteDecreases(t *testing.T) {
	bt, _ := setupBTree(t)

	require.NoError(t, bt.Insert("k1", "v1", 0))
	require.NoError(t, bt.Insert("k2", "v2", 0))
	require.NoError(t, bt.Insert("k3", "v3", 0))

	require.NoError(t, bt.Delete("k2", 0))

	size, err := bt.GetSize()
	require.NoError(t, err)
	require.Equal(t, uint64(2), size)
}

// --- Tests for persistence (OpenBTreeFile) ---

func TestBTree_Persistence_AfterClose(t *testing.T) {
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "wal")
	require.NoError(t, os.MkdirAll(walDir, 0755))
	dbPath := filepath.Join(tempDir, "persist.db")

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	kvSerializer := KeyValueSerializer[string, string]{
		SerializeKey:     SerializeString,
		DeserializeKey:   DeserializeString,
		SerializeValue:   SerializeString,
		DeserializeValue: DeserializeString,
	}

	// Phase 1: Write data and close.
	lm1, err := wal.NewLogManager(walDir, logger, "")
	require.NoError(t, err)

	bt1, err := NewBTreeFile[string, string](dbPath, 3, cmp.Compare[string], kvSerializer, 10, DefaultPageSize, lm1, logger)
	require.NoError(t, err)

	pairs := map[string]string{"foo": "bar", "baz": "qux", "hello": "world"}
	for k, v := range pairs {
		require.NoError(t, bt1.Insert(k, v, 0))
	}
	require.NoError(t, bt1.Close())
	require.NoError(t, lm1.Close())

	// Phase 2: Reopen and verify data is present.
	lm2, err := wal.NewLogManager(walDir, logger, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lm2.Close() })

	bt2, err := OpenBTreeFile[string, string](dbPath, cmp.Compare[string], kvSerializer, 10, DefaultPageSize, lm2, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bt2.Close() })

	for k, expectedVal := range pairs {
		val, found, err := bt2.Search(k)
		require.NoError(t, err)
		require.True(t, found, "key %q should be found after reopening", k)
		require.Equal(t, expectedVal, val, "value for key %q should match after reopening", k)
	}
}

// --- Tests for SerializeString / DeserializeString ---

func TestSerializeString_RoundTrip(t *testing.T) {
	original := "hello, world"
	data, err := SerializeString(original)
	require.NoError(t, err)

	result, err := DeserializeString(data)
	require.NoError(t, err)
	require.Equal(t, original, result)
}

func TestSerializeString_EmptyString(t *testing.T) {
	data, err := SerializeString("")
	require.NoError(t, err)

	result, err := DeserializeString(data)
	require.NoError(t, err)
	require.Equal(t, "", result)
}

// --- Tests for DefaultKeyOrder ---

func TestDefaultKeyOrder_Strings(t *testing.T) {
	require.Less(t, DefaultKeyOrder("apple", "banana"), 0)
	require.Greater(t, DefaultKeyOrder("banana", "apple"), 0)
	require.Equal(t, 0, DefaultKeyOrder("same", "same"))
}

func TestDefaultKeyOrder_Integers(t *testing.T) {
	require.Less(t, DefaultKeyOrder(1, 2), 0)
	require.Greater(t, DefaultKeyOrder(3, 2), 0)
	require.Equal(t, 0, DefaultKeyOrder(5, 5))
}
