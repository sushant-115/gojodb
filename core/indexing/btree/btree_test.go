package btree

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/sushant-115/gojodb/core/indexing"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	"go.uber.org/zap"
)

// --- Test Helpers ---

var stringSerializer = KeyValueSerializer[string, string]{
	SerializeKey:     SerializeString,
	DeserializeKey:   DeserializeString,
	SerializeValue:   SerializeString,
	DeserializeValue: DeserializeString,
}

func newTestBTree(t *testing.T) (*BTree[string, string], func()) {
	t.Helper()
	logger, _ := zap.NewDevelopment()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	walPath := filepath.Join(tmpDir, "wal")

	lm, err := wal.NewLogManager(walPath, logger, indexing.BTreeIndexType)
	require.NoError(t, err)

	bt, err := NewBTreeFile[string, string](
		dbPath, 3,
		DefaultKeyOrder[string],
		stringSerializer,
		256, 4096, 64,
		lm, logger.Named("btree"),
	)
	require.NoError(t, err)

	return bt, func() {
		bt.Close()
		lm.Close()
	}
}

// reopenBTree closes the current tree and reopens it from the same file/WAL directory.
func reopenBTree(t *testing.T, dbPath, walPath string) (*BTree[string, string], *wal.LogManager) {
	t.Helper()
	logger, _ := zap.NewDevelopment()

	lm, err := wal.NewLogManager(walPath, logger, indexing.BTreeIndexType)
	require.NoError(t, err)

	bt, err := OpenBTreeFile[string, string](
		dbPath,
		DefaultKeyOrder[string],
		stringSerializer,
		256, 4096, 64,
		lm, logger.Named("btree_reopen"),
	)
	require.NoError(t, err)
	return bt, lm
}

// --- Basic CRUD Tests ---

// TestBTree_InsertAndSearch verifies that an inserted key can be found by Search.
func TestBTree_InsertAndSearch(t *testing.T) {
	bt, cleanup := newTestBTree(t)
	defer cleanup()

	require.NoError(t, bt.Insert("hello", "world", 0))

	v, found, err := bt.Search("hello")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "world", v)
}

// TestBTree_SearchMissingKey verifies that searching for a non-existent key returns
// found=false without an error.
func TestBTree_SearchMissingKey(t *testing.T) {
	bt, cleanup := newTestBTree(t)
	defer cleanup()

	_, found, err := bt.Search("missing")
	require.NoError(t, err)
	require.False(t, found)
}

// TestBTree_InsertAndDelete verifies that after deleting a key it is no longer findable.
func TestBTree_InsertAndDelete(t *testing.T) {
	bt, cleanup := newTestBTree(t)
	defer cleanup()

	require.NoError(t, bt.Insert("key1", "val1", 0))
	require.NoError(t, bt.Delete("key1", 0))

	_, found, err := bt.Search("key1")
	require.NoError(t, err)
	require.False(t, found, "deleted key should not be found")
}

// TestBTree_DeleteMissingKeyErrors verifies that deleting a non-existent key returns an error.
func TestBTree_DeleteMissingKeyErrors(t *testing.T) {
	bt, cleanup := newTestBTree(t)
	defer cleanup()

	err := bt.Delete("nonexistent", 0)
	require.Error(t, err)
}

// TestBTree_MultipleInserts verifies that many distinct keys can be inserted and
// individually retrieved without interference.
func TestBTree_MultipleInserts(t *testing.T) {
	bt, cleanup := newTestBTree(t)
	defer cleanup()

	const n = 50
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key_%03d", i)
		val := fmt.Sprintf("val_%03d", i)
		require.NoError(t, bt.Insert(key, val, 0), "insert %d", i)
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key_%03d", i)
		expected := fmt.Sprintf("val_%03d", i)
		v, found, err := bt.Search(key)
		require.NoError(t, err, "search %d", i)
		require.True(t, found, "key %s should be found", key)
		require.Equal(t, expected, v)
	}
}

// TestBTree_GetSizeTracksEntries verifies that GetSize returns the correct count of
// entries after inserts and deletes.
func TestBTree_GetSizeTracksEntries(t *testing.T) {
	bt, cleanup := newTestBTree(t)
	defer cleanup()

	size, err := bt.GetSize()
	require.NoError(t, err)
	require.Equal(t, uint64(0), size)

	require.NoError(t, bt.Insert("a", "1", 0))
	require.NoError(t, bt.Insert("b", "2", 0))
	require.NoError(t, bt.Insert("c", "3", 0))

	size, err = bt.GetSize()
	require.NoError(t, err)
	require.Equal(t, uint64(3), size)

	require.NoError(t, bt.Delete("b", 0))

	size, err = bt.GetSize()
	require.NoError(t, err)
	require.Equal(t, uint64(2), size)
}

// --- Persistence Tests ---

// TestBTree_PersistenceAcrossReopen verifies that data written to a B-tree survives
// a Close and subsequent reopen of the same file.
func TestBTree_PersistenceAcrossReopen(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "persist.db")
	walPath := filepath.Join(tmpDir, "wal")

	lm1, err := wal.NewLogManager(walPath, logger, indexing.BTreeIndexType)
	require.NoError(t, err)

	bt1, err := NewBTreeFile[string, string](
		dbPath, 3,
		DefaultKeyOrder[string],
		stringSerializer,
		256, 4096, 64,
		lm1, logger.Named("bt1"),
	)
	require.NoError(t, err)

	// Insert some keys.
	keys := []string{"alpha", "beta", "gamma", "delta"}
	for _, k := range keys {
		require.NoError(t, bt1.Insert(k, "value_"+k, 0))
	}

	require.NoError(t, bt1.Close())
	require.NoError(t, lm1.Close())

	// Reopen and verify.
	bt2, lm2 := reopenBTree(t, dbPath, walPath)
	defer bt2.Close()
	defer lm2.Close()

	for _, k := range keys {
		v, found, err := bt2.Search(k)
		require.NoError(t, err, "search after reopen: %s", k)
		require.True(t, found, "key %s should survive reopen", k)
		require.Equal(t, "value_"+k, v)
	}
}

// TestBTree_DeletedKeyNotFoundAfterReopen verifies that deleted keys are absent after
// reopening the B-tree.
func TestBTree_DeletedKeyNotFoundAfterReopen(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "del_persist.db")
	walPath := filepath.Join(tmpDir, "wal")

	lm1, err := wal.NewLogManager(walPath, logger, indexing.BTreeIndexType)
	require.NoError(t, err)

	bt1, err := NewBTreeFile[string, string](dbPath, 3, DefaultKeyOrder[string], stringSerializer, 256, 4096, 64, lm1, logger.Named("bt1"))
	require.NoError(t, err)

	require.NoError(t, bt1.Insert("keep", "this", 0))
	require.NoError(t, bt1.Insert("remove", "this", 0))
	require.NoError(t, bt1.Delete("remove", 0))
	require.NoError(t, bt1.Close())
	require.NoError(t, lm1.Close())

	bt2, lm2 := reopenBTree(t, dbPath, walPath)
	defer bt2.Close()
	defer lm2.Close()

	_, found, err := bt2.Search("remove")
	require.NoError(t, err)
	require.False(t, found, "'remove' should be absent after reopen")

	v, found, err := bt2.Search("keep")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "this", v)
}

// --- Range Scan (Iterator) Tests ---

// drainIter collects all keys returned by a BTreeIterator into a slice.
func drainIter(t *testing.T, iter BTreeIterator[string, string]) []string {
	t.Helper()
	var keys []string
	for {
		k, _, ok, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		keys = append(keys, k)
	}
	return keys
}

// TestBTree_FullScanReturnsAllKeys verifies that FullScan visits every inserted key in
// ascending order.
func TestBTree_FullScanReturnsAllKeys(t *testing.T) {
	bt, cleanup := newTestBTree(t)
	defer cleanup()

	inserted := []string{"dog", "apple", "cat", "banana", "elderberry"}
	for _, k := range inserted {
		require.NoError(t, bt.Insert(k, k+"_val", 0))
	}

	iter, err := bt.FullScan()
	require.NoError(t, err)
	defer iter.Close()

	got := drainIter(t, iter)
	sort.Strings(inserted)
	require.Equal(t, inserted, got)
}

// TestBTree_IteratorRangeScan verifies that Iterator returns only keys within [start, end].
func TestBTree_IteratorRangeScan(t *testing.T) {
	bt, cleanup := newTestBTree(t)
	defer cleanup()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key_%02d", i)
		require.NoError(t, bt.Insert(key, "v", 0))
	}

	// Scan [key_03, key_06].
	iter, err := bt.Iterator("key_03", "key_06")
	require.NoError(t, err)
	defer iter.Close()

	got := drainIter(t, iter)
	expected := []string{"key_03", "key_04", "key_05", "key_06"}
	require.Equal(t, expected, got)
}

// TestBTree_FullScanEmptyTree verifies that FullScan on an empty tree returns no keys.
func TestBTree_FullScanEmptyTree(t *testing.T) {
	bt, cleanup := newTestBTree(t)
	defer cleanup()

	// Empty tree: Iterator/FullScan should return an error or an immediately-exhausted iterator.
	iter, err := bt.FullScan()
	if err != nil {
		// Acceptable: some implementations error on empty tree.
		return
	}
	defer iter.Close()

	_, _, ok, err := iter.Next()
	require.NoError(t, err)
	require.False(t, ok, "empty tree should produce no results")
}

// --- Split / Merge (Large Insert) Tests ---

// TestBTree_SplitOnInsert verifies that the B-tree correctly handles node splits when
// filling nodes beyond capacity (degree 3 → max 5 keys per node).
func TestBTree_SplitOnInsert(t *testing.T) {
	bt, cleanup := newTestBTree(t)
	defer cleanup()

	// Insert enough keys to force multiple splits.
	const n = 200
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("split_key_%05d", i)
		require.NoError(t, bt.Insert(key, "v", 0), "insert %d", i)
	}

	// All keys must be findable.
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("split_key_%05d", i)
		_, found, err := bt.Search(key)
		require.NoError(t, err)
		require.True(t, found, "key %s missing after split", key)
	}

	size, err := bt.GetSize()
	require.NoError(t, err)
	require.Equal(t, uint64(n), size)
}

// TestBTree_DeleteAndMerge verifies that deleting many keys (triggering node merges)
// leaves the remaining keys intact and searchable.
func TestBTree_DeleteAndMerge(t *testing.T) {
	bt, cleanup := newTestBTree(t)
	defer cleanup()

	const n = 100
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("merge_key_%05d", i)
		require.NoError(t, bt.Insert(keys[i], "v", 0))
	}

	// Delete every other key.
	for i := 0; i < n; i += 2 {
		require.NoError(t, bt.Delete(keys[i], 0))
	}

	// Odd-index keys should still be present; even-index should be gone.
	for i := 0; i < n; i++ {
		_, found, err := bt.Search(keys[i])
		require.NoError(t, err)
		if i%2 == 0 {
			require.False(t, found, "even key %s should be deleted", keys[i])
		} else {
			require.True(t, found, "odd key %s should remain", keys[i])
		}
	}
}

// --- Concurrency Tests ---

// TestBTree_ConcurrentInserts verifies that concurrent inserts from multiple goroutines
// all succeed and all inserted keys are searchable afterward.
func TestBTree_ConcurrentInserts(t *testing.T) {
	bt, cleanup := newTestBTree(t)
	defer cleanup()

	const numGoroutines = 10
	const keysPerGoroutine = 20

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for k := 0; k < keysPerGoroutine; k++ {
				key := fmt.Sprintf("g%d_k%03d", gID, k)
				if err := bt.Insert(key, "v", 0); err != nil {
					t.Errorf("goroutine %d: insert error: %v", gID, err)
				}
			}
		}(g)
	}
	wg.Wait()

	// All keys must be found.
	for g := 0; g < numGoroutines; g++ {
		for k := 0; k < keysPerGoroutine; k++ {
			key := fmt.Sprintf("g%d_k%03d", g, k)
			_, found, err := bt.Search(key)
			require.NoError(t, err)
			require.True(t, found, "key %s not found after concurrent insert", key)
		}
	}
}

// TestBTree_ConcurrentReads verifies that concurrent reads on a populated tree all
// return consistent results without panics or data races.
func TestBTree_ConcurrentReads(t *testing.T) {
	bt, cleanup := newTestBTree(t)
	defer cleanup()

	const n = 30
	for i := 0; i < n; i++ {
		require.NoError(t, bt.Insert(fmt.Sprintf("key_%02d", i), "value", 0))
	}

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("key_%02d", i)
				v, found, err := bt.Search(key)
				if err != nil {
					t.Errorf("search error: %v", err)
					return
				}
				if !found {
					t.Errorf("key %s not found during concurrent read", key)
					return
				}
				if v != "value" {
					t.Errorf("key %s returned wrong value: %s", key, v)
				}
			}
		}()
	}
	wg.Wait()
}

// --- Helper function tests ---

// TestSerializeDeserializeString verifies the string serializer round-trips correctly.
func TestSerializeDeserializeString(t *testing.T) {
	cases := []string{"", "hello", "foo bar baz", "unicode: 日本語"}
	for _, s := range cases {
		encoded, err := SerializeString(s)
		require.NoError(t, err)
		decoded, err := DeserializeString(encoded)
		require.NoError(t, err)
		require.Equal(t, s, decoded)
	}
}

// TestDefaultKeyOrder verifies the ordering function for strings.
func TestDefaultKeyOrder(t *testing.T) {
	require.Negative(t, DefaultKeyOrder[string]("apple", "banana"))
	require.Positive(t, DefaultKeyOrder[string]("zebra", "ant"))
	require.Zero(t, DefaultKeyOrder[string]("same", "same"))
}
