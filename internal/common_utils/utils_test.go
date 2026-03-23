package commonutils

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// collectSyncMap iterates over all entries in a sync.Map and returns them as a regular map.
func collectSyncMap[K comparable, V any](m *sync.Map) map[K]V {
	result := make(map[K]V)
	m.Range(func(key, value any) bool {
		result[key.(K)] = value.(V)
		return true
	})
	return result
}

func TestCopyToSyncMap_StringToString(t *testing.T) {
	src := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}
	var dst sync.Map
	CopyToSyncMap(src, &dst)

	got := collectSyncMap[string, string](&dst)
	require.Equal(t, src, got)
}

func TestCopyToSyncMap_IntToString(t *testing.T) {
	src := map[int]string{
		1: "one",
		2: "two",
		3: "three",
	}
	var dst sync.Map
	CopyToSyncMap(src, &dst)

	got := collectSyncMap[int, string](&dst)
	require.Equal(t, src, got)
}

func TestCopyToSyncMap_EmptySource(t *testing.T) {
	src := map[string]int{}
	var dst sync.Map
	CopyToSyncMap(src, &dst)

	count := 0
	dst.Range(func(_, _ any) bool {
		count++
		return true
	})
	require.Equal(t, 0, count)
}

func TestCopyToSyncMap_OverwritesExistingValues(t *testing.T) {
	var dst sync.Map
	dst.Store("key1", "old_value")

	src := map[string]string{
		"key1": "new_value",
		"key2": "another_value",
	}
	CopyToSyncMap(src, &dst)

	val, ok := dst.Load("key1")
	require.True(t, ok)
	require.Equal(t, "new_value", val)

	val2, ok2 := dst.Load("key2")
	require.True(t, ok2)
	require.Equal(t, "another_value", val2)
}

func TestCopyToSyncMap_SingleEntry(t *testing.T) {
	src := map[string]int{"only": 42}
	var dst sync.Map
	CopyToSyncMap(src, &dst)

	val, ok := dst.Load("only")
	require.True(t, ok)
	require.Equal(t, 42, val)
}
