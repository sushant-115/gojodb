package vector

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// --- Helpers ---

func smallConfig() Config {
	return Config{M: 4, EfConstruction: 20, EfSearch: 20}
}

func vec(vals ...float32) []float32 { return vals }

func randomVec(dim int, rng *rand.Rand) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()
	}
	return v
}

// bruteForceKNN returns the k exact nearest neighbours by scanning all nodes.
func bruteForceKNN(idx *HNSW, query []float32, k int) []string {
	type entry struct {
		id   string
		dist float32
	}
	var entries []entry
	idx.mu.RLock()
	for id, n := range idx.nodes {
		if n.deleted {
			continue
		}
		entries = append(entries, entry{id: id, dist: L2Distance(n.vector, query)})
	}
	idx.mu.RUnlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].dist < entries[j].dist })
	if len(entries) > k {
		entries = entries[:k]
	}
	ids := make([]string, len(entries))
	for i, e := range entries {
		ids[i] = e.id
	}
	return ids
}

// --- Basic Operation Tests ---

// TestHNSW_InsertAndSearch verifies that a single inserted vector can be found
// by a nearest-neighbour search with its own coordinates.
func TestHNSW_InsertAndSearch(t *testing.T) {
	idx := NewHNSW(smallConfig())
	require.NoError(t, idx.Insert("a", vec(1, 2, 3)))

	results, err := idx.Search(vec(1, 2, 3), 1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "a", results[0].ID)
	require.InDelta(t, 0, results[0].Distance, 1e-6, "exact match should have distance 0")
}

// TestHNSW_EmptySearch verifies that searching an empty index returns no results.
func TestHNSW_EmptySearch(t *testing.T) {
	idx := NewHNSW(smallConfig())
	results, err := idx.Search(vec(1, 2, 3), 5)
	require.NoError(t, err)
	require.Empty(t, results)
}

// TestHNSW_DuplicateID verifies that inserting a vector with an already-existing
// ID returns an error.
func TestHNSW_DuplicateID(t *testing.T) {
	idx := NewHNSW(smallConfig())
	require.NoError(t, idx.Insert("x", vec(1.0)))
	require.Error(t, idx.Insert("x", vec(2.0)), "duplicate ID should error")
}

// TestHNSW_DimensionMismatch verifies that inserting or searching with the wrong
// number of dimensions returns an error.
func TestHNSW_DimensionMismatch(t *testing.T) {
	idx := NewHNSW(smallConfig())
	require.NoError(t, idx.Insert("a", vec(1, 2, 3)))
	require.Error(t, idx.Insert("b", vec(1, 2)), "wrong dimension on insert should error")
	_, err := idx.Search(vec(1, 2), 1)
	require.Error(t, err, "wrong dimension on search should error")
}

// TestHNSW_Delete verifies that deleted nodes do not appear in search results.
func TestHNSW_Delete(t *testing.T) {
	idx := NewHNSW(smallConfig())
	require.NoError(t, idx.Insert("a", vec(0, 0, 0)))
	require.NoError(t, idx.Insert("b", vec(100, 100, 100)))

	require.NoError(t, idx.Delete("a"))
	require.False(t, idx.Contains("a"))

	results, err := idx.Search(vec(0, 0, 0), 5)
	require.NoError(t, err)
	for _, r := range results {
		require.NotEqual(t, "a", r.ID, "deleted node should not appear in results")
	}
}

// TestHNSW_DeleteNonExistent verifies that deleting an unknown ID returns an error.
func TestHNSW_DeleteNonExistent(t *testing.T) {
	idx := NewHNSW(smallConfig())
	require.Error(t, idx.Delete("ghost"))
}

// TestHNSW_Len verifies that Len tracks inserts and deletes correctly.
func TestHNSW_Len(t *testing.T) {
	idx := NewHNSW(smallConfig())
	require.Equal(t, 0, idx.Len())

	require.NoError(t, idx.Insert("a", vec(1.0)))
	require.NoError(t, idx.Insert("b", vec(2.0)))
	require.Equal(t, 2, idx.Len())

	require.NoError(t, idx.Delete("a"))
	require.Equal(t, 1, idx.Len())
}

// TestHNSW_SearchOrderedByDistance verifies that results are returned in
// ascending distance order.
func TestHNSW_SearchOrderedByDistance(t *testing.T) {
	idx := NewHNSW(smallConfig())
	for i := 0; i < 20; i++ {
		v := vec(float32(i))
		require.NoError(t, idx.Insert(fmt.Sprintf("v%d", i), v))
	}

	results, err := idx.Search(vec(0), 5)
	require.NoError(t, err)
	for i := 1; i < len(results); i++ {
		require.LessOrEqual(t, results[i-1].Distance, results[i].Distance,
			"results should be ordered by ascending distance")
	}
}

// TestHNSW_KNNRecall verifies that for a moderately sized index the HNSW recall
// (fraction of true nearest neighbours returned) is at least 80%.
func TestHNSW_KNNRecall(t *testing.T) {
	const (
		dim  = 32
		n    = 500
		k    = 10
		minRecall = 0.8
	)
	cfg := Config{M: 16, EfConstruction: 200, EfSearch: 128}
	idx := NewHNSW(cfg)
	rng := rand.New(rand.NewSource(99))

	for i := 0; i < n; i++ {
		require.NoError(t, idx.Insert(fmt.Sprintf("v%d", i), randomVec(dim, rng)))
	}

	const numQueries = 20
	totalHits := 0
	for q := 0; q < numQueries; q++ {
		query := randomVec(dim, rng)
		truth := bruteForceKNN(idx, query, k)
		truthSet := make(map[string]bool, len(truth))
		for _, id := range truth {
			truthSet[id] = true
		}
		results, err := idx.Search(query, k)
		require.NoError(t, err)
		for _, r := range results {
			if truthSet[r.ID] {
				totalHits++
			}
		}
	}
	recall := float64(totalHits) / float64(numQueries*k)
	t.Logf("HNSW recall@%d on %d nodes, dim=%d: %.2f", k, n, dim, recall)
	require.GreaterOrEqual(t, recall, minRecall, "recall should be at least %.0f%%", minRecall*100)
}

// TestHNSW_L2Distance verifies L2Distance returns 0 for identical vectors and a
// positive value for distinct vectors.
func TestHNSW_L2Distance(t *testing.T) {
	a := vec(1, 2, 3)
	require.InDelta(t, 0, L2Distance(a, a), 1e-6)
	require.Greater(t, L2Distance(a, vec(4, 5, 6)), float32(0))
}

// TestHNSW_CosineDistance verifies CosineDistance properties:
//   - identical vectors → 0
//   - orthogonal vectors → 1
//   - bounded in [0, 1]
func TestHNSW_CosineDistance(t *testing.T) {
	a := vec(1, 0, 0)
	b := vec(0, 1, 0)

	require.InDelta(t, 0, CosineDistance(a, a), 1e-6, "identical vectors")
	require.InDelta(t, 1, CosineDistance(a, b), 1e-6, "orthogonal vectors")

	// Zero vector edge case — should not panic.
	zero := vec(0, 0, 0)
	result := CosineDistance(a, zero)
	require.Equal(t, float32(1.0), result)
}

// TestHNSW_CosineIndex verifies that inserting and searching with cosine distance
// produces consistent results.
func TestHNSW_CosineIndex(t *testing.T) {
	cfg := Config{M: 8, EfConstruction: 50, EfSearch: 50, Distance: CosineDistance}
	idx := NewHNSW(cfg)

	// Insert unit vectors in various directions.
	vecs := [][]float32{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
		{1, 1, 0},
		{1, 0, 1},
	}
	for i, v := range vecs {
		require.NoError(t, idx.Insert(fmt.Sprintf("v%d", i), v))
	}

	// Querying with {1,0,0} should return v0 as the closest.
	results, err := idx.Search(vec(1, 0, 0), 1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "v0", results[0].ID)
	require.InDelta(t, 0, results[0].Distance, 1e-5)
}

// TestHNSW_SaveLoad verifies that saving the index to disk and reloading it
// produces identical search results.
func TestHNSW_SaveLoad(t *testing.T) {
	cfg := smallConfig()
	idx := NewHNSW(cfg)

	for i := 0; i < 30; i++ {
		require.NoError(t, idx.Insert(fmt.Sprintf("n%d", i), vec(float32(i), float32(i*2))))
	}

	tmpFile := filepath.Join(t.TempDir(), "hnsw.idx")
	require.NoError(t, idx.Save(tmpFile))

	idx2, err := Load(tmpFile, cfg)
	require.NoError(t, err)
	require.Equal(t, idx.Len(), idx2.Len())

	// Search results should be the same.
	query := vec(5, 10)
	r1, err := idx.Search(query, 3)
	require.NoError(t, err)
	r2, err := idx2.Search(query, 3)
	require.NoError(t, err)

	require.Len(t, r2, len(r1))
	for i := range r1 {
		require.Equal(t, r1[i].ID, r2[i].ID)
		require.InDelta(t, r1[i].Distance, r2[i].Distance, 1e-5)
	}
}

// TestHNSW_SaveLoad_NonExistent verifies that loading from a missing path errors.
func TestHNSW_SaveLoad_NonExistent(t *testing.T) {
	_, err := Load("/no/such/file.idx", smallConfig())
	require.Error(t, err)
}

// TestHNSW_SaveLoad_DeletedNodePersisted verifies that deleted nodes survive a
// save/load round-trip and remain absent from search results.
func TestHNSW_SaveLoad_DeletedNodePersisted(t *testing.T) {
	idx := NewHNSW(smallConfig())
	require.NoError(t, idx.Insert("keep", vec(1, 1)))
	require.NoError(t, idx.Insert("del", vec(1, 1)))
	require.NoError(t, idx.Delete("del"))

	p := filepath.Join(t.TempDir(), "hnsw.idx")
	require.NoError(t, idx.Save(p))

	idx2, err := Load(p, smallConfig())
	require.NoError(t, err)
	require.False(t, idx2.Contains("del"), "deleted node should remain deleted after reload")
	require.True(t, idx2.Contains("keep"))
}

// TestHNSW_LargeInsert verifies the index handles a larger number of insertions
// without panicking or corrupting internal state.
func TestHNSW_LargeInsert(t *testing.T) {
	cfg := Config{M: 16, EfConstruction: 100, EfSearch: 64}
	idx := NewHNSW(cfg)
	rng := rand.New(rand.NewSource(7))

	const n = 1000
	for i := 0; i < n; i++ {
		require.NoError(t, idx.Insert(fmt.Sprintf("node-%d", i), randomVec(16, rng)))
	}
	require.Equal(t, n, idx.Len())

	// A random query should return k results without error.
	results, err := idx.Search(randomVec(16, rng), 10)
	require.NoError(t, err)
	require.Len(t, results, 10)
}

// TestHNSW_SearchKGreaterThanN verifies that requesting more results than there are
// nodes returns only the available nodes.
func TestHNSW_SearchKGreaterThanN(t *testing.T) {
	idx := NewHNSW(smallConfig())
	require.NoError(t, idx.Insert("a", vec(1, 2, 3)))
	require.NoError(t, idx.Insert("b", vec(4, 5, 6)))

	results, err := idx.Search(vec(1, 2, 3), 100)
	require.NoError(t, err)
	require.LessOrEqual(t, len(results), 2)
}

// TestHNSW_ConcurrentInserts verifies that concurrent inserts from multiple
// goroutines do not cause data races or panics.
func TestHNSW_ConcurrentInserts(t *testing.T) {
	cfg := Config{M: 8, EfConstruction: 50, EfSearch: 50}
	idx := NewHNSW(cfg)
	rng := rand.New(rand.NewSource(1))

	const goroutines = 8
	const perGoroutine = 50
	vectors := make([][]float32, goroutines*perGoroutine)
	for i := range vectors {
		vectors[i] = randomVec(8, rng)
	}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for k := 0; k < perGoroutine; k++ {
				id := fmt.Sprintf("g%d_k%d", gID, k)
				if err := idx.Insert(id, vectors[gID*perGoroutine+k]); err != nil {
					t.Errorf("concurrent insert error: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()
	require.Equal(t, goroutines*perGoroutine, idx.Len())
}

// TestHNSW_ConcurrentReadsWrites verifies that simultaneous searches and inserts
// do not race.
func TestHNSW_ConcurrentReadsWrites(t *testing.T) {
	cfg := Config{M: 8, EfConstruction: 50, EfSearch: 20}
	idx := NewHNSW(cfg)
	rng := rand.New(rand.NewSource(2))

	// Pre-populate.
	for i := 0; i < 100; i++ {
		_ = idx.Insert(fmt.Sprintf("pre-%d", i), randomVec(8, rng))
	}

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(2)
		localRng := rand.New(rand.NewSource(int64(g)))
		go func(gID int) {
			defer wg.Done()
			for k := 0; k < 20; k++ {
				_ = idx.Insert(fmt.Sprintf("w%d-%d", gID, k), randomVec(8, localRng))
			}
		}(g)
		go func() {
			defer wg.Done()
			for k := 0; k < 20; k++ {
				_, _ = idx.Search(randomVec(8, localRng), 5)
			}
		}()
	}
	wg.Wait()
}

// TestHNSW_ExactMatchAlwaysFirst verifies that when the query vector is identical
// to an inserted vector, that vector is the top result.
func TestHNSW_ExactMatchAlwaysFirst(t *testing.T) {
	idx := NewHNSW(Config{M: 8, EfConstruction: 100, EfSearch: 100})
	rng := rand.New(rand.NewSource(55))

	target := randomVec(16, rng)
	require.NoError(t, idx.Insert("target", target))
	for i := 0; i < 50; i++ {
		require.NoError(t, idx.Insert(fmt.Sprintf("noise-%d", i), randomVec(16, rng)))
	}

	results, err := idx.Search(target, 1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "target", results[0].ID)
	require.InDelta(t, 0, results[0].Distance, 1e-5)
}

// TestHNSW_Contains verifies the Contains predicate.
func TestHNSW_Contains(t *testing.T) {
	idx := NewHNSW(smallConfig())
	require.False(t, idx.Contains("x"))
	require.NoError(t, idx.Insert("x", vec(1.0)))
	require.True(t, idx.Contains("x"))
	require.NoError(t, idx.Delete("x"))
	require.False(t, idx.Contains("x"))
}

// TestHNSW_SaveEmptyIndex verifies that saving and loading an empty index works.
func TestHNSW_SaveEmptyIndex(t *testing.T) {
	idx := NewHNSW(smallConfig())
	p := filepath.Join(t.TempDir(), "empty.idx")
	require.NoError(t, idx.Save(p))
	_, err := os.Stat(p)
	require.NoError(t, err)

	idx2, err := Load(p, smallConfig())
	require.NoError(t, err)
	require.Equal(t, 0, idx2.Len())
}

// TestL2Distance_Properties verifies triangle inequality and symmetry.
func TestL2Distance_Properties(t *testing.T) {
	a := vec(1, 2, 3)
	b := vec(4, 5, 6)
	c := vec(7, 8, 9)

	// Symmetry.
	require.InDelta(t, L2Distance(a, b), L2Distance(b, a), 1e-6)

	// Triangle inequality (squared L2 doesn't satisfy triangle inequality in
	// general; we verify with sqrt).
	sqrtL2 := func(x, y []float32) float32 { return float32(math.Sqrt(float64(L2Distance(x, y)))) }
	require.LessOrEqual(t, sqrtL2(a, c), sqrtL2(a, b)+sqrtL2(b, c)+float32(1e-5))
}
