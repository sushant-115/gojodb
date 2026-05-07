// Package vector provides a Hierarchical Navigable Small World (HNSW) index for
// approximate nearest-neighbour (ANN) search on float32 vectors.
//
// # Algorithm overview
//
// HNSW builds a multi-layer proximity graph.  Layer 0 contains every node; each
// higher layer contains an exponentially smaller random subset.  Insertions and
// searches enter from the top (sparsest) layer and refine greedy traversals as
// they descend.  The result is sub-linear query complexity while maintaining
// high recall.
//
// # Typical usage
//
//	idx := vector.NewHNSW(vector.Config{M: 16, EfConstruction: 200, EfSearch: 64})
//	idx.Insert("doc1", []float32{0.1, 0.2, 0.3})
//	results, _ := idx.Search([]float32{0.1, 0.2, 0.3}, 5)
package vector

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
)

// DistanceFunc computes a non-negative scalar distance between two vectors.
// Lower values mean more similar.
type DistanceFunc func(a, b []float32) float32

// L2Distance returns the squared Euclidean (L2) distance.  The square root is
// omitted for efficiency; relative ordering is preserved.
func L2Distance(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

// CosineDistance returns 1 − cosine_similarity, so that identical vectors score
// 0 and orthogonal vectors score 1.
func CosineDistance(a, b []float32) float32 {
	var dot, normA, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 1.0
	}
	sim := dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
	if sim > 1 {
		sim = 1
	}
	return 1 - sim
}

// Config holds the HNSW hyperparameters.
type Config struct {
	// M is the maximum number of bi-directional connections per node per layer.
	// Typical values: 8–64.  Default 16.
	M int
	// EfConstruction is the size of the dynamic candidate list used during
	// index construction.  Higher values improve recall at the cost of slower
	// inserts.  Default 200.
	EfConstruction int
	// EfSearch is the size of the dynamic candidate list used during queries.
	// Higher values improve recall at the cost of slower searches.  Default 64.
	EfSearch int
	// Distance is the distance function.  Default: L2Distance.
	Distance DistanceFunc
	// Seed is the random seed for level assignment reproducibility.  0 uses
	// a fixed default.
	Seed int64
}

func (c *Config) applyDefaults() {
	if c.M == 0 {
		c.M = 16
	}
	if c.EfConstruction == 0 {
		c.EfConstruction = 200
	}
	if c.EfSearch == 0 {
		c.EfSearch = 64
	}
	if c.Distance == nil {
		c.Distance = L2Distance
	}
}

// SearchResult is a single nearest-neighbour hit.
type SearchResult struct {
	ID       string
	Distance float32
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

type hnswNode struct {
	id        string
	vector    []float32
	neighbors [][]string // neighbors[level] → list of neighbor IDs
	deleted   bool
}

// candidate is used in both min- and max-heap priority queues.
type candidate struct {
	id   string
	dist float32
}

// minHeap orders candidates by ascending distance.
type minHeap []candidate

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].dist < h[j].dist }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(candidate)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// maxHeap orders candidates by descending distance (farthest first).
type maxHeap []candidate

func (h maxHeap) Len() int            { return len(h) }
func (h maxHeap) Less(i, j int) bool  { return h[i].dist > h[j].dist }
func (h maxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x interface{}) { *h = append(*h, x.(candidate)) }
func (h *maxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// ---------------------------------------------------------------------------
// HNSW index
// ---------------------------------------------------------------------------

// HNSW is a thread-safe Hierarchical Navigable Small World index.
type HNSW struct {
	mu         sync.RWMutex
	nodes      map[string]*hnswNode
	entryPoint string
	maxLevel   int
	dim        int // inferred from first insert; 0 if empty
	cfg        Config
	mL         float64 // level normalisation factor = 1 / ln(M)
	rng        *rand.Rand
}

// NewHNSW creates a new, empty HNSW index with the given configuration.
func NewHNSW(cfg Config) *HNSW {
	cfg.applyDefaults()
	seed := cfg.Seed
	if seed == 0 {
		seed = 42
	}
	return &HNSW{
		nodes: make(map[string]*hnswNode),
		cfg:   cfg,
		mL:    1.0 / math.Log(float64(cfg.M)),
		rng:   rand.New(rand.NewSource(seed)), // #nosec G404 — not security-critical
	}
}

// Insert adds a vector to the index under the given ID.
// Returns an error if the ID already exists or if the vector's dimension differs
// from previously inserted vectors.
func (h *HNSW) Insert(id string, vector []float32) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, exists := h.nodes[id]; exists {
		return fmt.Errorf("hnsw: ID %q already exists", id)
	}
	if h.dim == 0 {
		h.dim = len(vector)
	} else if len(vector) != h.dim {
		return fmt.Errorf("hnsw: dimension mismatch: index has dim %d, got %d", h.dim, len(vector))
	}

	nodeLevel := h.randomLevel()
	vec := make([]float32, len(vector))
	copy(vec, vector)
	node := &hnswNode{
		id:        id,
		vector:    vec,
		neighbors: make([][]string, nodeLevel+1),
	}
	for i := range node.neighbors {
		node.neighbors[i] = []string{}
	}

	// First node becomes the entry point.
	if h.entryPoint == "" {
		h.nodes[id] = node
		h.entryPoint = id
		h.maxLevel = nodeLevel
		return nil
	}

	// Register the node now so back-edge pruning can look up its vector.
	h.nodes[id] = node

	ep := h.entryPoint

	// Phase 1: greedy descent from maxLevel to nodeLevel+1 (coarse search).
	for lc := h.maxLevel; lc > nodeLevel; lc-- {
		ep = h.greedySearch(ep, vec, lc)
	}

	// Phase 2: beam search from min(nodeLevel, maxLevel) down to 0.
	topLevel := nodeLevel
	if h.maxLevel < topLevel {
		topLevel = h.maxLevel
	}
	for lc := topLevel; lc >= 0; lc-- {
		M := h.cfg.M
		if lc == 0 {
			M = h.cfg.M * 2 // layer 0 allows up to 2×M connections per node
		}
		candidates := h.searchLayer(ep, vec, h.cfg.EfConstruction, lc)
		neighbors := h.selectNeighbors(candidates, M)

		node.neighbors[lc] = make([]string, 0, len(neighbors))
		for _, nb := range neighbors {
			node.neighbors[lc] = append(node.neighbors[lc], nb.id)
			// Add reciprocal edge.
			nbNode := h.nodes[nb.id]
			if nbNode != nil && lc < len(nbNode.neighbors) {
				nbNode.neighbors[lc] = append(nbNode.neighbors[lc], id)
				if len(nbNode.neighbors[lc]) > M {
					nbNode.neighbors[lc] = h.pruneNeighbors(nbNode.vector, nbNode.neighbors[lc], M)
				}
			}
		}
		if len(neighbors) > 0 {
			ep = neighbors[0].id
		}
	}

	if nodeLevel > h.maxLevel {
		h.maxLevel = nodeLevel
		h.entryPoint = id
	}
	return nil
}

// Search returns up to k approximate nearest neighbours to query in ascending
// order of distance.  Deleted nodes are excluded.
func (h *HNSW) Search(query []float32, k int) ([]SearchResult, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.entryPoint == "" {
		return nil, nil
	}
	if len(query) != h.dim {
		return nil, fmt.Errorf("hnsw: dimension mismatch: index has dim %d, got %d", h.dim, len(query))
	}

	ep := h.entryPoint
	for lc := h.maxLevel; lc > 0; lc-- {
		ep = h.greedySearch(ep, query, lc)
	}

	ef := h.cfg.EfSearch
	if ef < k {
		ef = k
	}
	candidates := h.searchLayer(ep, query, ef, 0)

	results := make([]SearchResult, 0, k)
	for _, c := range candidates {
		n := h.nodes[c.id]
		if n == nil || n.deleted {
			continue
		}
		results = append(results, SearchResult{ID: c.id, Distance: c.dist})
		if len(results) == k {
			break
		}
	}
	return results, nil
}

// Delete soft-deletes a node.  It remains in the graph (for navigation) but is
// excluded from Search results.
func (h *HNSW) Delete(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	node, ok := h.nodes[id]
	if !ok {
		return fmt.Errorf("hnsw: ID %q not found", id)
	}
	node.deleted = true
	return nil
}

// Contains reports whether the index contains an active (non-deleted) node with id.
func (h *HNSW) Contains(id string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n, ok := h.nodes[id]
	return ok && !n.deleted
}

// Len returns the count of active (non-deleted) nodes.
func (h *HNSW) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	count := 0
	for _, n := range h.nodes {
		if !n.deleted {
			count++
		}
	}
	return count
}

// ---------------------------------------------------------------------------
// Core algorithm helpers (caller holds appropriate lock)
// ---------------------------------------------------------------------------

// randomLevel assigns a level using the same exponential distribution as in
// the original HNSW paper (Malkov & Yashunin, 2020).
func (h *HNSW) randomLevel() int {
	level := 0
	for h.rng.Float64() < 1.0/float64(h.cfg.M) && level < 32 {
		level++
	}
	return level
}

// greedySearch navigates greedily from startID toward query at the given layer,
// returning the ID of the closest node reachable.
func (h *HNSW) greedySearch(startID string, query []float32, level int) string {
	current := startID
	currentDist := h.dist(current, query)
	for {
		improved := false
		for _, nbID := range h.neighborsAt(current, level) {
			if d := h.dist(nbID, query); d < currentDist {
				currentDist = d
				current = nbID
				improved = true
			}
		}
		if !improved {
			break
		}
	}
	return current
}

// searchLayer performs a beam search from ep at level, accumulating up to ef
// candidates.  Returns candidates sorted closest-first.
func (h *HNSW) searchLayer(ep string, query []float32, ef, level int) []candidate {
	visited := make(map[string]bool, ef*2)
	visited[ep] = true

	epDist := h.dist(ep, query)

	cands := &minHeap{{id: ep, dist: epDist}}
	heap.Init(cands)

	result := &maxHeap{{id: ep, dist: epDist}}
	heap.Init(result)

	for cands.Len() > 0 {
		closest := heap.Pop(cands).(candidate)
		if result.Len() >= ef && closest.dist > (*result)[0].dist {
			break
		}
		for _, nbID := range h.neighborsAt(closest.id, level) {
			if visited[nbID] {
				continue
			}
			visited[nbID] = true
			d := h.dist(nbID, query)
			if result.Len() < ef || d < (*result)[0].dist {
				heap.Push(cands, candidate{id: nbID, dist: d})
				heap.Push(result, candidate{id: nbID, dist: d})
				if result.Len() > ef {
					heap.Pop(result)
				}
			}
		}
	}

	// Drain result heap into a slice sorted closest-first.
	out := make([]candidate, result.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(result).(candidate)
	}
	return out
}

// selectNeighbors returns the closest M candidates.
func (h *HNSW) selectNeighbors(candidates []candidate, M int) []candidate {
	if len(candidates) <= M {
		return candidates
	}
	return candidates[:M]
}

// pruneNeighbors trims a neighbor list to M entries, keeping the closest.
func (h *HNSW) pruneNeighbors(origin []float32, neighbors []string, M int) []string {
	type scored struct {
		id   string
		dist float32
	}
	s := make([]scored, 0, len(neighbors))
	for _, nbID := range neighbors {
		s = append(s, scored{id: nbID, dist: h.cfg.Distance(origin, h.vectorOf(nbID))})
	}
	// Insertion sort (neighbor lists are small).
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j].dist < s[j-1].dist; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
	top := M
	if len(s) < top {
		top = len(s)
	}
	result := make([]string, top)
	for i := range result {
		result[i] = s[i].id
	}
	return result
}

func (h *HNSW) neighborsAt(id string, level int) []string {
	n := h.nodes[id]
	if n == nil || level >= len(n.neighbors) {
		return nil
	}
	return n.neighbors[level]
}

func (h *HNSW) dist(id string, query []float32) float32 {
	n := h.nodes[id]
	if n == nil {
		return math.MaxFloat32
	}
	return h.cfg.Distance(n.vector, query)
}

func (h *HNSW) vectorOf(id string) []float32 {
	n := h.nodes[id]
	if n == nil {
		return nil
	}
	return n.vector
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

type serializedIndex struct {
	Dim        int                    `json:"dim"`
	EntryPoint string                 `json:"entry_point"`
	MaxLevel   int                    `json:"max_level"`
	Nodes      map[string]*serialNode `json:"nodes"`
}

type serialNode struct {
	Vector    []float32  `json:"vector"`
	Neighbors [][]string `json:"neighbors"`
	Deleted   bool       `json:"deleted,omitempty"`
}

// Save serialises the index to path (JSON).
func (h *HNSW) Save(path string) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	si := serializedIndex{
		Dim:        h.dim,
		EntryPoint: h.entryPoint,
		MaxLevel:   h.maxLevel,
		Nodes:      make(map[string]*serialNode, len(h.nodes)),
	}
	for id, n := range h.nodes {
		si.Nodes[id] = &serialNode{
			Vector:    n.vector,
			Neighbors: n.neighbors,
			Deleted:   n.deleted,
		}
	}

	f, err := os.Create(path) // #nosec G304 — path is caller-supplied
	if err != nil {
		return fmt.Errorf("hnsw: save: %w", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "")
	return enc.Encode(si)
}

// Load deserialises an HNSW index from path.  The distance function must be
// supplied again because it cannot be serialised.
func Load(path string, cfg Config) (*HNSW, error) {
	cfg.applyDefaults()
	f, err := os.Open(path) // #nosec G304
	if err != nil {
		return nil, fmt.Errorf("hnsw: load: %w", err)
	}
	defer f.Close()

	var si serializedIndex
	if err = json.NewDecoder(f).Decode(&si); err != nil {
		return nil, fmt.Errorf("hnsw: load decode: %w", err)
	}

	idx := NewHNSW(cfg)
	idx.dim = si.Dim
	idx.entryPoint = si.EntryPoint
	idx.maxLevel = si.MaxLevel
	for id, sn := range si.Nodes {
		idx.nodes[id] = &hnswNode{
			id:        id,
			vector:    sn.Vector,
			neighbors: sn.Neighbors,
			deleted:   sn.Deleted,
		}
	}
	return idx, nil
}
