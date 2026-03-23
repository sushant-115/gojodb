package inverted_index

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// --- Test Helpers ---

// setupInvertedIndex creates an InvertedIndex in an isolated temporary directory.
func setupInvertedIndex(t *testing.T) *InvertedIndex {
	t.Helper()
	tempDir := t.TempDir()

	filePath := filepath.Join(tempDir, "inverted.db")
	logDir := filepath.Join(tempDir, "wal")
	archiveDir := filepath.Join(tempDir, "archive")

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	idx, err := NewInvertedIndex(filePath, logDir, archiveDir, logger)
	require.NoError(t, err)
	require.NotNil(t, idx)

	t.Cleanup(func() {
		_ = idx.Close()
	})

	return idx
}

// --- Tests for Insert ---

func TestInvertedIndex_Insert_SingleDocument(t *testing.T) {
	idx := setupInvertedIndex(t)

	err := idx.Insert("quick brown fox", "doc1")
	require.NoError(t, err)
}

func TestInvertedIndex_Insert_EmptyText_IsNoOp(t *testing.T) {
	idx := setupInvertedIndex(t)

	err := idx.Insert("", "doc1")
	require.NoError(t, err)
}

func TestInvertedIndex_Insert_EmptyDocKey_IsNoOp(t *testing.T) {
	idx := setupInvertedIndex(t)

	err := idx.Insert("some text", "")
	require.NoError(t, err)
}

func TestInvertedIndex_Insert_MultipleDocuments(t *testing.T) {
	idx := setupInvertedIndex(t)

	require.NoError(t, idx.Insert("quick brown fox", "doc1"))
	require.NoError(t, idx.Insert("lazy dog jumps", "doc2"))
	require.NoError(t, idx.Insert("brown dog running", "doc3"))
}

func TestInvertedIndex_Insert_DuplicateDocKey_IsIdempotent(t *testing.T) {
	idx := setupInvertedIndex(t)

	require.NoError(t, idx.Insert("hello world", "doc1"))
	require.NoError(t, idx.Insert("hello world", "doc1")) // Duplicate, should not error

	results, err := idx.Search("hello")
	require.NoError(t, err)
	require.Len(t, results, 1, "duplicate docKey should appear only once in results")
	require.Equal(t, "doc1", results[0])
}

// --- Tests for Search ---

func TestInvertedIndex_Search_FoundTerm(t *testing.T) {
	idx := setupInvertedIndex(t)

	require.NoError(t, idx.Insert("quick brown fox", "doc1"))
	require.NoError(t, idx.Insert("the quick red car", "doc2"))

	results, err := idx.Search("quick")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"doc1", "doc2"}, results)
}

func TestInvertedIndex_Search_TermNotFound(t *testing.T) {
	idx := setupInvertedIndex(t)

	require.NoError(t, idx.Insert("hello world", "doc1"))

	results, err := idx.Search("nonexistent")
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestInvertedIndex_Search_EmptyQuery(t *testing.T) {
	idx := setupInvertedIndex(t)

	results, err := idx.Search("")
	require.NoError(t, err)
	require.Nil(t, results)
}

func TestInvertedIndex_Search_StopWordOnly(t *testing.T) {
	idx := setupInvertedIndex(t)

	require.NoError(t, idx.Insert("the quick brown fox", "doc1"))

	// "the" is a stop word and should be filtered out, so searching for it returns nothing.
	results, err := idx.Search("the")
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestInvertedIndex_Search_CaseInsensitive(t *testing.T) {
	idx := setupInvertedIndex(t)

	require.NoError(t, idx.Insert("Quick Brown Fox", "doc1"))

	// Search should be case-insensitive.
	results, err := idx.Search("quick")
	require.NoError(t, err)
	require.Contains(t, results, "doc1")
}

func TestInvertedIndex_Search_MultipleTerms_Union(t *testing.T) {
	idx := setupInvertedIndex(t)

	require.NoError(t, idx.Insert("the cat sat", "doc1"))
	require.NoError(t, idx.Insert("dog runs fast", "doc2"))
	require.NoError(t, idx.Insert("cat and dog play", "doc3"))

	// Searching for "cat dog" should return docs containing either "cat" or "dog".
	results, err := idx.Search("cat dog")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"doc1", "doc2", "doc3"}, results)
}

func TestInvertedIndex_Search_OnlyReturnsMatchingDocs(t *testing.T) {
	idx := setupInvertedIndex(t)

	require.NoError(t, idx.Insert("golang programming language", "doc1"))
	require.NoError(t, idx.Insert("python data science", "doc2"))
	require.NoError(t, idx.Insert("java enterprise application", "doc3"))

	results, err := idx.Search("golang")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"doc1"}, results)
}

func TestInvertedIndex_Search_AfterMultipleInserts(t *testing.T) {
	idx := setupInvertedIndex(t)

	docs := map[string]string{
		"doc1": "database management system",
		"doc2": "distributed database cluster",
		"doc3": "in-memory cache system",
		"doc4": "relational database index",
	}
	for docKey, text := range docs {
		require.NoError(t, idx.Insert(text, docKey))
	}

	results, err := idx.Search("database")
	require.NoError(t, err)
	// doc1, doc2, doc4 contain "database"
	require.ElementsMatch(t, []string{"doc1", "doc2", "doc4"}, results)
}

func TestInvertedIndex_Search_WithStemming(t *testing.T) {
	idx := setupInvertedIndex(t)

	// "running" should be stemmed to "runn" (strip "ing"), "runs" to "run" (strip "s").
	// "jumped" → "jump" (strip "ed")
	require.NoError(t, idx.Insert("the dog was running fast", "doc1"))
	require.NoError(t, idx.Insert("dogs jumped over fences", "doc2"))

	// Search for the stemmed form.
	results, err := idx.Search("running")
	require.NoError(t, err)
	require.Contains(t, results, "doc1")
}

// --- Tests for tokenizeAndNormalize via public behaviour ---

func TestInvertedIndex_Tokenize_PunctuationStripped(t *testing.T) {
	idx := setupInvertedIndex(t)

	// Insert text with punctuation; the terms should still be indexed.
	require.NoError(t, idx.Insert("hello, world! foo-bar.", "doc1"))

	// "hello" and "world" should be searchable.
	results, err := idx.Search("hello")
	require.NoError(t, err)
	require.Contains(t, results, "doc1")

	results2, err := idx.Search("world")
	require.NoError(t, err)
	require.Contains(t, results2, "doc1")
}

func TestInvertedIndex_Tokenize_NumbersIndexed(t *testing.T) {
	idx := setupInvertedIndex(t)

	require.NoError(t, idx.Insert("item 12345 in stock", "doc1"))

	results, err := idx.Search("12345")
	require.NoError(t, err)
	require.Contains(t, results, "doc1")
}
