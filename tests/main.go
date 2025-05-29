package main

import (
	"fmt"
	"log"
	"os"

	"btree/btree_core" // Adjust this import path to your actual module path
)

const (
	dbFilePath     = "data/test_db.db"
	logDir         = "data/logs"
	archiveDir     = "data/archives"
	logBufferSize  = 4096      // 4KB log buffer
	logSegmentSize = 16 * 1024 // 16KB log segment size for quick rotation demo
	bTreeDegree    = 3         // B-tree order (min degree)
	bufferPoolSize = 10        // Number of pages in buffer pool
	dbPageSize     = 4096      // Database page size
)

// cleanup removes the database file and log directories
func cleanup() {
	fmt.Println("\n--- Cleaning up previous test data ---")
	if err := os.RemoveAll("data"); err != nil {
		fmt.Printf("Warning: Failed to remove data directory: %v\n", err)
	}
	fmt.Println("Cleanup complete.")
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Include file and line number in logs for debugging

	// --- Scenario 1: Initial Creation and Inserts (Simulate Crash) ---
	fmt.Println("\n--- Scenario 1: Initial Creation and Inserts (Simulating Crash) ---")
	cleanup() // Start fresh

	// Initialize LogManager
	lm, err := btree_core.NewLogManager(logDir, archiveDir, logBufferSize, logSegmentSize)
	if err != nil {
		log.Fatalf("Failed to create LogManager: %v", err)
	}
	defer func() {
		if err := lm.Close(); err != nil {
			log.Printf("Error closing LogManager: %v", err)
		}
	}()

	// Create a new B-tree database file
	bTree, err := btree_core.NewBTreeFile[int64, string](
		dbFilePath,
		bTreeDegree,
		btree_core.DefaultKeyOrder[int64],
		btree_core.KeyValueSerializer[int64, string]{
			SerializeKey:     btree_core.SerializeInt64,
			DeserializeKey:   btree_core.DeserializeInt64,
			SerializeValue:   btree_core.SerializeString,
			DeserializeValue: btree_core.DeserializeString,
		},
		bufferPoolSize,
		dbPageSize,
		lm, // Pass the LogManager
	)
	if err != nil {
		log.Fatalf("Failed to create BTree file: %v", err)
	}

	fmt.Println("\n--- Inserting keys (Scenario 1) ---")
	keysToInsert := []int64{10, 20, 5, 30, 15, 25, 35, 2, 7, 12, 18, 22, 28, 32, 38, 1, 3, 6, 8, 11, 13, 16, 19, 21, 23, 26, 29, 31, 33, 36, 39, 4, 9, 14, 17, 24, 27, 34, 37, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50}
	for _, key := range keysToInsert {
		value := fmt.Sprintf("value_%d", key)
		if err := bTree.Insert(key, value); err != nil {
			log.Fatalf("Failed to insert key %d: %v", key, err)
		}
		fmt.Printf("Inserted: %d\n", key)
	}

	size, err := bTree.GetSize()
	if err != nil {
		log.Fatalf("Failed to get tree size: %v", err)
	}
	fmt.Printf("\nTree size after inserts: %d\n", size)
	fmt.Println("\n--- B-Tree structure after inserts (Scenario 1) ---")
	fmt.Println(bTree.String())

	fmt.Println("\n--- Verifying inserts (Scenario 1) ---")
	for _, key := range []int64{5, 15, 30, 1, 40, 99} {
		val, found, err := bTree.Search(key)
		if err != nil {
			log.Fatalf("Search for key %d failed: %v", key, err)
		}
		if found {
			fmt.Printf("Key %d found: %s\n", key, val)
		} else {
			fmt.Printf("Key %d not found.\n", key)
		}
	}

	fmt.Println("\n--- Simulating crash: NOT calling bTree.Close() ---")
	// bTree.Close() is intentionally skipped here to simulate a crash.
	// This leaves dirty pages in the buffer pool and un-synced log records.

	// --- Scenario 2: Re-opening and Recovery (Simulating Restart after Crash) ---
	fmt.Println("\n--- Scenario 2: Re-opening and Recovery ---")

	// Re-initialize LogManager (it will find existing segments)
	lm2, err := btree_core.NewLogManager(logDir, archiveDir, logBufferSize, logSegmentSize)
	if err != nil {
		log.Fatalf("Failed to create LogManager for restart: %v", err)
	}
	defer func() {
		if err := lm2.Close(); err != nil {
			log.Printf("Error closing LogManager on restart: %v", err)
		}
	}()

	// Open the existing B-tree database file
	bTree2, err := btree_core.OpenBTreeFile[int64, string](
		dbFilePath,
		btree_core.DefaultKeyOrder[int64],
		btree_core.KeyValueSerializer[int64, string]{
			SerializeKey:     btree_core.SerializeInt64,
			DeserializeKey:   btree_core.DeserializeInt64,
			SerializeValue:   btree_core.SerializeString,
			DeserializeValue: btree_core.DeserializeString,
		},
		bufferPoolSize,
		dbPageSize,
		lm2, // Pass the new LogManager
	)
	if err != nil {
		log.Fatalf("Failed to open BTree file: %v", err)
	}
	defer func() {
		if err := bTree2.Close(); err != nil {
			log.Printf("Error closing BTree on restart: %v", err)
		}
	}()

	fmt.Println("\n--- Verifying data after simulated crash and restart (Recovery would happen here) ---")
	// The recovery process (Analysis, Redo) would run within OpenBTreeFile if implemented.
	// We'll rely on the WAL and BPM flushing to disk for durability here.
	for _, key := range keysToInsert { // Verify all keys are still there
		val, found, err := bTree2.Search(key)
		if err != nil {
			log.Fatalf("Search for key %d failed after restart: %v", key, err)
		}
		fmt.Printf("Search key val: ", key, val)
		if found {
			// fmt.Printf("Key %d found: %s (after restart)\n", key, val)
		} else {
			fmt.Printf("ERROR: Key %d NOT found after restart (expected to be present).\n", key)
		}
	}
	size2, err := bTree2.GetSize()
	if err != nil {
		log.Fatalf("Failed to get tree size after restart: %v", err)
	}
	fmt.Printf("\nTree size after restart: %d (Expected: %d)\n", size2, len(keysToInsert))
	fmt.Println("\n--- B-Tree structure after restart ---")
	fmt.Println(bTree2.String())

	// --- Scenario 3: Deletions and Free Space Management ---
	fmt.Println("\n--- Scenario 3: Deletions and Free Space Management ---")
	keysToDelete := []int64{10, 20, 5, 30, 15, 25, 35, 2, 7, 12, 18, 22, 28, 32, 38, 4, 9, 14, 17, 24, 27, 34, 37, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50} // Delete many keys
	for _, key := range keysToDelete {
		if err := bTree2.Delete(key); err != nil {
			if err == btree_core.ErrKeyNotFound {
				fmt.Printf("Key %d not found for deletion (might have been deleted by merge).\n", key)
			} else {
				log.Fatalf("Failed to delete key %d: %v", key, err)
			}
		} else {
			fmt.Printf("Deleted: %d\n", key)
		}
	}

	size3, err := bTree2.GetSize()
	if err != nil {
		log.Fatalf("Failed to get tree size after deletions: %v", err)
	}
	fmt.Printf("\nTree size after deletions: %d\n", size3)
	fmt.Println("\n--- B-Tree structure after deletions ---")
	fmt.Println(bTree2.String())

	fmt.Println("\n--- Verifying deletions ---")
	for _, key := range []int64{10, 20, 5, 1, 3, 6, 8, 11, 13, 16, 19, 21, 23, 26, 29, 31, 33, 36, 39, 99} { // Some remaining, some deleted, some never existed
		val, found, err := bTree2.Search(key)
		if err != nil {
			log.Fatalf("Search for key %d failed after deletion: %v", key, err)
		}
		if found {
			fmt.Printf("Key %d found (expected deleted): %s\n", key, val) // Should ideally not be found if deleted
		} else {
			fmt.Printf("Key %d not found (expected not found or deleted).\n", key)
		}
	}

	fmt.Println("\n--- Attempting to deallocate a page (Free Space Management TODO) ---")
	// This will call the DiskManager.DeallocatePage, which is a TODO.
	// You'd observe log messages about this.
	if err := bTree2.DeallocatePage(btree_core.PageID(100)); err != nil {
		fmt.Printf("DeallocatePage (expected TODO error): %v\n", err)
	}

	fmt.Println("\n--- Cleanly closing B-tree and LogManager ---")
	// This will trigger FlushAllPages and final log segment roll/sync.
	if err := bTree2.Close(); err != nil {
		log.Fatalf("Failed to cleanly close BTree: %v", err)
	}
	fmt.Println("Database closed cleanly.")

	fmt.Println("\n--- Listing log files and archive files ---")
	listFilesInDir(logDir)
	listFilesInDir(archiveDir)

	fmt.Println("\n--- Test complete. ---")
}

func listFilesInDir(dir string) {
	fmt.Printf("Contents of %s:\n", dir)
	files, err := os.ReadDir(dir)
	if err != nil {
		fmt.Printf("  Error reading directory: %v\n", err)
		return
	}
	if len(files) == 0 {
		fmt.Println("  (empty)")
		return
	}
	for _, file := range files {
		if !file.IsDir() {
			fmt.Printf("  - %s\n", file.Name())
		}
	}
}
