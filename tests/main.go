package main

import (
	"fmt"
	"log"
	"os"
	"sort" // Required by slices.BinarySearchFunc and slices.Insert in btree.go

	// Import your btree package
	"github.com/sushant-115/gojodb/core/indexing/btree"
)

// Helper function to create default serializers for int64 keys and string values
func getDefaultSerializers() btree.KeyValueSerializer[string, string] {
	return btree.KeyValueSerializer[string, string]{
		SerializeKey:     btree.SerializeString,
		DeserializeKey:   btree.DeserializeString,
		SerializeValue:   btree.SerializeString,
		DeserializeValue: btree.DeserializeString,
	}
}

// Function to print B-tree content for debugging
func printBTreeContent(bt *btree.BTree[string, string], name string) {
	fmt.Printf("\n--- %s B-Tree Content ---\n", name)
	size, err := bt.GetSize()
	if err != nil {
		fmt.Printf("Error getting tree size: %v\n", err)
	} else {
		fmt.Printf("Tree Size: %d\n", size)
	}
	fmt.Printf("--------------------------\n")
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Include file and line number in logs for better debugging

	dbFilePath := "test_gojodb.db"
	logDir := "test_logs"
	archiveDir := "test_archives"
	poolSize := 10                         // Buffer pool size
	pageSize := 4096                       // Page size in bytes
	degree := 2                            // B-tree degree (minimum 2)
	segmentSizeLimit := int64(1024 * 1024) // 1 MB log segment size limit

	// Clean up previous test files
	os.Remove(dbFilePath)
	os.RemoveAll(logDir)
	os.RemoveAll(archiveDir)

	fmt.Println("--- Starting B-Tree Persistence and WAL Test ---")

	// 1. Initialize LogManager
	logManager, err := btree.NewLogManager(logDir, archiveDir, 4096, segmentSizeLimit)
	if err != nil {
		log.Fatalf("Failed to create LogManager: %v", err)
	}
	defer func() {
		if err := logManager.Close(); err != nil {
			log.Printf("Error closing LogManager: %v", err)
		}
	}()

	kvSerializers := getDefaultSerializers()

	// 2. Create a new B-tree database
	fmt.Printf("\nAttempting to create a new B-tree at %s...\n", dbFilePath)
	bt, err := btree.NewBTreeFile[string, string](dbFilePath, degree, btree.DefaultKeyOrder[string], kvSerializers, poolSize, pageSize, logManager)
	if err != nil {
		log.Fatalf("Failed to create new B-tree file: %v", err)
	}
	fmt.Println("B-tree database created successfully.")
	printBTreeContent(bt, "Initial Empty")

	// 3. Insert some data
	fmt.Println("\n--- Inserting data ---")
	dataToInsert := map[string]string{
		"10": "Value_10",
		"20": "Value_20",
		"5":  "Value_5",
		"15": "Value_15",
		"30": "Value_30",
		"25": "Value_25",
		"1":  "Value_1",
		"7":  "Value_7",
		"22": "Value_22",
		"18": "Value_18",
	}

	keys := make([]string, 0, len(dataToInsert))
	for k := range dataToInsert {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] }) // Insert in sorted order for easier debugging, though B-tree handles any order

	for _, k := range keys {
		v := dataToInsert[k]
		fmt.Printf("Inserting (%s, %s)...\n", k, v)
		if err := bt.Insert(k, v, 0); err != nil {
			log.Fatalf("Failed to insert key %s: %v", k, err)
		}
	}
	fmt.Println("Data insertion complete.")
	printBTreeContent(bt, "After Inserts")

	// 4. Search for data
	fmt.Println("\n--- Searching for data ---")
	keysToSearch := []string{"5", "15", "22", "99"} // 99 should not be found
	for _, k := range keysToSearch {
		val, found, err := bt.Search(k)
		if err != nil {
			log.Fatalf("Error searching for key %s: %v", k, err)
		}
		if found {
			fmt.Printf("Search for %s: Found! Value: %s\n", k, val)
		} else {
			fmt.Printf("Search for %s: Not Found.\n", k)
		}
	}

	// 5. Delete some data
	fmt.Println("\n--- Deleting data ---")
	keysToDelete := []string{"5", "15", "22", "99"} // 2 should not be found for deletion
	for _, k := range keysToDelete {
		fmt.Printf("Deleting key %s...\n", k)
		err := bt.Delete(k, 0)
		if err != nil {
			if err == btree.ErrKeyNotFound {
				fmt.Printf("Key %s not found for deletion.\n", k)
			} else {
				log.Fatalf("Failed to delete key %s: %v", k, err)
			}
		} else {
			fmt.Printf("Successfully deleted key %s.\n", k)
		}
	}
	printBTreeContent(bt, "After Deletions")

	// 6. Verify deletions
	fmt.Println("\n--- Verifying deletions ---")
	keysToVerify := []string{"5", "15", "22", "99"}
	for _, k := range keysToVerify {
		_, found, err := bt.Search(k)
		if err != nil {
			log.Fatalf("Error verifying key %s: %v", k, err)
		}
		if found {
			fmt.Printf("Verification for %s: STILL FOUND (Expected to be deleted: %t)\n", k, (k == "5" || k == "15" || k == "30"))
		} else {
			fmt.Printf("Verification for %s: NOT FOUND (Expected to be deleted: %t)\n", k, (k == "5" || k == "15" || k == "30"))
		}
	}

	// 7. Simulate a "crash" - do not call bt.Close()
	fmt.Println("\n--- Simulating crash (not calling bt.Close()) ---")
	fmt.Println("The B-tree will now be reopened, and recovery should ensure data consistency.")
	// Set bt to nil to ensure we're not using the old instance
	bt = nil

	// 8. Re-open the database, triggering recovery via LogManager
	fmt.Printf("\nAttempting to re-open B-tree at %s with recovery...\n", dbFilePath)
	btRecovered, err := btree.OpenBTreeFile[string, string](dbFilePath, btree.DefaultKeyOrder[string], kvSerializers, poolSize, pageSize, logManager)
	if err != nil {
		log.Fatalf("Failed to open existing B-tree file for recovery: %v", err)
	}
	fmt.Println("B-tree database reopened successfully. Recovery (Redo Pass) should have completed.")

	// 9. Verify state after recovery
	printBTreeContent(btRecovered, "After Recovery")
	fmt.Println("\n--- Verifying data consistency after recovery ---")
	keysAfterRecovery := []string{"1", "7", "10", "18", "20", "22", "25", "5", "15", "30"} // Check "original", deleted, and remaining
	for _, k := range keysAfterRecovery {
		val, found, err := btRecovered.Search(k)
		if err != nil {
			log.Fatalf("Error searching for key %s after recovery: %v", k, err)
		}
		expectedFound := !(k == "5" || k == "15" || k == "30") // These keys were deleted
		if found != expectedFound {
			fmt.Printf("Recovery check for %s: Mismatch! Found: %t, Expected Found: %t\n", k, found, expectedFound)
		} else if found {
			fmt.Printf("Recovery check for %s: OK. Found: %s\n", k, val)
		} else {
			fmt.Printf("Recovery check for %s: OK. Not Found.\n", k)
		}
	}

	// Final close of the recovered B-tree
	if err := btRecovered.Close(); err != nil {
		log.Printf("Error closing recovered B-tree: %v", err)
	}

	fmt.Println("\n--- B-Tree Persistence and WAL Test Complete ---")
}
