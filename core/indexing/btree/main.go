// gojodb_test/main.go
package main

import (
	"fmt"
	"log"
	"os"

	btree "btree/btree_core"
)

// Example Key-Value Serializers (using the ones from your btree package)
var kvSerializer = btree.KeyValueSerializer[int64, string]{
	SerializeKey: func(k int64) ([]byte, error) {
		return btree.SerializeInt64(k)
	},
	DeserializeKey: func(data []byte) (int64, error) {
		return btree.DeserializeInt64(data)
	},
	SerializeValue: func(v string) ([]byte, error) {
		return btree.SerializeString(v)
	},
	DeserializeValue: func(data []byte) (string, error) {
		return btree.DeserializeString(data)
	},
}

// Example Key Order function
func int64KeyOrder(a, b int64) int {
	return btree.DefaultKeyOrder(a, b)
}

func main() {
	dbFilePath := "my_btree.db"
	logPath := "log_manager001"
	archLogPath := "log_manager002"
	// Clean up existing DB file for fresh test run
	os.Remove(dbFilePath)

	// --- Test NewBTreeFile ---
	log.Println("Creating new BTree file...")
	// Note: LogManager is currently an interface{} placeholder in the BTree code.
	// For this test, we'll pass nil. A real LogManager would be needed for WAL.
	logManager, _ := btree.NewLogManager(logPath, archLogPath, 10000, 10000)
	bt, err := btree.NewBTreeFile[int64, string](
		dbFilePath,
		3, // Degree
		int64KeyOrder,
		kvSerializer,
		10, // Buffer pool size (number of pages)
		btree.DefaultPageSize,
		logManager,
	)
	if err != nil {
		log.Fatalf("Error creating BTree file: %v", err)
	}
	log.Println("BTree file created successfully.")

	// --- Test Insert ---
	log.Println("Inserting data...")
	keysToInsert := []int64{10}
	for _, k := range keysToInsert {
		val := fmt.Sprintf("value_for_%d", k)
		if err := bt.Insert(k, val); err != nil {
			log.Printf("Error inserting key %d: %v", k, err)
		}
	}
	log.Println("Data insertion complete.")
	log.Println("BTree structure after inserts:\n", bt.String())

	// // --- Test Search ---
	// log.Println("Searching for keys...")
	// searchKeys := []int64{15, 7, 30, 99, 1}
	// for _, k := range searchKeys {
	// 	val, found, searchErr := bt.Search(k)
	// 	if searchErr != nil {
	// 		log.Printf("Error searching for key %d: %v", k, searchErr)
	// 	} else if found {
	// 		log.Printf("Found key %d: value = %s", k, val)
	// 	} else {
	// 		log.Printf("Key %d not found.", k)
	// 	}
	// }

	// --- Test GetSize ---
	currentSize, sizeErr := bt.GetSize()
	if sizeErr != nil {
		log.Printf("Error getting tree size: %v", sizeErr)
	} else {
		log.Printf("Current tree size: %d", currentSize)
	}

	// --- Test Delete ---
	log.Println("Deleting some keys...")
	keysToDelete := []int64{10 /* non-existent */}
	for _, k := range keysToDelete {
		log.Printf("Attempting to delete key: %d", k)
		delErr := bt.Delete(k)
		if delErr != nil {
			log.Printf("Error deleting key %d: %v", k, delErr)
		} else {
			log.Printf("Successfully deleted key %d (or it was already gone).", k)
		}
	}
	log.Println("Deletion attempts complete.")
	log.Println("BTree structure after deletes:\n", bt.String())

	currentSizeAfterDelete, sizeErr := bt.GetSize()
	if sizeErr != nil {
		log.Printf("Error getting tree size after delete: %v", sizeErr)
	} else {
		log.Printf("Current tree size after delete: %d", currentSizeAfterDelete)
	}

	// Verify deleted keys are gone
	log.Println("Verifying deleted keys...")
	for _, k := range keysToDelete {
		if k == 100 {
			continue
		} // Skip non-existent key check
		_, found, searchErr := bt.Search(k)
		if searchErr != nil {
			log.Printf("Error searching for (deleted) key %d: %v", k, searchErr)
		} else if found {
			log.Printf("ERROR: Key %d found after deletion!", k)
		} else {
			log.Printf("Key %d correctly not found after deletion.", k)
		}
	}

	// --- Test Close and Reopen (Persistence) ---
	log.Println("Closing BTree...")
	if err := bt.Close(); err != nil {
		log.Fatalf("Error closing BTree: %v", err)
	}
	log.Println("BTree closed.")

	log.Println("Reopening BTree file...")
	btReopened, err := btree.OpenBTreeFile[int64, string](
		dbFilePath,
		int64KeyOrder,
		kvSerializer,
		10, // Buffer pool size
		btree.DefaultPageSize,
		logManager,
	)
	if err != nil {
		log.Fatalf("Error reopening BTree file: %v", err)
	}
	log.Println("BTree reopened successfully.")
	log.Println("BTree structure after reopen:\n", btReopened.String())

	// Verify data in reopened tree
	log.Println("Searching for keys in reopened tree...")
	persistedKeysToVerify := []int64{10, 5, 7, 25} // Keys that should still exist
	for _, k := range persistedKeysToVerify {
		val, found, searchErr := btReopened.Search(k)
		if searchErr != nil {
			log.Printf("Error searching for key %d in reopened tree: %v", k, searchErr)
		} else if found {
			log.Printf("Found key %d in reopened tree: value = %s", k, val)
		} else {
			log.Printf("ERROR: Key %d not found in reopened tree!", k)
		}
	}
	deletedKeyToVerify := int64(15) // Key that should NOT exist
	_, found, _ := btReopened.Search(deletedKeyToVerify)
	if found {
		log.Printf("ERROR: Deleted key %d FOUND in reopened tree!", deletedKeyToVerify)
	} else {
		log.Printf("Deleted key %d correctly NOT found in reopened tree.", deletedKeyToVerify)
	}

	reopenedSize, sizeErr := btReopened.GetSize()
	if sizeErr != nil {
		log.Printf("Error getting reopened tree size: %v", sizeErr)
	} else {
		log.Printf("Reopened tree size: %d (should match size before close)", reopenedSize)
		if reopenedSize != currentSizeAfterDelete {
			log.Printf("ERROR: Size mismatch! Before close: %d, After reopen: %d", currentSizeAfterDelete, reopenedSize)
		}
	}

	log.Println("Closing reopened BTree...")
	if err := btReopened.Close(); err != nil {
		log.Fatalf("Error closing reopened BTree: %v", err)
	}

	log.Println("BTree tests finished.")
}
