package main

import (
	"log"
	"path/filepath"
	"strconv"
	"time"

	"github.com/sushant-115/gojodb/core/indexing"
	"github.com/sushant-115/gojodb/core/indexing/btree"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	"github.com/sushant-115/gojodb/pkg/logger"
)

func main() {
	// Initialize B-tree Index
	baseDataDir := "/tmp/gojodb"
	dbPath := filepath.Join(baseDataDir, "btree.db")
	walPath := filepath.Join(baseDataDir, "wal")
	zlogger, _ := logger.New(logger.Config{Level: "debug"})
	logManager, err := wal.NewLogManager(walPath, zlogger, indexing.BTreeIndexType)
	if err != nil {
		log.Fatalf("failed to create main log manager: %w", err)
	}
	dbInstance, err := btree.NewBTreeFile[string, string](
		dbPath, 3,
		btree.DefaultKeyOrder[string],
		btree.KeyValueSerializer[string, string]{
			SerializeKey:     btree.SerializeString,
			DeserializeKey:   btree.DeserializeString,
			SerializeValue:   btree.SerializeString,
			DeserializeValue: btree.DeserializeString,
		},
		10000,
		4096,
		logManager,
		zlogger.Named("btree_index"),
	)
	// errChan := make(chan error, 100)
	// wg := sync.WaitGroup{}
	for i := 6000; i < 10000; i++ {
		key := "key-" + strconv.Itoa(i)
		value := "value-" + strconv.Itoa(i)
		// wg.Add(1)
		// go func() {
		// 	defer wg.Done()
		err := dbInstance.Insert(key, value, 0)
		log.Println("Error: ", err)
		// 	errChan <- err
		// }()
	}
	// go func() {
	// 	for er := range errChan {
	// 		log.Println("Error: ", er)
	// 	}
	// }()
	// wg.Wait()
	time.Sleep(500 * time.Microsecond)
}
