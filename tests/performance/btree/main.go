package main

import (
	"log"
	"path/filepath"
	"strconv"
	"sync"
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
	zlogger, _ := logger.New(logger.Config{Level: "error"})
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
	write(dbInstance)

	read(dbInstance)
}

func read(dbInstance *btree.BTree[string, string]) {
	wg := sync.WaitGroup{}
	maxWorkers := 10
	sem := make(chan struct{}, maxWorkers)
	for i := 9000; i < 11000; i++ {
		sem <- struct{}{}
		key := "key-" + strconv.Itoa(i)
		value := "value-" + strconv.Itoa(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			v, found, err := dbInstance.Search(key)
			if err != nil {
				log.Println("1000011000 Search Error: ", err)
				return
			}
			if !found {
				log.Println("1000011000 NOT FOUND: ", key)
				return
			}
			if v != value {
				log.Println("1000011000 MISMATCH: ", key)
				return
			}
		}()
	}
	wg.Wait()
	time.Sleep(1000 * time.Millisecond)
}

func write(dbInstance *btree.BTree[string, string]) {
	wg := sync.WaitGroup{}
	maxWorkers := 20
	sem := make(chan struct{}, maxWorkers)
	for i := 9000; i < 11000; i++ {
		sem <- struct{}{}
		key := "key-" + strconv.Itoa(i)
		value := "value-" + strconv.Itoa(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			err := dbInstance.Insert(key, value, 0)
			if err != nil {
				log.Println("1000011000 Write Error: ", err)
			}
		}()
	}
	wg.Wait()
	time.Sleep(1000 * time.Millisecond)
}
