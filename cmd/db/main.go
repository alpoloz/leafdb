package main

import (
	"fmt"
	"log"

	"leafdb"
)

func main() {
	db, err := leafdb.OpenWithOptions("example.db", &leafdb.Options{FlushEvery: 2})
	if err != nil {
		log.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("name"), []byte("leaf")); err != nil {
		log.Fatalf("set failed: %v", err)
	}
	if err := db.Set([]byte("version"), []byte("1")); err != nil {
		log.Fatalf("set failed: %v", err)
	}

	val, ok := db.Get([]byte("name"))
	if !ok {
		log.Fatalf("key not found")
	}
	fmt.Printf("name=%s\n", val)

	batch := leafdb.NewBatch()
	batch.Set([]byte("feature"), []byte("bptree"))
	batch.Delete([]byte("version"))
	if err := db.Apply(batch); err != nil {
		log.Fatalf("batch apply failed: %v", err)
	}

	if err := db.View(func(tx *leafdb.Tx) error {
		val, ok := tx.Get([]byte("feature"))
		if !ok {
			return fmt.Errorf("missing feature")
		}
		fmt.Printf("feature=%s\n", val)
		return nil
	}); err != nil {
		log.Fatalf("view failed: %v", err)
	}

	if err := db.Update(func(tx *leafdb.Tx) error {
		return tx.Set([]byte("name"), []byte("leafdb"))
	}); err != nil {
		log.Fatalf("update failed: %v", err)
	}
}
