package main

import (
	"fmt"
	"log"

	"leafdb"
)

func main() {
	db, err := leafdb.Open("example.db")
	if err != nil {
		log.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	if err := db.Write(func(tx *leafdb.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("config"))
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("name"), []byte("leaf")); err != nil {
			return err
		}
		if err := bucket.Put([]byte("version"), []byte("1")); err != nil {
			return err
		}
		child, err := bucket.CreateBucketIfNotExists([]byte("nested"))
		if err != nil {
			return err
		}
		return child.Put([]byte("feature"), []byte("bptree"))
	}); err != nil {
		log.Fatalf("update failed: %v", err)
	}

	if err := db.Read(func(tx *leafdb.Tx) error {
		bucket := tx.Bucket([]byte("config"))
		if bucket == nil {
			return fmt.Errorf("missing bucket")
		}
		val := bucket.Get([]byte("name"))
		fmt.Printf("name=%s\n", val)

		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			fmt.Printf("%s=%s\n", k, v)
		}
		return nil
	}); err != nil {
		log.Fatalf("view failed: %v", err)
	}
}
