# leafdb
Leafdb is a small Go key/value store backed by a B+ tree stored on disk via
memory-mapped pages. It uses Bolt-style buckets, nested buckets, and
single-writer/multi-reader transactions.

## Features
- Memory-mapped, page-based B+ tree storage
- Buckets with nested buckets for namespacing
- Single-writer, multiple-reader transactions
- Cursor iteration over bucket key/value pairs
- Simple example app

## Usage

```go
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
		return bucket.Delete([]byte("deprecated-key"))
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
		return nil
	}); err != nil {
		log.Fatalf("view failed: %v", err)
	}

}
```

## Transactions and Buckets

```go
err := db.Write(func(tx *leafdb.Tx) error {
	parent, err := tx.CreateBucketIfNotExists([]byte("parent"))
	if err != nil {
		return err
	}
	child, err := parent.CreateBucketIfNotExists([]byte("child"))
	if err != nil {
		return err
	}
	if err := child.Put([]byte("k"), []byte("v")); err != nil {
		return err
	}
	return parent.DeleteBucket([]byte("old-child"))
})
if err != nil {
	log.Fatalf("update failed: %v", err)
}

err = db.Read(func(tx *leafdb.Tx) error {
	parent := tx.Bucket([]byte("parent"))
	if parent == nil {
		return fmt.Errorf("missing parent")
	}
	child := parent.Bucket([]byte("child"))
	if child == nil {
		return fmt.Errorf("missing child")
	}
	cursor := child.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		fmt.Printf("%s=%s\n", k, v)
	}
	return nil
})
if err != nil {
	log.Fatalf("view failed: %v", err)
}
```

## Cursor

```go
err := db.Read(func(tx *leafdb.Tx) error {
	bucket := tx.Bucket([]byte("config"))
	if bucket == nil {
		return fmt.Errorf("missing bucket")
	}
	cursor := bucket.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		fmt.Printf("%s=%s\n", k, v)
	}
	return nil
})
if err != nil {
	log.Fatalf("cursor failed: %v", err)
}
```

## Example app
Run the bundled example:

```bash
go run ./cmd/db
```

## Notes
- The on-disk format is not stable yet and may change.
- Writes are committed via mmap page updates in a single writer transaction.
