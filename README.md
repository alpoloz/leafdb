# leafdb
Leafdb is a small Go key/value store backed by a B+ tree with page-based
persistence and a tiny API: get, set,
delete, and optional buffered flushes.

## Features
- In-memory B+ tree with fixed-size page persistence
- Point reads and inserts
- Buffered writes with configurable flush intervals
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
	db, err := leafdb.OpenWithOptions("example.db", &leafdb.Options{FlushEvery: 2})
	if err != nil {
		log.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("name"), []byte("leaf")); err != nil {
		log.Fatalf("set failed: %v", err)
	}

	val, ok := db.Get([]byte("name"))
	if !ok {
		log.Fatalf("missing key")
	}
	fmt.Printf("name=%s\n", val)

	if _, err := db.Delete([]byte("name")); err != nil {
		log.Fatalf("delete failed: %v", err)
	}
}
```

## Example app
Run the bundled example:

```bash
go run ./cmd/db
```

## Notes
- The on-disk format is not stable yet and may change.
- Deletions currently free empty leaf pages but do not rebalance the tree.
