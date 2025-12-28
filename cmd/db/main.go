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
}
