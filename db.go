package leafdb

import "os"

// DB is a simple key/value store backed by a B+ tree.
type DB struct {
	index      *bptree
	file       *os.File
	pager      *pager
	flushEvery int
	pending    int
	dirty      bool
}

// Options controls persistence behavior.
type Options struct {
	// FlushEvery controls how many Set calls are buffered before persisting.
	// Values less than 1 are treated as 1 (flush every Set).
	FlushEvery int
}

// New creates an empty database with a reasonable default order.
func New() *DB {
	return newDB(defaultOrder, nil)
}

// Open creates or opens a database file and loads its contents.
func Open(path string) (*DB, error) {
	return OpenWithOptions(path, nil)
}

// OpenWithOptions creates or opens a database file with persistence options.
func OpenWithOptions(path string, opts *Options) (*DB, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	db := newDB(defaultOrder, opts)
	db.file = file
	if err := db.loadFromFile(); err != nil {
		file.Close()
		return nil, err
	}
	return db, nil
}

// Close closes the underlying file, if any.
func (db *DB) Close() error {
	if db == nil || db.file == nil {
		return nil
	}
	if err := db.Flush(); err != nil {
		return err
	}
	return db.file.Close()
}

// Set inserts or replaces the value for key.
func (db *DB) Set(key, value []byte) error {
	if db == nil {
		return nil
	}
	db.index.insert(cloneBytes(key), cloneBytes(value))
	if db.file == nil {
		return nil
	}
	db.dirty = true
	db.pending++
	if db.pending < db.flushEvery {
		return nil
	}
	if err := db.saveToFile(); err != nil {
		return err
	}
	db.pending = 0
	db.dirty = false
	return nil
}

// Delete removes a key if it exists. It returns true when a key was deleted.
func (db *DB) Delete(key []byte) (bool, error) {
	if db == nil {
		return false, nil
	}
	removed := db.index.delete(key)
	if !removed {
		return false, nil
	}
	if db.file == nil {
		return true, nil
	}
	db.dirty = true
	db.pending++
	if db.pending < db.flushEvery {
		return true, nil
	}
	if err := db.saveToFile(); err != nil {
		return true, err
	}
	db.pending = 0
	db.dirty = false
	return true, nil
}

// Get returns the value for key. The returned value is a copy.
func (db *DB) Get(key []byte) ([]byte, bool) {
	if db == nil {
		return nil, false
	}
	val, ok := db.index.get(key)
	if !ok {
		return nil, false
	}
	return cloneBytes(val), true
}

// Flush writes buffered changes to disk, if any.
func (db *DB) Flush() error {
	if db == nil || db.file == nil || !db.dirty {
		return nil
	}
	if err := db.saveToFile(); err != nil {
		return err
	}
	db.pending = 0
	db.dirty = false
	return nil
}

func newDB(order int, opts *Options) *DB {
	flushEvery := 1
	if opts != nil && opts.FlushEvery > 0 {
		flushEvery = opts.FlushEvery
	}
	return &DB{
		index:      newBPTree(order, nil),
		flushEvery: flushEvery,
	}
}
