package leafdb

import (
	"errors"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
)

var (
	ErrTxClosed       = errors.New("leafdb: transaction closed")
	ErrTxReadOnly     = errors.New("leafdb: read-only transaction")
	ErrBucketExists   = errors.New("leafdb: bucket exists")
	ErrBucketNotFound = errors.New("leafdb: bucket not found")
)

// DB is a memory-mapped key/value store with B+ tree pages on disk.
type DB struct {
	file     *os.File
	data     mmap.MMap
	pageSize int
	meta     meta
	mu       sync.RWMutex
}

// Open opens or creates a database file.
func Open(path string) (*DB, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if info.Size() == 0 {
		if err := file.Truncate(int64(defaultPageSize * 2)); err != nil {
			file.Close()
			return nil, err
		}
	}

	data, err := mmap.MapRegion(file, -1, mmap.RDWR, 0, 0)
	if err != nil {
		file.Close()
		return nil, err
	}

	db := &DB{file: file, data: data, pageSize: defaultPageSize}

	if info.Size() == 0 {
		rootID := uint64(1)
		leaf := &node{pageID: rootID, isLeaf: true}
		buf, err := encodeNodePage(db.pageSize, leaf)
		if err != nil {
			db.Close()
			return nil, err
		}
		copy(db.page(rootID), buf)

		db.meta = meta{root: rootID, nextPage: 2}
		if err := writeMeta(db.page(metaPageID), db.meta, db.pageSize); err != nil {
			db.Close()
			return nil, err
		}
		if err := db.data.Flush(); err != nil {
			db.Close()
			return nil, err
		}
		return db, nil
	}

	meta, err := readMeta(db.page(metaPageID), db.pageSize)
	if err != nil {
		db.Close()
		return nil, err
	}
	db.meta = meta
	return db, nil
}

// Close flushes and closes the database.
func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.data != nil {
		_ = db.data.Flush()
		_ = db.data.Unmap()
		db.data = nil
	}
	if db.file != nil {
		return db.file.Close()
	}
	return nil
}

// View runs a read-only transaction.
func (db *DB) View(fn func(*Tx) error) error {
	if fn == nil {
		return nil
	}
	tx := db.Begin(false)
	defer tx.Rollback()
	return fn(tx)
}

// Update runs a read-write transaction.
func (db *DB) Update(fn func(*Tx) error) error {
	if fn == nil {
		return nil
	}
	tx := db.Begin(true)
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Begin starts a new transaction. Writable transactions are exclusive.
func (db *DB) Begin(writable bool) *Tx {
	if db == nil {
		return &Tx{closed: true}
	}
	if writable {
		db.mu.Lock()
		mgr := newTxPageManager(db, true)
		return &Tx{db: db, writable: true, mgr: mgr}
	}
	db.mu.RLock()
	mgr := newTxPageManager(db, false)
	return &Tx{db: db, mgr: mgr}
}

func (db *DB) page(id uint64) []byte {
	start := int(id) * db.pageSize
	end := start + db.pageSize
	return db.data[start:end]
}

func (db *DB) remap(size int) error {
	if err := db.data.Unmap(); err != nil {
		return err
	}
	data, err := mmap.MapRegion(db.file, size, mmap.RDWR, 0, 0)
	if err != nil {
		return err
	}
	db.data = data
	return nil
}
