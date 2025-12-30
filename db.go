package leafdb

import (
	"bytes"
	"errors"
	"os"
	"sync"
)

// DB is a simple key/value store backed by a B+ tree.
type DB struct {
	index      *bptree
	file       *os.File
	pager      *pager
	flushEvery int
	pending    int
	dirty      bool
	mu         sync.RWMutex
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
	file, err := openFile(path)
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
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.flushLocked(); err != nil {
		return err
	}
	return db.file.Close()
}

// Set inserts or replaces the value for key.
func (db *DB) Set(key, value []byte) error {
	if db == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	op := batchOp{key: cloneBytes(key), value: cloneBytes(value)}
	return db.applyOpsLocked([]batchOp{op})
}

// Delete removes a key if it exists. It returns true when a key was deleted.
func (db *DB) Delete(key []byte) (bool, error) {
	if db == nil {
		return false, nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	_, removed := db.index.get(key)
	if !removed {
		return false, nil
	}
	op := batchOp{key: cloneBytes(key), del: true}
	return true, db.applyOpsLocked([]batchOp{op})
}

// Get returns the value for key. The returned value is a copy.
func (db *DB) Get(key []byte) ([]byte, bool) {
	if db == nil {
		return nil, false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
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
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.flushLocked()
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

func (db *DB) flushLocked() error {
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

func (db *DB) applyOpsLocked(ops []batchOp) error {
	for _, op := range ops {
		if op.del {
			db.index.delete(op.key)
			continue
		}
		db.index.insert(op.key, op.value)
	}

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

// Batch groups multiple updates to be applied atomically.
type Batch struct {
	ops []batchOp
}

type batchOp struct {
	key   []byte
	value []byte
	del   bool
}

// NewBatch creates an empty batch.
func NewBatch() *Batch {
	return &Batch{}
}

// Set buffers a key/value upsert in the batch.
func (b *Batch) Set(key, value []byte) {
	if b == nil {
		return
	}
	b.ops = append(b.ops, batchOp{key: cloneBytes(key), value: cloneBytes(value)})
}

// Delete buffers a deletion in the batch.
func (b *Batch) Delete(key []byte) {
	if b == nil {
		return
	}
	b.ops = append(b.ops, batchOp{key: cloneBytes(key), del: true})
}

// Apply executes the batch atomically and persists once when needed.
func (db *DB) Apply(b *Batch) error {
	if db == nil || b == nil || len(b.ops) == 0 {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.applyOpsLocked(b.ops)
}

var (
	// ErrTxClosed is returned when operating on a closed transaction.
	ErrTxClosed = errors.New("leafdb: transaction closed")
	// ErrTxReadOnly is returned when a write is attempted on a read-only transaction.
	ErrTxReadOnly = errors.New("leafdb: read-only transaction")
)

// Tx represents a database transaction.
type Tx struct {
	db       *DB
	writable bool
	ops      []batchOp
	closed   bool
	snapshot *bptree
	lockHeld bool
}

// Begin starts a transaction. Writable transactions lock the database exclusively.
func (db *DB) Begin(writable bool) *Tx {
	if db == nil {
		return &Tx{closed: true}
	}
	if writable {
		db.mu.Lock()
		return &Tx{db: db, writable: true, lockHeld: true}
	}
	db.mu.RLock()
	snap := db.index.snapshot()
	db.mu.RUnlock()
	return &Tx{db: db, snapshot: snap}
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

// Get returns the value for key within the transaction. The returned value is a copy.
func (tx *Tx) Get(key []byte) ([]byte, bool) {
	if tx == nil || tx.closed {
		return nil, false
	}
	if tx.writable {
		for i := len(tx.ops) - 1; i >= 0; i-- {
			op := tx.ops[i]
			if bytes.Equal(op.key, key) {
				if op.del {
					return nil, false
				}
				return cloneBytes(op.value), true
			}
		}
	}
	if !tx.writable && tx.snapshot != nil {
		val, ok := tx.snapshot.get(key)
		if !ok {
			return nil, false
		}
		return cloneBytes(val), true
	}
	val, ok := tx.db.index.get(key)
	if !ok {
		return nil, false
	}
	return cloneBytes(val), true
}

// Set buffers a write in the transaction.
func (tx *Tx) Set(key, value []byte) error {
	if tx == nil || tx.closed {
		return ErrTxClosed
	}
	if !tx.writable {
		return ErrTxReadOnly
	}
	tx.ops = append(tx.ops, batchOp{key: cloneBytes(key), value: cloneBytes(value)})
	return nil
}

// Delete buffers a delete in the transaction.
func (tx *Tx) Delete(key []byte) (bool, error) {
	if tx == nil || tx.closed {
		return false, ErrTxClosed
	}
	if !tx.writable {
		return false, ErrTxReadOnly
	}
	_, exists := tx.Get(key)
	if !exists {
		return false, nil
	}
	tx.ops = append(tx.ops, batchOp{key: cloneBytes(key), del: true})
	return true, nil
}

// Commit applies a writable transaction atomically.
func (tx *Tx) Commit() error {
	if tx == nil || tx.closed {
		return ErrTxClosed
	}
	defer tx.close()
	if !tx.writable {
		return nil
	}
	return tx.db.applyOpsLocked(tx.ops)
}

// Rollback closes the transaction without applying changes.
func (tx *Tx) Rollback() {
	if tx == nil || tx.closed {
		return
	}
	tx.close()
}

func (tx *Tx) close() {
	if tx.closed {
		return
	}
	tx.closed = true
	if tx.writable {
		tx.db.mu.Unlock()
	} else if tx.lockHeld {
		tx.db.mu.RUnlock()
	}
}
