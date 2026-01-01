package leafdb

import (
	"errors"
	"os"
	"sync"

	"golang.org/x/sys/unix"
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
	data     []byte
	pageSize int
	meta     meta
	metaPage uint64
	mu       sync.Mutex
	metaMu   sync.RWMutex
	mapMu    sync.RWMutex
	readMu   sync.Mutex
	readTxs  map[uint64]int
	pending  []pendingFree
}

type pendingFree struct {
	txid uint64
	id   uint64
}

// Open opens or creates a database file.
func Open(path string) (*DB, error) {
	file, info, err := openFile(path)
	if err != nil {
		return nil, err
	}

	db, err := mapFile(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	if info.Size() == 0 {
		if err := db.initEmpty(); err != nil {
			db.Close()
			return nil, err
		}
		return db, nil
	}

	if err := db.loadExisting(); err != nil {
		db.Close()
		return nil, err
	}
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
		db.mapMu.Lock()
		if len(db.data) > 0 {
			_ = unix.Msync(db.data, unix.MS_SYNC)
		}
		_ = unix.Munmap(db.data)
		db.mapMu.Unlock()
		db.data = nil
	}
	if db.file != nil {
		return db.file.Close()
	}
	return nil
}

// Read runs a read-only transaction.
func (db *DB) Read(fn func(*Tx) error) error {
	if fn == nil {
		return nil
	}
	tx := db.begin(false)
	defer tx.Rollback()
	return fn(tx)
}

// Write runs a read-write transaction.
func (db *DB) Write(fn func(*Tx) error) error {
	if fn == nil {
		return nil
	}
	tx := db.begin(true)
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// begin starts a new transaction. Writable transactions are exclusive.
func (db *DB) begin(writable bool) *Tx {
	if db == nil {
		return &Tx{closed: true}
	}
	if writable {
		db.mu.Lock()
		meta := db.snapshotMeta()
		mgr := newTxPageManager(db, true, meta)
		return &Tx{db: db, writable: true, mgr: mgr}
	}
	db.mapMu.RLock()
	meta := db.snapshotMeta()
	mgr := newTxPageManager(db, false, meta)
	db.addReadTx(meta.txid)
	return &Tx{db: db, mgr: mgr, mapLock: true, readTxID: meta.txid}
}

func (db *DB) page(id uint64) []byte {
	start := int(id) * db.pageSize
	end := start + db.pageSize
	return db.data[start:end]
}

func (db *DB) remap(size int) error {
	db.mapMu.Lock()
	defer db.mapMu.Unlock()
	if db.data != nil {
		if err := unix.Munmap(db.data); err != nil {
			return err
		}
	}
	data, err := unix.Mmap(int(db.file.Fd()), 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return err
	}
	db.data = data
	return nil
}

func (db *DB) msync() error {
	db.mapMu.RLock()
	defer db.mapMu.RUnlock()
	if db.data == nil || len(db.data) == 0 {
		return nil
	}
	return unix.Msync(db.data, unix.MS_SYNC)
}

func (db *DB) snapshotMeta() meta {
	db.metaMu.RLock()
	defer db.metaMu.RUnlock()
	return meta{
		txid:         db.meta.txid,
		root:         db.meta.root,
		nextPage:     db.meta.nextPage,
		freelistPage: db.meta.freelistPage,
		freelist:     append([]uint64(nil), db.meta.freelist...),
	}
}

func (db *DB) addReadTx(txid uint64) {
	db.readMu.Lock()
	defer db.readMu.Unlock()
	db.readTxs[txid]++
}

func (db *DB) removeReadTx(txid uint64) {
	db.readMu.Lock()
	defer db.readMu.Unlock()
	if count, ok := db.readTxs[txid]; ok {
		if count <= 1 {
			delete(db.readTxs, txid)
		} else {
			db.readTxs[txid] = count - 1
		}
	}
}

func (db *DB) minReadTxID() (uint64, bool) {
	db.readMu.Lock()
	defer db.readMu.Unlock()
	if len(db.readTxs) == 0 {
		return 0, false
	}
	var min uint64
	for txid := range db.readTxs {
		if min == 0 || txid < min {
			min = txid
		}
	}
	return min, true
}

func (db *DB) readMetaPair() (meta, uint64, error) {
	meta0, ok0, err := readMetaPage(db.page(metaPage0), db.pageSize)
	if err != nil {
		return meta{}, 0, err
	}
	meta1, ok1, err := readMetaPage(db.page(metaPage1), db.pageSize)
	if err != nil {
		return meta{}, 0, err
	}
	if ok0 && ok1 {
		if meta1.txid > meta0.txid {
			return meta1, metaPage1, nil
		}
		return meta0, metaPage0, nil
	}
	if ok0 {
		return meta0, metaPage0, nil
	}
	if ok1 {
		return meta1, metaPage1, nil
	}
	return meta{}, 0, errors.New("leafdb: no valid meta page")
}

func openFile(path string) (*os.File, os.FileInfo, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	if info.Size() == 0 {
		if err := file.Truncate(int64(defaultPageSize * 3)); err != nil {
			file.Close()
			return nil, nil, err
		}
	}
	return file, info, nil
}

func mapFile(file *os.File) (*DB, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size <= 0 {
		return nil, errors.New("leafdb: invalid file size")
	}
	if size > int64(int(^uint(0)>>1)) {
		return nil, errors.New("leafdb: file too large to mmap")
	}
	data, err := unix.Mmap(int(file.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	db := &DB{file: file, data: data, pageSize: defaultPageSize}
	db.readTxs = make(map[uint64]int)
	return db, nil
}

func (db *DB) initEmpty() error {
	rootID := uint64(2)
	leaf := &node{pageID: rootID, isLeaf: true}
	buf, err := encodeNodePage(db.pageSize, leaf)
	if err != nil {
		return err
	}
	copy(db.page(rootID), buf)

	db.meta = meta{txid: 1, root: rootID, nextPage: 3}
	db.metaPage = metaPage0
	if err := writeMetaPage(db.page(metaPage0), db.meta, db.pageSize); err != nil {
		return err
	}
	empty := meta{txid: 0}
	if err := writeMetaPage(db.page(metaPage1), empty, db.pageSize); err != nil {
		return err
	}
	return db.msync()
}

func (db *DB) loadExisting() error {
	meta, metaPage, err := db.readMetaPair()
	if err != nil {
		return err
	}
	if meta.freelistPage != 0 {
		freeIDs, _, err := db.readFreelistChain(meta.freelistPage)
		if err != nil {
			return err
		}
		meta.freelist = append(meta.freelist, freeIDs...)
	}
	db.meta = meta
	db.metaPage = metaPage
	return nil
}

func (db *DB) readFreelistChain(pageID uint64) ([]uint64, []uint64, error) {
	if pageID == 0 {
		return nil, nil, nil
	}
	ids := make([]uint64, 0, 64)
	pages := make([]uint64, 0, 8)
	current := pageID
	for current != 0 {
		pages = append(pages, current)
		next, pageIDs, err := readFreelistPage(db.page(current), db.pageSize)
		if err != nil {
			return nil, nil, err
		}
		ids = append(ids, pageIDs...)
		current = next
	}
	return ids, pages, nil
}

func (db *DB) freelistPageIDs() ([]uint64, error) {
	db.metaMu.RLock()
	pageID := db.meta.freelistPage
	db.metaMu.RUnlock()
	_, pages, err := db.readFreelistChain(pageID)
	return pages, err
}
