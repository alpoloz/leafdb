package leafdb

import (
	"encoding/binary"
	"errors"
)

type Tx struct {
	db       *DB
	writable bool
	closed   bool
	mgr      *txPageManager
	mapLock  bool
}

func (tx *Tx) Bucket(name []byte) *Bucket {
	if tx == nil || tx.closed {
		return nil
	}
	if len(name) == 0 {
		return nil
	}
	root := tx.mgr.root
	tree := newBPTree(&root, tx.mgr)
	val, ok, err := tree.get(name)
	if err != nil || !ok {
		return nil
	}
	pageID := decodePageID(val)
	kvRoot, bucketRoot, err := readBucketHeader(tx.mgr, pageID)
	if err != nil {
		return nil
	}
	return &Bucket{tx: tx, name: cloneBytes(name), header: pageID, kvRoot: kvRoot, bucketRoot: bucketRoot}
}

func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	if tx == nil || tx.closed {
		return nil, ErrTxClosed
	}
	if !tx.writable {
		return nil, ErrTxReadOnly
	}
	if len(name) == 0 {
		return nil, errors.New("leafdb: bucket name required")
	}
	root := tx.mgr.root
	tree := newBPTree(&root, tx.mgr)
	if _, ok, err := tree.get(name); err != nil {
		return nil, err
	} else if ok {
		return nil, ErrBucketExists
	}

	bucket, err := tx.createBucket()
	if err != nil {
		return nil, err
	}
	if err := tree.set(name, encodePageID(bucket.header)); err != nil {
		return nil, err
	}
	tx.mgr.root = root
	bucket.name = cloneBytes(name)
	return bucket, nil
}

func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	bucket := tx.Bucket(name)
	if bucket != nil {
		return bucket, nil
	}
	return tx.CreateBucket(name)
}

func (tx *Tx) DeleteBucket(name []byte) error {
	if tx == nil || tx.closed {
		return ErrTxClosed
	}
	if !tx.writable {
		return ErrTxReadOnly
	}
	if len(name) == 0 {
		return errors.New("leafdb: bucket name required")
	}
	root := tx.mgr.root
	tree := newBPTree(&root, tx.mgr)
	val, ok, err := tree.get(name)
	if err != nil {
		return err
	}
	if !ok {
		return ErrBucketNotFound
	}
	if _, err := tree.delete(name); err != nil {
		return err
	}
	bucketID := decodePageID(val)
	tx.releaseBucket(bucketID)
	tx.mgr.root = root
	return nil
}

func (tx *Tx) Commit() error {
	if tx == nil || tx.closed {
		return ErrTxClosed
	}
	if !tx.writable {
		return nil
	}
	if err := tx.mgr.commit(); err != nil {
		tx.close()
		return err
	}
	tx.close()
	return nil
}

func (tx *Tx) Rollback() {
	if tx == nil || tx.closed {
		return
	}
	if tx.writable {
		tx.mgr.rollback()
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
	} else if tx.mapLock {
		tx.db.mapMu.RUnlock()
	}
}

func (tx *Tx) createBucket() (*Bucket, error) {
	headerID := tx.mgr.AllocPage()
	kvRootID := tx.mgr.AllocPage()
	bucketRootID := tx.mgr.AllocPage()

	leaf := &node{pageID: kvRootID, isLeaf: true}
	buf, err := encodeNodePage(tx.mgr.pageSize, leaf)
	if err != nil {
		return nil, err
	}
	if err := tx.mgr.WritePage(kvRootID, buf); err != nil {
		return nil, err
	}

	leaf = &node{pageID: bucketRootID, isLeaf: true}
	buf, err = encodeNodePage(tx.mgr.pageSize, leaf)
	if err != nil {
		return nil, err
	}
	if err := tx.mgr.WritePage(bucketRootID, buf); err != nil {
		return nil, err
	}

	if err := writeBucketHeader(tx.mgr, headerID, kvRootID, bucketRootID); err != nil {
		return nil, err
	}
	return &Bucket{tx: tx, header: headerID, kvRoot: kvRootID, bucketRoot: bucketRootID}, nil
}

func (tx *Tx) releaseBucket(headerID uint64) {
	kvRoot, bucketRoot, err := readBucketHeader(tx.mgr, headerID)
	if err != nil {
		return
	}
	freeTree(tx.mgr, kvRoot)
	freeTree(tx.mgr, bucketRoot)
	tx.mgr.FreePage(headerID)
}

func freeTree(store pageStore, rootID uint64) {
	if rootID == 0 {
		return
	}
	node, err := readNode(store, rootID)
	if err != nil || node == nil {
		return
	}
	if !node.isLeaf {
		for _, child := range node.children {
			freeTree(store, child)
		}
	}
	store.FreePage(rootID)
}

func encodePageID(id uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, id)
	return buf
}

func decodePageID(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

// txPageManager provides per-transaction page access.
type txPageManager struct {
	db       *DB
	writable bool
	pageSize int
	root     uint64
	txid     uint64
	nextPage uint64
	freelist []uint64
	dirty    map[uint64][]byte
	maxPage  uint64
}

func newTxPageManager(db *DB, writable bool, m meta) *txPageManager {
	mgr := &txPageManager{
		db:       db,
		writable: writable,
		pageSize: db.pageSize,
		root:     m.root,
		txid:     m.txid,
		nextPage: m.nextPage,
		freelist: append([]uint64(nil), m.freelist...),
		dirty:    make(map[uint64][]byte),
	}
	if m.nextPage > 0 {
		mgr.maxPage = m.nextPage - 1
	}
	return mgr
}

func (m *txPageManager) PageSize() int {
	return m.pageSize
}

func (m *txPageManager) ReadPage(id uint64) ([]byte, error) {
	if !m.writable {
		return m.db.page(id), nil
	}
	if buf, ok := m.dirty[id]; ok {
		return buf, nil
	}
	page := m.db.page(id)
	copyBuf := make([]byte, len(page))
	copy(copyBuf, page)
	return copyBuf, nil
}

func (m *txPageManager) WritePage(id uint64, buf []byte) error {
	if !m.writable {
		return ErrTxReadOnly
	}
	page := make([]byte, len(buf))
	copy(page, buf)
	m.dirty[id] = page
	if id > m.maxPage {
		m.maxPage = id
	}
	return nil
}

func (m *txPageManager) AllocPage() uint64 {
	// MVCC safety: avoid reusing freed pages until a GC is added.
	id := m.nextPage
	m.nextPage++
	if id > m.maxPage {
		m.maxPage = id
	}
	return id
}

func (m *txPageManager) FreePage(id uint64) {
	if id == metaPage0 || id == metaPage1 {
		return
	}
	m.freelist = append(m.freelist, id)
}

func (m *txPageManager) commit() error {
	requiredSize := int((m.maxPage + 1) * uint64(m.pageSize))
	if requiredSize > len(m.db.data) {
		if err := m.db.file.Truncate(int64(requiredSize)); err != nil {
			return err
		}
		if err := m.db.remap(requiredSize); err != nil {
			return err
		}
	}

	for id, buf := range m.dirty {
		copy(m.db.page(id), buf)
	}

	newMeta := meta{txid: m.txid + 1, root: m.root, nextPage: m.nextPage, freelist: m.freelist}
	var nextMetaPage uint64 = metaPage0
	if m.db.metaPage == metaPage0 {
		nextMetaPage = metaPage1
	}
	if err := writeMetaPage(m.db.page(nextMetaPage), newMeta, m.pageSize); err != nil {
		return err
	}
	m.db.metaMu.Lock()
	m.db.meta = newMeta
	m.db.metaPage = nextMetaPage
	m.db.metaMu.Unlock()
	return m.db.data.Flush()
}

func (m *txPageManager) rollback() {
	m.dirty = nil
}
