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
	readTxID uint64
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
	kvRoot, bucketRoot, sequence, err := readBucketHeader(tx.mgr, pageID)
	if err != nil {
		return nil
	}
	return &Bucket{
		tx:         tx,
		name:       cloneBytes(name),
		header:     pageID,
		kvRoot:     kvRoot,
		bucketRoot: bucketRoot,
		sequence:   sequence,
	}
}

func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	if err := tx.validateWritable(name); err != nil {
		return nil, err
	}
	root := tx.mgr.root
	tree := newBPTree(&root, tx.mgr)
	if err := ensureBucketMissing(tree, name); err != nil {
		return nil, err
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
		tx.close()
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
		if tx.readTxID != 0 {
			tx.db.removeReadTx(tx.readTxID)
		}
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

	if err := writeBucketHeader(tx.mgr, headerID, kvRootID, bucketRootID, 0); err != nil {
		return nil, err
	}
	return &Bucket{tx: tx, header: headerID, kvRoot: kvRootID, bucketRoot: bucketRootID, sequence: 0}, nil
}

func (tx *Tx) releaseBucket(headerID uint64) {
	kvRoot, bucketRoot, _, err := readBucketHeader(tx.mgr, headerID)
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

func (tx *Tx) validateWritable(name []byte) error {
	if tx == nil || tx.closed {
		return ErrTxClosed
	}
	if !tx.writable {
		return ErrTxReadOnly
	}
	if len(name) == 0 {
		return errors.New("leafdb: bucket name required")
	}
	return nil
}

func ensureBucketMissing(tree *bptree, name []byte) error {
	if _, ok, err := tree.get(name); err != nil {
		return err
	} else if ok {
		return ErrBucketExists
	}
	return nil
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
	pending  []uint64
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
	if len(m.freelist) > 0 {
		id := m.freelist[len(m.freelist)-1]
		m.freelist = m.freelist[:len(m.freelist)-1]
		if id > m.maxPage {
			m.maxPage = id
		}
		return id
	}
	return m.allocPageFromEnd()
}

func (m *txPageManager) FreePage(id uint64) {
	if id == metaPage0 || id == metaPage1 {
		return
	}
	m.pending = append(m.pending, id)
}

func (m *txPageManager) commit() error {
	if err := m.ensureMapSize(); err != nil {
		return err
	}
	if err := m.flushDirty(); err != nil {
		return err
	}
	if err := m.db.msync(); err != nil {
		return err
	}
	if err := m.finalizeMeta(); err != nil {
		return err
	}
	if err := m.db.msync(); err != nil {
		return err
	}
	return fdatasync(m.db.file)
}

func (m *txPageManager) rollback() {
	m.dirty = nil
	m.pending = nil
}

func (m *txPageManager) allocPageFromEnd() uint64 {
	id := m.nextPage
	m.nextPage++
	if id > m.maxPage {
		m.maxPage = id
	}
	return id
}

func (m *txPageManager) ensureMapSize() error {
	requiredSize := int((m.maxPage + 1) * uint64(m.pageSize))
	if requiredSize <= len(m.db.data) {
		return nil
	}
	if err := m.db.file.Truncate(int64(requiredSize)); err != nil {
		return err
	}
	return m.db.remap(requiredSize)
}

func (m *txPageManager) flushDirty() error {
	for id, buf := range m.dirty {
		copy(m.db.page(id), buf)
	}
	return nil
}

func (m *txPageManager) finalizeMeta() error {
	newMeta := meta{txid: m.txid + 1, root: m.root, nextPage: m.nextPage, freelist: nil}
	nextMetaPage := m.nextMetaPage()
	minRead, threshold := m.reuseThreshold(newMeta.txid)
	reusable, remaining := m.collectReusable(newMeta.txid, minRead, threshold)
	// Avoid overwriting existing freelist pages before the meta page flips.
	oldFreelistPages, err := m.db.freelistPageIDs()
	if err != nil {
		return err
	}
	free := append([]uint64(nil), m.freelist...)
	free = append(free, reusable...)
	free = append(free, oldFreelistPages...)
	inlineFree, freelistPage, err := m.persistFreelist(free, oldFreelistPages)
	if err != nil {
		return err
	}
	newMeta.freelist = inlineFree
	newMeta.freelistPage = freelistPage

	m.db.metaMu.Lock()
	defer m.db.metaMu.Unlock()
	if err := writeMetaPage(m.db.page(nextMetaPage), newMeta, m.pageSize); err != nil {
		return err
	}
	m.db.pending = remaining
	m.db.meta = newMeta
	m.db.metaPage = nextMetaPage
	return nil
}

func (m *txPageManager) nextMetaPage() uint64 {
	if m.db.metaPage == metaPage0 {
		return metaPage1
	}
	return metaPage0
}

func (m *txPageManager) reuseThreshold(txid uint64) (uint64, uint64) {
	minRead, ok := m.db.minReadTxID()
	if ok {
		return minRead, minRead
	}
	return 0, txid + 1
}

func (m *txPageManager) collectReusable(txid uint64, minRead uint64, threshold uint64) ([]uint64, []pendingFree) {
	pending := make([]pendingFree, 0, len(m.db.pending)+len(m.pending))
	pending = append(pending, m.db.pending...)
	for _, id := range m.pending {
		pending = append(pending, pendingFree{txid: txid, id: id})
	}
	reusable := make([]uint64, 0, len(pending))
	remaining := make([]pendingFree, 0, len(pending))
	for _, entry := range pending {
		if entry.txid < threshold && (minRead == 0 || entry.txid < minRead) {
			reusable = append(reusable, entry.id)
		} else {
			remaining = append(remaining, entry)
		}
	}
	return reusable, remaining
}

func (m *txPageManager) persistFreelist(free []uint64, protected []uint64) ([]uint64, uint64, error) {
	inlineCap := metaInlineFreeCapacity(m.pageSize)
	if len(free) <= inlineCap {
		return free, 0, nil
	}
	perPage := freelistPageCapacity(m.pageSize)
	overflowCount := len(free) - inlineCap
	pagesNeeded := (overflowCount + perPage) / (perPage + 1)

	protectedSet := make(map[uint64]bool, len(protected))
	for _, id := range protected {
		protectedSet[id] = true
	}

	pageIDs := make([]uint64, 0, pagesNeeded)
	selected := make(map[uint64]bool, pagesNeeded)
	for i := len(free) - 1; i >= 0 && len(pageIDs) < pagesNeeded; i-- {
		id := free[i]
		if protectedSet[id] || selected[id] {
			continue
		}
		pageIDs = append(pageIDs, id)
		selected[id] = true
	}
	for len(pageIDs) < pagesNeeded {
		id := m.allocPageFromEnd()
		pageIDs = append(pageIDs, id)
	}

	if len(selected) > 0 {
		kept := free[:0]
		for _, id := range free {
			if !selected[id] {
				kept = append(kept, id)
			}
		}
		free = kept
	}

	if len(free) <= inlineCap {
		inlineCap = len(free)
	}
	inline := free[:inlineCap]
	overflow := free[inlineCap:]
	if len(overflow) > len(pageIDs)*perPage {
		return nil, 0, errors.New("leafdb: freelist overflow")
	}
	if err := m.writeFreelistPages(pageIDs, overflow); err != nil {
		return nil, 0, err
	}
	return inline, pageIDs[0], nil
}

func (m *txPageManager) writeFreelistPages(pageIDs []uint64, ids []uint64) error {
	if len(pageIDs) == 0 {
		return nil
	}
	perPage := freelistPageCapacity(m.pageSize)
	index := 0
	for i, pageID := range pageIDs {
		next := uint64(0)
		if i+1 < len(pageIDs) {
			next = pageIDs[i+1]
		}
		end := index + perPage
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[index:end]
		index = end
		buf := make([]byte, m.pageSize)
		if err := writeFreelistPage(buf, chunk, next, m.pageSize); err != nil {
			return err
		}
		if err := m.WritePage(pageID, buf); err != nil {
			return err
		}
	}
	return nil
}
