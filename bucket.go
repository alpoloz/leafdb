package leafdb

import (
	"encoding/binary"
	"errors"
)

// Bucket is a namespace for key/value pairs and nested buckets.
type Bucket struct {
	tx         *Tx
	header     uint64
	kvRoot     uint64
	bucketRoot uint64
}

func (b *Bucket) Get(key []byte) []byte {
	if b == nil || b.tx == nil || b.tx.closed {
		return nil
	}
	tree := newBPTree(&b.kvRoot, b.tx.mgr)
	val, ok, err := tree.get(key)
	if err != nil || !ok {
		return nil
	}
	return val
}

func (b *Bucket) Put(key, value []byte) error {
	if b == nil || b.tx == nil || b.tx.closed {
		return ErrTxClosed
	}
	if !b.tx.writable {
		return ErrTxReadOnly
	}
	tree := newBPTree(&b.kvRoot, b.tx.mgr)
	if err := tree.set(key, value); err != nil {
		return err
	}
	return writeBucketHeader(b.tx.mgr, b.header, b.kvRoot, b.bucketRoot)
}

func (b *Bucket) Delete(key []byte) error {
	if b == nil || b.tx == nil || b.tx.closed {
		return ErrTxClosed
	}
	if !b.tx.writable {
		return ErrTxReadOnly
	}
	tree := newBPTree(&b.kvRoot, b.tx.mgr)
	_, err := tree.delete(key)
	if err != nil {
		return err
	}
	return writeBucketHeader(b.tx.mgr, b.header, b.kvRoot, b.bucketRoot)
}

func (b *Bucket) Bucket(name []byte) *Bucket {
	if b == nil || b.tx == nil || b.tx.closed {
		return nil
	}
	if len(name) == 0 {
		return nil
	}
	tree := newBPTree(&b.bucketRoot, b.tx.mgr)
	val, ok, err := tree.get(name)
	if err != nil || !ok {
		return nil
	}
	pageID := decodePageID(val)
	kvRoot, bucketRoot, err := readBucketHeader(b.tx.mgr, pageID)
	if err != nil {
		return nil
	}
	return &Bucket{tx: b.tx, header: pageID, kvRoot: kvRoot, bucketRoot: bucketRoot}
}

func (b *Bucket) CreateBucket(name []byte) (*Bucket, error) {
	if b == nil || b.tx == nil || b.tx.closed {
		return nil, ErrTxClosed
	}
	if !b.tx.writable {
		return nil, ErrTxReadOnly
	}
	if len(name) == 0 {
		return nil, errors.New("leafdb: bucket name required")
	}
	tree := newBPTree(&b.bucketRoot, b.tx.mgr)
	if _, ok, err := tree.get(name); err != nil {
		return nil, err
	} else if ok {
		return nil, ErrBucketExists
	}

	child, err := b.tx.createBucket()
	if err != nil {
		return nil, err
	}
	if err := tree.set(name, encodePageID(child.header)); err != nil {
		return nil, err
	}
	if err := writeBucketHeader(b.tx.mgr, b.header, b.kvRoot, b.bucketRoot); err != nil {
		return nil, err
	}
	return child, nil
}

func (b *Bucket) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	bucket := b.Bucket(name)
	if bucket != nil {
		return bucket, nil
	}
	return b.CreateBucket(name)
}

func (b *Bucket) DeleteBucket(name []byte) error {
	if b == nil || b.tx == nil || b.tx.closed {
		return ErrTxClosed
	}
	if !b.tx.writable {
		return ErrTxReadOnly
	}
	if len(name) == 0 {
		return errors.New("leafdb: bucket name required")
	}
	tree := newBPTree(&b.bucketRoot, b.tx.mgr)
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
	b.tx.releaseBucket(bucketID)
	return writeBucketHeader(b.tx.mgr, b.header, b.kvRoot, b.bucketRoot)
}

func (b *Bucket) Cursor() *Cursor {
	if b == nil || b.tx == nil || b.tx.closed {
		return nil
	}
	return &Cursor{tree: newBPTree(&b.kvRoot, b.tx.mgr)}
}

func readBucketHeader(store pageStore, pageID uint64) (uint64, uint64, error) {
	buf, err := store.ReadPage(pageID)
	if err != nil {
		return 0, 0, err
	}
	if len(buf) < store.PageSize() {
		return 0, 0, errors.New("leafdb: invalid bucket header")
	}
	if buf[0] != pageBucket {
		return 0, 0, errors.New("leafdb: invalid bucket page")
	}
	kvRoot := binary.LittleEndian.Uint64(buf[1:])
	bucketRoot := binary.LittleEndian.Uint64(buf[9:])
	return kvRoot, bucketRoot, nil
}

func writeBucketHeader(store pageStore, pageID, kvRoot, bucketRoot uint64) error {
	buf := make([]byte, store.PageSize())
	buf[0] = pageBucket
	binary.LittleEndian.PutUint64(buf[1:], kvRoot)
	binary.LittleEndian.PutUint64(buf[9:], bucketRoot)
	return store.WritePage(pageID, buf)
}
