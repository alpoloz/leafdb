package leafdb

import (
	"encoding/binary"
	"errors"
)

// Bucket is a namespace for key/value pairs and nested buckets.
type Bucket struct {
	tx         *Tx
	name       []byte
	parent     *Bucket
	header     uint64
	kvRoot     uint64
	bucketRoot uint64
	sequence   uint64
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
	return b.persistHeader()
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
	return b.persistHeader()
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
	kvRoot, bucketRoot, sequence, err := readBucketHeader(b.tx.mgr, pageID)
	if err != nil {
		return nil
	}
	return &Bucket{
		tx:         b.tx,
		name:       cloneBytes(name),
		parent:     b,
		header:     pageID,
		kvRoot:     kvRoot,
		bucketRoot: bucketRoot,
		sequence:   sequence,
	}
}

func (b *Bucket) CreateBucket(name []byte) (*Bucket, error) {
	if err := b.validateWritable(name); err != nil {
		return nil, err
	}
	if err := b.ensureBucketMissing(name); err != nil {
		return nil, err
	}
	child, err := b.tx.createBucket()
	if err != nil {
		return nil, err
	}
	if err := b.linkChild(name, child.header); err != nil {
		return nil, err
	}
	child.name = cloneBytes(name)
	child.parent = b
	return child, b.persistHeader()
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
	return b.persistHeader()
}

func (b *Bucket) Cursor() *Cursor {
	if b == nil || b.tx == nil || b.tx.closed {
		return nil
	}
	return &Cursor{tree: newBPTree(&b.kvRoot, b.tx.mgr)}
}

// Sequence returns the current sequence value for the bucket.
func (b *Bucket) Sequence() uint64 {
	if b == nil {
		return 0
	}
	return b.sequence
}

// NextSequence increments and returns the next sequence value.
func (b *Bucket) NextSequence() (uint64, error) {
	if b == nil || b.tx == nil || b.tx.closed {
		return 0, ErrTxClosed
	}
	if !b.tx.writable {
		return 0, ErrTxReadOnly
	}
	b.sequence++
	if err := b.persistHeader(); err != nil {
		return 0, err
	}
	return b.sequence, nil
}

func (b *Bucket) persistHeader() error {
	headID := b.tx.mgr.AllocPage()
	if err := writeBucketHeader(b.tx.mgr, headID, b.kvRoot, b.bucketRoot, b.sequence); err != nil {
		return err
	}
	b.header = headID
	if b.parent == nil {
		root := b.tx.mgr.root
		tree := newBPTree(&root, b.tx.mgr)
		if err := tree.set(b.name, encodePageID(headID)); err != nil {
			return err
		}
		b.tx.mgr.root = root
		return nil
	}
	return b.parent.updateChild(b.name, headID)
}

func (b *Bucket) updateChild(name []byte, headerID uint64) error {
	tree := newBPTree(&b.bucketRoot, b.tx.mgr)
	if err := tree.set(name, encodePageID(headerID)); err != nil {
		return err
	}
	return b.persistHeader()
}

func (b *Bucket) validateWritable(name []byte) error {
	if b == nil || b.tx == nil || b.tx.closed {
		return ErrTxClosed
	}
	if !b.tx.writable {
		return ErrTxReadOnly
	}
	if len(name) == 0 {
		return errors.New("leafdb: bucket name required")
	}
	return nil
}

func (b *Bucket) ensureBucketMissing(name []byte) error {
	tree := newBPTree(&b.bucketRoot, b.tx.mgr)
	if _, ok, err := tree.get(name); err != nil {
		return err
	} else if ok {
		return ErrBucketExists
	}
	return nil
}

func (b *Bucket) linkChild(name []byte, headerID uint64) error {
	tree := newBPTree(&b.bucketRoot, b.tx.mgr)
	return tree.set(name, encodePageID(headerID))
}

func readBucketHeader(store pageStore, pageID uint64) (uint64, uint64, uint64, error) {
	buf, err := store.ReadPage(pageID)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(buf) < store.PageSize() {
		return 0, 0, 0, errors.New("leafdb: invalid bucket header")
	}
	if buf[0] != pageBucket {
		return 0, 0, 0, errors.New("leafdb: invalid bucket page")
	}
	kvRoot := binary.LittleEndian.Uint64(buf[1:])
	bucketRoot := binary.LittleEndian.Uint64(buf[9:])
	sequence := binary.LittleEndian.Uint64(buf[17:])
	return kvRoot, bucketRoot, sequence, nil
}

func writeBucketHeader(store pageStore, pageID, kvRoot, bucketRoot, sequence uint64) error {
	buf := make([]byte, store.PageSize())
	buf[0] = pageBucket
	binary.LittleEndian.PutUint64(buf[1:], kvRoot)
	binary.LittleEndian.PutUint64(buf[9:], bucketRoot)
	binary.LittleEndian.PutUint64(buf[17:], sequence)
	return store.WritePage(pageID, buf)
}
