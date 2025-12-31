package leafdb

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type pageStore interface {
	PageSize() int
	ReadPage(id uint64) ([]byte, error)
	WritePage(id uint64, buf []byte) error
	AllocPage() uint64
	FreePage(id uint64)
}

type bptree struct {
	root  *uint64
	store pageStore
}

type node struct {
	pageID   uint64
	isLeaf   bool
	keys     [][]byte
	values   [][]byte
	children []uint64
	next     uint64
}

func newBPTree(root *uint64, store pageStore) *bptree {
	return &bptree{root: root, store: store}
}

func (t *bptree) get(key []byte) ([]byte, bool, error) {
	leaf, err := t.findLeaf(key)
	if err != nil {
		return nil, false, err
	}
	idx, ok := findKeyIndex(leaf.keys, key)
	if !ok {
		return nil, false, nil
	}
	return cloneBytes(leaf.values[idx]), true, nil
}

func (t *bptree) set(key, value []byte) error {
	promoted, right, split, err := t.insert(*t.root, key, value)
	if err != nil {
		return err
	}
	if split {
		rootID := t.store.AllocPage()
		root := &node{
			pageID:   rootID,
			isLeaf:   false,
			keys:     [][]byte{promoted},
			children: []uint64{*t.root, right},
		}
		buf, err := encodeNodePage(t.store.PageSize(), root)
		if err != nil {
			return err
		}
		if err := t.store.WritePage(rootID, buf); err != nil {
			return err
		}
		*t.root = rootID
	}
	return nil
}

func (t *bptree) delete(key []byte) (bool, error) {
	leaf, err := t.findLeaf(key)
	if err != nil {
		return false, err
	}
	idx, ok := findKeyIndex(leaf.keys, key)
	if !ok {
		return false, nil
	}
	removeAt(&leaf.keys, idx)
	removeAt(&leaf.values, idx)
	buf, err := encodeNodePage(t.store.PageSize(), leaf)
	if err != nil {
		return false, err
	}
	return true, t.store.WritePage(leaf.pageID, buf)
}

func (t *bptree) findLeaf(key []byte) (*node, error) {
	currentID := *t.root
	for {
		n, err := readNode(t.store, currentID)
		if err != nil {
			return nil, err
		}
		if n.isLeaf {
			return n, nil
		}
		idx := findChildIndex(n.keys, key)
		currentID = n.children[idx]
	}
}

func (t *bptree) firstLeaf() (*node, error) {
	currentID := *t.root
	for {
		n, err := readNode(t.store, currentID)
		if err != nil {
			return nil, err
		}
		if n.isLeaf {
			return n, nil
		}
		currentID = n.children[0]
	}
}

func (t *bptree) insert(pageID uint64, key, value []byte) ([]byte, uint64, bool, error) {
	n, err := readNode(t.store, pageID)
	if err != nil {
		return nil, 0, false, err
	}
	if n.isLeaf {
		idx, exists := findKeyIndex(n.keys, key)
		if exists {
			n.values[idx] = cloneBytes(value)
			buf, err := encodeNodePage(t.store.PageSize(), n)
			if err != nil {
				return nil, 0, false, err
			}
			return nil, 0, false, t.store.WritePage(n.pageID, buf)
		}
		insertAt(&n.keys, idx, cloneBytes(key))
		insertAt(&n.values, idx, cloneBytes(value))
		if nodeFits(t.store.PageSize(), n) {
			buf, err := encodeNodePage(t.store.PageSize(), n)
			if err != nil {
				return nil, 0, false, err
			}
			return nil, 0, false, t.store.WritePage(n.pageID, buf)
		}
		return t.splitLeaf(n)
	}

	idx := findChildIndex(n.keys, key)
	promoted, rightID, split, err := t.insert(n.children[idx], key, value)
	if err != nil {
		return nil, 0, false, err
	}
	if split {
		insertAt(&n.keys, idx, promoted)
		insertAtUint64(&n.children, idx+1, rightID)
	}
	if nodeFits(t.store.PageSize(), n) {
		buf, err := encodeNodePage(t.store.PageSize(), n)
		if err != nil {
			return nil, 0, false, err
		}
		return nil, 0, false, t.store.WritePage(n.pageID, buf)
	}
	return t.splitBranch(n)
}

func (t *bptree) splitLeaf(n *node) ([]byte, uint64, bool, error) {
	mid := len(n.keys) / 2
	right := &node{
		pageID: t.store.AllocPage(),
		isLeaf: true,
		keys:   append([][]byte{}, n.keys[mid:]...),
		values: append([][]byte{}, n.values[mid:]...),
		next:   n.next,
	}

	n.keys = n.keys[:mid]
	n.values = n.values[:mid]
	n.next = right.pageID

	leftBuf, err := encodeNodePage(t.store.PageSize(), n)
	if err != nil {
		return nil, 0, false, err
	}
	if err := t.store.WritePage(n.pageID, leftBuf); err != nil {
		return nil, 0, false, err
	}

	rightBuf, err := encodeNodePage(t.store.PageSize(), right)
	if err != nil {
		return nil, 0, false, err
	}
	if err := t.store.WritePage(right.pageID, rightBuf); err != nil {
		return nil, 0, false, err
	}
	return cloneBytes(right.keys[0]), right.pageID, true, nil
}

func (t *bptree) splitBranch(n *node) ([]byte, uint64, bool, error) {
	mid := len(n.keys) / 2
	promoted := n.keys[mid]

	right := &node{
		pageID:   t.store.AllocPage(),
		isLeaf:   false,
		keys:     append([][]byte{}, n.keys[mid+1:]...),
		children: append([]uint64{}, n.children[mid+1:]...),
	}

	n.keys = n.keys[:mid]
	n.children = n.children[:mid+1]

	leftBuf, err := encodeNodePage(t.store.PageSize(), n)
	if err != nil {
		return nil, 0, false, err
	}
	if err := t.store.WritePage(n.pageID, leftBuf); err != nil {
		return nil, 0, false, err
	}

	rightBuf, err := encodeNodePage(t.store.PageSize(), right)
	if err != nil {
		return nil, 0, false, err
	}
	if err := t.store.WritePage(right.pageID, rightBuf); err != nil {
		return nil, 0, false, err
	}

	return cloneBytes(promoted), right.pageID, true, nil
}

func readNode(store pageStore, pageID uint64) (*node, error) {
	buf, err := store.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	if len(buf) < store.PageSize() {
		return nil, errors.New("leafdb: short page")
	}
	kind := buf[0]
	keyCount := int(binary.LittleEndian.Uint16(buf[1:]))
	next := binary.LittleEndian.Uint64(buf[3:])
	pos := nodeHeaderSize

	switch kind {
	case pageLeaf:
		n := &node{pageID: pageID, isLeaf: true, next: next}
		n.keys = make([][]byte, keyCount)
		n.values = make([][]byte, keyCount)
		for i := 0; i < keyCount; i++ {
			var key []byte
			key, pos, err = readKey(buf, pos)
			if err != nil {
				return nil, err
			}
			n.keys[i] = key
			var val []byte
			val, pos, err = readValue(buf, pos)
			if err != nil {
				return nil, err
			}
			n.values[i] = val
		}
		return n, nil
	case pageBranch:
		n := &node{pageID: pageID, isLeaf: false}
		childCount := keyCount + 1
		n.children = make([]uint64, childCount)
		for i := 0; i < childCount; i++ {
			if pos+8 > len(buf) {
				return nil, errors.New("leafdb: invalid branch page")
			}
			n.children[i] = binary.LittleEndian.Uint64(buf[pos:])
			pos += 8
		}
		n.keys = make([][]byte, keyCount)
		for i := 0; i < keyCount; i++ {
			var key []byte
			key, pos, err = readKey(buf, pos)
			if err != nil {
				return nil, err
			}
			n.keys[i] = key
		}
		return n, nil
	default:
		return nil, errors.New("leafdb: invalid node page")
	}
}

func encodeNodePage(pageSize int, n *node) ([]byte, error) {
	buf := make([]byte, pageSize)
	if n.isLeaf {
		buf[0] = pageLeaf
	} else {
		buf[0] = pageBranch
	}
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(n.keys)))
	binary.LittleEndian.PutUint64(buf[3:], n.next)
	pos := nodeHeaderSize

	if n.isLeaf {
		for i, key := range n.keys {
			var err error
			pos, err = writeKeyValue(buf, pos, key, n.values[i])
			if err != nil {
				return nil, err
			}
		}
		return buf, nil
	}

	for _, child := range n.children {
		if pos+8 > len(buf) {
			return nil, errors.New("leafdb: node too large for page")
		}
		binary.LittleEndian.PutUint64(buf[pos:], child)
		pos += 8
	}
	for _, key := range n.keys {
		var err error
		pos, err = writeKey(buf, pos, key)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func nodeFits(pageSize int, n *node) bool {
	size := nodeHeaderSize
	if n.isLeaf {
		for i, key := range n.keys {
			size += 2 + len(key) + 4 + len(n.values[i])
		}
		return size <= pageSize
	}
	size += len(n.children) * 8
	for _, key := range n.keys {
		size += 2 + len(key)
	}
	return size <= pageSize
}

func findChildIndex(keys [][]byte, key []byte) int {
	low, high := 0, len(keys)
	for low < high {
		mid := (low + high) / 2
		if bytes.Compare(key, keys[mid]) < 0 {
			high = mid
		} else {
			low = mid + 1
		}
	}
	return low
}

func findKeyIndex(keys [][]byte, key []byte) (int, bool) {
	low, high := 0, len(keys)
	for low < high {
		mid := (low + high) / 2
		cmp := bytes.Compare(key, keys[mid])
		if cmp == 0 {
			return mid, true
		}
		if cmp < 0 {
			high = mid
		} else {
			low = mid + 1
		}
	}
	return low, false
}

func writeKeyValue(buf []byte, pos int, key, value []byte) (int, error) {
	if pos+2+len(key)+4+len(value) > len(buf) {
		return pos, errors.New("leafdb: node too large for page")
	}
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2
	copy(buf[pos:], key)
	pos += len(key)
	binary.LittleEndian.PutUint32(buf[pos:], uint32(len(value)))
	pos += 4
	copy(buf[pos:], value)
	pos += len(value)
	return pos, nil
}

func writeKey(buf []byte, pos int, key []byte) (int, error) {
	if pos+2+len(key) > len(buf) {
		return pos, errors.New("leafdb: node too large for page")
	}
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2
	copy(buf[pos:], key)
	pos += len(key)
	return pos, nil
}

func readKey(buf []byte, pos int) ([]byte, int, error) {
	if pos+2 > len(buf) {
		return nil, pos, errors.New("leafdb: corrupted key length")
	}
	length := int(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2
	if pos+length > len(buf) {
		return nil, pos, errors.New("leafdb: corrupted key data")
	}
	key := make([]byte, length)
	copy(key, buf[pos:pos+length])
	pos += length
	return key, pos, nil
}

func readValue(buf []byte, pos int) ([]byte, int, error) {
	if pos+4 > len(buf) {
		return nil, pos, errors.New("leafdb: corrupted value length")
	}
	length := int(binary.LittleEndian.Uint32(buf[pos:]))
	pos += 4
	if pos+length > len(buf) {
		return nil, pos, errors.New("leafdb: corrupted value data")
	}
	value := make([]byte, length)
	copy(value, buf[pos:pos+length])
	pos += length
	return value, pos, nil
}

func insertAt[T any](slice *[]T, idx int, value T) {
	*slice = append(*slice, value)
	copy((*slice)[idx+1:], (*slice)[idx:])
	(*slice)[idx] = value
}

func insertAtUint64(slice *[]uint64, idx int, value uint64) {
	*slice = append(*slice, value)
	copy((*slice)[idx+1:], (*slice)[idx:])
	(*slice)[idx] = value
}

func removeAt[T any](slice *[]T, idx int) {
	copy((*slice)[idx:], (*slice)[idx+1:])
	*slice = (*slice)[:len(*slice)-1]
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
