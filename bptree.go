package leafdb

import "bytes"

const defaultOrder = 16

type bptree struct {
	root  *bpnode
	order int
	pager *pager
}

type bpnode struct {
	isLeaf     bool
	keys       [][]byte
	values     [][]byte  // only for leaf
	children   []*bpnode // only for internal
	next       *bpnode   // only for leaf
	pageID     uint64
	nextPageID uint64
	dirty      bool
}

func newBPTree(order int, pager *pager) *bptree {
	if order < 4 {
		order = 4
	}
	root := &bpnode{isLeaf: true}
	if pager != nil {
		root.pageID = pager.allocPage()
		root.dirty = true
	}
	return &bptree{root: root, order: order, pager: pager}
}

func (t *bptree) maxKeys() int {
	return t.order - 1
}

func (t *bptree) get(key []byte) ([]byte, bool) {
	leaf := t.findLeaf(key)
	idx, ok := findKeyIndex(leaf.keys, key)
	if !ok {
		return nil, false
	}
	return leaf.values[idx], true
}

func (t *bptree) findLeaf(key []byte) *bpnode {
	n := t.root
	for !n.isLeaf {
		idx := findChildIndex(n.keys, key)
		n = n.children[idx]
	}
	return n
}

func (t *bptree) snapshot() *bptree {
	if t == nil || t.root == nil {
		return newBPTree(defaultOrder, nil)
	}
	root := cloneNode(t.root)
	linkLeaves(root)
	return &bptree{root: root, order: t.order}
}

type pathElem struct {
	node *bpnode
	idx  int
}

func (t *bptree) findLeafWithPath(key []byte) (*bpnode, []pathElem) {
	n := t.root
	var path []pathElem
	for !n.isLeaf {
		idx := findChildIndex(n.keys, key)
		path = append(path, pathElem{node: n, idx: idx})
		n = n.children[idx]
	}
	return n, path
}

func (t *bptree) insert(key, value []byte) {
	promoted, right, split := t.insertInto(t.root, key, value)
	if split {
		newRoot := &bpnode{
			isLeaf:   false,
			keys:     [][]byte{promoted},
			children: []*bpnode{t.root, right},
		}
		newRoot.dirty = true
		if t.pager != nil {
			newRoot.pageID = t.pager.allocPage()
		}
		t.root = newRoot
	}
}

func (t *bptree) delete(key []byte) bool {
	leaf, path := t.findLeafWithPath(key)
	idx, exists := findKeyIndex(leaf.keys, key)
	if !exists {
		return false
	}
	removeAt(&leaf.keys, idx)
	removeAt(&leaf.values, idx)
	leaf.dirty = true

	if len(leaf.keys) == 0 && leaf != t.root && len(path) > 0 {
		parent := path[len(path)-1].node
		childIdx := path[len(path)-1].idx
		removeChild(parent, childIdx)
		parent.dirty = true
		if t.pager != nil {
			t.pager.freePage(leaf.pageID)
		}
		if parent == t.root && len(parent.children) == 1 {
			child := parent.children[0]
			t.root = child
			if t.pager != nil {
				t.pager.freePage(parent.pageID)
			}
		}
	}
	return true
}

func (t *bptree) insertInto(n *bpnode, key, value []byte) ([]byte, *bpnode, bool) {
	if n.isLeaf {
		idx, exists := findKeyIndex(n.keys, key)
		if exists {
			n.values[idx] = value
			n.dirty = true
			return nil, nil, false
		}
		insertAt(&n.keys, idx, key)
		insertAt(&n.values, idx, value)
		n.dirty = true
		if len(n.keys) <= t.maxKeys() {
			return nil, nil, false
		}
		return t.splitLeaf(n)
	}

	childIdx := findChildIndex(n.keys, key)
	promoted, right, split := t.insertInto(n.children[childIdx], key, value)
	if !split {
		return nil, nil, false
	}

	insertAt(&n.keys, childIdx, promoted)
	insertAtNode(&n.children, childIdx+1, right)
	n.dirty = true
	if len(n.keys) <= t.maxKeys() {
		return nil, nil, false
	}
	return t.splitInternal(n)
}

func (t *bptree) splitLeaf(n *bpnode) ([]byte, *bpnode, bool) {
	mid := len(n.keys) / 2
	right := &bpnode{isLeaf: true}
	right.keys = append(right.keys, n.keys[mid:]...)
	right.values = append(right.values, n.values[mid:]...)

	n.keys = n.keys[:mid]
	n.values = n.values[:mid]

	right.next = n.next
	n.next = right
	n.dirty = true
	right.dirty = true
	if t.pager != nil {
		right.pageID = t.pager.allocPage()
	}

	promoted := right.keys[0]
	return promoted, right, true
}

func (t *bptree) splitInternal(n *bpnode) ([]byte, *bpnode, bool) {
	mid := len(n.keys) / 2
	promoted := n.keys[mid]

	right := &bpnode{isLeaf: false}
	right.keys = append(right.keys, n.keys[mid+1:]...)
	right.children = append(right.children, n.children[mid+1:]...)

	n.keys = n.keys[:mid]
	n.children = n.children[:mid+1]
	n.dirty = true
	right.dirty = true
	if t.pager != nil {
		right.pageID = t.pager.allocPage()
	}

	return promoted, right, true
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

func insertAt[T any](slice *[]T, idx int, value T) {
	*slice = append(*slice, value)
	copy((*slice)[idx+1:], (*slice)[idx:])
	(*slice)[idx] = value
}

func insertAtNode(slice *[]*bpnode, idx int, value *bpnode) {
	*slice = append(*slice, value)
	copy((*slice)[idx+1:], (*slice)[idx:])
	(*slice)[idx] = value
}

func removeAt[T any](slice *[]T, idx int) {
	copy((*slice)[idx:], (*slice)[idx+1:])
	*slice = (*slice)[:len(*slice)-1]
}

func removeChild(parent *bpnode, idx int) {
	removeAt(&parent.children, idx)
	if len(parent.keys) == 0 {
		return
	}
	if idx < len(parent.keys) {
		removeAt(&parent.keys, idx)
	} else {
		removeAt(&parent.keys, len(parent.keys)-1)
	}
}

func cloneNode(n *bpnode) *bpnode {
	if n == nil {
		return nil
	}
	out := &bpnode{isLeaf: n.isLeaf}
	out.keys = append(out.keys, n.keys...)
	if n.isLeaf {
		out.values = append(out.values, n.values...)
		return out
	}
	out.children = make([]*bpnode, len(n.children))
	for i, child := range n.children {
		out.children[i] = cloneNode(child)
	}
	return out
}

func linkLeaves(root *bpnode) {
	var prev *bpnode
	var walk func(n *bpnode)
	walk = func(n *bpnode) {
		if n == nil {
			return
		}
		if n.isLeaf {
			if prev != nil {
				prev.next = n
			}
			prev = n
			return
		}
		for _, child := range n.children {
			walk(child)
		}
	}
	walk(root)
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
