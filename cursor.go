package leafdb

// Cursor iterates over keys in a bucket.
type Cursor struct {
	tree   *bptree
	leafID uint64
	index  int
}

// First moves to the first key/value pair.
func (c *Cursor) First() ([]byte, []byte) {
	if c == nil || c.tree == nil {
		return nil, nil
	}
	leaf, err := c.tree.firstLeaf()
	if err != nil || leaf == nil || len(leaf.keys) == 0 {
		return nil, nil
	}
	c.leafID = leaf.pageID
	c.index = 0
	return cloneBytes(leaf.keys[0]), cloneBytes(leaf.values[0])
}

// Next moves to the next key/value pair.
func (c *Cursor) Next() ([]byte, []byte) {
	if c == nil || c.tree == nil || c.leafID == 0 {
		return nil, nil
	}
	leaf, err := readNode(c.tree.store, c.leafID)
	if err != nil {
		return nil, nil
	}
	c.index++
	if c.index < len(leaf.keys) {
		return cloneBytes(leaf.keys[c.index]), cloneBytes(leaf.values[c.index])
	}
	if leaf.next == 0 {
		return nil, nil
	}
	leaf, err = readNode(c.tree.store, leaf.next)
	if err != nil || len(leaf.keys) == 0 {
		return nil, nil
	}
	c.leafID = leaf.pageID
	c.index = 0
	return cloneBytes(leaf.keys[0]), cloneBytes(leaf.values[0])
}

// Seek moves to the first key >= seek.
func (c *Cursor) Seek(seek []byte) ([]byte, []byte) {
	if c == nil || c.tree == nil {
		return nil, nil
	}
	leaf, err := c.tree.findLeaf(seek)
	if err != nil {
		return nil, nil
	}
	idx, _ := findKeyIndex(leaf.keys, seek)
	if idx >= len(leaf.keys) {
		if leaf.next == 0 {
			return nil, nil
		}
		leaf, err = readNode(c.tree.store, leaf.next)
		if err != nil || len(leaf.keys) == 0 {
			return nil, nil
		}
		idx = 0
	}
	c.leafID = leaf.pageID
	c.index = idx
	return cloneBytes(leaf.keys[idx]), cloneBytes(leaf.values[idx])
}
