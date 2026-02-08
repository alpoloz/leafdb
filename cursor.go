package leafdb

// Cursor iterates over keys in a bucket.
type Cursor struct {
	tree  *bptree
	stack []cursorFrame
	leaf  *node
	index int
}

// First moves to the first key/value pair.
func (c *Cursor) First() ([]byte, []byte) {
	if c == nil || c.tree == nil {
		return nil, nil
	}
	c.stack = c.stack[:0]
	leaf, err := c.descendLeft(*c.tree.root)
	if err != nil || leaf == nil || len(leaf.keys) == 0 {
		return nil, nil
	}
	c.leaf = leaf
	c.index = 0
	return cloneBytes(leaf.keys[0]), cloneBytes(leaf.values[0])
}

// Next moves to the next key/value pair.
func (c *Cursor) Next() ([]byte, []byte) {
	if c == nil || c.tree == nil || c.leaf == nil {
		return nil, nil
	}
	c.index++
	if c.index < len(c.leaf.keys) {
		return cloneBytes(c.leaf.keys[c.index]), cloneBytes(c.leaf.values[c.index])
	}

	for c.leaf.next != 0 {
		leaf, err := readNode(c.tree.store, c.leaf.next)
		if err != nil || leaf == nil || !leaf.isLeaf {
			return nil, nil
		}
		c.leaf = leaf
		c.index = 0
		if len(leaf.keys) > 0 {
			return cloneBytes(leaf.keys[0]), cloneBytes(leaf.values[0])
		}
	}
	return nil, nil
}

// Seek moves to the first key >= seek.
func (c *Cursor) Seek(seek []byte) ([]byte, []byte) {
	if c == nil || c.tree == nil {
		return nil, nil
	}
	c.stack = c.stack[:0]
	leaf, idx, err := c.seekLeaf(*c.tree.root, seek)
	if err != nil || leaf == nil {
		return nil, nil
	}
	if idx >= len(leaf.keys) {
		c.leaf = leaf
		c.index = len(leaf.keys) - 1
		return c.Next()
	}
	c.leaf = leaf
	c.index = idx
	return cloneBytes(leaf.keys[idx]), cloneBytes(leaf.values[idx])
}

func (c *Cursor) descendLeft(pageID uint64) (*node, error) {
	current := pageID
	for {
		n, err := readNode(c.tree.store, current)
		if err != nil {
			return nil, err
		}
		if n.isLeaf {
			return n, nil
		}
		c.stack = append(c.stack, cursorFrame{node: n, index: 0})
		current = n.children[0]
	}
}

func (c *Cursor) seekLeaf(pageID uint64, seek []byte) (*node, int, error) {
	current := pageID
	for {
		n, err := readNode(c.tree.store, current)
		if err != nil {
			return nil, 0, err
		}
		if n.isLeaf {
			idx, _ := findKeyIndex(n.keys, seek)
			return n, idx, nil
		}
		idx := findChildIndex(n.keys, seek)
		c.stack = append(c.stack, cursorFrame{node: n, index: idx})
		current = n.children[idx]
	}
}
