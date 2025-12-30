package leafdb

import (
	"encoding/binary"
	"errors"
)

const (
	headerSize   = 13
	nodeLeaf     = 1
	nodeInternal = 2
)

func encodeNodePage(pageSize int, n *bpnode) ([]byte, error) {
	buf := make([]byte, pageSize)
	kind := byte(nodeInternal)
	if n.isLeaf {
		kind = nodeLeaf
	}
	buf[0] = kind
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(n.keys)))
	if n.isLeaf {
		nextID := n.nextPageID
		if n.next != nil {
			nextID = n.next.pageID
		}
		binary.LittleEndian.PutUint64(buf[3:], nextID)
		binary.LittleEndian.PutUint16(buf[11:], 0)
		pos := headerSize
		for i, key := range n.keys {
			var err error
			pos, err = writeKeyValue(buf, pos, key, n.values[i])
			if err != nil {
				return nil, err
			}
		}
		return buf, nil
	}

	binary.LittleEndian.PutUint64(buf[3:], 0)
	binary.LittleEndian.PutUint16(buf[11:], uint16(len(n.children)))
	pos := headerSize
	for _, child := range n.children {
		if pos+8 > len(buf) {
			return nil, errors.New("leafdb: node too large for page")
		}
		binary.LittleEndian.PutUint64(buf[pos:], child.pageID)
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

func readNodePage(p *pager, id uint64, nodes map[uint64]*bpnode) (*bpnode, error) {
	if n, ok := nodes[id]; ok {
		return n, nil
	}
	buf, err := p.readPage(id)
	if err != nil {
		return nil, err
	}
	kind := buf[0]
	keyCount := int(binary.LittleEndian.Uint16(buf[1:]))
	nextID := binary.LittleEndian.Uint64(buf[3:])
	childCount := int(binary.LittleEndian.Uint16(buf[11:]))
	pos := headerSize

	node := &bpnode{pageID: id}
	nodes[id] = node
	if kind == nodeLeaf {
		node.isLeaf = true
		node.keys = make([][]byte, keyCount)
		node.values = make([][]byte, keyCount)
		node.nextPageID = nextID
		for i := 0; i < keyCount; i++ {
			var key []byte
			key, pos, err = readKey(buf, pos)
			if err != nil {
				return nil, err
			}
			node.keys[i] = key
			var value []byte
			value, pos, err = readValue(buf, pos)
			if err != nil {
				return nil, err
			}
			node.values[i] = value
		}
		return node, nil
	}
	if kind != nodeInternal {
		return nil, errors.New("leafdb: invalid node type")
	}
	if childCount != keyCount+1 {
		return nil, errors.New("leafdb: invalid child count")
	}
	node.isLeaf = false
	node.children = make([]*bpnode, childCount)
	childIDs := make([]uint64, childCount)
	for i := 0; i < childCount; i++ {
		childIDs[i] = binary.LittleEndian.Uint64(buf[pos:])
		pos += 8
	}
	node.keys = make([][]byte, keyCount)
	for i := 0; i < keyCount; i++ {
		var key []byte
		key, pos, err = readKey(buf, pos)
		if err != nil {
			return nil, err
		}
		node.keys[i] = key
	}
	for i, childID := range childIDs {
		child, err := readNodePage(p, childID, nodes)
		if err != nil {
			return nil, err
		}
		node.children[i] = child
	}
	return node, nil
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

func linkLeafPointers(nodes map[uint64]*bpnode) {
	for _, node := range nodes {
		if node.isLeaf && node.nextPageID != 0 {
			if next := nodes[node.nextPageID]; next != nil {
				node.next = next
			}
		}
	}
}
