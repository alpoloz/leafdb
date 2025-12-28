package leafdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	fileMagic    = "LDBF"
	defaultPage  = 4096
	metaPageID   = 0
	headerSize   = 13
	nodeLeaf     = 1
	nodeInternal = 2
)

type pager struct {
	file      *os.File
	pageSize  int
	nextPage  uint64
	freeList  []uint64
}

func newPager(file *os.File, pageSize int) *pager {
	if pageSize < 1024 {
		pageSize = defaultPage
	}
	return &pager{file: file, pageSize: pageSize, nextPage: 1}
}

func (p *pager) allocPage() uint64 {
	if len(p.freeList) > 0 {
		id := p.freeList[len(p.freeList)-1]
		p.freeList = p.freeList[:len(p.freeList)-1]
		return id
	}
	id := p.nextPage
	p.nextPage++
	return id
}

func (p *pager) freePage(id uint64) {
	if id == metaPageID {
		return
	}
	p.freeList = append(p.freeList, id)
}

func (p *pager) readPage(id uint64) ([]byte, error) {
	buf := make([]byte, p.pageSize)
	off := int64(id) * int64(p.pageSize)
	n, err := p.file.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n < len(buf) {
		return nil, errors.New("leafdb: short page read")
	}
	return buf, nil
}

func (p *pager) writePage(id uint64, buf []byte) error {
	if len(buf) != p.pageSize {
		return fmt.Errorf("leafdb: invalid page size %d", len(buf))
	}
	off := int64(id) * int64(p.pageSize)
	_, err := p.file.WriteAt(buf, off)
	return err
}

func (p *pager) writeMeta(order int, rootPage uint64) error {
	buf := make([]byte, p.pageSize)
	copy(buf[:4], []byte(fileMagic))
	binary.LittleEndian.PutUint32(buf[4:], uint32(p.pageSize))
	binary.LittleEndian.PutUint32(buf[8:], uint32(order))
	binary.LittleEndian.PutUint64(buf[12:], rootPage)
	binary.LittleEndian.PutUint64(buf[20:], p.nextPage)
	binary.LittleEndian.PutUint32(buf[28:], uint32(len(p.freeList)))
	pos := 32
	for _, id := range p.freeList {
		if pos+8 > len(buf) {
			return errors.New("leafdb: freelist too large for meta page")
		}
		binary.LittleEndian.PutUint64(buf[pos:], id)
		pos += 8
	}
	return p.writePage(metaPageID, buf)
}

func (p *pager) readMeta() (order int, rootPage uint64, err error) {
	buf, err := p.readPage(metaPageID)
	if err != nil {
		return 0, 0, err
	}
	if string(buf[:4]) != fileMagic {
		return 0, 0, errors.New("leafdb: invalid file header")
	}
	pageSize := int(binary.LittleEndian.Uint32(buf[4:]))
	if pageSize != p.pageSize {
		return 0, 0, errors.New("leafdb: page size mismatch")
	}
	order = int(binary.LittleEndian.Uint32(buf[8:]))
	rootPage = binary.LittleEndian.Uint64(buf[12:])
	p.nextPage = binary.LittleEndian.Uint64(buf[20:])
	freeCount := int(binary.LittleEndian.Uint32(buf[28:]))
	maxFree := (p.pageSize - 32) / 8
	if freeCount > maxFree {
		return 0, 0, errors.New("leafdb: freelist exceeds meta capacity")
	}
	p.freeList = make([]uint64, freeCount)
	pos := 32
	for i := 0; i < freeCount; i++ {
		p.freeList[i] = binary.LittleEndian.Uint64(buf[pos:])
		pos += 8
	}
	return order, rootPage, nil
}

func (db *DB) loadFromFile() error {
	info, err := db.file.Stat()
	if err != nil {
		return err
	}

	p := newPager(db.file, defaultPage)
	db.pager = p

	if info.Size() == 0 {
		root := &bpnode{isLeaf: true, pageID: p.allocPage(), dirty: true}
		db.index = &bptree{root: root, order: defaultOrder, pager: p}
		buf, err := encodeNode(p.pageSize, root)
		if err != nil {
			return err
		}
		if err := p.writePage(root.pageID, buf); err != nil {
			return err
		}
		return p.writeMeta(db.index.order, root.pageID)
	}

	order, rootPage, err := p.readMeta()
	if err != nil {
		return err
	}
	if order < 4 {
		order = 4
	}

	nodes := make(map[uint64]*bpnode)
	root, err := loadNode(p, rootPage, nodes)
	if err != nil {
		return err
	}
	linkLeafPages(nodes)

	db.index = &bptree{root: root, order: order, pager: p}
	return nil
}

func (db *DB) saveToFile() error {
	if db.file == nil || db.pager == nil {
		return nil
	}
	p := db.pager
	var write func(n *bpnode) error
	write = func(n *bpnode) error {
		if n == nil {
			return nil
		}
		if !n.isLeaf {
			for _, child := range n.children {
				if err := write(child); err != nil {
					return err
				}
			}
		}
		if n.dirty {
			buf, err := encodeNode(p.pageSize, n)
			if err != nil {
				return err
			}
			if err := p.writePage(n.pageID, buf); err != nil {
				return err
			}
			n.dirty = false
		}
		return nil
	}
	if err := write(db.index.root); err != nil {
		return err
	}
	if err := p.writeMeta(db.index.order, db.index.root.pageID); err != nil {
		return err
	}
	return db.file.Sync()
}

func encodeNode(pageSize int, n *bpnode) ([]byte, error) {
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

func loadNode(p *pager, id uint64, nodes map[uint64]*bpnode) (*bpnode, error) {
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
		child, err := loadNode(p, childID, nodes)
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

func linkLeafPages(nodes map[uint64]*bpnode) {
	for _, node := range nodes {
		if node.isLeaf && node.nextPageID != 0 {
			if next := nodes[node.nextPageID]; next != nil {
				node.next = next
			}
		}
	}
}
