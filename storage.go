package leafdb

import "os"

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
		buf, err := encodeNodePage(p.pageSize, root)
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
	root, err := readNodePage(p, rootPage, nodes)
	if err != nil {
		return err
	}
	linkLeafPointers(nodes)

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
			buf, err := encodeNodePage(p.pageSize, n)
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

func openFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
}
