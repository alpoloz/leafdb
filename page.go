package leafdb

import (
	"encoding/binary"
	"errors"
)

const (
	fileMagic       = "LDBM"
	defaultPageSize = 4096
	metaPageID      = 0
	pageLeaf        = 1
	pageBranch      = 2
	pageBucket      = 3
	nodeHeaderSize  = 13
)

type meta struct {
	root     uint64
	nextPage uint64
	freelist []uint64
}

func readMeta(page []byte, pageSize int) (meta, error) {
	if len(page) < pageSize {
		return meta{}, errors.New("leafdb: invalid meta page")
	}
	if string(page[:4]) != fileMagic {
		return meta{}, errors.New("leafdb: invalid file header")
	}
	ps := int(binary.LittleEndian.Uint32(page[4:]))
	if ps != pageSize {
		return meta{}, errors.New("leafdb: page size mismatch")
	}
	m := meta{}
	m.root = binary.LittleEndian.Uint64(page[8:])
	m.nextPage = binary.LittleEndian.Uint64(page[16:])
	freeCount := int(binary.LittleEndian.Uint32(page[24:]))
	maxFree := (pageSize - 28) / 8
	if freeCount > maxFree {
		return meta{}, errors.New("leafdb: freelist exceeds meta capacity")
	}
	m.freelist = make([]uint64, freeCount)
	off := 28
	for i := 0; i < freeCount; i++ {
		m.freelist[i] = binary.LittleEndian.Uint64(page[off:])
		off += 8
	}
	return m, nil
}

func writeMeta(page []byte, m meta, pageSize int) error {
	if len(page) < pageSize {
		return errors.New("leafdb: invalid meta page")
	}
	copy(page[:4], []byte(fileMagic))
	binary.LittleEndian.PutUint32(page[4:], uint32(pageSize))
	binary.LittleEndian.PutUint64(page[8:], m.root)
	binary.LittleEndian.PutUint64(page[16:], m.nextPage)
	binary.LittleEndian.PutUint32(page[24:], uint32(len(m.freelist)))
	off := 28
	for _, id := range m.freelist {
		if off+8 > pageSize {
			return errors.New("leafdb: freelist too large for meta page")
		}
		binary.LittleEndian.PutUint64(page[off:], id)
		off += 8
	}
	return nil
}
