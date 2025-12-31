package leafdb

import (
	"encoding/binary"
	"errors"
)

const (
	fileMagic       = "LDB2"
	defaultPageSize = 4096
	metaPage0       = 0
	metaPage1       = 1
	pageLeaf        = 1
	pageBranch      = 2
	pageBucket      = 3
	nodeHeaderSize  = 13
)

type meta struct {
	txid     uint64
	root     uint64
	nextPage uint64
	freelist []uint64
}

func readMetaPage(page []byte, pageSize int) (meta, bool, error) {
	if len(page) < pageSize {
		return meta{}, false, errors.New("leafdb: invalid meta page")
	}
	valid, err := readMetaHeader(page, pageSize)
	if err != nil || !valid {
		return meta{}, false, err
	}
	m := meta{
		txid:     binary.LittleEndian.Uint64(page[8:]),
		root:     binary.LittleEndian.Uint64(page[16:]),
		nextPage: binary.LittleEndian.Uint64(page[24:]),
	}
	freeCount := int(binary.LittleEndian.Uint32(page[32:]))
	maxFree := (pageSize - 36) / 8
	if freeCount > maxFree {
		return meta{}, false, errors.New("leafdb: freelist exceeds meta capacity")
	}
	m.freelist = make([]uint64, freeCount)
	off := 36
	for i := 0; i < freeCount; i++ {
		m.freelist[i] = binary.LittleEndian.Uint64(page[off:])
		off += 8
	}
	return m, true, nil
}

func writeMetaPage(page []byte, m meta, pageSize int) error {
	if len(page) < pageSize {
		return errors.New("leafdb: invalid meta page")
	}
	copy(page[:4], []byte(fileMagic))
	binary.LittleEndian.PutUint32(page[4:], uint32(pageSize))
	binary.LittleEndian.PutUint64(page[8:], m.txid)
	binary.LittleEndian.PutUint64(page[16:], m.root)
	binary.LittleEndian.PutUint64(page[24:], m.nextPage)
	binary.LittleEndian.PutUint32(page[32:], uint32(len(m.freelist)))
	off := 36
	for _, id := range m.freelist {
		if off+8 > pageSize {
			return errors.New("leafdb: freelist too large for meta page")
		}
		binary.LittleEndian.PutUint64(page[off:], id)
		off += 8
	}
	return nil
}

func readMetaHeader(page []byte, pageSize int) (bool, error) {
	if string(page[:4]) != fileMagic {
		return false, nil
	}
	ps := int(binary.LittleEndian.Uint32(page[4:]))
	if ps != pageSize {
		return false, errors.New("leafdb: page size mismatch")
	}
	return true, nil
}
