package leafdb

import (
	"encoding/binary"
	"errors"
)

const (
	fileMagicV2        = "LDB2"
	fileMagicV3        = "LDB3"
	defaultPageSize    = 4096
	metaPage0          = 0
	metaPage1          = 1
	pageLeaf           = 1
	pageBranch         = 2
	pageBucket         = 3
	pageFreelist       = 4
	nodeHeaderSize     = 13
	freelistHeaderSize = 11
	metaHeaderSizeV2   = 36
	metaHeaderSizeV3   = 44
)

type meta struct {
	txid         uint64
	root         uint64
	nextPage     uint64
	freelistPage uint64
	freelist     []uint64
}

func readMetaPage(page []byte, pageSize int) (meta, bool, error) {
	if len(page) < pageSize {
		return meta{}, false, errors.New("leafdb: invalid meta page")
	}
	magic := string(page[:4])
	if magic != fileMagicV2 && magic != fileMagicV3 {
		return meta{}, false, nil
	}
	ps := int(binary.LittleEndian.Uint32(page[4:]))
	if ps != pageSize {
		return meta{}, false, errors.New("leafdb: page size mismatch")
	}
	m := meta{
		txid:     binary.LittleEndian.Uint64(page[8:]),
		root:     binary.LittleEndian.Uint64(page[16:]),
		nextPage: binary.LittleEndian.Uint64(page[24:]),
	}
	var freeCount int
	var freeOffset int
	switch magic {
	case fileMagicV2:
		freeCount = int(binary.LittleEndian.Uint32(page[32:]))
		freeOffset = metaHeaderSizeV2
	case fileMagicV3:
		m.freelistPage = binary.LittleEndian.Uint64(page[32:])
		freeCount = int(binary.LittleEndian.Uint32(page[40:]))
		freeOffset = metaHeaderSizeV3
	}
	maxFree := (pageSize - freeOffset) / 8
	if freeCount > maxFree {
		return meta{}, false, errors.New("leafdb: freelist exceeds meta capacity")
	}
	m.freelist = make([]uint64, freeCount)
	off := freeOffset
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
	copy(page[:4], []byte(fileMagicV3))
	binary.LittleEndian.PutUint32(page[4:], uint32(pageSize))
	binary.LittleEndian.PutUint64(page[8:], m.txid)
	binary.LittleEndian.PutUint64(page[16:], m.root)
	binary.LittleEndian.PutUint64(page[24:], m.nextPage)
	binary.LittleEndian.PutUint64(page[32:], m.freelistPage)
	binary.LittleEndian.PutUint32(page[40:], uint32(len(m.freelist)))
	off := metaHeaderSizeV3
	for _, id := range m.freelist {
		if off+8 > pageSize {
			return errors.New("leafdb: freelist too large for meta page")
		}
		binary.LittleEndian.PutUint64(page[off:], id)
		off += 8
	}
	return nil
}

func writeFreelistPage(page []byte, ids []uint64, next uint64, pageSize int) error {
	if len(page) < pageSize {
		return errors.New("leafdb: invalid freelist page")
	}
	maxIDs := freelistPageCapacity(pageSize)
	if len(ids) > maxIDs {
		return errors.New("leafdb: freelist page too small")
	}
	page[0] = pageFreelist
	binary.LittleEndian.PutUint16(page[1:], uint16(len(ids)))
	binary.LittleEndian.PutUint64(page[3:], next)
	off := freelistHeaderSize
	for _, id := range ids {
		if off+8 > pageSize {
			return errors.New("leafdb: freelist page too small")
		}
		binary.LittleEndian.PutUint64(page[off:], id)
		off += 8
	}
	return nil
}

func readFreelistPage(page []byte, pageSize int) (uint64, []uint64, error) {
	if len(page) < pageSize {
		return 0, nil, errors.New("leafdb: invalid freelist page")
	}
	if page[0] != pageFreelist {
		return 0, nil, errors.New("leafdb: invalid freelist page")
	}
	count := int(binary.LittleEndian.Uint16(page[1:]))
	next := binary.LittleEndian.Uint64(page[3:])
	maxIDs := freelistPageCapacity(pageSize)
	if count > maxIDs {
		return 0, nil, errors.New("leafdb: freelist page too large")
	}
	ids := make([]uint64, count)
	off := freelistHeaderSize
	for i := 0; i < count; i++ {
		ids[i] = binary.LittleEndian.Uint64(page[off:])
		off += 8
	}
	return next, ids, nil
}

func freelistPageCapacity(pageSize int) int {
	return (pageSize - freelistHeaderSize) / 8
}

func metaInlineFreeCapacity(pageSize int) int {
	return (pageSize - metaHeaderSizeV3) / 8
}
