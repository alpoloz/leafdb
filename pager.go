package leafdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	fileMagic   = "LDBF"
	defaultPage = 4096
	metaPageID  = 0
)

type pager struct {
	file     *os.File
	pageSize int
	nextPage uint64
	freeList []uint64
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
