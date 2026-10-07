package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"sync/atomic"
)

// SSTable layout:
//
//	data blocks   each: entries..., then CRC-32C of the block (4 bytes)
//	bloom filter
//	index         uvarint count, then per block: uvarint klen, last key,
//	              uint64 offset, uint32 length
//	footer        uint64 indexOff, uint64 indexLen, uint64 bloomOff,
//	              uint64 bloomLen, uint64 entries, uint64 magic
//
// An entry is: kind byte, uvarint klen, uvarint vlen, key, value.
const (
	blockSize  = 4 << 10
	footerSize = 48
	tableMagic = 0x5354524154414c53 // "STRATALS"
)

var (
	crcTable      = crc32.MakeTable(crc32.Castagnoli)
	errCorruption = errors.New("lsm: corrupt sstable")
)

type blockHandle struct {
	lastKey []byte
	offset  uint64
	length  uint32 // excluding the CRC
}

// tableWriter streams sorted entries into a new SSTable file.
type tableWriter struct {
	f       *os.File
	w       *bufio.Writer
	off     uint64
	block   bytes.Buffer
	lastKey []byte
	index   []blockHandle
	keys    [][]byte
	count   uint64
}

func newTableWriter(path string) (*tableWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &tableWriter{f: f, w: bufio.NewWriterSize(f, 256<<10)}, nil
}

func (t *tableWriter) add(e entry) error {
	if t.lastKey != nil && bytes.Compare(e.key, t.lastKey) <= 0 {
		return fmt.Errorf("lsm: keys out of order: %q after %q", e.key, t.lastKey)
	}
	var hdr [1 + 2*binary.MaxVarintLen64]byte
	hdr[0] = e.kind
	n := 1
	n += binary.PutUvarint(hdr[n:], uint64(len(e.key)))
	n += binary.PutUvarint(hdr[n:], uint64(len(e.value)))
	t.block.Write(hdr[:n])
	t.block.Write(e.key)
	t.block.Write(e.value)
	t.lastKey = append(t.lastKey[:0], e.key...)
	t.keys = append(t.keys, e.key)
	t.count++
	if t.block.Len() >= blockSize {
		return t.finishBlock()
	}
	return nil
}

func (t *tableWriter) finishBlock() error {
	if t.block.Len() == 0 {
		return nil
	}
	data := t.block.Bytes()
	var sum [4]byte
	binary.LittleEndian.PutUint32(sum[:], crc32.Checksum(data, crcTable))
	if _, err := t.w.Write(data); err != nil {
		return err
	}
	if _, err := t.w.Write(sum[:]); err != nil {
		return err
	}
	t.index = append(t.index, blockHandle{lastKey: append([]byte(nil), t.lastKey...), offset: t.off, length: uint32(len(data))})
	t.off += uint64(len(data) + 4)
	t.block.Reset()
	return nil
}

// finish writes the bloom filter, index and footer and fsyncs the file.
func (t *tableWriter) finish() error {
	if err := t.finishBlock(); err != nil {
		return err
	}
	bf := newBloom(len(t.keys))
	for _, k := range t.keys {
		bf.add(k)
	}
	bloomData := bf.encode()
	bloomOff := t.off
	if _, err := t.w.Write(bloomData); err != nil {
		return err
	}
	t.off += uint64(len(bloomData))

	var idx bytes.Buffer
	var tmp [binary.MaxVarintLen64]byte
	idx.Write(tmp[:binary.PutUvarint(tmp[:], uint64(len(t.index)))])
	for _, h := range t.index {
		idx.Write(tmp[:binary.PutUvarint(tmp[:], uint64(len(h.lastKey)))])
		idx.Write(h.lastKey)
		var b [12]byte
		binary.LittleEndian.PutUint64(b[0:8], h.offset)
		binary.LittleEndian.PutUint32(b[8:12], h.length)
		idx.Write(b[:])
	}
	indexOff := t.off
	if _, err := t.w.Write(idx.Bytes()); err != nil {
		return err
	}
	var footer [footerSize]byte
	binary.LittleEndian.PutUint64(footer[0:], indexOff)
	binary.LittleEndian.PutUint64(footer[8:], uint64(idx.Len()))
	binary.LittleEndian.PutUint64(footer[16:], bloomOff)
	binary.LittleEndian.PutUint64(footer[24:], uint64(len(bloomData)))
	binary.LittleEndian.PutUint64(footer[32:], t.count)
	binary.LittleEndian.PutUint64(footer[40:], tableMagic)
	if _, err := t.w.Write(footer[:]); err != nil {
		return err
	}
	if err := t.w.Flush(); err != nil {
		return err
	}
	if err := t.f.Sync(); err != nil {
		return err
	}
	return t.f.Close()
}

func (t *tableWriter) abort() {
	t.f.Close()
	os.Remove(t.f.Name())
}

// table is an open, immutable SSTable. Readers hold a reference while using
// it so compaction can delete the file only after the last reader is done.
type table struct {
	num     uint64
	path    string
	f       *os.File
	index   []blockHandle
	bloom   *bloom
	entries uint64
	size    int64

	refs     atomic.Int32
	obsolete atomic.Bool
}

func openTable(path string, num uint64) (*table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	t, err := loadTable(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	t.num, t.path = num, path
	t.refs.Store(1) // the DB's own reference
	return t, nil
}

func loadTable(f *os.File) (*table, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < footerSize {
		return nil, errCorruption
	}
	var footer [footerSize]byte
	if _, err := f.ReadAt(footer[:], st.Size()-footerSize); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint64(footer[40:]) != tableMagic {
		return nil, errCorruption
	}
	indexOff := binary.LittleEndian.Uint64(footer[0:])
	indexLen := binary.LittleEndian.Uint64(footer[8:])
	bloomOff := binary.LittleEndian.Uint64(footer[16:])
	bloomLen := binary.LittleEndian.Uint64(footer[24:])
	if indexOff+indexLen > uint64(st.Size()) || bloomOff+bloomLen > indexOff {
		return nil, errCorruption
	}
	bloomData := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomData, int64(bloomOff)); err != nil {
		return nil, err
	}
	idx := make([]byte, indexLen)
	if _, err := f.ReadAt(idx, int64(indexOff)); err != nil {
		return nil, err
	}
	n, k := binary.Uvarint(idx)
	if k <= 0 {
		return nil, errCorruption
	}
	idx = idx[k:]
	handles := make([]blockHandle, 0, n)
	for i := uint64(0); i < n; i++ {
		kl, k := binary.Uvarint(idx)
		if k <= 0 || uint64(len(idx)-k) < kl+12 {
			return nil, errCorruption
		}
		idx = idx[k:]
		h := blockHandle{lastKey: idx[:kl]}
		idx = idx[kl:]
		h.offset = binary.LittleEndian.Uint64(idx[0:8])
		h.length = binary.LittleEndian.Uint32(idx[8:12])
		idx = idx[12:]
		handles = append(handles, h)
	}
	return &table{
		f:       f,
		index:   handles,
		bloom:   decodeBloom(bloomData),
		entries: binary.LittleEndian.Uint64(footer[32:]),
		size:    st.Size(),
	}, nil
}

func (t *table) ref() { t.refs.Add(1) }

func (t *table) unref() {
	if t.refs.Add(-1) == 0 {
		t.f.Close()
		if t.obsolete.Load() {
			os.Remove(t.path)
		}
	}
}

func (t *table) readBlock(i int) ([]byte, error) {
	h := t.index[i]
	buf := make([]byte, h.length+4)
	if _, err := t.f.ReadAt(buf, int64(h.offset)); err != nil {
		return nil, err
	}
	data := buf[:h.length]
	if crc32.Checksum(data, crcTable) != binary.LittleEndian.Uint32(buf[h.length:]) {
		return nil, errCorruption
	}
	return data, nil
}

// decodeEntry parses one entry at the start of b and returns the rest.
func decodeEntry(b []byte) (entry, []byte, error) {
	if len(b) < 1 {
		return entry{}, nil, errCorruption
	}
	kind := b[0]
	b = b[1:]
	kl, n := binary.Uvarint(b)
	if n <= 0 {
		return entry{}, nil, errCorruption
	}
	b = b[n:]
	vl, n := binary.Uvarint(b)
	if n <= 0 || uint64(len(b)-n) < kl+vl {
		return entry{}, nil, errCorruption
	}
	b = b[n:]
	return entry{kind: kind, key: b[:kl], value: b[kl : kl+vl]}, b[kl+vl:], nil
}

// blockFor returns the first block that may contain keys >= key.
func (t *table) blockFor(key []byte) int {
	return sort.Search(len(t.index), func(i int) bool { return bytes.Compare(t.index[i].lastKey, key) >= 0 })
}

func (t *table) get(key []byte) (entry, bool, error) {
	if !t.bloom.mayContain(key) {
		return entry{}, false, nil
	}
	i := t.blockFor(key)
	if i == len(t.index) {
		return entry{}, false, nil
	}
	b, err := t.readBlock(i)
	if err != nil {
		return entry{}, false, err
	}
	for len(b) > 0 {
		var e entry
		if e, b, err = decodeEntry(b); err != nil {
			return entry{}, false, err
		}
		switch c := bytes.Compare(e.key, key); {
		case c == 0:
			return e, true, nil
		case c > 0:
			return entry{}, false, nil
		}
	}
	return entry{}, false, nil
}

// tableIter walks entries >= a start key in order.
type tableIter struct {
	t     *table
	block int
	rest  []byte
	cur   entry
	valid bool
	err   error
}

func (t *table) seek(start []byte) *tableIter {
	it := &tableIter{t: t, block: t.blockFor(start) - 1}
	it.nextBlock()
	for it.valid && bytes.Compare(it.cur.key, start) < 0 {
		it.next()
	}
	return it
}

func (it *tableIter) nextBlock() {
	it.block++
	if it.block >= len(it.t.index) {
		it.valid = false
		return
	}
	b, err := it.t.readBlock(it.block)
	if err != nil {
		it.err, it.valid = err, false
		return
	}
	it.rest = b
	it.next()
}

func (it *tableIter) next() {
	if len(it.rest) == 0 {
		it.nextBlock()
		return
	}
	e, rest, err := decodeEntry(it.rest)
	if err != nil {
		it.err, it.valid = err, false
		return
	}
	it.cur, it.rest, it.valid = e, rest, true
}
