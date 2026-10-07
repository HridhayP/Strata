// Package lsm is a small log-structured merge-tree key-value engine.
//
// Writes go to an in-memory skiplist (the memtable), optionally preceded by a
// write-ahead log record. When the memtable passes a size threshold it is
// written out as an immutable, sorted SSTable file. Reads check the memtable
// and then SSTables from newest to oldest, using a per-table Bloom filter to
// skip files that cannot contain the key. When too many tables accumulate a
// background compaction merges them into one, dropping overwritten values and
// tombstones.
//
// The set of live tables is recorded in a MANIFEST file that is replaced
// atomically (write temp file, fsync, rename), so a crash at any point leaves
// either the old or the new set of tables, never a mix.
package lsm

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/HridhayP/strata/storage/wal"
)

// ErrClosed is returned after Close.
var ErrClosed = errors.New("lsm: closed")

// Options configures a DB.
type Options struct {
	// MemtableSize is the approximate memtable size that triggers a flush.
	MemtableSize int
	// CompactionTrigger is the table count that triggers a compaction.
	CompactionTrigger int
	// WAL enables a write-ahead log so writes survive a crash before flush.
	// Disable it when an outer log (such as Raft's) already provides that.
	WAL        bool
	WALOptions wal.Options
}

func (o *Options) defaults() {
	if o.MemtableSize == 0 {
		o.MemtableSize = 4 << 20
	}
	if o.CompactionTrigger == 0 {
		o.CompactionTrigger = 4
	}
}

// Stats are cumulative counters.
type Stats struct {
	Tables      int
	Flushes     int
	Compactions int
	TableBytes  int64
}

type manifest struct {
	NextFile uint64   `json:"next_file"`
	Tables   []uint64 `json:"tables"` // newest first
}

// DB is safe for concurrent use.
type DB struct {
	dir  string
	opts Options

	mu          sync.RWMutex
	mem         *memtable
	tables      []*table // newest first
	nextFile    uint64
	log         *wal.WAL
	closed      bool
	compacting  bool
	compactDone *sync.Cond
	stats       Stats
	bgErr       error
}

// Open opens or creates a database in dir.
func Open(dir string, opts Options) (*DB, error) {
	opts.defaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{dir: dir, opts: opts, mem: newMemtable(), nextFile: 1}
	db.compactDone = sync.NewCond(&db.mu)

	m, err := readManifest(filepath.Join(dir, "MANIFEST"))
	if err != nil {
		return nil, err
	}
	live := map[uint64]bool{}
	for _, num := range m.Tables {
		t, err := openTable(db.tablePath(num), num)
		if err != nil {
			db.closeTables()
			return nil, err
		}
		db.tables = append(db.tables, t)
		live[num] = true
	}
	db.nextFile = max(m.NextFile, 1)
	removeOrphans(dir, live)

	if opts.WAL {
		w, recs, err := wal.Open(filepath.Join(dir, "lsm.wal"), opts.WALOptions)
		if err != nil {
			db.closeTables()
			return nil, err
		}
		db.log = w
		for _, r := range recs {
			b := &Batch{data: r.Data}
			if err := b.each(func(e entry) { db.mem.put(e.key, e.value, e.kind) }); err != nil {
				db.closeTables()
				w.Close()
				return nil, fmt.Errorf("lsm: replaying wal: %w", err)
			}
		}
	}
	return db, nil
}

func (db *DB) tablePath(num uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("%06d.sst", num))
}

func readManifest(path string) (manifest, error) {
	var m manifest
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return m, nil
	}
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("lsm: bad manifest: %w", err)
	}
	return m, nil
}

// removeOrphans deletes table files a crash left behind before they were
// recorded in (or after they were dropped from) the manifest.
func removeOrphans(dir string, live map[uint64]bool) {
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		name := e.Name()
		var num uint64
		if strings.HasSuffix(name, ".sst") {
			if _, err := fmt.Sscanf(name, "%06d.sst", &num); err == nil && !live[num] {
				os.Remove(filepath.Join(dir, name))
			}
		}
		if strings.HasSuffix(name, ".tmp") {
			os.Remove(filepath.Join(dir, name))
		}
	}
}

// writeManifest atomically records the current table set. db.mu held.
func (db *DB) writeManifest() error {
	m := manifest{NextFile: db.nextFile}
	for _, t := range db.tables {
		m.Tables = append(m.Tables, t.num)
	}
	b, _ := json.Marshal(m)
	path := filepath.Join(db.dir, "MANIFEST")
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err == nil && runtime.GOOS != "windows" {
		if d, derr := os.Open(db.dir); derr == nil {
			d.Sync()
			d.Close()
		}
	}
	return err
}

// Batch is an atomic group of writes.
type Batch struct {
	data []byte
	n    int
}

// Put adds a write of key=value to the batch.
func (b *Batch) Put(key, value []byte) { b.add(kindPut, key, value) }

// Delete adds a deletion of key to the batch.
func (b *Batch) Delete(key []byte) { b.add(kindDelete, key, nil) }

// Len is the number of operations in the batch.
func (b *Batch) Len() int { return b.n }

func (b *Batch) add(kind byte, key, value []byte) {
	var hdr [1 + 2*binary.MaxVarintLen64]byte
	hdr[0] = kind
	n := 1
	n += binary.PutUvarint(hdr[n:], uint64(len(key)))
	n += binary.PutUvarint(hdr[n:], uint64(len(value)))
	b.data = append(b.data, hdr[:n]...)
	b.data = append(b.data, key...)
	b.data = append(b.data, value...)
	b.n++
}

func (b *Batch) each(fn func(entry)) error {
	rest := b.data
	for len(rest) > 0 {
		e, r, err := decodeEntry(rest)
		if err != nil {
			return err
		}
		fn(e)
		rest = r
	}
	return nil
}

// Apply writes the batch atomically. With the WAL enabled it returns once the
// batch is durable; concurrent callers share fsyncs.
func (db *DB) Apply(b *Batch) error {
	if b.n == 0 {
		return nil
	}
	// The memtable keeps references into the batch, so it must own the bytes.
	data := append([]byte(nil), b.data...)
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	var seq uint64
	if db.log != nil {
		var err error
		if seq, err = db.log.Append(1, data); err != nil {
			db.mu.Unlock()
			return err
		}
	}
	(&Batch{data: data}).each(func(e entry) { db.mem.put(e.key, e.value, e.kind) })
	var err error
	if db.mem.size >= db.opts.MemtableSize {
		err = db.flushLocked()
	}
	log := db.log
	db.mu.Unlock()
	if err != nil {
		return err
	}
	if log != nil {
		return log.Sync(seq)
	}
	return nil
}

// Put writes a single key.
func (db *DB) Put(key, value []byte) error {
	var b Batch
	b.Put(key, value)
	return db.Apply(&b)
}

// Delete removes a single key.
func (db *DB) Delete(key []byte) error {
	var b Batch
	b.Delete(key)
	return db.Apply(&b)
}

// Get returns the value for key and whether it exists.
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, false, ErrClosed
	}
	if v, kind, ok := db.mem.get(key); ok {
		db.mu.RUnlock()
		if kind == kindDelete {
			return nil, false, nil
		}
		return append([]byte(nil), v...), true, nil
	}
	tables := db.refTables()
	db.mu.RUnlock()
	defer unrefAll(tables)

	for _, t := range tables {
		e, ok, err := t.get(key)
		if err != nil {
			return nil, false, err
		}
		if ok {
			if e.kind == kindDelete {
				return nil, false, nil
			}
			return append([]byte(nil), e.value...), true, nil
		}
	}
	return nil, false, nil
}

func (db *DB) refTables() []*table {
	ts := append([]*table(nil), db.tables...)
	for _, t := range ts {
		t.ref()
	}
	return ts
}

func unrefAll(ts []*table) {
	for _, t := range ts {
		t.unref()
	}
}

// Scan calls fn for every live key with the given prefix, in key order,
// until fn returns false. It sees a consistent snapshot of the database.
func (db *DB) Scan(prefix []byte, fn func(key, value []byte) bool) error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	memEnts := db.mem.entries(prefix)
	tables := db.refTables()
	db.mu.RUnlock()
	defer unrefAll(tables)

	sources := []source{&sliceIter{ents: memEnts}}
	for _, t := range tables {
		sources = append(sources, t.seek(prefix))
	}
	return mergeSources(sources, func(e entry) bool {
		if !bytes.HasPrefix(e.key, prefix) {
			return false
		}
		if e.kind == kindDelete {
			return true
		}
		return fn(e.key, e.value)
	})
}

// Flush writes the memtable to a new SSTable.
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.flushLocked()
}

func (db *DB) flushLocked() error {
	if db.mem.count == 0 {
		return nil
	}
	num := db.nextFile
	db.nextFile++
	w, err := newTableWriter(db.tablePath(num))
	if err != nil {
		return err
	}
	for _, e := range db.mem.entries(nil) {
		if err := w.add(e); err != nil {
			w.abort()
			return err
		}
	}
	if err := w.finish(); err != nil {
		w.abort()
		return err
	}
	t, err := openTable(db.tablePath(num), num)
	if err != nil {
		return err
	}
	db.tables = append([]*table{t}, db.tables...)
	if err := db.writeManifest(); err != nil {
		return err
	}
	if db.log != nil {
		// Everything in the log is now in an SSTable.
		if err := db.log.Rewrite(nil); err != nil {
			return err
		}
	}
	db.mem = newMemtable()
	db.stats.Flushes++
	if len(db.tables) >= db.opts.CompactionTrigger && !db.compacting {
		db.compacting = true
		go db.compact()
	}
	return nil
}

// Compact synchronously merges all tables into one.
func (db *DB) Compact() error {
	db.mu.Lock()
	for db.compacting {
		db.compactDone.Wait()
	}
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	db.compacting = true
	db.mu.Unlock()
	return db.compactOnce()
}

func (db *DB) compact() {
	if err := db.compactOnce(); err != nil {
		db.mu.Lock()
		db.bgErr = err
		db.mu.Unlock()
	}
}

// compactOnce merges the current table set (which always includes the
// oldest table, so tombstones can be dropped). Caller set db.compacting.
func (db *DB) compactOnce() error {
	db.mu.Lock()
	in := db.refTables()
	num := db.nextFile
	db.nextFile++
	db.mu.Unlock()

	finish := func(err error) error {
		unrefAll(in)
		db.mu.Lock()
		db.compacting = false
		db.compactDone.Broadcast()
		db.mu.Unlock()
		return err
	}
	if len(in) < 2 {
		return finish(nil)
	}

	path := db.tablePath(num)
	w, err := newTableWriter(path)
	if err != nil {
		return finish(err)
	}
	sources := make([]source, len(in))
	for i, t := range in {
		sources[i] = t.seek(nil)
	}
	written := 0
	err = mergeSources(sources, func(e entry) bool {
		if e.kind == kindDelete {
			return true
		}
		if err := w.add(e); err != nil {
			return false
		}
		written++
		return true
	})
	if err == nil {
		err = w.finish()
	}
	if err != nil {
		w.abort()
		return finish(err)
	}
	var merged *table
	if written > 0 {
		if merged, err = openTable(path, num); err != nil {
			return finish(err)
		}
	} else {
		os.Remove(path)
	}

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		if merged != nil {
			merged.unref()
		}
		return finish(ErrClosed)
	}
	// Tables flushed during the merge sit in front of the inputs.
	keep := db.tables[:len(db.tables)-len(in)]
	next := append([]*table(nil), keep...)
	if merged != nil {
		next = append(next, merged)
	}
	old := db.tables
	db.tables = next
	if err := db.writeManifest(); err != nil {
		db.tables = old
		db.mu.Unlock()
		if merged != nil {
			merged.obsolete.Store(true)
			merged.unref()
		}
		return finish(err)
	}
	for _, t := range in {
		t.obsolete.Store(true)
		t.unref() // the DB's reference
	}
	db.stats.Compactions++
	db.mu.Unlock()
	return finish(nil)
}

// Reset deletes all data (used when installing a replacement snapshot).
func (db *DB) Reset() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for db.compacting {
		db.compactDone.Wait()
	}
	if db.closed {
		return ErrClosed
	}
	old := db.tables
	db.tables = nil
	if err := db.writeManifest(); err != nil {
		db.tables = old
		return err
	}
	for _, t := range old {
		t.obsolete.Store(true)
		t.unref()
	}
	db.mem = newMemtable()
	if db.log != nil {
		return db.log.Rewrite(nil)
	}
	return nil
}

// Stats returns counters and the current table count.
func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	s := db.stats
	s.Tables = len(db.tables)
	for _, t := range db.tables {
		s.TableBytes += t.size
	}
	return s
}

// Close flushes the memtable and closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	for db.compacting {
		db.compactDone.Wait()
	}
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	err := db.flushLocked()
	db.closed = true
	db.closeTables()
	if db.log != nil {
		if cerr := db.log.Close(); err == nil {
			err = cerr
		}
	}
	db.mu.Unlock()
	return err
}

// CrashClose simulates a crash: the memtable is lost and unsynced WAL data
// is dropped. Only for tests.
func (db *DB) CrashClose() {
	db.mu.Lock()
	for db.compacting {
		db.compactDone.Wait()
	}
	defer db.mu.Unlock()
	if db.closed {
		return
	}
	db.closed = true
	db.closeTables()
	if db.log != nil {
		db.log.CrashClose()
	}
}

func (db *DB) closeTables() {
	for _, t := range db.tables {
		t.unref()
	}
	db.tables = nil
}

// ---- k-way merge ----

type source interface {
	ok() bool
	entry() entry
	advance()
	error() error
}

type sliceIter struct {
	ents []entry
	i    int
}

func (s *sliceIter) ok() bool     { return s.i < len(s.ents) }
func (s *sliceIter) entry() entry { return s.ents[s.i] }
func (s *sliceIter) advance()     { s.i++ }
func (s *sliceIter) error() error { return nil }

func (it *tableIter) ok() bool     { return it.valid }
func (it *tableIter) entry() entry { return it.cur }
func (it *tableIter) advance()     { it.next() }
func (it *tableIter) error() error { return it.err }

// mergeHeap orders sources by current key; for equal keys the newer source
// (lower index) comes first.
type mergeHeap struct {
	srcs []source
	rank []int
}

func (h *mergeHeap) Len() int { return len(h.srcs) }
func (h *mergeHeap) Less(i, j int) bool {
	c := bytes.Compare(h.srcs[i].entry().key, h.srcs[j].entry().key)
	return c < 0 || (c == 0 && h.rank[i] < h.rank[j])
}
func (h *mergeHeap) Swap(i, j int) {
	h.srcs[i], h.srcs[j] = h.srcs[j], h.srcs[i]
	h.rank[i], h.rank[j] = h.rank[j], h.rank[i]
}
func (h *mergeHeap) Push(x any) { panic("unused") }
func (h *mergeHeap) Pop() any {
	n := len(h.srcs) - 1
	h.srcs, h.rank = h.srcs[:n], h.rank[:n]
	return nil
}

// mergeSources emits the newest version of each key in order (tombstones
// included) until fn returns false. sources are ordered newest first.
func mergeSources(sources []source, fn func(entry) bool) error {
	h := &mergeHeap{}
	for i, s := range sources {
		if err := s.error(); err != nil {
			return err
		}
		if s.ok() {
			h.srcs = append(h.srcs, s)
			h.rank = append(h.rank, i)
		}
	}
	heap.Init(h)
	var last []byte
	for h.Len() > 0 {
		s := h.srcs[0]
		e := s.entry()
		dup := last != nil && bytes.Equal(e.key, last)
		if !dup {
			last = append(last[:0], e.key...)
			if !fn(e) {
				return nil
			}
		}
		s.advance()
		if err := s.error(); err != nil {
			return err
		}
		if s.ok() {
			heap.Fix(h, 0)
		} else {
			heap.Pop(h)
		}
	}
	return nil
}
