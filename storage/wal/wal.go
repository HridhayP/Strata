// Package wal implements an append-only, CRC-framed write-ahead log with
// group commit.
//
// Callers Append records (buffered, cheap) and then call Sync with the
// sequence number Append returned. Concurrent Sync callers share fsyncs:
// while one fsync is in flight, later appends accumulate and the next
// fsync covers all of them. With Options.NoBatch every Append is flushed
// and fsynced on its own, which is the baseline for the batching benchmark.
//
// On-disk record framing (little endian):
//
//	[4: payload length][4: CRC-32C of type+payload][1: type][payload]
//
// Open replays all valid records and truncates a torn or corrupt tail, so a
// crash in the middle of a write loses only records that were never synced.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

const headerSize = 9

// MaxRecordSize bounds a single record so a corrupt length field cannot
// trigger a huge allocation during recovery.
const MaxRecordSize = 64 << 20

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrClosed is returned by operations on a closed WAL.
var ErrClosed = errors.New("wal: closed")

// Record is one entry of the log.
type Record struct {
	Type byte
	Data []byte
}

// Options configures a WAL.
type Options struct {
	// NoBatch disables group commit: every Append writes and fsyncs
	// immediately while holding the log lock.
	NoBatch bool
}

// Stats are cumulative counters, useful for benchmarks and metrics.
type Stats struct {
	Records uint64
	Fsyncs  uint64
	Bytes   uint64
}

// WAL is safe for concurrent use.
type WAL struct {
	path string
	opts Options

	mu      sync.Mutex
	cond    *sync.Cond
	f       *os.File
	buf     *bufio.Writer
	written uint64 // sequence number of the last appended record
	synced  uint64 // all records with seq <= synced are durable
	syncing bool
	closed  bool
	size    int64 // bytes appended (including buffered)
	durable int64 // bytes known to be on stable storage
	stats   Stats
	scratch [headerSize]byte
}

// Open opens (or creates) the log at path and returns the records it holds.
// A torn or corrupt tail is truncated away.
func Open(path string, opts Options) (*WAL, []Record, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	recs, valid, err := readAll(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if err := f.Truncate(valid); err != nil {
		f.Close()
		return nil, nil, err
	}
	if _, err := f.Seek(valid, io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, nil, err
	}
	w := &WAL{
		path:    path,
		opts:    opts,
		f:       f,
		buf:     bufio.NewWriterSize(f, 256<<10),
		size:    valid,
		durable: valid,
	}
	w.cond = sync.NewCond(&w.mu)
	return w, recs, nil
}

// readAll scans records from the start of f and returns them along with the
// byte offset just past the last valid record.
func readAll(f *os.File) ([]Record, int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}
	r := bufio.NewReaderSize(f, 256<<10)
	var recs []Record
	var off int64
	var hdr [headerSize]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return recs, off, nil // clean EOF or torn header
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		sum := binary.LittleEndian.Uint32(hdr[4:8])
		if n > MaxRecordSize {
			return recs, off, nil
		}
		data := make([]byte, n)
		if _, err := io.ReadFull(r, data); err != nil {
			return recs, off, nil // torn payload
		}
		c := crc32.Update(0, crcTable, hdr[8:9])
		c = crc32.Update(c, crcTable, data)
		if c != sum {
			return recs, off, nil // corrupt record: treat as end of log
		}
		recs = append(recs, Record{Type: hdr[8], Data: data})
		off += int64(headerSize) + int64(n)
	}
}

func (w *WAL) writeLocked(typ byte, data []byte) error {
	if len(data) > MaxRecordSize {
		return fmt.Errorf("wal: record of %d bytes exceeds limit", len(data))
	}
	binary.LittleEndian.PutUint32(w.scratch[0:4], uint32(len(data)))
	w.scratch[8] = typ
	c := crc32.Update(0, crcTable, w.scratch[8:9])
	c = crc32.Update(c, crcTable, data)
	binary.LittleEndian.PutUint32(w.scratch[4:8], c)
	if _, err := w.buf.Write(w.scratch[:]); err != nil {
		return err
	}
	if _, err := w.buf.Write(data); err != nil {
		return err
	}
	w.size += int64(headerSize + len(data))
	w.written++
	w.stats.Records++
	w.stats.Bytes += uint64(headerSize + len(data))
	return nil
}

// Append adds a record to the log and returns its sequence number. The
// record is not durable until Sync(seq) returns (or, with NoBatch, until
// Append returns).
func (w *WAL) Append(typ byte, data []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	if err := w.writeLocked(typ, data); err != nil {
		return 0, err
	}
	seq := w.written
	if w.opts.NoBatch {
		for w.syncing {
			w.cond.Wait()
		}
		if err := w.flushAndSyncLocked(); err != nil {
			return 0, err
		}
	}
	return seq, nil
}

// AppendBatch appends several records and returns the sequence number of the
// last one. With NoBatch each record is still fsynced individually.
func (w *WAL) AppendBatch(recs []Record) (uint64, error) {
	if w.opts.NoBatch {
		var seq uint64
		for _, r := range recs {
			s, err := w.Append(r.Type, r.Data)
			if err != nil {
				return 0, err
			}
			seq = s
		}
		return seq, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	for _, r := range recs {
		if err := w.writeLocked(r.Type, r.Data); err != nil {
			return 0, err
		}
	}
	return w.written, nil
}

// flushAndSyncLocked flushes and fsyncs while holding the lock (NoBatch path).
func (w *WAL) flushAndSyncLocked() error {
	if err := w.buf.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.stats.Fsyncs++
	w.synced = w.written
	w.durable = w.size
	return nil
}

// Sync blocks until every record with sequence number <= seq is durable.
// Concurrent callers are coalesced into as few fsyncs as possible.
func (w *WAL) Sync(seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.synced < seq {
		if w.closed {
			return ErrClosed
		}
		if w.syncing {
			w.cond.Wait()
			continue
		}
		// Become the sync leader for everything written so far.
		w.syncing = true
		target, targetSize := w.written, w.size
		err := w.buf.Flush()
		f := w.f
		w.mu.Unlock()
		if err == nil {
			err = f.Sync()
		}
		w.mu.Lock()
		w.syncing = false
		if err == nil {
			w.stats.Fsyncs++
			if target > w.synced {
				w.synced = target
				w.durable = targetSize
			}
		}
		w.cond.Broadcast()
		if err != nil {
			return err
		}
	}
	return nil
}

// SyncAll makes every appended record durable.
func (w *WAL) SyncAll() error {
	w.mu.Lock()
	seq := w.written
	w.mu.Unlock()
	return w.Sync(seq)
}

// Rewrite atomically replaces the whole log with recs (used for compaction).
// After it returns, recs are durable and earlier records are gone.
func (w *WAL) Rewrite(recs []Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.syncing {
		w.cond.Wait()
	}
	if w.closed {
		return ErrClosed
	}
	tmp := w.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	old := w.f
	w.f = f
	w.buf = bufio.NewWriterSize(f, 256<<10)
	w.size = 0
	for _, r := range recs {
		if err = w.writeLocked(r.Type, r.Data); err != nil {
			break
		}
	}
	if err == nil {
		err = w.buf.Flush()
	}
	if err == nil {
		err = f.Sync()
	}
	if err == nil {
		// Windows cannot rename a file that is open, so close both handles,
		// swap the files, and reopen the new log for appending.
		old.Close()
		f.Close()
		err = os.Rename(tmp, w.path)
		if err == nil {
			syncDir(filepath.Dir(w.path))
			f, err = os.OpenFile(w.path, os.O_RDWR, 0o644)
		}
		if err == nil {
			_, err = f.Seek(0, io.SeekEnd)
		}
		if err == nil {
			w.f = f
			w.buf = bufio.NewWriterSize(f, 256<<10)
		}
	}
	if err != nil {
		// The log is in an unknown state; refuse further use.
		w.closed = true
		if f != nil {
			f.Close()
		}
		w.cond.Broadcast()
		return fmt.Errorf("wal: rewrite: %w", err)
	}
	w.stats.Fsyncs++
	w.synced = w.written
	w.durable = w.size
	w.cond.Broadcast()
	return nil
}

// Size returns the number of bytes in the log, including buffered bytes.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// DurableSize returns the number of bytes known to be fsynced.
func (w *WAL) DurableSize() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.durable
}

// Stats returns cumulative counters.
func (w *WAL) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

// Close flushes, fsyncs and closes the log.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.syncing {
		w.cond.Wait()
	}
	if w.closed {
		return nil
	}
	w.closed = true
	w.cond.Broadcast()
	err := w.buf.Flush()
	if err == nil {
		err = w.f.Sync()
	}
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// CrashClose simulates a process crash: buffered (unflushed) bytes are
// dropped and the file is closed without fsync. Tests use it together with
// DurableSize to model power loss.
func (w *WAL) CrashClose() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.syncing {
		w.cond.Wait()
	}
	if w.closed {
		return
	}
	w.closed = true
	w.cond.Broadcast()
	w.f.Close()
}

func syncDir(dir string) {
	if runtime.GOOS == "windows" {
		return // directories cannot be fsynced on Windows
	}
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
