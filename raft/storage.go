package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/HridhayP/strata/proto/raftpb"
	"github.com/HridhayP/strata/storage/wal"
	"google.golang.org/protobuf/proto"
)

// Storage persists Raft's hard state, log and optional snapshot.
//
// Writes are buffered and return a sequence number; they become durable when
// Sync(seq) returns. Appending an entry whose index is <= the last stored
// index implicitly truncates the log from that index.
type Storage interface {
	Load() (*State, error)
	SetHardState(hs *raftpb.HardState) (uint64, error)
	Append(entries []*raftpb.Entry) (uint64, error)
	Sync(seq uint64) error
	// Compact durably replaces the stored state: everything up to c.Index is
	// discarded, snapshot (may be nil) is the state machine image at c.Index
	// and remaining are the log entries after it.
	Compact(c *raftpb.Compaction, hs *raftpb.HardState, snapshot []byte, remaining []*raftpb.Entry) error
	Close() error
}

// State is what Load returns.
type State struct {
	HardState  *raftpb.HardState
	Compaction *raftpb.Compaction // log prefix discarded up to here
	Snapshot   []byte             // nil when the state machine owns durability
	Entries    []*raftpb.Entry    // entries after Compaction.Index
}

func emptyState() *State {
	return &State{
		HardState:  &raftpb.HardState{VotedFor: -1},
		Compaction: &raftpb.Compaction{},
	}
}

// appendEntries applies entries to an in-memory log with Raft's implicit
// truncation rule.
func appendEntries(base uint64, log []*raftpb.Entry, ents []*raftpb.Entry) ([]*raftpb.Entry, error) {
	for _, e := range ents {
		if e.Index <= base {
			continue // already compacted away
		}
		pos := int(e.Index - base - 1)
		if pos > len(log) {
			return nil, fmt.Errorf("raft storage: gap before index %d (have %d)", e.Index, base+uint64(len(log)))
		}
		log = append(log[:pos], e)
	}
	return log, nil
}

// MemoryStorage keeps everything in memory. Every write counts as durable, so
// it models a node whose disk never loses synced data; tests reuse the same
// MemoryStorage across a simulated crash and restart.
type MemoryStorage struct {
	mu  sync.Mutex
	st  *State
	seq uint64
}

// NewMemoryStorage returns empty storage.
func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{st: emptyState()} }

func (m *MemoryStorage) Load() (*State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := &State{
		HardState:  proto.Clone(m.st.HardState).(*raftpb.HardState),
		Compaction: proto.Clone(m.st.Compaction).(*raftpb.Compaction),
		Snapshot:   m.st.Snapshot,
		Entries:    append([]*raftpb.Entry(nil), m.st.Entries...),
	}
	return cp, nil
}

func (m *MemoryStorage) SetHardState(hs *raftpb.HardState) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.st.HardState = proto.Clone(hs).(*raftpb.HardState)
	m.seq++
	return m.seq, nil
}

func (m *MemoryStorage) Append(ents []*raftpb.Entry) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	log, err := appendEntries(m.st.Compaction.Index, m.st.Entries, ents)
	if err != nil {
		return 0, err
	}
	m.st.Entries = log
	m.seq++
	return m.seq, nil
}

func (m *MemoryStorage) Sync(uint64) error { return nil }

func (m *MemoryStorage) Compact(c *raftpb.Compaction, hs *raftpb.HardState, snap []byte, remaining []*raftpb.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.st = &State{
		HardState:  proto.Clone(hs).(*raftpb.HardState),
		Compaction: proto.Clone(c).(*raftpb.Compaction),
		Snapshot:   snap,
		Entries:    append([]*raftpb.Entry(nil), remaining...),
	}
	return nil
}

func (m *MemoryStorage) Close() error { return nil }

// WAL record types used by DiskStorage.
const (
	recHardState  byte = 1
	recEntry      byte = 2
	recCompaction byte = 3
)

// DiskStorage persists Raft state in a directory:
//
//	raft.wal   hard-state, entry and compaction records (see storage/wal)
//	snapshot   [8: index][8: term][data], written atomically via rename
type DiskStorage struct {
	dir     string
	w       *wal.WAL
	initial *State
}

// OpenDiskStorage opens or creates storage in dir and replays its contents.
func OpenDiskStorage(dir string, opts wal.Options) (*DiskStorage, error) {
	w, recs, err := wal.Open(filepath.Join(dir, "raft.wal"), opts)
	if err != nil {
		return nil, err
	}
	st := emptyState()
	for _, r := range recs {
		switch r.Type {
		case recHardState:
			hs := &raftpb.HardState{}
			if err := proto.Unmarshal(r.Data, hs); err != nil {
				w.Close()
				return nil, fmt.Errorf("raft storage: bad hard state: %w", err)
			}
			st.HardState = hs
		case recEntry:
			e := &raftpb.Entry{}
			if err := proto.Unmarshal(r.Data, e); err != nil {
				w.Close()
				return nil, fmt.Errorf("raft storage: bad entry: %w", err)
			}
			st.Entries, err = appendEntries(st.Compaction.Index, st.Entries, []*raftpb.Entry{e})
			if err != nil {
				w.Close()
				return nil, err
			}
		case recCompaction:
			c := &raftpb.Compaction{}
			if err := proto.Unmarshal(r.Data, c); err != nil {
				w.Close()
				return nil, fmt.Errorf("raft storage: bad compaction: %w", err)
			}
			st.Entries = dropThrough(st.Compaction.Index, st.Entries, c.Index)
			st.Compaction = c
		default:
			w.Close()
			return nil, fmt.Errorf("raft storage: unknown record type %d", r.Type)
		}
	}
	snap, idx, term, err := readSnapshotFile(filepath.Join(dir, "snapshot"))
	if err != nil {
		w.Close()
		return nil, err
	}
	// The snapshot file is only meaningful if it matches the compaction point;
	// a stale file can be left behind by a crash between the two writes.
	if snap != nil && idx == st.Compaction.Index && term == st.Compaction.Term {
		st.Snapshot = snap
	}
	return &DiskStorage{dir: dir, w: w, initial: st}, nil
}

func dropThrough(base uint64, log []*raftpb.Entry, index uint64) []*raftpb.Entry {
	if index <= base {
		return log
	}
	n := int(index - base)
	if n >= len(log) {
		return nil
	}
	return append([]*raftpb.Entry(nil), log[n:]...)
}

// Load returns the state replayed when the storage was opened.
func (d *DiskStorage) Load() (*State, error) { return d.initial, nil }

func (d *DiskStorage) SetHardState(hs *raftpb.HardState) (uint64, error) {
	b, err := proto.Marshal(hs)
	if err != nil {
		return 0, err
	}
	return d.w.Append(recHardState, b)
}

func (d *DiskStorage) Append(ents []*raftpb.Entry) (uint64, error) {
	recs := make([]wal.Record, len(ents))
	for i, e := range ents {
		b, err := proto.Marshal(e)
		if err != nil {
			return 0, err
		}
		recs[i] = wal.Record{Type: recEntry, Data: b}
	}
	return d.w.AppendBatch(recs)
}

func (d *DiskStorage) Sync(seq uint64) error { return d.w.Sync(seq) }

func (d *DiskStorage) Compact(c *raftpb.Compaction, hs *raftpb.HardState, snap []byte, remaining []*raftpb.Entry) error {
	path := filepath.Join(d.dir, "snapshot")
	if snap != nil {
		if err := writeSnapshotFile(path, c.Index, c.Term, snap); err != nil {
			return err
		}
	}
	recs := make([]wal.Record, 0, len(remaining)+2)
	for _, m := range []proto.Message{c, hs} {
		b, err := proto.Marshal(m)
		if err != nil {
			return err
		}
		typ := recCompaction
		if _, ok := m.(*raftpb.HardState); ok {
			typ = recHardState
		}
		recs = append(recs, wal.Record{Type: typ, Data: b})
	}
	for _, e := range remaining {
		b, err := proto.Marshal(e)
		if err != nil {
			return err
		}
		recs = append(recs, wal.Record{Type: recEntry, Data: b})
	}
	if err := d.w.Rewrite(recs); err != nil {
		return err
	}
	if snap == nil {
		// The state machine now owns durability up to c.Index.
		os.Remove(path)
	}
	return nil
}

func (d *DiskStorage) Close() error { return d.w.Close() }

// CrashClose drops unsynced data without flushing (tests only).
func (d *DiskStorage) CrashClose() { d.w.CrashClose() }

// WAL exposes the underlying log for metrics.
func (d *DiskStorage) WAL() *wal.WAL { return d.w }

func writeSnapshotFile(path string, index, term uint64, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	var hdr [16]byte
	binary.LittleEndian.PutUint64(hdr[0:8], index)
	binary.LittleEndian.PutUint64(hdr[8:16], term)
	_, err = f.Write(hdr[:])
	if err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func readSnapshotFile(path string) ([]byte, uint64, uint64, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, 0, nil
	}
	if err != nil {
		return nil, 0, 0, err
	}
	if len(b) < 16 {
		return nil, 0, 0, nil
	}
	return b[16:], binary.LittleEndian.Uint64(b[0:8]), binary.LittleEndian.Uint64(b[8:16]), nil
}
