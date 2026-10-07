// Package kv is a linearizable key-value service replicated with Raft and
// stored in the LSM engine.
//
// Writes (Put, Append) are proposed to Raft and applied in log order. Each
// carries (client ID, sequence number); the state machine records the last
// sequence applied per client, so a retried write is applied at most once.
// Reads use Raft's ReadIndex: the leader confirms it is still leader with a
// heartbeat round, waits until it has applied up to the commit index it saw,
// and reads locally, which keeps reads out of the log entirely.
//
// The LSM has no WAL of its own; the Raft log is the write-ahead log. Every
// applied batch also writes the applied Raft index into the LSM, so whatever
// state the LSM recovers after a crash names exactly where Raft must resume.
package kv

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/HridhayP/strata/proto/kvpb"
	"github.com/HridhayP/strata/raft"
	"github.com/HridhayP/strata/storage/lsm"
	"google.golang.org/protobuf/proto"
)

// Config configures a Server.
type Config struct {
	ID          int
	Peers       []int
	Group       string
	Dir         string // LSM directory
	RaftStorage raft.Storage
	Transport   raft.Transport

	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	// MaxRaftLog is the number of log entries that triggers compaction.
	MaxRaftLog   int
	MemtableSize int
	Observer     raft.Observer
}

type writeWaiter struct {
	term uint64
	ch   chan uint64 // receives the term of the entry applied at the index
}

// Server is one replica of a KV group.
type Server struct {
	cfg     Config
	rf      *raft.Raft
	db      *lsm.DB
	applyCh chan raft.ApplyMsg

	mu           sync.Mutex
	applied      uint64
	writeWaiters map[uint64]*writeWaiter
	readWaiters  map[uint64][]chan struct{}
	sinceCheck   int

	stopCh chan struct{}
	done   chan struct{}
}

// NewServer opens the LSM, starts Raft and the apply loop.
func NewServer(cfg Config) (*Server, error) {
	if cfg.MaxRaftLog == 0 {
		cfg.MaxRaftLog = 20000
	}
	db, err := lsm.Open(cfg.Dir, lsm.Options{MemtableSize: cfg.MemtableSize})
	if err != nil {
		return nil, err
	}
	v, _, err := db.Get(appliedKey)
	if err != nil {
		db.Close()
		return nil, err
	}
	s := &Server{
		cfg:          cfg,
		db:           db,
		applyCh:      make(chan raft.ApplyMsg, 1024),
		applied:      readU64(v),
		writeWaiters: make(map[uint64]*writeWaiter),
		readWaiters:  make(map[uint64][]chan struct{}),
		stopCh:       make(chan struct{}),
		done:         make(chan struct{}),
	}
	s.rf, err = raft.New(raft.Config{
		ID:                cfg.ID,
		Peers:             cfg.Peers,
		Group:             cfg.Group,
		Transport:         cfg.Transport,
		Storage:           cfg.RaftStorage,
		ApplyCh:           s.applyCh,
		ElectionTimeout:   cfg.ElectionTimeout,
		HeartbeatInterval: cfg.HeartbeatInterval,
		AppliedIndex:      s.applied,
		SnapshotFn:        s.snapshot,
		Observer:          cfg.Observer,
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	go s.applyLoop()
	return s, nil
}

// Raft exposes the replica's Raft instance (for transports and metrics).
func (s *Server) Raft() *raft.Raft { return s.rf }

// DB exposes the storage engine (for metrics).
func (s *Server) DB() *lsm.DB { return s.db }

// Stop shuts the replica down. With crash set, the LSM memtable is dropped
// as it would be in a real crash; Raft's log replays it on restart.
func (s *Server) Stop(crash bool) {
	s.rf.Stop()
	close(s.stopCh)
	<-s.done
	if crash {
		s.db.CrashClose()
	} else {
		s.db.Close()
	}
}

func (s *Server) leaderHint() int32 { return int32(s.rf.Leader()) }

// Do executes one client request.
func (s *Server) Do(ctx context.Context, req *kvpb.Request) *kvpb.Response {
	switch req.Op {
	case kvpb.Op_GET:
		return s.get(ctx, req)
	case kvpb.Op_PUT, kvpb.Op_APPEND:
		return s.write(ctx, req)
	}
	return &kvpb.Response{Err: kvpb.Err_WRONG_LEADER, LeaderHint: -1}
}

func (s *Server) get(ctx context.Context, req *kvpb.Request) *kvpb.Response {
	idx, err := s.rf.ReadIndex(ctx)
	if err != nil {
		return s.failure(ctx, err)
	}
	if err := s.waitApplied(ctx, idx); err != nil {
		return s.failure(ctx, err)
	}
	v, ok, err := s.db.Get(dataKey(KeyShard(req.Key), req.Key))
	if err != nil {
		return &kvpb.Response{Err: kvpb.Err_TIMEOUT, LeaderHint: -1}
	}
	if !ok {
		return &kvpb.Response{Err: kvpb.Err_NO_KEY}
	}
	return &kvpb.Response{Err: kvpb.Err_OK, Value: v}
}

func (s *Server) failure(ctx context.Context, err error) *kvpb.Response {
	if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
		return &kvpb.Response{Err: kvpb.Err_TIMEOUT, LeaderHint: s.leaderHint()}
	}
	return &kvpb.Response{Err: kvpb.Err_WRONG_LEADER, LeaderHint: s.leaderHint()}
}

func (s *Server) write(ctx context.Context, req *kvpb.Request) *kvpb.Response {
	cmd, err := proto.Marshal(&kvpb.Command{Cmd: &kvpb.Command_Client{Client: req}})
	if err != nil {
		return &kvpb.Response{Err: kvpb.Err_WRONG_LEADER, LeaderHint: -1}
	}
	// Hold s.mu across Start so the waiter is registered before the entry
	// can possibly be applied.
	s.mu.Lock()
	idx, term, ok := s.rf.Start(cmd)
	if !ok {
		s.mu.Unlock()
		return &kvpb.Response{Err: kvpb.Err_WRONG_LEADER, LeaderHint: s.leaderHint()}
	}
	w := &writeWaiter{term: term, ch: make(chan uint64, 1)}
	if old := s.writeWaiters[idx]; old != nil {
		old.ch <- 0 // superseded: a different entry now owns this index
	}
	s.writeWaiters[idx] = w
	s.mu.Unlock()

	select {
	case got := <-w.ch:
		if got == term {
			return &kvpb.Response{Err: kvpb.Err_OK}
		}
		return &kvpb.Response{Err: kvpb.Err_WRONG_LEADER, LeaderHint: s.leaderHint()}
	case <-ctx.Done():
		s.mu.Lock()
		if s.writeWaiters[idx] == w {
			delete(s.writeWaiters, idx)
		}
		s.mu.Unlock()
		return &kvpb.Response{Err: kvpb.Err_TIMEOUT, LeaderHint: s.leaderHint()}
	case <-s.stopCh:
		return &kvpb.Response{Err: kvpb.Err_WRONG_LEADER, LeaderHint: -1}
	}
}

func (s *Server) waitApplied(ctx context.Context, idx uint64) error {
	s.mu.Lock()
	if s.applied >= idx {
		s.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	s.readWaiters[idx] = append(s.readWaiters[idx], ch)
	s.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopCh:
		return raft.ErrStopped
	}
}

func (s *Server) applyLoop() {
	defer close(s.done)
	for {
		var m raft.ApplyMsg
		select {
		case m = <-s.applyCh:
		case <-s.stopCh:
			return
		}
		switch {
		case m.SnapshotValid:
			if err := s.installSnapshot(m.Snapshot); err != nil {
				panic(fmt.Sprintf("kv: installing snapshot: %v", err))
			}
			s.setApplied(m.SnapshotIndex, 0, true)
		case m.CommandValid:
			if m.Index <= s.appliedIndex() {
				continue
			}
			var b lsm.Batch
			if m.Command != nil {
				if err := s.applyCommand(&b, m.Command); err != nil {
					panic(fmt.Sprintf("kv: applying index %d: %v", m.Index, err))
				}
			}
			b.Put(appliedKey, u64(m.Index))
			if err := s.db.Apply(&b); err != nil {
				panic(fmt.Sprintf("kv: storage: %v", err))
			}
			s.setApplied(m.Index, m.Term, false)
			s.maybeCompact()
		}
	}
}

func (s *Server) appliedIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied
}

// setApplied advances the applied index and wakes waiters. A snapshot can
// jump over many indices; writes waiting inside the jump are told to retry.
func (s *Server) setApplied(idx, term uint64, jump bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = idx
	if jump {
		for i, w := range s.writeWaiters {
			if i <= idx {
				w.ch <- 0
				delete(s.writeWaiters, i)
			}
		}
		for i, ws := range s.readWaiters {
			if i <= idx {
				for _, ch := range ws {
					close(ch)
				}
				delete(s.readWaiters, i)
			}
		}
		return
	}
	if w := s.writeWaiters[idx]; w != nil {
		w.ch <- term
		delete(s.writeWaiters, idx)
	}
	for _, ch := range s.readWaiters[idx] {
		close(ch)
	}
	delete(s.readWaiters, idx)
}

func (s *Server) applyCommand(b *lsm.Batch, data []byte) error {
	var cmd kvpb.Command
	if err := proto.Unmarshal(data, &cmd); err != nil {
		return err
	}
	switch c := cmd.Cmd.(type) {
	case *kvpb.Command_Client:
		return s.applyClient(b, c.Client)
	}
	return fmt.Errorf("unknown command %T", cmd.Cmd)
}

func (s *Server) applyClient(b *lsm.Batch, req *kvpb.Request) error {
	shard := KeyShard(req.Key)
	dk := dedupKey(shard, req.ClientId)
	last, _, err := s.db.Get(dk)
	if err != nil {
		return err
	}
	if req.Seq <= readU64(last) {
		return nil // duplicate of a write already applied
	}
	key := dataKey(shard, req.Key)
	switch req.Op {
	case kvpb.Op_PUT:
		b.Put(key, req.Value)
	case kvpb.Op_APPEND:
		old, _, err := s.db.Get(key)
		if err != nil {
			return err
		}
		b.Put(key, append(old, req.Value...))
	}
	b.Put(dk, u64(req.Seq))
	return nil
}

// maybeCompact flushes the LSM and lets Raft drop its log prefix once the
// log grows past MaxRaftLog entries.
func (s *Server) maybeCompact() {
	s.sinceCheck++
	if s.sinceCheck < 256 {
		return
	}
	s.sinceCheck = 0
	if s.rf.Status().LogEntries < s.cfg.MaxRaftLog {
		return
	}
	if err := s.db.Flush(); err != nil {
		panic(fmt.Sprintf("kv: flush: %v", err))
	}
	if err := s.rf.Snapshot(s.appliedIndex(), nil); err != nil {
		panic(fmt.Sprintf("kv: raft compaction: %v", err))
	}
}

// snapshot serializes the whole store for a lagging follower. The LSM scan
// is a consistent image, and the applied index inside it says which log
// prefix the image reflects.
func (s *Server) snapshot() ([]byte, uint64) {
	var buf []byte
	var idx uint64
	err := s.db.Scan(nil, func(k, v []byte) bool {
		if string(k) == string(appliedKey) {
			idx = readU64(v)
		}
		buf = binary.AppendUvarint(buf, uint64(len(k)))
		buf = append(buf, k...)
		buf = binary.AppendUvarint(buf, uint64(len(v)))
		buf = append(buf, v...)
		return true
	})
	if err != nil {
		panic(fmt.Sprintf("kv: snapshot scan: %v", err))
	}
	return buf, idx
}

func (s *Server) installSnapshot(data []byte) error {
	if err := s.db.Reset(); err != nil {
		return err
	}
	var b lsm.Batch
	for len(data) > 0 {
		kl, n := binary.Uvarint(data)
		if n <= 0 || uint64(len(data)-n) < kl {
			return errors.New("corrupt snapshot")
		}
		k := data[n : n+int(kl)]
		data = data[n+int(kl):]
		vl, n := binary.Uvarint(data)
		if n <= 0 || uint64(len(data)-n) < vl {
			return errors.New("corrupt snapshot")
		}
		b.Put(k, data[n:n+int(vl)])
		data = data[n+int(vl):]
		if b.Len() >= 1024 {
			if err := s.db.Apply(&b); err != nil {
				return err
			}
			b = lsm.Batch{}
		}
	}
	if err := s.db.Apply(&b); err != nil {
		return err
	}
	return s.db.Flush()
}
