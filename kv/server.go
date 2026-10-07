// Package kv is a linearizable, sharded key-value service replicated with
// Raft and stored in the LSM engine.
//
// Writes (Put, Append) are proposed to Raft and applied in log order. Each
// carries (client ID, sequence number); the state machine records the last
// sequence applied per client and shard, so a retried write is applied at
// most once, even if the shard moved to another group in between. Reads use
// Raft's ReadIndex: the leader confirms it is still leader with a heartbeat
// round, waits until it has applied up to the commit index it saw, and reads
// locally, which keeps reads out of the log entirely.
//
// The LSM has no WAL of its own; the Raft log is the write-ahead log. Every
// applied batch also writes the applied Raft index into the LSM, so whatever
// state the LSM recovers after a crash names exactly where Raft must resume.
//
// In sharded mode (GID != 0) the group follows the shard controller's
// configurations one at a time. A shard it gains is pulled from the previous
// owner, installed through its own Raft log, and then the previous owner is
// told to delete its copy. A group only moves to the next configuration once
// every shard has settled, which keeps migrations simple to reason about.
package kv

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/HridhayP/strata/proto/ctrlpb"
	"github.com/HridhayP/strata/proto/kvpb"
	"github.com/HridhayP/strata/raft"
	"github.com/HridhayP/strata/storage/lsm"
	"google.golang.org/protobuf/proto"
)

// Shard states within a group.
const (
	shardAbsent   byte = 0 // not ours
	shardServing  byte = 1 // ours, accepting requests
	shardPulling  byte = 2 // ours in the current config; waiting for data
	shardOffering byte = 3 // no longer ours; frozen until the new owner has it
	shardCleaning byte = 4 // ours and serving; previous owner not yet told to delete
)

// ConfigSource supplies shard configurations (the controller clerk).
type ConfigSource interface {
	Query(ctx context.Context, num int64) (*ctrlpb.Config, error)
}

// PeerCaller reaches other groups' servers for shard migration.
type PeerCaller interface {
	PullShards(ctx context.Context, server int, req *kvpb.PullShardsRequest) (*kvpb.PullShardsResponse, error)
	DeleteShards(ctx context.Context, server int, req *kvpb.DeleteShardsRequest) (*kvpb.DeleteShardsResponse, error)
}

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
	MaxInflight       int // pipelined AppendEntries per follower (0: Raft default)
	// MaxRaftLog is the number of log entries that triggers compaction.
	MaxRaftLog   int
	MemtableSize int
	Observer     raft.Observer

	// Sharding. GID 0 means unsharded: the group serves every shard.
	GID          int64
	Ctrl         ConfigSource
	PeerCaller   PeerCaller
	PollInterval time.Duration
}

type result struct {
	term uint64
	err  kvpb.Err
}

type writeWaiter struct {
	term uint64
	ch   chan result
}

// Server is one replica of a KV group.
type Server struct {
	cfg     Config
	rf      *raft.Raft
	db      *lsm.DB
	applyCh chan raft.ApplyMsg

	// state guards the LSM contents together with the shard table, so a
	// read sees ownership and data from the same applied state.
	state  sync.RWMutex
	shards [NShards]byte
	cur    *ctrlpb.Config
	prev   *ctrlpb.Config

	mu           sync.Mutex
	applied      uint64
	writeWaiters map[uint64]*writeWaiter
	readWaiters  map[uint64][]chan struct{}
	sinceCheck   int

	stopCh chan struct{}
	done   chan struct{}
	bg     sync.WaitGroup
}

// Meta keys for sharding state.
var (
	curConfigKey  = []byte("m/config")
	prevConfigKey = []byte("m/prevconfig")
)

func shardStateKey(s int) []byte { return []byte(fmt.Sprintf("m/shard/%02d", s)) }

// NewServer opens the LSM, starts Raft and the apply loop.
func NewServer(cfg Config) (*Server, error) {
	if cfg.MaxRaftLog == 0 {
		cfg.MaxRaftLog = 20000
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	db, err := lsm.Open(cfg.Dir, lsm.Options{MemtableSize: cfg.MemtableSize})
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg:          cfg,
		db:           db,
		applyCh:      make(chan raft.ApplyMsg, 1024),
		writeWaiters: make(map[uint64]*writeWaiter),
		readWaiters:  make(map[uint64][]chan struct{}),
		stopCh:       make(chan struct{}),
		done:         make(chan struct{}),
	}
	v, _, err := db.Get(appliedKey)
	if err == nil {
		s.applied = readU64(v)
		err = s.loadState()
	}
	if err != nil {
		db.Close()
		return nil, err
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
		MaxInflight:       cfg.MaxInflight,
		AppliedIndex:      s.applied,
		SnapshotFn:        s.snapshot,
		Observer:          cfg.Observer,
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	go s.applyLoop()
	if cfg.GID != 0 {
		s.bg.Add(1)
		go s.migrationLoop()
	}
	return s, nil
}

// loadState reads the shard table and configs from the LSM.
func (s *Server) loadState() error {
	if s.cfg.GID == 0 {
		for i := range s.shards {
			s.shards[i] = shardServing
		}
		return nil
	}
	s.cur, s.prev = &ctrlpb.Config{Shards: make([]int64, NShards)}, &ctrlpb.Config{Shards: make([]int64, NShards)}
	for _, kc := range []struct {
		key []byte
		dst *ctrlpb.Config
	}{{curConfigKey, s.cur}, {prevConfigKey, s.prev}} {
		b, ok, err := s.db.Get(kc.key)
		if err != nil {
			return err
		}
		if ok {
			if err := proto.Unmarshal(b, kc.dst); err != nil {
				return err
			}
		}
	}
	for i := range s.shards {
		b, _, err := s.db.Get(shardStateKey(i))
		if err != nil {
			return err
		}
		if len(b) == 1 {
			s.shards[i] = b[0]
		} else {
			s.shards[i] = shardAbsent
		}
	}
	return nil
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
	s.bg.Wait()
	if crash {
		s.db.CrashClose()
	} else {
		s.db.Close()
	}
}

func (s *Server) leaderHint() int32 { return int32(s.rf.Leader()) }

func serves(st byte) bool { return st == shardServing || st == shardCleaning }

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
	shard := KeyShard(req.Key)
	s.state.RLock()
	defer s.state.RUnlock()
	if !serves(s.shards[shard]) {
		return &kvpb.Response{Err: kvpb.Err_WRONG_GROUP}
	}
	v, ok, err := s.db.Get(dataKey(shard, req.Key))
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
	s.state.RLock()
	owned := serves(s.shards[KeyShard(req.Key)])
	s.state.RUnlock()
	if !owned {
		return &kvpb.Response{Err: kvpb.Err_WRONG_GROUP}
	}
	cmd, err := proto.Marshal(&kvpb.Command{Cmd: &kvpb.Command_Client{Client: req}})
	if err != nil {
		return &kvpb.Response{Err: kvpb.Err_WRONG_LEADER, LeaderHint: -1}
	}
	r, ok := s.propose(ctx, cmd)
	if !ok {
		return s.failure(ctx, ctx.Err())
	}
	return &kvpb.Response{Err: r, LeaderHint: s.leaderHint()}
}

// propose submits cmd and waits for it to be applied. ok is false if this
// replica is not the leader, lost leadership, or ctx expired.
func (s *Server) propose(ctx context.Context, cmd []byte) (kvpb.Err, bool) {
	// Hold s.mu across Start so the waiter is registered before the entry
	// can possibly be applied.
	s.mu.Lock()
	idx, term, ok := s.rf.Start(cmd)
	if !ok {
		s.mu.Unlock()
		return 0, false
	}
	w := &writeWaiter{term: term, ch: make(chan result, 1)}
	if old := s.writeWaiters[idx]; old != nil {
		old.ch <- result{} // a different entry now owns this index
	}
	s.writeWaiters[idx] = w
	s.mu.Unlock()

	select {
	case r := <-w.ch:
		return r.err, r.term == term
	case <-ctx.Done():
		s.mu.Lock()
		if s.writeWaiters[idx] == w {
			delete(s.writeWaiters, idx)
		}
		s.mu.Unlock()
		return 0, false
	case <-s.stopCh:
		return 0, false
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
			s.state.Lock()
			err := s.installSnapshot(m.Snapshot)
			if err == nil {
				err = s.loadState()
			}
			s.state.Unlock()
			if err != nil {
				panic(fmt.Sprintf("kv: installing snapshot: %v", err))
			}
			s.setApplied(m.SnapshotIndex, result{}, true)
		case m.CommandValid:
			if m.Index <= s.appliedIndex() {
				continue
			}
			s.state.Lock()
			var b lsm.Batch
			res := result{term: m.Term}
			if m.Command != nil {
				var err error
				if res.err, err = s.applyCommand(&b, m.Command); err != nil {
					panic(fmt.Sprintf("kv: applying index %d: %v", m.Index, err))
				}
			}
			b.Put(appliedKey, u64(m.Index))
			if err := s.db.Apply(&b); err != nil {
				panic(fmt.Sprintf("kv: storage: %v", err))
			}
			s.state.Unlock()
			s.setApplied(m.Index, res, false)
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
func (s *Server) setApplied(idx uint64, res result, jump bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = idx
	if jump {
		for i, w := range s.writeWaiters {
			if i <= idx {
				w.ch <- result{}
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
		w.ch <- res
		delete(s.writeWaiters, idx)
	}
	for _, ch := range s.readWaiters[idx] {
		close(ch)
	}
	delete(s.readWaiters, idx)
}

// applyCommand stages cmd's effects in b. s.state is held for writing.
func (s *Server) applyCommand(b *lsm.Batch, data []byte) (kvpb.Err, error) {
	var cmd kvpb.Command
	if err := proto.Unmarshal(data, &cmd); err != nil {
		return 0, err
	}
	switch c := cmd.Cmd.(type) {
	case *kvpb.Command_Client:
		return s.applyClient(b, c.Client)
	}
	if s.cfg.GID == 0 {
		return kvpb.Err_OK, nil // migration commands mean nothing unsharded
	}
	switch c := cmd.Cmd.(type) {
	case *kvpb.Command_Config:
		return kvpb.Err_OK, s.applyConfig(b, c.Config.Config)
	case *kvpb.Command_Install:
		return kvpb.Err_OK, s.applyInstall(b, c.Install)
	case *kvpb.Command_Delete:
		return kvpb.Err_OK, s.applyDelete(b, c.Delete)
	case *kvpb.Command_Cleaned:
		s.applyCleaned(b, c.Cleaned)
		return kvpb.Err_OK, nil
	}
	return 0, fmt.Errorf("unknown command %T", cmd.Cmd)
}

func (s *Server) applyClient(b *lsm.Batch, req *kvpb.Request) (kvpb.Err, error) {
	shard := KeyShard(req.Key)
	if !serves(s.shards[shard]) {
		return kvpb.Err_WRONG_GROUP, nil
	}
	dk := dedupKey(shard, req.ClientId)
	last, _, err := s.db.Get(dk)
	if err != nil {
		return 0, err
	}
	if req.Seq <= readU64(last) {
		return kvpb.Err_OK, nil // duplicate of a write already applied
	}
	key := dataKey(shard, req.Key)
	switch req.Op {
	case kvpb.Op_PUT:
		b.Put(key, req.Value)
	case kvpb.Op_APPEND:
		old, _, err := s.db.Get(key)
		if err != nil {
			return 0, err
		}
		b.Put(key, append(old, req.Value...))
	}
	b.Put(dk, u64(req.Seq))
	return kvpb.Err_OK, nil
}

func (s *Server) setShard(b *lsm.Batch, shard int, st byte) {
	s.shards[shard] = st
	b.Put(shardStateKey(shard), []byte{st})
}

func (s *Server) settled() bool {
	for _, st := range s.shards {
		if st != shardServing && st != shardAbsent {
			return false
		}
	}
	return true
}

func (s *Server) applyConfig(b *lsm.Batch, next *ctrlpb.Config) error {
	if s.cfg.GID == 0 || next.Num != s.cur.Num+1 || !s.settled() {
		return nil // stale or premature proposal
	}
	gid := s.cfg.GID
	for sh := 0; sh < NShards; sh++ {
		was, now := s.cur.Shards[sh] == gid, next.Shards[sh] == gid
		switch {
		case now && !was && s.cur.Shards[sh] == 0:
			s.setShard(b, sh, shardServing) // first assignment: nothing to pull
		case now && !was:
			s.setShard(b, sh, shardPulling)
		case was && !now:
			s.setShard(b, sh, shardOffering)
		}
	}
	s.prev, s.cur = s.cur, next
	pb, _ := proto.Marshal(s.prev)
	cb, _ := proto.Marshal(s.cur)
	b.Put(prevConfigKey, pb)
	b.Put(curConfigKey, cb)
	return nil
}

func (s *Server) applyInstall(b *lsm.Batch, in *kvpb.InstallShards) error {
	if in.ConfigNum != s.cur.Num {
		return nil
	}
	for _, sd := range in.Shards {
		sh := int(sd.Shard)
		if sh < 0 || sh >= NShards || s.shards[sh] != shardPulling {
			continue // duplicate install
		}
		if err := decodePairs(sd.Pairs, func(k, v []byte) { b.Put(k, v) }); err != nil {
			return err
		}
		s.setShard(b, sh, shardCleaning)
	}
	return nil
}

func (s *Server) applyDelete(b *lsm.Batch, del *kvpb.DeleteShards) error {
	if del.ConfigNum != s.cur.Num {
		return nil
	}
	for _, sh32 := range del.Shards {
		sh := int(sh32)
		if sh < 0 || sh >= NShards || s.shards[sh] != shardOffering {
			continue
		}
		for _, tag := range []byte{tagData, tagDedup} {
			err := s.db.Scan(shardPrefix(tag, sh), func(k, _ []byte) bool {
				b.Delete(append([]byte(nil), k...))
				return true
			})
			if err != nil {
				return err
			}
		}
		s.setShard(b, sh, shardAbsent)
	}
	return nil
}

func (s *Server) applyCleaned(b *lsm.Batch, c *kvpb.ShardsCleaned) {
	if c.ConfigNum != s.cur.Num {
		return
	}
	for _, sh32 := range c.Shards {
		if sh := int(sh32); sh >= 0 && sh < NShards && s.shards[sh] == shardCleaning {
			s.setShard(b, sh, shardServing)
		}
	}
}

// PullShards hands frozen shard data to the group that now owns it. Any
// replica that has applied the requested configuration can answer, because
// an offered shard no longer changes.
func (s *Server) PullShards(ctx context.Context, req *kvpb.PullShardsRequest) *kvpb.PullShardsResponse {
	s.state.RLock()
	defer s.state.RUnlock()
	if s.cur == nil || s.cur.Num < req.ConfigNum {
		return &kvpb.PullShardsResponse{Err: kvpb.Err_NOT_READY, LeaderHint: s.leaderHint()}
	}
	resp := &kvpb.PullShardsResponse{Err: kvpb.Err_OK}
	for _, sh32 := range req.Shards {
		sh := int(sh32)
		if sh < 0 || sh >= NShards {
			continue
		}
		if s.cur.Num == req.ConfigNum && s.shards[sh] != shardOffering {
			return &kvpb.PullShardsResponse{Err: kvpb.Err_NOT_READY}
		}
		var pairs []byte
		for _, tag := range []byte{tagData, tagDedup} {
			err := s.db.Scan(shardPrefix(tag, sh), func(k, v []byte) bool {
				pairs = appendPair(pairs, k, v)
				return true
			})
			if err != nil {
				return &kvpb.PullShardsResponse{Err: kvpb.Err_TIMEOUT}
			}
		}
		resp.Shards = append(resp.Shards, &kvpb.ShardData{Shard: sh32, Pairs: pairs})
	}
	return resp
}

// DeleteShards garbage-collects shards the new owner has installed.
func (s *Server) DeleteShards(ctx context.Context, req *kvpb.DeleteShardsRequest) *kvpb.DeleteShardsResponse {
	s.state.RLock()
	num := uint64(0)
	if s.cur != nil {
		num = s.cur.Num
	}
	s.state.RUnlock()
	switch {
	case num < req.ConfigNum:
		return &kvpb.DeleteShardsResponse{Err: kvpb.Err_NOT_READY, LeaderHint: s.leaderHint()}
	case num > req.ConfigNum:
		// We only move past a config once our offered shards are deleted.
		return &kvpb.DeleteShardsResponse{Err: kvpb.Err_OK}
	}
	cmd, _ := proto.Marshal(&kvpb.Command{Cmd: &kvpb.Command_Delete{Delete: &kvpb.DeleteShards{ConfigNum: req.ConfigNum, Shards: req.Shards}}})
	if _, ok := s.propose(ctx, cmd); !ok {
		return &kvpb.DeleteShardsResponse{Err: kvpb.Err_WRONG_LEADER, LeaderHint: s.leaderHint()}
	}
	return &kvpb.DeleteShardsResponse{Err: kvpb.Err_OK}
}

// migrationLoop runs on every replica but only acts on the leader: it moves
// to the next configuration when settled, pulls shards it is waiting for,
// and tells previous owners to delete shards it has installed.
func (s *Server) migrationLoop() {
	defer s.bg.Done()
	t := time.NewTicker(s.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-t.C:
		}
		if _, leader := s.rf.State(); !leader {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*s.cfg.PollInterval+time.Second)
		s.migrateOnce(ctx)
		cancel()
	}
}

func (s *Server) migrateOnce(ctx context.Context) {
	s.state.RLock()
	settled := s.settled()
	cur := s.cur
	prev := s.prev
	pulling := map[int64][]int32{}
	cleaning := map[int64][]int32{}
	for sh, st := range s.shards {
		switch st {
		case shardPulling:
			pulling[prev.Shards[sh]] = append(pulling[prev.Shards[sh]], int32(sh))
		case shardCleaning:
			cleaning[prev.Shards[sh]] = append(cleaning[prev.Shards[sh]], int32(sh))
		}
	}
	s.state.RUnlock()

	if settled {
		next, err := s.cfg.Ctrl.Query(ctx, int64(cur.Num+1))
		if err == nil && next.Num == cur.Num+1 {
			cmd, _ := proto.Marshal(&kvpb.Command{Cmd: &kvpb.Command_Config{Config: &kvpb.ConfigChange{Config: next}}})
			s.rf.Start(cmd)
		}
		return
	}
	var wg sync.WaitGroup
	for gid, shards := range pulling {
		wg.Add(1)
		go func(gid int64, shards []int32) {
			defer wg.Done()
			req := &kvpb.PullShardsRequest{Group: groupName(gid), ConfigNum: cur.Num, Shards: shards}
			for _, srv := range prev.Groups[gid].GetIds() {
				resp, err := s.cfg.PeerCaller.PullShards(ctx, int(srv), req)
				if err != nil || resp.Err != kvpb.Err_OK {
					continue
				}
				cmd, _ := proto.Marshal(&kvpb.Command{Cmd: &kvpb.Command_Install{Install: &kvpb.InstallShards{ConfigNum: cur.Num, Shards: resp.Shards}}})
				s.rf.Start(cmd)
				return
			}
		}(gid, shards)
	}
	for gid, shards := range cleaning {
		wg.Add(1)
		go func(gid int64, shards []int32) {
			defer wg.Done()
			req := &kvpb.DeleteShardsRequest{Group: groupName(gid), ConfigNum: cur.Num, Shards: shards}
			for _, srv := range prev.Groups[gid].GetIds() {
				resp, err := s.cfg.PeerCaller.DeleteShards(ctx, int(srv), req)
				if err != nil || resp.Err != kvpb.Err_OK {
					continue
				}
				cmd, _ := proto.Marshal(&kvpb.Command{Cmd: &kvpb.Command_Cleaned{Cleaned: &kvpb.ShardsCleaned{ConfigNum: cur.Num, Shards: shards}}})
				s.rf.Start(cmd)
				return
			}
		}(gid, shards)
	}
	wg.Wait()
}

// groupName is the routing name of a replica group.
func groupName(gid int64) string { return fmt.Sprintf("g%d", gid) }

// GroupName is exported for transports that host several groups.
func GroupName(gid int64) string { return groupName(gid) }

// ShardStates returns a copy of the shard table and the current config
// number (for tests and metrics).
func (s *Server) ShardStates() ([NShards]byte, uint64) {
	s.state.RLock()
	defer s.state.RUnlock()
	n := uint64(0)
	if s.cur != nil {
		n = s.cur.Num
	}
	return s.shards, n
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

func appendPair(buf, k, v []byte) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(k)))
	buf = append(buf, k...)
	buf = binary.AppendUvarint(buf, uint64(len(v)))
	return append(buf, v...)
}

func decodePairs(data []byte, fn func(k, v []byte)) error {
	for len(data) > 0 {
		kl, n := binary.Uvarint(data)
		if n <= 0 || uint64(len(data)-n) < kl {
			return errors.New("kv: corrupt pair encoding")
		}
		k := data[n : n+int(kl)]
		data = data[n+int(kl):]
		vl, n := binary.Uvarint(data)
		if n <= 0 || uint64(len(data)-n) < vl {
			return errors.New("kv: corrupt pair encoding")
		}
		fn(k, data[n:n+int(vl)])
		data = data[n+int(vl):]
	}
	return nil
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
		buf = appendPair(buf, k, v)
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
	var applyErr error
	err := decodePairs(data, func(k, v []byte) {
		b.Put(k, v)
		if b.Len() >= 1024 && applyErr == nil {
			applyErr = s.db.Apply(&b)
			b = lsm.Batch{}
		}
	})
	if err != nil {
		return err
	}
	if applyErr != nil {
		return applyErr
	}
	if err := s.db.Apply(&b); err != nil {
		return err
	}
	return s.db.Flush()
}
