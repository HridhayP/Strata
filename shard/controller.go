package shard

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/HridhayP/strata/kv"
	"github.com/HridhayP/strata/proto/ctrlpb"
	"github.com/HridhayP/strata/raft"
	"google.golang.org/protobuf/proto"
)

// ControllerConfig configures a controller replica.
type ControllerConfig struct {
	ID                int
	Peers             []int
	Group             string
	Storage           raft.Storage
	Transport         raft.Transport
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	MaxRaftLog        int
}

// Controller is one replica of the shard controller. Its state is the list
// of all configurations ever created (config 0 assigns nothing) plus client
// dedup records. The state is small, so snapshots are stored by Raft.
type Controller struct {
	cfg     ControllerConfig
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	mu       sync.Mutex
	configs  []*ctrlpb.Config
	dedup    map[uint64]uint64
	applied  uint64
	waiters  map[uint64]*writeWaiter
	readWait map[uint64][]chan struct{}

	stopCh chan struct{}
	done   chan struct{}
}

type writeWaiter struct {
	term uint64
	ch   chan uint64
}

func emptyConfig() *ctrlpb.Config {
	return &ctrlpb.Config{Shards: make([]int64, kv.NShards), Groups: map[int64]*ctrlpb.Servers{}, Pinned: map[int32]int64{}}
}

// NewController starts a controller replica.
func NewController(cfg ControllerConfig) (*Controller, error) {
	if cfg.MaxRaftLog == 0 {
		cfg.MaxRaftLog = 1000
	}
	c := &Controller{
		cfg:      cfg,
		applyCh:  make(chan raft.ApplyMsg, 256),
		configs:  []*ctrlpb.Config{emptyConfig()},
		dedup:    map[uint64]uint64{},
		waiters:  map[uint64]*writeWaiter{},
		readWait: map[uint64][]chan struct{}{},
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}
	rf, err := raft.New(raft.Config{
		ID:                cfg.ID,
		Peers:             cfg.Peers,
		Group:             cfg.Group,
		Transport:         cfg.Transport,
		Storage:           cfg.Storage,
		ApplyCh:           c.applyCh,
		ElectionTimeout:   cfg.ElectionTimeout,
		HeartbeatInterval: cfg.HeartbeatInterval,
	})
	if err != nil {
		return nil, err
	}
	c.rf = rf
	go c.applyLoop()
	return c, nil
}

// Raft exposes the replica's Raft instance.
func (c *Controller) Raft() *raft.Raft { return c.rf }

// Stop shuts the replica down.
func (c *Controller) Stop() {
	c.rf.Stop()
	close(c.stopCh)
	<-c.done
}

func (c *Controller) hint() int32 { return int32(c.rf.Leader()) }

// Do executes a controller request.
func (c *Controller) Do(ctx context.Context, req *ctrlpb.Request) *ctrlpb.Response {
	if req.Op == ctrlpb.Op_QUERY {
		idx, err := c.rf.ReadIndex(ctx)
		if err == nil {
			err = c.waitApplied(ctx, idx)
		}
		if err != nil {
			return &ctrlpb.Response{Err: errCode(ctx), LeaderHint: c.hint()}
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		n := req.Num
		if n < 0 || n >= int64(len(c.configs)) {
			n = int64(len(c.configs) - 1)
		}
		return &ctrlpb.Response{Err: ctrlpb.Err_OK, Config: proto.Clone(c.configs[n]).(*ctrlpb.Config)}
	}
	cmd, _ := proto.Marshal(req)
	c.mu.Lock()
	idx, term, ok := c.rf.Start(cmd)
	if !ok {
		c.mu.Unlock()
		return &ctrlpb.Response{Err: ctrlpb.Err_WRONG_LEADER, LeaderHint: c.hint()}
	}
	w := &writeWaiter{term: term, ch: make(chan uint64, 1)}
	if old := c.waiters[idx]; old != nil {
		old.ch <- 0
	}
	c.waiters[idx] = w
	c.mu.Unlock()
	select {
	case got := <-w.ch:
		if got == term {
			return &ctrlpb.Response{Err: ctrlpb.Err_OK}
		}
		return &ctrlpb.Response{Err: ctrlpb.Err_WRONG_LEADER, LeaderHint: c.hint()}
	case <-ctx.Done():
		return &ctrlpb.Response{Err: ctrlpb.Err_TIMEOUT, LeaderHint: c.hint()}
	case <-c.stopCh:
		return &ctrlpb.Response{Err: ctrlpb.Err_WRONG_LEADER, LeaderHint: -1}
	}
}

func errCode(ctx context.Context) ctrlpb.Err {
	if ctx.Err() != nil {
		return ctrlpb.Err_TIMEOUT
	}
	return ctrlpb.Err_WRONG_LEADER
}

func (c *Controller) waitApplied(ctx context.Context, idx uint64) error {
	c.mu.Lock()
	if c.applied >= idx {
		c.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	c.readWait[idx] = append(c.readWait[idx], ch)
	c.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.stopCh:
		return raft.ErrStopped
	}
}

func (c *Controller) applyLoop() {
	defer close(c.done)
	for {
		var m raft.ApplyMsg
		select {
		case m = <-c.applyCh:
		case <-c.stopCh:
			return
		}
		c.mu.Lock()
		switch {
		case m.SnapshotValid:
			if err := c.restore(m.Snapshot); err != nil {
				panic(fmt.Sprintf("ctrl: bad snapshot: %v", err))
			}
			c.applied = m.SnapshotIndex
			for i, w := range c.waiters {
				if i <= m.SnapshotIndex {
					w.ch <- 0
					delete(c.waiters, i)
				}
			}
			for i, ws := range c.readWait {
				if i <= m.SnapshotIndex {
					for _, ch := range ws {
						close(ch)
					}
					delete(c.readWait, i)
				}
			}
		case m.CommandValid && m.Index > c.applied:
			if m.Command != nil {
				var req ctrlpb.Request
				if err := proto.Unmarshal(m.Command, &req); err != nil {
					panic(fmt.Sprintf("ctrl: bad command: %v", err))
				}
				if req.Seq > c.dedup[req.ClientId] {
					c.apply(&req)
					c.dedup[req.ClientId] = req.Seq
				}
			}
			c.applied = m.Index
			if w := c.waiters[m.Index]; w != nil {
				w.ch <- m.Term
				delete(c.waiters, m.Index)
			}
			for _, ch := range c.readWait[m.Index] {
				close(ch)
			}
			delete(c.readWait, m.Index)
		}
		var snap []byte
		snapIdx := c.applied
		if m.CommandValid && m.Index%64 == 0 && c.rf.Status().LogEntries > c.cfg.MaxRaftLog {
			snap = c.encode()
		}
		c.mu.Unlock()
		if snap != nil {
			c.rf.Snapshot(snapIdx, snap)
		}
	}
}

// apply creates the next configuration. c.mu held.
func (c *Controller) apply(req *ctrlpb.Request) {
	last := c.configs[len(c.configs)-1]
	next := proto.Clone(last).(*ctrlpb.Config)
	next.Num = last.Num + 1
	if next.Groups == nil {
		next.Groups = map[int64]*ctrlpb.Servers{}
	}
	if next.Pinned == nil {
		next.Pinned = map[int32]int64{}
	}
	switch req.Op {
	case ctrlpb.Op_JOIN:
		for gid, srv := range req.Join {
			next.Groups[gid] = proto.Clone(srv).(*ctrlpb.Servers)
		}
		next.Shards = Assign(slices.Collect(maps.Keys(next.Groups)), next.Pinned)
	case ctrlpb.Op_LEAVE:
		for _, gid := range req.Leave {
			delete(next.Groups, gid)
			for s, g := range next.Pinned {
				if g == gid {
					delete(next.Pinned, s)
				}
			}
		}
		next.Shards = Assign(slices.Collect(maps.Keys(next.Groups)), next.Pinned)
	case ctrlpb.Op_MOVE:
		if _, ok := next.Groups[req.Gid]; !ok || req.Shard < 0 || int(req.Shard) >= kv.NShards {
			return // invalid move: no new config
		}
		next.Pinned[req.Shard] = req.Gid
		next.Shards[req.Shard] = req.Gid
	default:
		return
	}
	c.configs = append(c.configs, next)
}

type ctrlSnapshot struct {
	Configs [][]byte
	Dedup   map[uint64]uint64
}

func (c *Controller) encode() []byte {
	s := ctrlSnapshot{Dedup: c.dedup}
	for _, cfg := range c.configs {
		b, _ := proto.Marshal(cfg)
		s.Configs = append(s.Configs, b)
	}
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(&s)
	return buf.Bytes()
}

func (c *Controller) restore(data []byte) error {
	var s ctrlSnapshot
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&s); err != nil {
		return err
	}
	configs := make([]*ctrlpb.Config, len(s.Configs))
	for i, b := range s.Configs {
		configs[i] = &ctrlpb.Config{}
		if err := proto.Unmarshal(b, configs[i]); err != nil {
			return err
		}
	}
	if len(configs) == 0 {
		return errors.New("snapshot without configs")
	}
	c.configs = configs
	c.dedup = s.Dedup
	if c.dedup == nil {
		c.dedup = map[uint64]uint64{}
	}
	return nil
}

// CtrlCaller delivers a request to one controller replica.
type CtrlCaller interface {
	Do(ctx context.Context, server int, req *ctrlpb.Request) (*ctrlpb.Response, error)
}

// CtrlClerk is a client of the shard controller.
type CtrlClerk struct {
	servers []int
	call    CtrlCaller
	id      uint64
	seq     uint64
	leader  int
	mu      sync.Mutex // a CtrlClerk may be shared by goroutines
}

// NewCtrlClerk returns a controller client.
func NewCtrlClerk(servers []int, call CtrlCaller) *CtrlClerk {
	return &CtrlClerk{servers: servers, call: call, id: rand.Uint64()}
}

// Query returns config num, or the latest config if num < 0.
func (ck *CtrlClerk) Query(ctx context.Context, num int64) (*ctrlpb.Config, error) {
	resp, err := ck.do(ctx, &ctrlpb.Request{Op: ctrlpb.Op_QUERY, Num: num}, false)
	if err != nil {
		return nil, err
	}
	return resp.Config, nil
}

// Join adds replica groups.
func (ck *CtrlClerk) Join(ctx context.Context, groups map[int64][]int32) error {
	req := &ctrlpb.Request{Op: ctrlpb.Op_JOIN, Join: map[int64]*ctrlpb.Servers{}}
	for g, s := range groups {
		req.Join[g] = &ctrlpb.Servers{Ids: s}
	}
	_, err := ck.do(ctx, req, true)
	return err
}

// Leave removes replica groups.
func (ck *CtrlClerk) Leave(ctx context.Context, gids ...int64) error {
	_, err := ck.do(ctx, &ctrlpb.Request{Op: ctrlpb.Op_LEAVE, Leave: gids}, true)
	return err
}

// Move pins a shard to a group.
func (ck *CtrlClerk) Move(ctx context.Context, shard int, gid int64) error {
	_, err := ck.do(ctx, &ctrlpb.Request{Op: ctrlpb.Op_MOVE, Shard: int32(shard), Gid: gid}, true)
	return err
}

func (ck *CtrlClerk) do(ctx context.Context, req *ctrlpb.Request, write bool) (*ctrlpb.Response, error) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if write {
		ck.seq++
		req.ClientId, req.Seq = ck.id, ck.seq
	}
	for tried := 1; ; tried++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		actx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		resp, err := ck.call.Do(actx, ck.servers[ck.leader], req)
		cancel()
		if err == nil && resp.Err == ctrlpb.Err_OK {
			return resp, nil
		}
		next := (ck.leader + 1) % len(ck.servers)
		if err == nil && resp.LeaderHint >= 0 {
			for i, s := range ck.servers {
				if s == int(resp.LeaderHint) && i != ck.leader {
					next = i
				}
			}
		}
		ck.leader = next
		if tried%len(ck.servers) == 0 {
			select {
			case <-time.After(20 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
}
