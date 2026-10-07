package harness

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/HridhayP/strata/kv"
	"github.com/HridhayP/strata/proto/ctrlpb"
	"github.com/HridhayP/strata/proto/kvpb"
	"github.com/HridhayP/strata/raft"
	"github.com/HridhayP/strata/shard"
	"github.com/HridhayP/strata/transport/sim"
	"github.com/anishathalye/porcupine"
)

// Address layout on the simulated network: controller replicas at 0..C-1,
// replica i of group g at 100*g + i.
func groupAddr(gid int64, i int) int { return int(gid)*100 + i }

// ShardedOptions describes a sharded run.
type ShardedOptions struct {
	Groups           int // KV groups, gids 1..Groups
	ReplicasPerGroup int
	Clients          int
	Keys             int
	OpsPerClient     int
	Duration         time.Duration
	Crashes          bool
	Unreliable       bool
	MaxRaftLog       int
	MaxInflight      int
	Dir              string
	Seed             uint64
}

// ShardedCluster runs a controller group and several KV groups.
type ShardedCluster struct {
	Net  *sim.Network
	opts ShardedOptions

	ctrl []*shard.Controller

	mu      sync.Mutex
	servers map[int]*kv.Server
	stores  map[int]raft.Storage
}

func raftLookup(h any) sim.RaftHandler {
	switch v := h.(type) {
	case *kv.Server:
		return v.Raft()
	case *shard.Controller:
		return v.Raft()
	}
	panic(fmt.Sprintf("no raft at %T", h))
}

type simKV struct {
	net  *sim.Network
	from int
}

func (c simKV) Do(ctx context.Context, server int, req *kvpb.Request) (*kvpb.Response, error) {
	return sim.Call(ctx, c.net, c.from, server, func(h any) *kvpb.Response { return h.(*kv.Server).Do(ctx, req) })
}

func (c simKV) PullShards(ctx context.Context, server int, req *kvpb.PullShardsRequest) (*kvpb.PullShardsResponse, error) {
	return sim.Call(ctx, c.net, c.from, server, func(h any) *kvpb.PullShardsResponse { return h.(*kv.Server).PullShards(ctx, req) })
}

func (c simKV) DeleteShards(ctx context.Context, server int, req *kvpb.DeleteShardsRequest) (*kvpb.DeleteShardsResponse, error) {
	return sim.Call(ctx, c.net, c.from, server, func(h any) *kvpb.DeleteShardsResponse { return h.(*kv.Server).DeleteShards(ctx, req) })
}

type simCtrl struct {
	net  *sim.Network
	from int
}

func (c simCtrl) Do(ctx context.Context, server int, req *ctrlpb.Request) (*ctrlpb.Response, error) {
	return sim.Call(ctx, c.net, c.from, server, func(h any) *ctrlpb.Response { return h.(*shard.Controller).Do(ctx, req) })
}

const ctrlReplicas = 3

func ctrlAddrs() []int { return []int{0, 1, 2} }

// NewShardedCluster starts the controller and all groups (none joined yet).
func NewShardedCluster(opts ShardedOptions) (*ShardedCluster, error) {
	c := &ShardedCluster{Net: sim.New(), opts: opts, servers: map[int]*kv.Server{}, stores: map[int]raft.Storage{}}
	for i := 0; i < ctrlReplicas; i++ {
		ct, err := shard.NewController(shard.ControllerConfig{
			ID:              i,
			Peers:           ctrlAddrs(),
			Storage:         raft.NewMemoryStorage(),
			Transport:       &sim.RaftTransport{Net: c.Net, From: i, Lookup: raftLookup},
			ElectionTimeout: 100 * time.Millisecond,
		})
		if err != nil {
			return nil, err
		}
		c.ctrl = append(c.ctrl, ct)
		c.Net.Register(i, ct)
	}
	for g := 1; g <= opts.Groups; g++ {
		for i := 0; i < opts.ReplicasPerGroup; i++ {
			addr := groupAddr(int64(g), i)
			c.stores[addr] = raft.NewMemoryStorage()
			if err := c.Start(int64(g), i); err != nil {
				c.Shutdown()
				return nil, err
			}
		}
	}
	return c, nil
}

func (c *ShardedCluster) groupPeers(gid int64) []int {
	var p []int
	for i := 0; i < c.opts.ReplicasPerGroup; i++ {
		p = append(p, groupAddr(gid, i))
	}
	return p
}

// GroupServers lists a group's replica addresses as controller IDs.
func (c *ShardedCluster) GroupServers(gid int64) []int32 {
	var ids []int32
	for _, a := range c.groupPeers(gid) {
		ids = append(ids, int32(a))
	}
	return ids
}

// Start boots replica i of group gid.
func (c *ShardedCluster) Start(gid int64, i int) error {
	addr := groupAddr(gid, i)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.servers[addr] != nil {
		return nil
	}
	s, err := kv.NewServer(kv.Config{
		ID:                addr,
		Peers:             c.groupPeers(gid),
		Group:             kv.GroupName(gid),
		Dir:               filepath.Join(c.opts.Dir, fmt.Sprint(addr)),
		RaftStorage:       c.stores[addr],
		Transport:         &sim.RaftTransport{Net: c.Net, From: addr, Lookup: raftLookup},
		ElectionTimeout:   100 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond,
		MaxInflight:       c.opts.MaxInflight,
		MaxRaftLog:        c.opts.MaxRaftLog,
		MemtableSize:      16 << 10,
		GID:               gid,
		Ctrl:              shard.NewCtrlClerk(ctrlAddrs(), simCtrl{c.Net, addr}),
		PeerCaller:        simKV{c.Net, addr},
		PollInterval:      50 * time.Millisecond,
	})
	if err != nil {
		return err
	}
	c.servers[addr] = s
	c.Net.Register(addr, s)
	return nil
}

// Crash stops replica i of group gid.
func (c *ShardedCluster) Crash(gid int64, i int) {
	addr := groupAddr(gid, i)
	c.mu.Lock()
	s := c.servers[addr]
	delete(c.servers, addr)
	c.mu.Unlock()
	if s != nil {
		c.Net.Unregister(addr)
		s.Stop(true)
	}
}

// Shutdown stops everything.
func (c *ShardedCluster) Shutdown() {
	for g := 1; g <= c.opts.Groups; g++ {
		for i := 0; i < c.opts.ReplicasPerGroup; i++ {
			c.Crash(int64(g), i)
		}
	}
	for i, ct := range c.ctrl {
		c.Net.Unregister(i)
		ct.Stop()
	}
}

// Admin returns a controller clerk usable from the test.
func (c *ShardedCluster) Admin() *shard.CtrlClerk {
	return shard.NewCtrlClerk(ctrlAddrs(), simCtrl{c.Net, sim.Client})
}

// Clerk returns a sharded client.
func (c *ShardedCluster) Clerk() *shard.Clerk {
	return shard.NewClerk(c.Admin(), simKV{c.Net, sim.Client})
}

// Settled reports whether every running replica has reached config num with
// no migration in flight.
func (c *ShardedCluster) Settled(num uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.servers {
		st, n := s.ShardStates()
		if n != num {
			return false
		}
		for _, x := range st {
			if x != 0 && x != 1 {
				return false
			}
		}
	}
	return true
}

// RunSharded executes a randomized history with reconfigurations (joins,
// leaves, moves) and optional crashes, then checks linearizability.
func RunSharded(opts ShardedOptions) (*Result, error) {
	if opts.Groups == 0 {
		opts.Groups = 3
	}
	if opts.ReplicasPerGroup == 0 {
		opts.ReplicasPerGroup = 3
	}
	if opts.Clients == 0 {
		opts.Clients = 5
	}
	if opts.Keys == 0 {
		opts.Keys = 10
	}
	if opts.OpsPerClient == 0 {
		opts.OpsPerClient = 100
	}
	if opts.Duration == 0 {
		opts.Duration = 2 * time.Second
	}
	if opts.Dir == "" {
		dir, err := os.MkdirTemp("", "strata-sharded-")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(dir)
		opts.Dir = dir
	}
	rng := rand.New(rand.NewPCG(opts.Seed, opts.Seed^0x51ed270b))
	c, err := NewShardedCluster(opts)
	if err != nil {
		return nil, err
	}
	defer c.Shutdown()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.Admin()
	if err := admin.Join(ctx, map[int64][]int32{1: c.GroupServers(1)}); err != nil {
		return nil, err
	}
	if opts.Unreliable {
		c.Net.SetReliable(false, 0.05, 5*time.Millisecond, 0.01)
	}

	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }
	var hmu sync.Mutex
	var history []porcupine.Operation
	var wg sync.WaitGroup
	for cl := 0; cl < opts.Clients; cl++ {
		seed := rng.Uint64()
		wg.Add(1)
		go func(cl int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, uint64(cl)))
			ck := c.Clerk()
			think := 2 * opts.Duration / time.Duration(opts.OpsPerClient)
			for n := 0; n < opts.OpsPerClient && ctx.Err() == nil; n++ {
				time.Sleep(time.Duration(r.Int64N(int64(think) + 1)))
				key := fmt.Sprint("k", r.IntN(opts.Keys))
				in := KVInput{Key: key, Value: fmt.Sprintf("[%d.%d]", cl, n)}
				var out KVOutput
				call := now()
				var err error
				switch p := r.IntN(10); {
				case p < 5:
					in.Op, in.Value = OpGet, ""
					out.Value, _, err = ck.Get(ctx, key)
				case p < 7:
					in.Op = OpPut
					err = ck.Put(ctx, key, in.Value)
				default:
					in.Op = OpAppend
					err = ck.Append(ctx, key, in.Value)
				}
				ret := now()
				if err != nil {
					if in.Op == OpGet {
						return
					}
					ret = math.MaxInt64
				}
				hmu.Lock()
				history = append(history, porcupine.Operation{ClientId: cl, Input: in, Call: call, Output: out, Return: ret})
				hmu.Unlock()
				if err != nil {
					return
				}
			}
		}(cl)
	}

	res := &Result{}
	joined := map[int64]bool{1: true}
	deadline := time.Now().Add(opts.Duration)
	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(100+rng.IntN(200)) * time.Millisecond)
		gid := int64(1 + rng.IntN(opts.Groups))
		reconfig := true
		switch op := rng.IntN(4); {
		case op <= 1 && !joined[gid]:
			err = admin.Join(ctx, map[int64][]int32{gid: c.GroupServers(gid)})
			joined[gid] = true
		case op == 2 && joined[gid] && len(joined) > 1:
			err = admin.Leave(ctx, gid)
			delete(joined, gid)
		case op == 3 && joined[gid]:
			err = admin.Move(ctx, rng.IntN(kv.NShards), gid)
		default:
			reconfig = false
		}
		if err != nil {
			return nil, err
		}
		if reconfig {
			res.Reconfigs++
		}
		if opts.Crashes && rng.IntN(3) == 0 {
			g, i := int64(1+rng.IntN(opts.Groups)), rng.IntN(opts.ReplicasPerGroup)
			c.Crash(g, i)
			res.Crashes++
			time.Sleep(50 * time.Millisecond)
			if err := c.Start(g, i); err != nil {
				return nil, err
			}
		}
	}
	c.Net.SetReliable(true, 0, 0, 0)
	for g := 1; g <= opts.Groups; g++ {
		for i := 0; i < opts.ReplicasPerGroup; i++ {
			if err := c.Start(int64(g), i); err != nil {
				return nil, err
			}
		}
	}
	// Let clients finish; give up on stragglers after a bound so a stuck run
	// reports what it recorded instead of hanging.
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		cancel()
		<-finished
		res.Stuck = true
	}
	cancel()
	res.Ops = len(history)
	res.History = history
	res.Linearizable = porcupine.CheckOperationsTimeout(KVModel, history, 30*time.Second)
	return res, nil
}
