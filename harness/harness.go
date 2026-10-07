// Package harness runs KV clusters on the simulated network under injected
// faults (partitions, crashes and restarts, message loss, delay and
// reordering), records every client operation, and checks the resulting
// history for linearizability with porcupine.
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
	"github.com/HridhayP/strata/proto/kvpb"
	"github.com/HridhayP/strata/raft"
	"github.com/HridhayP/strata/storage/wal"
	"github.com/HridhayP/strata/transport/sim"
	"github.com/anishathalye/porcupine"
)

// Options describes one randomized run.
type Options struct {
	Servers  int
	Clients  int
	Keys     int
	Duration time.Duration // fault phase length

	Unreliable bool // drop, delay and reorder messages
	Partitions bool
	Crashes    bool
	// DiskRaft persists Raft to real files (crashes drop unsynced bytes);
	// otherwise Raft state is kept in memory across simulated crashes.
	DiskRaft bool

	// OpsPerClient caps each client's operations; clients pace themselves
	// to spread them over the fault phase. Keeps histories checkable.
	OpsPerClient int

	MaxRaftLog      int // small values force frequent compaction/snapshots
	ElectionTimeout time.Duration
	Dir             string
	Seed            uint64
}

func (o *Options) defaults() {
	if o.Servers == 0 {
		o.Servers = 5
	}
	if o.Clients == 0 {
		o.Clients = 5
	}
	if o.Keys == 0 {
		o.Keys = 3
	}
	if o.Duration == 0 {
		o.Duration = time.Second
	}
	if o.OpsPerClient == 0 {
		o.OpsPerClient = 100
	}
	if o.ElectionTimeout == 0 {
		o.ElectionTimeout = 100 * time.Millisecond
	}
}

// Cluster is a KV replica group on a simulated network.
type Cluster struct {
	Net  *sim.Network
	opts Options

	mu      sync.Mutex
	servers []*kv.Server
	stores  []raft.Storage
	dirs    []string
}

// NewCluster starts opts.Servers replicas.
func NewCluster(opts Options) (*Cluster, error) {
	opts.defaults()
	c := &Cluster{
		Net:     sim.New(),
		opts:    opts,
		servers: make([]*kv.Server, opts.Servers),
		stores:  make([]raft.Storage, opts.Servers),
		dirs:    make([]string, opts.Servers),
	}
	for i := range c.servers {
		c.dirs[i] = filepath.Join(opts.Dir, fmt.Sprint("s", i))
		if !opts.DiskRaft {
			c.stores[i] = raft.NewMemoryStorage()
		}
		if err := c.Start(i); err != nil {
			c.Shutdown()
			return nil, err
		}
	}
	return c, nil
}

func (c *Cluster) peers() []int {
	p := make([]int, c.opts.Servers)
	for i := range p {
		p[i] = i
	}
	return p
}

// Start boots replica i from its persisted state.
func (c *Cluster) Start(i int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.servers[i] != nil {
		return nil
	}
	if c.opts.DiskRaft {
		ds, err := raft.OpenDiskStorage(filepath.Join(c.dirs[i], "raft"), wal.Options{})
		if err != nil {
			return err
		}
		c.stores[i] = ds
	}
	s, err := kv.NewServer(kv.Config{
		ID:          i,
		Peers:       c.peers(),
		Dir:         filepath.Join(c.dirs[i], "lsm"),
		RaftStorage: c.stores[i],
		Transport: &sim.RaftTransport{Net: c.Net, From: i, Lookup: func(h any) sim.RaftHandler {
			return h.(*kv.Server).Raft()
		}},
		ElectionTimeout:   c.opts.ElectionTimeout,
		HeartbeatInterval: c.opts.ElectionTimeout / 5,
		MaxRaftLog:        c.opts.MaxRaftLog,
		MemtableSize:      16 << 10,
	})
	if err != nil {
		return err
	}
	c.servers[i] = s
	c.Net.Register(i, s)
	return nil
}

// Crash stops replica i abruptly.
func (c *Cluster) Crash(i int) {
	c.mu.Lock()
	s := c.servers[i]
	c.servers[i] = nil
	c.mu.Unlock()
	if s == nil {
		return
	}
	c.Net.Unregister(i)
	s.Stop(true)
	if ds, ok := c.stores[i].(*raft.DiskStorage); ok {
		ds.CrashClose()
	}
}

// Up reports whether replica i is running.
func (c *Cluster) Up(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i] != nil
}

// Shutdown stops every replica.
func (c *Cluster) Shutdown() {
	for i := range c.servers {
		c.Crash(i)
	}
}

// Caller returns a kv.Caller that reaches replicas over the network.
func (c *Cluster) Caller() kv.Caller { return simCaller{c.Net} }

type simCaller struct{ net *sim.Network }

func (s simCaller) Do(ctx context.Context, server int, req *kvpb.Request) (*kvpb.Response, error) {
	return sim.Call(ctx, s.net, sim.Client, server, func(h any) *kvpb.Response {
		return h.(*kv.Server).Do(ctx, req)
	})
}

// Result summarizes a run.
type Result struct {
	Ops          int
	Linearizable porcupine.CheckResult
	Crashes      int
	Partitions   int
	Reconfigs    int
	Stuck        bool // clients had not finished when the run gave up
	History      []porcupine.Operation
}

// Run executes one randomized history and checks it.
func Run(opts Options) (*Result, error) {
	opts.defaults()
	if opts.Dir == "" {
		dir, err := os.MkdirTemp("", "strata-harness-")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(dir)
		opts.Dir = dir
	}
	rng := rand.New(rand.NewPCG(opts.Seed, opts.Seed^0x9e3779b97f4a7c15))
	c, err := NewCluster(opts)
	if err != nil {
		return nil, err
	}
	defer c.Shutdown()
	if opts.Unreliable {
		c.Net.SetReliable(false, 0.05, 5*time.Millisecond, 0.01)
	}

	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }
	var (
		hmu     sync.Mutex
		history []porcupine.Operation
	)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for cl := 0; cl < opts.Clients; cl++ {
		seed := rng.Uint64()
		wg.Add(1)
		go func(cl int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, uint64(cl)))
			ck := kv.NewClerk(c.peers(), "", c.Caller())
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
					in.Op = OpGet
					in.Value = ""
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
						return // an unfinished read constrains nothing
					}
					// An unfinished write may or may not have taken effect.
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
	deadline := time.Now().Add(opts.Duration)
	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(50+rng.IntN(150)) * time.Millisecond)
		if opts.Partitions && rng.IntN(2) == 0 {
			perm := rng.Perm(opts.Servers)
			cut := 1 + rng.IntN(opts.Servers-1)
			c.Net.Partition(perm[:cut], perm[cut:])
			res.Partitions++
		} else if opts.Partitions {
			c.Net.Heal()
		}
		if opts.Crashes && rng.IntN(3) == 0 {
			i := rng.IntN(opts.Servers)
			if c.Up(i) {
				c.Crash(i)
				res.Crashes++
			} else if err := c.Start(i); err != nil {
				cancel()
				wg.Wait()
				return nil, err
			}
		}
	}
	// Recovery phase: heal everything so outstanding operations finish.
	c.Net.Heal()
	c.Net.SetReliable(true, 0, 0, 0)
	for i := 0; i < opts.Servers; i++ {
		if err := c.Start(i); err != nil {
			cancel()
			wg.Wait()
			return nil, err
		}
	}
	time.Sleep(300 * time.Millisecond)
	cancel()
	wg.Wait()

	res.Ops = len(history)
	res.History = history
	res.Linearizable = porcupine.CheckOperationsTimeout(KVModel, history, 30*time.Second)
	return res, nil
}
