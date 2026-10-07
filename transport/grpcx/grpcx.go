// Package grpcx carries Raft, KV, shard-migration and controller RPCs over
// gRPC. One process runs one gRPC server (a Host) that may serve a replica
// of the controller group and replicas of KV groups; requests are routed to
// the right local replica by group name.
package grpcx

import (
	"context"
	"fmt"
	"sync"

	"github.com/HridhayP/strata/kv"
	"github.com/HridhayP/strata/proto/ctrlpb"
	"github.com/HridhayP/strata/proto/kvpb"
	"github.com/HridhayP/strata/proto/raftpb"
	"github.com/HridhayP/strata/raft"
	"github.com/HridhayP/strata/shard"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// CtrlGroup is the routing name of the controller's Raft group.
const CtrlGroup = "ctrl"

// Host routes incoming RPCs to the replicas running in this process.
type Host struct {
	raftpb.UnimplementedRaftServer
	kvpb.UnimplementedKVServer

	mu    sync.RWMutex
	rafts map[string]*raft.Raft
	kvs   map[string]*kv.Server
	ctrl  *shard.Controller
}

// NewHost returns an empty host.
func NewHost() *Host {
	return &Host{rafts: map[string]*raft.Raft{}, kvs: map[string]*kv.Server{}}
}

// Register attaches a gRPC server to the host's services.
func (h *Host) Register(s *grpc.Server) {
	raftpb.RegisterRaftServer(s, h)
	kvpb.RegisterKVServer(s, h)
	ctrlpb.RegisterControllerServer(s, ctrlService{h: h})
}

// AddKV makes a KV replica reachable under group.
func (h *Host) AddKV(group string, s *kv.Server) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.kvs[group] = s
	h.rafts[group] = s.Raft()
}

// SetController makes the controller replica reachable.
func (h *Host) SetController(c *shard.Controller) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ctrl = c
	h.rafts[CtrlGroup] = c.Raft()
}

func (h *Host) raftFor(group string) (*raft.Raft, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if r := h.rafts[group]; r != nil {
		return r, nil
	}
	return nil, status.Errorf(codes.NotFound, "no raft group %q here", group)
}

func (h *Host) kvFor(group string) (*kv.Server, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if s := h.kvs[group]; s != nil {
		return s, nil
	}
	return nil, status.Errorf(codes.NotFound, "no kv group %q here", group)
}

func (h *Host) RequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	r, err := h.raftFor(req.Group)
	if err != nil {
		return nil, err
	}
	return r.HandleRequestVote(req), nil
}

func (h *Host) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	r, err := h.raftFor(req.Group)
	if err != nil {
		return nil, err
	}
	return r.HandleAppendEntries(req), nil
}

func (h *Host) InstallSnapshot(_ context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	r, err := h.raftFor(req.Group)
	if err != nil {
		return nil, err
	}
	return r.HandleInstallSnapshot(req), nil
}

func (h *Host) Do(ctx context.Context, req *kvpb.Request) (*kvpb.Response, error) {
	s, err := h.kvFor(req.Group)
	if err != nil {
		return nil, err
	}
	return s.Do(ctx, req), nil
}

func (h *Host) PullShards(ctx context.Context, req *kvpb.PullShardsRequest) (*kvpb.PullShardsResponse, error) {
	s, err := h.kvFor(req.Group)
	if err != nil {
		return nil, err
	}
	return s.PullShards(ctx, req), nil
}

func (h *Host) DeleteShards(ctx context.Context, req *kvpb.DeleteShardsRequest) (*kvpb.DeleteShardsResponse, error) {
	s, err := h.kvFor(req.Group)
	if err != nil {
		return nil, err
	}
	return s.DeleteShards(ctx, req), nil
}

// ctrlService adapts the controller to its gRPC interface; its Do method
// would otherwise clash with the KV service's Do on Host.
type ctrlService struct {
	ctrlpb.UnimplementedControllerServer
	h *Host
}

func (c ctrlService) Do(ctx context.Context, req *ctrlpb.Request) (*ctrlpb.Response, error) {
	c.h.mu.RLock()
	ct := c.h.ctrl
	c.h.mu.RUnlock()
	if ct == nil {
		return nil, status.Error(codes.NotFound, "no controller here")
	}
	return ct.Do(ctx, req), nil
}

// Pool keeps one client connection per node address.
type Pool struct {
	addrs map[int]string

	mu    sync.Mutex
	conns map[int]*grpc.ClientConn
}

// NewPool returns a pool for the given node ID -> address book.
func NewPool(addrs map[int]string) *Pool {
	return &Pool{addrs: addrs, conns: map[int]*grpc.ClientConn{}}
}

func (p *Pool) conn(id int) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c := p.conns[id]; c != nil {
		return c, nil
	}
	addr, ok := p.addrs[id]
	if !ok {
		return nil, fmt.Errorf("grpcx: unknown node %d", id)
	}
	c, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256<<20), grpc.MaxCallSendMsgSize(256<<20)))
	if err != nil {
		return nil, err
	}
	p.conns[id] = c
	return c, nil
}

// Close closes every connection.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
	p.conns = map[int]*grpc.ClientConn{}
}

// RaftTransport implements raft.Transport. Raft fills in the group name of
// each request, so one transport can serve every group in the process.
type RaftTransport struct {
	Pool *Pool
}

func (t *RaftTransport) RequestVote(ctx context.Context, to int, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	c, err := t.Pool.conn(to)
	if err != nil {
		return nil, err
	}
	return raftpb.NewRaftClient(c).RequestVote(ctx, req)
}

func (t *RaftTransport) AppendEntries(ctx context.Context, to int, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	c, err := t.Pool.conn(to)
	if err != nil {
		return nil, err
	}
	return raftpb.NewRaftClient(c).AppendEntries(ctx, req)
}

func (t *RaftTransport) InstallSnapshot(ctx context.Context, to int, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	c, err := t.Pool.conn(to)
	if err != nil {
		return nil, err
	}
	return raftpb.NewRaftClient(c).InstallSnapshot(ctx, req)
}

// KVCaller implements kv.Caller and kv.PeerCaller.
type KVCaller struct{ Pool *Pool }

func (k KVCaller) Do(ctx context.Context, server int, req *kvpb.Request) (*kvpb.Response, error) {
	c, err := k.Pool.conn(server)
	if err != nil {
		return nil, err
	}
	return kvpb.NewKVClient(c).Do(ctx, req)
}

func (k KVCaller) PullShards(ctx context.Context, server int, req *kvpb.PullShardsRequest) (*kvpb.PullShardsResponse, error) {
	c, err := k.Pool.conn(server)
	if err != nil {
		return nil, err
	}
	return kvpb.NewKVClient(c).PullShards(ctx, req)
}

func (k KVCaller) DeleteShards(ctx context.Context, server int, req *kvpb.DeleteShardsRequest) (*kvpb.DeleteShardsResponse, error) {
	c, err := k.Pool.conn(server)
	if err != nil {
		return nil, err
	}
	return kvpb.NewKVClient(c).DeleteShards(ctx, req)
}

// CtrlCaller implements shard.CtrlCaller.
type CtrlCaller struct{ Pool *Pool }

func (k CtrlCaller) Do(ctx context.Context, server int, req *ctrlpb.Request) (*ctrlpb.Response, error) {
	c, err := k.Pool.conn(server)
	if err != nil {
		return nil, err
	}
	return ctrlpb.NewControllerClient(c).Do(ctx, req)
}
