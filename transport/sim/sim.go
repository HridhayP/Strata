// Package sim is an in-memory network for fault-injection tests. Endpoints
// register a handler object under an integer address; callers send RPCs
// through the network, which can partition nodes, take them down, drop
// requests or replies, add random latency and occasionally hold a message
// back long enough to reorder it behind later ones.
package sim

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HridhayP/strata/proto/raftpb"
	"google.golang.org/protobuf/proto"
)

// ErrUnreachable is returned when the destination is down, partitioned
// away, or the message was dropped.
var ErrUnreachable = errors.New("sim: unreachable")

// Client is an address that is never partitioned: test clients use it so
// that only server-to-server links are cut by Partition.
const Client = -1

// Network is safe for concurrent use.
type Network struct {
	mu        sync.RWMutex
	handlers  map[int]any
	partition map[int]int // address -> partition id; missing means 0
	reliable  bool
	dropRate  float64
	maxDelay  time.Duration
	reorder   float64 // probability of a long delay

	rpcs atomic.Int64
}

// New returns a reliable network with no latency.
func New() *Network {
	return &Network{handlers: make(map[int]any), partition: make(map[int]int), reliable: true}
}

// SetReliable toggles fault injection on message delivery. When unreliable,
// each message is delayed up to maxDelay, dropped with probability drop and
// held back for a long time with probability reorder.
func (n *Network) SetReliable(reliable bool, drop float64, maxDelay time.Duration, reorder float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reliable, n.dropRate, n.maxDelay, n.reorder = reliable, drop, maxDelay, reorder
}

// Register attaches a handler to addr (a node coming up).
func (n *Network) Register(addr int, h any) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[addr] = h
}

// Unregister detaches addr (a node crashing). In-flight calls to it fail.
func (n *Network) Unregister(addr int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, addr)
}

// Partition splits the addresses into groups that can only talk within
// themselves. Addresses not listed join group 0 with the first group.
func (n *Network) Partition(groups ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partition = make(map[int]int)
	for i, g := range groups {
		for _, a := range g {
			n.partition[a] = i
		}
	}
}

// Heal removes all partitions.
func (n *Network) Heal() { n.Partition() }

// RPCCount is the number of calls attempted.
func (n *Network) RPCCount() int64 { return n.rpcs.Load() }

// connected reports whether a message can flow between the addresses. A
// sender that is not registered (crashed or disconnected) cannot send.
// Callers hold n.mu.
func (n *Network) connected(from, to int) bool {
	if from == Client || to == Client {
		return true
	}
	if _, up := n.handlers[from]; !up {
		return false
	}
	return n.partition[from] == n.partition[to]
}

func (n *Network) fault() (delay time.Duration, drop bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.reliable {
		return 0, false
	}
	if rand.Float64() < n.dropRate {
		return 0, true
	}
	if n.maxDelay > 0 {
		delay = rand.N(n.maxDelay)
	}
	if rand.Float64() < n.reorder {
		delay += 200*time.Millisecond + rand.N(1500*time.Millisecond)
	}
	return delay, false
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Call delivers a request from one address to another. fn runs on the
// destination's handler and must not retain the request.
func Call[Resp any](ctx context.Context, n *Network, from, to int, fn func(h any) Resp) (Resp, error) {
	var zero Resp
	n.rpcs.Add(1)

	delay, drop := n.fault()
	if err := sleepCtx(ctx, delay); err != nil {
		return zero, err
	}
	n.mu.RLock()
	h, up := n.handlers[to]
	ok := up && n.connected(from, to)
	n.mu.RUnlock()
	if drop || !ok {
		// Behave like a lost packet: the caller waits for its timeout,
		// but not unbounded, so tests do not stall forever on a down node.
		sleepCtx(ctx, 20*time.Millisecond)
		return zero, ErrUnreachable
	}

	done := make(chan Resp, 1)
	go func() { done <- fn(h) }()
	var resp Resp
	select {
	case resp = <-done:
	case <-ctx.Done():
		return zero, ctx.Err()
	}

	delay, drop = n.fault()
	if err := sleepCtx(ctx, delay); err != nil {
		return zero, err
	}
	n.mu.RLock()
	_, up = n.handlers[to]
	ok = up && n.handlers[to] == h && n.connected(from, to)
	n.mu.RUnlock()
	if drop || !ok {
		return zero, ErrUnreachable
	}
	return resp, nil
}

// RaftHandler is implemented by *raft.Raft.
type RaftHandler interface {
	HandleRequestVote(*raftpb.RequestVoteRequest) *raftpb.RequestVoteResponse
	HandleAppendEntries(*raftpb.AppendEntriesRequest) *raftpb.AppendEntriesResponse
	HandleInstallSnapshot(*raftpb.InstallSnapshotRequest) *raftpb.InstallSnapshotResponse
}

// RaftTransport adapts the network to raft.Transport for the node at From.
// Requests and replies are deep-copied so nodes never share memory.
type RaftTransport struct {
	Net  *Network
	From int
	// Lookup maps a destination to the handler object registered at its
	// address; by default the registered object itself must be a RaftHandler.
	Lookup func(h any) RaftHandler
}

func (t *RaftTransport) handler(h any) RaftHandler {
	if t.Lookup != nil {
		return t.Lookup(h)
	}
	return h.(RaftHandler)
}

func clone[M proto.Message](m M) M { return proto.Clone(m).(M) }

func (t *RaftTransport) RequestVote(ctx context.Context, to int, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return Call(ctx, t.Net, t.From, to, func(h any) *raftpb.RequestVoteResponse {
		return clone(t.handler(h).HandleRequestVote(clone(req)))
	})
}

func (t *RaftTransport) AppendEntries(ctx context.Context, to int, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return Call(ctx, t.Net, t.From, to, func(h any) *raftpb.AppendEntriesResponse {
		return clone(t.handler(h).HandleAppendEntries(clone(req)))
	})
}

func (t *RaftTransport) InstallSnapshot(ctx context.Context, to int, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return Call(ctx, t.Net, t.From, to, func(h any) *raftpb.InstallSnapshotResponse {
		return clone(t.handler(h).HandleInstallSnapshot(clone(req)))
	})
}
