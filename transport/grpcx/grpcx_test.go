package grpcx

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/HridhayP/strata/kv"
	"github.com/HridhayP/strata/raft"
	"google.golang.org/grpc"
)

type node struct {
	gs   *grpc.Server
	kv   *kv.Server
	pool *Pool
}

// TestKVOverLoopback runs a 3-replica KV group over real gRPC connections,
// then stops the leader and checks the survivors keep serving its data.
func TestKVOverLoopback(t *testing.T) {
	const n = 3
	lis := make([]net.Listener, n)
	addrs := map[int]string{}
	for i := range lis {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lis[i] = l
		addrs[i+1] = l.Addr().String()
	}
	peers := []int{1, 2, 3}
	group := kv.GroupName(1)
	dir := t.TempDir()
	nodes := make([]*node, n)
	for i := range nodes {
		id := i + 1
		pool := NewPool(addrs)
		s, err := kv.NewServer(kv.Config{
			ID: id, Peers: peers, Group: group,
			Dir:             filepath.Join(dir, fmt.Sprint(id)),
			RaftStorage:     raft.NewMemoryStorage(),
			Transport:       &RaftTransport{Pool: pool},
			ElectionTimeout: 150 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		host := NewHost()
		host.AddKV(group, s)
		gs := grpc.NewServer()
		host.Register(gs)
		go gs.Serve(lis[i])
		nodes[i] = &node{gs: gs, kv: s, pool: pool}
	}
	stop := func(nd *node) {
		if nd.gs != nil {
			nd.gs.Stop()
			nd.kv.Stop(false)
			nd.pool.Close()
			nd.gs = nil
		}
	}
	t.Cleanup(func() {
		for _, nd := range nodes {
			stop(nd)
		}
	})

	clientPool := NewPool(addrs)
	defer clientPool.Close()
	ck := kv.NewClerk(peers, group, KVCaller{Pool: clientPool})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for i := 0; i < 50; i++ {
		if err := ck.Put(ctx, fmt.Sprint("k", i), fmt.Sprint("v", i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := ck.Append(ctx, "k0", "+x"); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := ck.Get(ctx, "k0"); err != nil || !ok || v != "v0+x" {
		t.Fatalf("Get(k0) = %q, %v, %v", v, ok, err)
	}

	leader := -1
	for i, nd := range nodes {
		if _, isLeader := nd.kv.Raft().State(); isLeader {
			leader = i
		}
	}
	if leader < 0 {
		t.Fatal("no leader")
	}
	stop(nodes[leader])

	if err := ck.Put(ctx, "after", "failover"); err != nil {
		t.Fatalf("put after leader stop: %v", err)
	}
	for i := 0; i < 50; i++ {
		v, ok, err := ck.Get(ctx, fmt.Sprint("k", i))
		want := fmt.Sprint("v", i)
		if i == 0 {
			want += "+x"
		}
		if err != nil || !ok || v != want {
			t.Fatalf("Get(k%d) after failover = %q, %v, %v; want %q", i, v, ok, err, want)
		}
	}
}
