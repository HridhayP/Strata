package harness

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

func waitSettled(t *testing.T, c *ShardedCluster, num uint64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !c.Settled(num) {
		if time.Now().After(deadline) {
			t.Fatalf("groups did not settle on config %d", num)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestShardedJoinLeaveKeepsData(t *testing.T) {
	c, err := NewShardedCluster(ShardedOptions{Groups: 3, ReplicasPerGroup: 3, Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin := c.Admin()
	if err := admin.Join(ctx, map[int64][]int32{1: c.GroupServers(1)}); err != nil {
		t.Fatal(err)
	}
	ck := c.Clerk()
	want := map[string]string{}
	for i := 0; i < 50; i++ {
		k, v := fmt.Sprint("key", i), fmt.Sprint("v", i)
		if err := ck.Put(ctx, k, v); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	check := func(stage string) {
		for k, v := range want {
			got, _, err := ck.Get(ctx, k)
			if err != nil || got != v {
				t.Fatalf("%s: Get(%s) = %q, %v; want %q", stage, k, got, err, v)
			}
		}
	}
	admin.Join(ctx, map[int64][]int32{2: c.GroupServers(2)})
	admin.Join(ctx, map[int64][]int32{3: c.GroupServers(3)})
	waitSettled(t, c, 3)
	check("after joins")
	for k := range want {
		ck.Append(ctx, k, "+")
		want[k] += "+"
	}
	admin.Leave(ctx, 1)
	waitSettled(t, c, 4)
	check("after leave")

	// The departed group must have garbage-collected every shard.
	cfg, _ := admin.Query(ctx, -1)
	for sh, g := range cfg.Shards {
		if g == 1 {
			t.Fatalf("shard %d still assigned to departed group", sh)
		}
	}
	c.mu.Lock()
	for addr, s := range c.servers {
		if addr/100 == 1 {
			st, _ := s.ShardStates()
			for sh, x := range st {
				if x != 0 {
					t.Fatalf("replica %d still holds shard %d (state %d)", addr, sh, x)
				}
			}
		}
	}
	c.mu.Unlock()
}

func runSharded(t *testing.T, opts ShardedOptions) {
	t.Helper()
	opts.Dir = t.TempDir()
	res, err := RunSharded(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ops=%d reconfigs=%d crashes=%d stuck=%v result=%s", res.Ops, res.Reconfigs, res.Crashes, res.Stuck, res.Linearizable)
	if res.Stuck || res.Ops < 50 {
		t.Fatalf("insufficient progress: ops=%d stuck=%v", res.Ops, res.Stuck)
	}
	if res.Linearizable != porcupine.Ok {
		t.Fatalf("history is %s", res.Linearizable)
	}
}

func TestShardedLinearizableReconfig(t *testing.T) {
	runSharded(t, ShardedOptions{Seed: 11})
}

func TestShardedLinearizableReconfigCrashes(t *testing.T) {
	runSharded(t, ShardedOptions{Seed: 12, Crashes: true, Unreliable: true, MaxRaftLog: 100, Duration: 3 * time.Second})
}
