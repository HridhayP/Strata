package harness

import (
	"context"
	"testing"
	"time"

	"github.com/HridhayP/strata/kv"
	"github.com/anishathalye/porcupine"
)

func TestCheckerRejectsStaleRead(t *testing.T) {
	// put(x,a) completes, then a later get returns "" -- not linearizable.
	h := []porcupine.Operation{
		{ClientId: 0, Input: KVInput{Op: OpPut, Key: "x", Value: "a"}, Call: 0, Return: 10},
		{ClientId: 1, Input: KVInput{Op: OpGet, Key: "x"}, Output: KVOutput{Value: ""}, Call: 20, Return: 30},
	}
	if porcupine.CheckOperations(KVModel, h) {
		t.Fatal("checker accepted a stale read")
	}
}

func TestBasicOps(t *testing.T) {
	c, err := NewCluster(Options{Servers: 3, Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown()
	ck := kv.NewClerk([]int{0, 1, 2}, "", c.Caller())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, ok, err := ck.Get(ctx, "missing"); err != nil || ok {
		t.Fatalf("Get(missing) ok=%v err=%v", ok, err)
	}
	ck.Put(ctx, "a", "1")
	ck.Append(ctx, "a", "2")
	ck.Append(ctx, "a", "3")
	if v, _, err := ck.Get(ctx, "a"); err != nil || v != "123" {
		t.Fatalf("Get(a) = %q, %v", v, err)
	}
}

func runCheck(t *testing.T, opts Options) {
	t.Helper()
	opts.Dir = t.TempDir()
	res, err := Run(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ops=%d crashes=%d partitions=%d result=%s", res.Ops, res.Crashes, res.Partitions, res.Linearizable)
	if res.Ops < 20 {
		t.Fatalf("too little progress: %d ops", res.Ops)
	}
	if res.Linearizable != porcupine.Ok {
		t.Fatalf("history is %s", res.Linearizable)
	}
}

func TestLinearizableReliable(t *testing.T) {
	runCheck(t, Options{Seed: 1})
}

func TestLinearizableUnreliable(t *testing.T) {
	runCheck(t, Options{Seed: 2, Unreliable: true})
}

func TestLinearizablePartitions(t *testing.T) {
	runCheck(t, Options{Seed: 3, Partitions: true, Duration: 2 * time.Second})
}

func TestLinearizableCrashes(t *testing.T) {
	runCheck(t, Options{Seed: 4, Crashes: true, Duration: 2 * time.Second})
}

func TestLinearizableEverythingWithSnapshots(t *testing.T) {
	runCheck(t, Options{Seed: 5, Unreliable: true, Partitions: true, Crashes: true, MaxRaftLog: 50, Duration: 3 * time.Second})
}

func TestLinearizableDiskCrashes(t *testing.T) {
	runCheck(t, Options{Seed: 6, Crashes: true, DiskRaft: true, MaxRaftLog: 100, Duration: 2 * time.Second})
}
