package shard

import (
	"math"
	"testing"

	"github.com/HridhayP/strata/kv"
)

func counts(a []int64) map[int64]int {
	c := map[int64]int{}
	for _, g := range a {
		c[g]++
	}
	return c
}

func moved(a, b []int64) int {
	n := 0
	for i := range a {
		if a[i] != b[i] {
			n++
		}
	}
	return n
}

func TestAssignDeterministicAndComplete(t *testing.T) {
	a := Assign([]int64{3, 1, 2}, nil)
	b := Assign([]int64{1, 2, 3}, nil)
	if moved(a, b) != 0 {
		t.Fatal("assignment depends on group order")
	}
	for s, g := range a {
		if g < 1 || g > 3 {
			t.Fatalf("shard %d assigned to %d", s, g)
		}
	}
	if got := Assign(nil, nil); got[0] != 0 {
		t.Fatal("no groups should leave shards unassigned")
	}
}

func TestJoinMovesOnlyShardsToNewGroup(t *testing.T) {
	gids := []int64{}
	prev := Assign(gids, nil)
	for g := int64(1); g <= 8; g++ {
		gids = append(gids, g)
		next := Assign(gids, nil)
		// Ideal movement is 64/g shards; bounded loads may cascade a few more.
		if m := moved(prev, next); g > 1 && m > 2*kv.NShards/int(g)+4 {
			t.Fatalf("join of %d moved %d shards", g, m)
		}
		t.Logf("join %d: moved %d/%d shards, counts %v", g, moved(prev, next), kv.NShards, counts(next))
		prev = next
	}
}

func TestLeaveMovesOnlyLeavingGroupsShards(t *testing.T) {
	all := []int64{1, 2, 3, 4, 5}
	before := Assign(all, nil)
	after := Assign([]int64{1, 2, 4, 5}, nil)
	if m := moved(before, after); m > 2*kv.NShards/5+4 {
		t.Fatalf("leave moved %d shards", m)
	}
	for s := range before {
		if after[s] == 3 {
			t.Fatalf("shard %d still on departed group", s)
		}
	}
}

func TestBalanceWithVirtualNodes(t *testing.T) {
	for n := 1; n <= 10; n++ {
		var gids []int64
		for g := 1; g <= n; g++ {
			gids = append(gids, int64(g))
		}
		limit := int(math.Ceil(LoadFactor * float64(kv.NShards) / float64(n)))
		for g, c := range counts(Assign(gids, nil)) {
			if c > limit {
				t.Fatalf("%d groups: group %d owns %d > cap %d", n, g, c, limit)
			}
		}
	}
}

func TestPinsHonoredWhileGroupPresent(t *testing.T) {
	a := Assign([]int64{1, 2}, map[int32]int64{5: 2, 6: 1})
	if a[5] != 2 || a[6] != 1 {
		t.Fatalf("pins ignored: %d %d", a[5], a[6])
	}
	b := Assign([]int64{1}, map[int32]int64{5: 2})
	if b[5] != 1 {
		t.Fatal("pin to departed group kept")
	}
}
