// Package shard assigns the keyspace's shards to replica groups and runs
// the shard controller, a Raft-replicated service that stores the history
// of configurations.
package shard

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"slices"
	"sort"

	"github.com/HridhayP/strata/kv"
)

// VirtualNodes is the number of points each group places on the ring. More
// points even out the load at the cost of a larger ring.
const VirtualNodes = 64

type point struct {
	hash uint64
	gid  int64
}

func hash64(parts ...uint64) uint64 {
	h := fnv.New64a()
	var b [8]byte
	for _, p := range parts {
		binary.LittleEndian.PutUint64(b[:], p)
		h.Write(b[:])
	}
	// fnv's low bits mix poorly for small inputs; finalize like splitmix64.
	x := h.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// Ring is a consistent-hash ring of replica groups.
type Ring struct {
	points []point
}

// NewRing places VirtualNodes points per group.
func NewRing(gids []int64) *Ring {
	r := &Ring{}
	for _, g := range gids {
		for v := 0; v < VirtualNodes; v++ {
			r.points = append(r.points, point{hash: hash64(uint64(g), uint64(v)), gid: g})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash != r.points[j].hash {
			return r.points[i].hash < r.points[j].hash
		}
		return r.points[i].gid < r.points[j].gid
	})
	return r
}

// LoadFactor bounds each group's shard count at ceil(LoadFactor * average)
// ("consistent hashing with bounded loads"). Plain consistent hashing over
// only 64 shards leaves some groups with several times the average.
const LoadFactor = 1.25

func shardHash(shard int) uint64 { return hash64(0x5348415244, uint64(shard)) } // "SHARD"

// successor returns the index of the first point clockwise from h.
func (r *Ring) successor(h uint64) int {
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		i = 0
	}
	return i
}

// Owner returns the unbounded owner of a shard: the first point clockwise
// from the shard's position.
func (r *Ring) Owner(shard int) int64 {
	if len(r.points) == 0 {
		return 0
	}
	return r.points[r.successor(shardHash(shard))].gid
}

// Assign computes the shard -> group table for the given groups, honoring
// pins whose group is still present. Each shard goes to the first group
// clockwise from it that is below the load cap, so adding or removing a group
// moves few shards while keeping every group near the average.
func Assign(gids []int64, pinned map[int32]int64) []int64 {
	gids = slices.Clone(gids)
	slices.Sort(gids)
	out := make([]int64, kv.NShards)
	if len(gids) == 0 {
		return out
	}
	present := map[int64]bool{}
	for _, g := range gids {
		present[g] = true
	}
	capacity := int(math.Ceil(LoadFactor * float64(kv.NShards) / float64(len(gids))))
	load := map[int64]int{}
	for s := range out {
		if g, ok := pinned[int32(s)]; ok && present[g] {
			out[s] = g
			load[g]++
		}
	}
	r := NewRing(gids)
	for s := range out {
		if out[s] != 0 {
			continue
		}
		for i, n := r.successor(shardHash(s)), 0; n < len(r.points); i, n = (i+1)%len(r.points), n+1 {
			if g := r.points[i].gid; load[g] < capacity {
				out[s] = g
				load[g]++
				break
			}
		}
	}
	return out
}
