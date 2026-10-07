package kv

import (
	"encoding/binary"
	"hash/fnv"
)

// NShards is the fixed number of shards the keyspace is split into. Shards
// (not keys) are the unit of assignment to replica groups and migration.
const NShards = 64

// KeyShard maps a key to its shard.
func KeyShard(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % NShards)
}

// LSM key layout. Everything a shard owns lives under its own prefix so a
// shard can be exported, imported or deleted with a prefix scan.
//
//	'd' shard(2) userkey      -> value
//	'c' shard(2) clientID(8)  -> last applied seq (8)
//	"m/applied"               -> last applied Raft index (8)
const (
	tagData  = 'd'
	tagDedup = 'c'
)

var appliedKey = []byte("m/applied")

func shardPrefix(tag byte, shard int) []byte {
	return []byte{tag, byte(shard >> 8), byte(shard)}
}

func dataKey(shard int, key string) []byte {
	return append(shardPrefix(tagData, shard), key...)
}

func dedupKey(shard int, client uint64) []byte {
	return binary.BigEndian.AppendUint64(shardPrefix(tagDedup, shard), client)
}

func u64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

func readU64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
