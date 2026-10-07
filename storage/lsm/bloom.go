package lsm

import "hash/fnv"

// bloom is a standard Bloom filter using double hashing (Kirsch-Mitzenmacher):
// probe i is h1 + i*h2, with h1/h2 the halves of a 64-bit FNV-1a hash.
type bloom struct {
	bits []byte
	k    uint32
}

const bloomBitsPerKey = 10 // ~1% false-positive rate with k=7

func newBloom(n int) *bloom {
	nbits := max(64, n*bloomBitsPerKey)
	return &bloom{bits: make([]byte, (nbits+7)/8), k: 7}
}

func bloomHash(key []byte) (uint32, uint32) {
	h := fnv.New64a()
	h.Write(key)
	s := h.Sum64()
	return uint32(s), uint32(s>>32) | 1
}

func (b *bloom) add(key []byte) {
	h1, h2 := bloomHash(key)
	n := uint32(len(b.bits) * 8)
	for i := uint32(0); i < b.k; i++ {
		p := (h1 + i*h2) % n
		b.bits[p/8] |= 1 << (p % 8)
	}
}

func (b *bloom) mayContain(key []byte) bool {
	if len(b.bits) == 0 {
		return true
	}
	h1, h2 := bloomHash(key)
	n := uint32(len(b.bits) * 8)
	for i := uint32(0); i < b.k; i++ {
		p := (h1 + i*h2) % n
		if b.bits[p/8]&(1<<(p%8)) == 0 {
			return false
		}
	}
	return true
}

// encode appends k as the last byte.
func (b *bloom) encode() []byte { return append(append([]byte(nil), b.bits...), byte(b.k)) }

func decodeBloom(data []byte) *bloom {
	if len(data) < 1 {
		return &bloom{}
	}
	return &bloom{bits: data[:len(data)-1], k: uint32(data[len(data)-1])}
}
