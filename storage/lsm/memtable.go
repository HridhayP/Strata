package lsm

import (
	"bytes"
	"math/rand/v2"
)

// Value kinds stored alongside every key.
const (
	kindDelete byte = 0
	kindPut    byte = 1
)

const maxHeight = 16

type skipNode struct {
	key   []byte
	value []byte
	kind  byte
	next  []*skipNode
}

// memtable is a skiplist ordered by key. Inserting an existing key replaces
// its value in place, so the table holds only the newest version. It is not
// synchronized; the DB guards it.
type memtable struct {
	head   *skipNode
	height int
	size   int // approximate bytes
	count  int
}

func newMemtable() *memtable {
	return &memtable{head: &skipNode{next: make([]*skipNode, maxHeight)}, height: 1}
}

func randomHeight() int {
	h := 1
	for h < maxHeight && rand.N(4) == 0 {
		h++
	}
	return h
}

// findGE fills prev with the rightmost node before key at each level and
// returns the first node with node.key >= key.
func (m *memtable) findGE(key []byte, prev []*skipNode) *skipNode {
	x := m.head
	for lvl := m.height - 1; lvl >= 0; lvl-- {
		for n := x.next[lvl]; n != nil && bytes.Compare(n.key, key) < 0; n = x.next[lvl] {
			x = n
		}
		if prev != nil {
			prev[lvl] = x
		}
	}
	return x.next[0]
}

func (m *memtable) put(key, value []byte, kind byte) {
	var prev [maxHeight]*skipNode
	n := m.findGE(key, prev[:])
	if n != nil && bytes.Equal(n.key, key) {
		m.size += len(value) - len(n.value)
		n.value, n.kind = value, kind
		return
	}
	h := randomHeight()
	if h > m.height {
		for lvl := m.height; lvl < h; lvl++ {
			prev[lvl] = m.head
		}
		m.height = h
	}
	n = &skipNode{key: key, value: value, kind: kind, next: make([]*skipNode, h)}
	for lvl := 0; lvl < h; lvl++ {
		n.next[lvl] = prev[lvl].next[lvl]
		prev[lvl].next[lvl] = n
	}
	m.size += len(key) + len(value) + 16
	m.count++
}

func (m *memtable) get(key []byte) (value []byte, kind byte, ok bool) {
	n := m.findGE(key, nil)
	if n != nil && bytes.Equal(n.key, key) {
		return n.value, n.kind, true
	}
	return nil, 0, false
}

// entries copies out all entries with the given prefix, in key order.
func (m *memtable) entries(prefix []byte) []entry {
	var out []entry
	for n := m.findGE(prefix, nil); n != nil && bytes.HasPrefix(n.key, prefix); n = n.next[0] {
		out = append(out, entry{key: n.key, value: n.value, kind: n.kind})
	}
	return out
}

type entry struct {
	key, value []byte
	kind       byte
}
