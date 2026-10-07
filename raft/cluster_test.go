package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/HridhayP/strata/storage/wal"
	"github.com/HridhayP/strata/transport/sim"
)

// cluster runs n Raft nodes on a simulated network and checks that every
// node applies the same command at the same index.
type cluster struct {
	t         *testing.T
	n         int
	net       *sim.Network
	snapEvery int // >0: nodes snapshot every snapEvery applied entries
	inflight  int // Config.MaxInflight
	disk      bool
	dirs      []string

	mu       sync.Mutex
	rafts    []*Raft
	stores   []Storage
	logs     []map[uint64]string // per node: index -> command
	applied  []uint64
	stops    []chan struct{}
	applyErr error
}

func newCluster(t *testing.T, n int, opts ...func(*cluster)) *cluster {
	c := &cluster{
		t:       t,
		n:       n,
		net:     sim.New(),
		rafts:   make([]*Raft, n),
		stores:  make([]Storage, n),
		logs:    make([]map[uint64]string, n),
		applied: make([]uint64, n),
		stops:   make([]chan struct{}, n),
		dirs:    make([]string, n),
	}
	for _, o := range opts {
		o(c)
	}
	for i := 0; i < n; i++ {
		c.dirs[i] = filepath.Join(t.TempDir(), fmt.Sprint(i))
		if !c.disk {
			c.stores[i] = NewMemoryStorage()
		}
		c.start(i)
	}
	t.Cleanup(c.shutdown)
	return c
}

func withSnapshots(every int) func(*cluster) { return func(c *cluster) { c.snapEvery = every } }
func withDisk() func(*cluster)               { return func(c *cluster) { c.disk = true } }
func withInflight(n int) func(*cluster)      { return func(c *cluster) { c.inflight = n } }

// bothReplicationModes runs f with one AppendEntries in flight per follower
// and with a pipelined window.
func bothReplicationModes(t *testing.T, f func(t *testing.T, opt func(*cluster))) {
	for _, n := range []int{1, 4} {
		t.Run(fmt.Sprintf("inflight=%d", n), func(t *testing.T) { f(t, withInflight(n)) })
	}
}

func (c *cluster) peers() []int {
	p := make([]int, c.n)
	for i := range p {
		p[i] = i
	}
	return p
}

func (c *cluster) start(i int) {
	if c.disk {
		ds, err := OpenDiskStorage(c.dirs[i], wal.Options{})
		if err != nil {
			c.t.Fatalf("open storage %d: %v", i, err)
		}
		c.stores[i] = ds
	}
	applyCh := make(chan ApplyMsg)
	stop := make(chan struct{})
	c.mu.Lock()
	c.logs[i] = make(map[uint64]string)
	c.applied[i] = 0
	c.stops[i] = stop
	c.mu.Unlock()

	rf, err := New(Config{
		ID:                i,
		Peers:             c.peers(),
		Transport:         &sim.RaftTransport{Net: c.net, From: i},
		Storage:           c.stores[i],
		ApplyCh:           applyCh,
		ElectionTimeout:   150 * time.Millisecond,
		HeartbeatInterval: 30 * time.Millisecond,
		MaxInflight:       c.inflight,
	})
	if err != nil {
		c.t.Fatalf("start %d: %v", i, err)
	}
	c.mu.Lock()
	c.rafts[i] = rf
	c.mu.Unlock()
	go c.applyLoop(i, rf, applyCh, stop)
	c.net.Register(i, rf)
}

func (c *cluster) applyLoop(i int, rf *Raft, ch chan ApplyMsg, stop chan struct{}) {
	for {
		var m ApplyMsg
		select {
		case m = <-ch:
		case <-stop:
			return
		}
		c.mu.Lock()
		switch {
		case m.SnapshotValid:
			var log map[uint64]string
			if err := gob.NewDecoder(bytes.NewReader(m.Snapshot)).Decode(&log); err != nil {
				c.fail(fmt.Errorf("node %d: bad snapshot: %v", i, err))
			}
			c.logs[i] = log
			c.applied[i] = m.SnapshotIndex
		case m.CommandValid:
			if m.Index != c.applied[i]+1 {
				c.fail(fmt.Errorf("node %d applied index %d after %d", i, m.Index, c.applied[i]))
			}
			c.applied[i] = m.Index
			if m.Command != nil {
				cmd := string(m.Command)
				for j, l := range c.logs {
					if old, ok := l[m.Index]; ok && old != cmd {
						c.fail(fmt.Errorf("index %d: node %d applied %q but node %d applied %q", m.Index, i, cmd, j, old))
					}
				}
				c.logs[i][m.Index] = cmd
			}
		}
		snap := c.snapEvery > 0 && m.CommandValid && m.Index%uint64(c.snapEvery) == 0
		var data []byte
		if snap {
			var buf bytes.Buffer
			gob.NewEncoder(&buf).Encode(c.logs[i])
			data = buf.Bytes()
		}
		c.mu.Unlock()
		if snap {
			if err := rf.Snapshot(m.Index, data); err != nil {
				c.fail(fmt.Errorf("node %d snapshot: %v", i, err))
			}
		}
	}
}

func (c *cluster) fail(err error) {
	if c.applyErr == nil {
		c.applyErr = err
	}
}

func (c *cluster) checkErr() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr != nil {
		c.t.Fatal(c.applyErr)
	}
}

// crash stops node i and detaches it from the network. With disk storage
// unsynced writes are dropped as in a real crash.
func (c *cluster) crash(i int) {
	c.net.Unregister(i)
	c.mu.Lock()
	rf, stop := c.rafts[i], c.stops[i]
	c.rafts[i] = nil
	c.mu.Unlock()
	if rf == nil {
		return
	}
	rf.Stop()
	close(stop)
	if ds, ok := c.stores[i].(*DiskStorage); ok {
		ds.CrashClose()
	}
}

func (c *cluster) restart(i int) {
	c.crash(i)
	c.start(i)
}

func (c *cluster) shutdown() {
	for i := 0; i < c.n; i++ {
		c.crash(i)
	}
}

func (c *cluster) raft(i int) *Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rafts[i]
}

// checkOneLeader waits for exactly one leader among the connected nodes.
func (c *cluster) checkOneLeader(among ...int) int {
	if len(among) == 0 {
		among = c.peers()
	}
	for iter := 0; iter < 30; iter++ {
		time.Sleep(100 * time.Millisecond)
		leaders := map[uint64][]int{}
		for _, i := range among {
			if rf := c.raft(i); rf != nil {
				if term, ok := rf.State(); ok {
					leaders[term] = append(leaders[term], i)
				}
			}
		}
		var last uint64
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
			last = max(last, term)
		}
		if len(leaders) > 0 {
			return leaders[last][0]
		}
	}
	c.t.Fatal("expected one leader, got none")
	return -1
}

// nCommitted reports how many nodes have applied index and the command.
func (c *cluster) nCommitted(index uint64) (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr != nil {
		c.t.Fatal(c.applyErr)
	}
	count, cmd := 0, ""
	for i := range c.logs {
		if c.rafts[i] == nil {
			continue
		}
		if v, ok := c.logs[i][index]; ok {
			count++
			cmd = v
		}
	}
	return count, cmd
}

// one submits cmd until it commits on at least expected nodes.
func (c *cluster) one(cmd string, expected int, retry bool) uint64 {
	deadline := time.Now().Add(10 * time.Second)
	start := 0
	for time.Now().Before(deadline) {
		for k := 0; k < c.n; k++ {
			i := (start + k) % c.n
			rf := c.raft(i)
			if rf == nil {
				continue
			}
			idx, _, ok := rf.Start([]byte(cmd))
			if !ok {
				continue
			}
			start = i
			t1 := time.Now()
			for time.Since(t1) < 2*time.Second {
				if n, got := c.nCommitted(idx); n >= expected && got == cmd {
					return idx
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Fatalf("one(%q) failed to reach agreement", cmd)
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("one(%q) failed to reach agreement", cmd)
	return 0
}

func (c *cluster) disconnect(i int) {
	c.net.Unregister(i)
}

func (c *cluster) connect(i int) {
	if rf := c.raft(i); rf != nil {
		c.net.Register(i, rf)
	}
}

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3)
	l := c.checkOneLeader()
	term1, _ := c.raft(l).State()
	time.Sleep(600 * time.Millisecond)
	term2, _ := c.raft(c.checkOneLeader()).State()
	if term1 != term2 {
		t.Logf("warning: term changed without failures (%d -> %d)", term1, term2)
	}
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3)
	l1 := c.checkOneLeader()
	c.disconnect(l1)
	l2 := c.checkOneLeader(others(3, l1)...)
	c.connect(l1)
	c.checkOneLeader()

	// No quorum: no leader should emerge among the remaining node.
	c.disconnect(l2)
	c.disconnect((l2 + 1) % 3)
	time.Sleep(800 * time.Millisecond)
	rem := (l2 + 2) % 3
	if _, ok := c.raft(rem).State(); ok {
		t.Fatalf("node %d is leader without a quorum", rem)
	}
	c.connect((l2 + 1) % 3)
	c.checkOneLeader(others(3, l2)...)
	c.connect(l2)
	c.checkOneLeader()
}

func others(n, except int) []int {
	var r []int
	for i := 0; i < n; i++ {
		if i != except {
			r = append(r, i)
		}
	}
	return r
}

func TestBasicAgree(t *testing.T) {
	c := newCluster(t, 5)
	for i := 1; i <= 10; i++ {
		c.one("cmd"+strconv.Itoa(i), 5, false)
	}
	c.checkErr()
}

func TestFollowerFailure(t *testing.T) {
	c := newCluster(t, 3)
	c.one("a", 3, false)
	l := c.checkOneLeader()
	c.disconnect((l + 1) % 3)
	c.one("b", 2, false)
	c.one("c", 2, false)
	c.connect((l + 1) % 3)
	c.one("d", 3, true)
}

func TestNoAgreeWithoutMajority(t *testing.T) {
	c := newCluster(t, 5)
	c.one("start", 5, false)
	l := c.checkOneLeader()
	for k := 1; k <= 3; k++ {
		c.disconnect((l + k) % 5)
	}
	idx, _, ok := c.raft(l).Start([]byte("lost"))
	if !ok {
		t.Fatal("leader rejected Start")
	}
	time.Sleep(500 * time.Millisecond)
	if n, _ := c.nCommitted(idx); n > 0 {
		t.Fatalf("%d nodes committed without a majority", n)
	}
	for k := 1; k <= 3; k++ {
		c.connect((l + k) % 5)
	}
	c.one("after", 5, true)
}

func TestRejoinOfPartitionedLeader(t *testing.T) {
	c := newCluster(t, 3)
	c.one("101", 3, false)
	l1 := c.checkOneLeader()
	c.disconnect(l1)
	// The old leader accepts entries that can never commit.
	c.raft(l1).Start([]byte("x1"))
	c.raft(l1).Start([]byte("x2"))
	c.one("103", 2, true)
	l2 := c.checkOneLeader(others(3, l1)...)
	c.disconnect(l2)
	c.connect(l1)
	c.one("104", 2, true)
	c.connect(l2)
	c.one("105", 3, true)
	c.checkErr()
}

func TestBackupManyEntries(t *testing.T) {
	bothReplicationModes(t, testBackupManyEntries)
}

func testBackupManyEntries(t *testing.T, opt func(*cluster)) {
	c := newCluster(t, 5, opt)
	c.one("init", 5, false)
	l := c.checkOneLeader()
	// Leave the leader with one follower; it collects uncommitted entries.
	c.disconnect((l + 2) % 5)
	c.disconnect((l + 3) % 5)
	c.disconnect((l + 4) % 5)
	for i := 0; i < 50; i++ {
		c.raft(l).Start([]byte(fmt.Sprintf("stale%d", i)))
	}
	time.Sleep(300 * time.Millisecond)
	c.disconnect(l)
	c.disconnect((l + 1) % 5)
	for k := 2; k <= 4; k++ {
		c.connect((l + k) % 5)
	}
	for i := 0; i < 50; i++ {
		c.one(fmt.Sprintf("fresh%d", i), 3, true)
	}
	for k := 0; k < 5; k++ {
		c.connect(k)
	}
	c.one("final", 5, true)
	c.checkErr()
}

func TestPersistAcrossFullRestart(t *testing.T) {
	c := newCluster(t, 3)
	c.one("11", 3, false)
	for i := 0; i < 3; i++ {
		c.restart(i)
	}
	c.one("12", 3, true)
	l := c.checkOneLeader()
	c.restart(l)
	c.one("13", 3, true)
	c.checkErr()
}

// TestFigure8Unreliable crashes leaders at random points on a lossy,
// reordering network, then checks the cluster still converges.
func TestFigure8Unreliable(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	bothReplicationModes(t, testFigure8Unreliable)
}

func testFigure8Unreliable(t *testing.T, opt func(*cluster)) {
	c := newCluster(t, 5, opt)
	c.net.SetReliable(false, 0.1, 20*time.Millisecond, 0.02)
	c.one("start", 1, true)
	up := 5
	for iter := 0; iter < 60; iter++ {
		leader := -1
		for i := 0; i < 5; i++ {
			if rf := c.raft(i); rf != nil {
				if _, _, ok := rf.Start([]byte(fmt.Sprint("f8-", iter, "-", i))); ok {
					leader = i
				}
			}
		}
		time.Sleep(time.Duration(10+iter%7*20) * time.Millisecond)
		if leader != -1 && iter%3 == 0 {
			c.crash(leader)
			up--
		}
		if up < 3 {
			for i := 0; i < 5; i++ {
				if c.raft(i) == nil {
					c.start(i)
					up++
				}
			}
		}
	}
	for i := 0; i < 5; i++ {
		if c.raft(i) == nil {
			c.start(i)
		}
	}
	c.net.SetReliable(true, 0, 0, 0)
	c.one("end", 5, true)
	c.checkErr()
}

func TestSnapshotsAndCatchUp(t *testing.T) {
	c := newCluster(t, 3, withSnapshots(10))
	c.one("s0", 3, false)
	l := c.checkOneLeader()
	lagger := (l + 1) % 3
	c.disconnect(lagger)
	for i := 0; i < 60; i++ {
		c.one(fmt.Sprint("s", i+1), 2, true)
	}
	// The lagging follower must be caught up by InstallSnapshot because the
	// leader compacted its log.
	if st := c.raft(l).Status(); st.LogEntries > 20 {
		t.Fatalf("leader log not compacted: %d entries", st.LogEntries)
	}
	c.connect(lagger)
	c.one("caught-up", 3, true)
	// And restarts recover from the stored snapshot.
	c.restart(lagger)
	c.restart(l)
	c.one("after-restart", 3, true)
	c.checkErr()
}

func TestReadIndex(t *testing.T) {
	c := newCluster(t, 3)
	idx := c.one("w1", 3, false)
	l := c.checkOneLeader()
	ctx, cancel := contextWithTimeout(time.Second)
	defer cancel()
	ri, err := c.raft(l).ReadIndex(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ri < idx {
		t.Fatalf("read index %d below committed %d", ri, idx)
	}
	// A leader cut off from the majority must not serve reads.
	c.disconnect((l + 1) % 3)
	c.disconnect((l + 2) % 3)
	ctx2, cancel2 := contextWithTimeout(400 * time.Millisecond)
	defer cancel2()
	if _, err := c.raft(l).ReadIndex(ctx2); err == nil {
		t.Fatal("isolated leader served a read")
	}
	// Followers refuse.
	if _, err := c.raft((l + 1) % 3).ReadIndex(ctx); err != ErrNotLeader {
		t.Fatalf("follower ReadIndex: %v", err)
	}
}

func TestDiskCrashRecovery(t *testing.T) {
	c := newCluster(t, 3, withDisk())
	for i := 0; i < 20; i++ {
		c.one(fmt.Sprint("d", i), 3, true)
	}
	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.start(i)
	}
	c.one("after-crash", 3, true)
	// Every acknowledged command must still be there on every node.
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < 3; i++ {
		got := map[string]bool{}
		for _, v := range c.logs[i] {
			got[v] = true
		}
		for k := 0; k < 20; k++ {
			if !got[fmt.Sprint("d", k)] {
				t.Fatalf("node %d lost committed command d%d", i, k)
			}
		}
	}
}
