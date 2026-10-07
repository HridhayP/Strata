// Package raft implements the Raft consensus algorithm: leader election
// (with pre-vote and check-quorum), log replication with fast backtracking,
// persistence with group-committed fsyncs, log compaction with snapshots,
// and ReadIndex for linearizable reads that skip the log.
//
// A Raft instance talks to peers only through a Transport, so the same code
// runs over gRPC in production and over a simulated lossy network in tests.
package raft

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/HridhayP/strata/proto/raftpb"
)

// None is the voted-for / leader value meaning "nobody".
const None = -1

var (
	ErrNotLeader = errors.New("raft: not leader")
	ErrStopped   = errors.New("raft: stopped")
)

// Transport sends RPCs to peers. Implementations must be safe for concurrent use.
type Transport interface {
	RequestVote(ctx context.Context, to int, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, to int, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, to int, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)
}

// ApplyMsg is delivered on Config.ApplyCh in log order. A command with nil
// data is a leader's no-op entry; the state machine should just record the
// index as applied.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	Index        uint64
	Term         uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// Observer receives metrics events. All methods must be cheap and non-blocking.
type Observer interface {
	CommitLatency(d time.Duration)
	LeaderChange(term uint64, leader int)
	ReplicationLag(peer int, entries uint64)
}

type nopObserver struct{}

func (nopObserver) CommitLatency(time.Duration) {}
func (nopObserver) LeaderChange(uint64, int)    {}
func (nopObserver) ReplicationLag(int, uint64)  {}

// Config configures a Raft instance.
type Config struct {
	ID        int
	Peers     []int // all member IDs, including ID
	Group     string
	Transport Transport
	Storage   Storage
	ApplyCh   chan<- ApplyMsg

	ElectionTimeout   time.Duration // base; actual timeout is uniform in [T, 2T)
	HeartbeatInterval time.Duration
	RPCTimeout        time.Duration
	MaxEntriesPerMsg  int

	// AppliedIndex is the index the state machine has already made durable on
	// its own. Raft resumes delivering entries after it.
	AppliedIndex uint64
	// SnapshotFn returns the current state machine image and the index it
	// reflects. Used to catch up followers when no stored snapshot covers the
	// compaction point (state machines that compact with Snapshot(i, nil)).
	SnapshotFn func() ([]byte, uint64)

	Observer Observer
}

type role int

const (
	follower role = iota
	preCandidate
	candidate
	leader
)

func (r role) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[r]
}

type readWaiter struct {
	seq   uint64
	index uint64
	ch    chan uint64 // receives the read index; closed on failure
}

// Raft is one member of a replication group.
type Raft struct {
	mu  sync.Mutex
	cfg Config
	me  int
	obs Observer

	role     role
	term     uint64
	votedFor int
	leaderID int

	// log[0] is a sentinel holding the index/term of the last compacted entry.
	log      []*raftpb.Entry
	snapshot []byte // stored image at log[0].Index, or nil

	commitIndex  uint64
	lastApplied  uint64
	durableIndex uint64 // every local entry <= durableIndex is on disk
	lastSeq      uint64 // storage sequence of the latest write
	syncedSeq    uint64
	pendingSnap  *ApplyMsg

	electionDeadline  time.Time
	lastLeaderContact time.Time

	// Leader state.
	nextIndex  map[int]uint64
	matchIndex map[int]uint64
	lastAck    map[int]time.Time
	termStart  uint64 // index of this term's no-op entry
	proposed   map[uint64]time.Time
	readSeq    uint64
	readAcks   map[int]uint64
	readers    []*readWaiter

	kicks     map[int]chan struct{}
	syncKick  chan struct{}
	applyCond *sync.Cond
	stopped   bool
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// New creates and starts a Raft instance from the state in cfg.Storage.
func New(cfg Config) (*Raft, error) {
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 300 * time.Millisecond
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = cfg.ElectionTimeout / 6
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = cfg.ElectionTimeout
	}
	if cfg.MaxEntriesPerMsg == 0 {
		cfg.MaxEntriesPerMsg = 1024
	}
	if cfg.Observer == nil {
		cfg.Observer = nopObserver{}
	}
	st, err := cfg.Storage.Load()
	if err != nil {
		return nil, err
	}
	r := &Raft{
		cfg:      cfg,
		me:       cfg.ID,
		obs:      cfg.Observer,
		role:     follower,
		term:     st.HardState.Term,
		votedFor: int(st.HardState.VotedFor),
		leaderID: None,
		log:      append([]*raftpb.Entry{{Index: st.Compaction.Index, Term: st.Compaction.Term}}, st.Entries...),
		snapshot: st.Snapshot,
		kicks:    make(map[int]chan struct{}),
		syncKick: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
	}
	r.applyCond = sync.NewCond(&r.mu)
	r.durableIndex = r.lastIndex()

	base := r.base()
	switch {
	case st.Snapshot != nil && base > cfg.AppliedIndex:
		r.pendingSnap = &ApplyMsg{SnapshotValid: true, Snapshot: st.Snapshot, SnapshotIndex: base, SnapshotTerm: r.log[0].Term}
		r.lastApplied = base
	case cfg.AppliedIndex < base:
		return nil, errors.New("raft: state machine is behind the compaction point and no snapshot is stored")
	case cfg.AppliedIndex > r.lastIndex():
		return nil, errors.New("raft: state machine claims entries the log never stored")
	default:
		r.lastApplied = cfg.AppliedIndex
	}
	r.commitIndex = r.lastApplied

	for _, p := range cfg.Peers {
		if p != r.me {
			r.kicks[p] = make(chan struct{}, 1)
		}
	}
	r.resetElectionTimer()

	r.wg.Add(3)
	go r.ticker()
	go r.applier()
	go r.syncer()
	return r, nil
}

// Stop halts all goroutines. It does not close the storage.
func (r *Raft) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	close(r.stopCh)
	r.failReaders()
	r.applyCond.Broadcast()
	r.mu.Unlock()
	r.wg.Wait()
}

// ---- log helpers (r.mu held) ----

func (r *Raft) base() uint64      { return r.log[0].Index }
func (r *Raft) lastIndex() uint64 { return r.log[len(r.log)-1].Index }
func (r *Raft) lastTerm() uint64  { return r.log[len(r.log)-1].Term }

func (r *Raft) termAt(i uint64) (uint64, bool) {
	if i < r.base() || i > r.lastIndex() {
		return 0, false
	}
	return r.log[i-r.base()].Term, true
}

func (r *Raft) majority() int { return len(r.cfg.Peers)/2 + 1 }

func (r *Raft) resetElectionTimer() {
	t := r.cfg.ElectionTimeout
	r.electionDeadline = time.Now().Add(t + rand.N(t))
}

func (r *Raft) hardState() *raftpb.HardState {
	return &raftpb.HardState{Term: r.term, VotedFor: int32(r.votedFor)}
}

func (r *Raft) persistHardState() {
	seq, err := r.cfg.Storage.SetHardState(r.hardState())
	if err != nil {
		panic("raft: persisting hard state: " + err.Error())
	}
	r.lastSeq = seq
}

func (r *Raft) persistEntries(ents []*raftpb.Entry) {
	seq, err := r.cfg.Storage.Append(ents)
	if err != nil {
		panic("raft: persisting entries: " + err.Error())
	}
	r.lastSeq = seq
}

// syncLocked makes every write so far durable. It is called with r.mu held,
// releases it around the fsync, and returns with it re-acquired.
func (r *Raft) syncLocked() {
	if r.lastSeq <= r.syncedSeq {
		return
	}
	seq, idx, term := r.lastSeq, r.lastIndex(), r.lastTerm()
	r.mu.Unlock()
	err := r.cfg.Storage.Sync(seq)
	r.mu.Lock()
	if err != nil {
		if r.stopped {
			return
		}
		panic("raft: sync: " + err.Error())
	}
	r.markDurable(seq, idx, term)
}

func (r *Raft) markDurable(seq, idx, term uint64) {
	if seq > r.syncedSeq {
		r.syncedSeq = seq
	}
	// Log matching: if the entry at idx still has the same term, the whole
	// prefix up to idx is unchanged and therefore durable.
	if t, ok := r.termAt(idx); ok && t == term && idx > r.durableIndex {
		r.durableIndex = idx
		if r.role == leader {
			r.advanceCommit()
		}
		r.applyCond.Broadcast()
	}
}

// ---- state transitions (r.mu held) ----

func (r *Raft) becomeFollower(term uint64, leaderID int) {
	wasLeader := r.role == leader
	if term > r.term {
		r.term = term
		r.votedFor = None
		r.persistHardState()
	}
	r.role = follower
	if leaderID != r.leaderID {
		r.leaderID = leaderID
		if leaderID != None {
			r.obs.LeaderChange(r.term, leaderID)
		}
	}
	if wasLeader {
		r.failReaders()
		r.proposed = nil
	}
}

func (r *Raft) becomeLeader() {
	r.role = leader
	r.leaderID = r.me
	r.nextIndex = make(map[int]uint64)
	r.matchIndex = make(map[int]uint64)
	r.lastAck = make(map[int]time.Time)
	r.readAcks = make(map[int]uint64)
	r.proposed = make(map[uint64]time.Time)
	now := time.Now()
	for p := range r.kicks {
		r.nextIndex[p] = r.lastIndex() + 1
		r.matchIndex[p] = 0
		r.lastAck[p] = now
	}
	// A no-op entry commits the previous terms' entries and is the point
	// after which ReadIndex may serve reads.
	noop := &raftpb.Entry{Term: r.term, Index: r.lastIndex() + 1}
	r.log = append(r.log, noop)
	r.persistEntries([]*raftpb.Entry{noop})
	r.termStart = noop.Index
	r.obs.LeaderChange(r.term, r.me)
	for p := range r.kicks {
		r.wg.Add(1)
		go r.replicator(p, r.term)
	}
	r.kickSync()
	r.advanceCommit() // single-node clusters commit on their own
}

// ---- background goroutines ----

func (r *Raft) ticker() {
	defer r.wg.Done()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-tick.C:
		}
		r.mu.Lock()
		now := time.Now()
		switch r.role {
		case leader:
			r.checkQuorum(now)
		default:
			if now.After(r.electionDeadline) {
				r.startElection(true)
			}
		}
		r.mu.Unlock()
	}
}

// checkQuorum steps down a leader that cannot reach a majority, so clients
// in a minority partition find out quickly instead of timing out forever.
func (r *Raft) checkQuorum(now time.Time) {
	alive := 1
	for _, t := range r.lastAck {
		if now.Sub(t) < 2*r.cfg.ElectionTimeout {
			alive++
		}
	}
	if alive < r.majority() {
		r.becomeFollower(r.term, None)
		r.resetElectionTimer()
	}
}

func (r *Raft) syncer() {
	defer r.wg.Done()
	for {
		select {
		case <-r.stopCh:
			return
		case <-r.syncKick:
		}
		r.mu.Lock()
		if !r.stopped {
			r.syncLocked()
		}
		r.mu.Unlock()
	}
}

func (r *Raft) kickSync() {
	select {
	case r.syncKick <- struct{}{}:
	default:
	}
}

func (r *Raft) kickAll() {
	for _, ch := range r.kicks {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (r *Raft) applier() {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		for !r.stopped && r.pendingSnap == nil && r.lastApplied >= min(r.commitIndex, r.durableIndex) {
			r.applyCond.Wait()
		}
		if r.stopped {
			r.mu.Unlock()
			return
		}
		var msgs []ApplyMsg
		if r.pendingSnap != nil {
			msgs = append(msgs, *r.pendingSnap)
			r.pendingSnap = nil
		} else {
			hi := min(r.commitIndex, r.durableIndex, r.lastApplied+256)
			for i := r.lastApplied + 1; i <= hi; i++ {
				e := r.log[i-r.base()]
				msgs = append(msgs, ApplyMsg{CommandValid: true, Command: e.Data, Index: e.Index, Term: e.Term})
			}
			r.lastApplied = hi
		}
		r.mu.Unlock()
		for _, m := range msgs {
			select {
			case r.cfg.ApplyCh <- m:
			case <-r.stopCh:
				return
			}
		}
	}
}

// ---- elections ----

func (r *Raft) startElection(pre bool) {
	r.resetElectionTimer()
	var reqTerm uint64
	if pre {
		r.role = preCandidate
		reqTerm = r.term + 1
	} else {
		r.role = candidate
		r.term++
		r.votedFor = r.me
		r.leaderID = None
		r.persistHardState()
		reqTerm = r.term
		// Our own vote must be durable before we ask for others'.
		r.syncLocked()
		if r.role != candidate || r.term != reqTerm || r.stopped {
			return
		}
	}
	req := &raftpb.RequestVoteRequest{
		Group:        r.cfg.Group,
		Term:         reqTerm,
		CandidateId:  int32(r.me),
		LastLogIndex: r.lastIndex(),
		LastLogTerm:  r.lastTerm(),
		PreVote:      pre,
	}
	myRole, myTerm := r.role, r.term
	votes := 1
	if votes >= r.majority() {
		r.wonElection(pre)
		return
	}
	for p := range r.kicks {
		go func(p int) {
			ctx, cancel := context.WithTimeout(context.Background(), r.cfg.RPCTimeout)
			defer cancel()
			resp, err := r.cfg.Transport.RequestVote(ctx, p, req)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.stopped || r.role != myRole || r.term != myTerm {
				return
			}
			if resp.Term > r.term {
				r.becomeFollower(resp.Term, None)
				return
			}
			if resp.VoteGranted {
				votes++
				if votes == r.majority() {
					r.wonElection(pre)
				}
			}
		}(p)
	}
}

func (r *Raft) wonElection(pre bool) {
	if pre {
		r.startElection(false)
	} else {
		r.becomeLeader()
	}
}

func (r *Raft) logUpToDate(lastIndex, lastTerm uint64) bool {
	return lastTerm > r.lastTerm() || (lastTerm == r.lastTerm() && lastIndex >= r.lastIndex())
}

// HandleRequestVote is the RequestVote RPC handler.
func (r *Raft) HandleRequestVote(req *raftpb.RequestVoteRequest) *raftpb.RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return &raftpb.RequestVoteResponse{Term: r.term}
	}
	if req.PreVote {
		// Grant only if we would grant a real vote and have not heard from a
		// live leader recently; this stops a rejoining partitioned node from
		// forcing an election with an inflated term.
		recent := r.leaderID != None && time.Since(r.lastLeaderContact) < r.cfg.ElectionTimeout
		if r.role == leader {
			recent = true
		}
		grant := req.Term > r.term && !recent && r.logUpToDate(req.LastLogIndex, req.LastLogTerm)
		return &raftpb.RequestVoteResponse{Term: r.term, VoteGranted: grant}
	}
	if req.Term < r.term {
		return &raftpb.RequestVoteResponse{Term: r.term}
	}
	if req.Term > r.term {
		r.becomeFollower(req.Term, None)
	}
	grant := false
	cand := int(req.CandidateId)
	if (r.votedFor == None || r.votedFor == cand) && r.logUpToDate(req.LastLogIndex, req.LastLogTerm) {
		grant = true
		if r.votedFor != cand {
			r.votedFor = cand
			r.persistHardState()
		}
		r.resetElectionTimer()
	}
	term := r.term
	r.syncLocked()
	if r.stopped {
		grant = false // the vote may not be durable
	}
	return &raftpb.RequestVoteResponse{Term: term, VoteGranted: grant}
}

// ---- replication ----

func (r *Raft) replicator(peer int, term uint64) {
	defer r.wg.Done()
	hb := time.NewTicker(r.cfg.HeartbeatInterval)
	defer hb.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-r.kicks[peer]:
		case <-hb.C:
		}
		for {
			more, alive := r.replicateOnce(peer, term)
			if !alive {
				return
			}
			if !more {
				break
			}
		}
	}
}

// replicateOnce sends one AppendEntries or InstallSnapshot to peer. more
// reports whether there is more to send right away; alive is false once
// this replicator's term of leadership is over.
func (r *Raft) replicateOnce(peer int, term uint64) (more, alive bool) {
	r.mu.Lock()
	if r.stopped || r.role != leader || r.term != term {
		r.mu.Unlock()
		return false, false
	}
	if r.nextIndex[peer] <= r.base() {
		return r.sendSnapshot(peer, term) // releases r.mu
	}
	next := r.nextIndex[peer]
	prevIdx := next - 1
	prevTerm, _ := r.termAt(prevIdx)
	hi := min(r.lastIndex(), prevIdx+uint64(r.cfg.MaxEntriesPerMsg))
	var ents []*raftpb.Entry
	if hi >= next {
		ents = slices.Clone(r.log[next-r.base() : hi-r.base()+1])
	}
	req := &raftpb.AppendEntriesRequest{
		Group:        r.cfg.Group,
		Term:         term,
		LeaderId:     int32(r.me),
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      ents,
		LeaderCommit: r.commitIndex,
		ReadCtx:      r.readSeq,
	}
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.RPCTimeout)
	resp, err := r.cfg.Transport.AppendEntries(ctx, peer, req)
	cancel()

	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		return false, true
	}
	if resp.Term > r.term {
		r.becomeFollower(resp.Term, None)
		r.resetElectionTimer()
		return false, false
	}
	if r.stopped || r.role != leader || r.term != term {
		return false, false
	}
	r.lastAck[peer] = time.Now()
	if resp.ReadCtx > r.readAcks[peer] {
		r.readAcks[peer] = resp.ReadCtx
		r.checkReads()
	}
	if resp.Success {
		match := prevIdx + uint64(len(ents))
		if match > r.matchIndex[peer] {
			r.matchIndex[peer] = match
			r.advanceCommit()
		}
		if match+1 > r.nextIndex[peer] {
			r.nextIndex[peer] = match + 1
		}
		r.obs.ReplicationLag(peer, r.lastIndex()-r.matchIndex[peer])
		return r.nextIndex[peer] <= r.lastIndex(), true
	}
	// Rejected: jump back using the follower's conflict hint.
	next = resp.ConflictIndex
	if resp.ConflictTerm != 0 {
		for i := r.lastIndex(); i > r.base(); i-- {
			if t, _ := r.termAt(i); t == resp.ConflictTerm {
				next = i + 1
				break
			} else if t < resp.ConflictTerm {
				break
			}
		}
	}
	next = max(next, r.matchIndex[peer]+1, 1)
	r.nextIndex[peer] = min(next, r.lastIndex()+1)
	return true, true
}

// sendSnapshot is called with r.mu held and releases it.
func (r *Raft) sendSnapshot(peer int, term uint64) (more, alive bool) {
	var data []byte
	var idx, snapTerm uint64
	if r.snapshot != nil {
		data, idx, snapTerm = r.snapshot, r.base(), r.log[0].Term
	} else {
		if r.cfg.SnapshotFn == nil {
			r.mu.Unlock()
			panic("raft: follower needs a snapshot but no SnapshotFn is configured")
		}
		r.mu.Unlock()
		data, idx = r.cfg.SnapshotFn()
		r.mu.Lock()
		if r.stopped || r.role != leader || r.term != term {
			r.mu.Unlock()
			return false, false
		}
		var ok bool
		if snapTerm, ok = r.termAt(idx); !ok {
			r.mu.Unlock()
			return true, true // compacted past it meanwhile; try again
		}
	}
	req := &raftpb.InstallSnapshotRequest{
		Group:             r.cfg.Group,
		Term:              term,
		LeaderId:          int32(r.me),
		LastIncludedIndex: idx,
		LastIncludedTerm:  snapTerm,
		Data:              data,
	}
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 4*r.cfg.RPCTimeout)
	resp, err := r.cfg.Transport.InstallSnapshot(ctx, peer, req)
	cancel()

	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		return false, true
	}
	if resp.Term > r.term {
		r.becomeFollower(resp.Term, None)
		r.resetElectionTimer()
		return false, false
	}
	if r.stopped || r.role != leader || r.term != term {
		return false, false
	}
	r.lastAck[peer] = time.Now()
	if idx > r.matchIndex[peer] {
		r.matchIndex[peer] = idx
		r.advanceCommit()
	}
	if idx+1 > r.nextIndex[peer] {
		r.nextIndex[peer] = idx + 1
	}
	return r.nextIndex[peer] <= r.lastIndex(), true
}

// advanceCommit moves commitIndex to the highest index stored durably on a
// majority, restricted to entries of the current term (Raft §5.4.2).
func (r *Raft) advanceCommit() {
	matches := make([]uint64, 0, len(r.cfg.Peers))
	matches = append(matches, r.durableIndex)
	for _, m := range r.matchIndex {
		matches = append(matches, m)
	}
	slices.Sort(matches)
	n := matches[len(matches)-r.majority()]
	if n <= r.commitIndex {
		return
	}
	if t, ok := r.termAt(n); !ok || t != r.term {
		return
	}
	r.commitIndex = n
	now := time.Now()
	for idx, at := range r.proposed {
		if idx <= n {
			r.obs.CommitLatency(now.Sub(at))
			delete(r.proposed, idx)
		}
	}
	r.applyCond.Broadcast()
	r.checkReads()
}

// HandleAppendEntries is the AppendEntries RPC handler.
func (r *Raft) HandleAppendEntries(req *raftpb.AppendEntriesRequest) *raftpb.AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	resp := &raftpb.AppendEntriesResponse{Term: r.term}
	if r.stopped || req.Term < r.term {
		return resp
	}
	if req.Term > r.term || r.role != follower || r.leaderID != int(req.LeaderId) {
		r.becomeFollower(req.Term, int(req.LeaderId))
	}
	resp.Term = r.term
	resp.ReadCtx = req.ReadCtx
	r.lastLeaderContact = time.Now()
	r.resetElectionTimer()

	prevIdx, prevTerm, ents := req.PrevLogIndex, req.PrevLogTerm, req.Entries
	if prevIdx < r.base() {
		// The prefix up to base is committed and therefore matches; skip it.
		skip := int(min(uint64(len(ents)), r.base()-prevIdx))
		ents = ents[skip:]
		prevIdx, prevTerm = r.base(), r.log[0].Term
		if len(ents) > 0 {
			prevIdx = ents[0].Index - 1
			prevTerm, _ = r.termAt(prevIdx)
		}
	}
	if prevIdx > r.lastIndex() {
		resp.ConflictIndex = r.lastIndex() + 1
		return resp
	}
	if t, _ := r.termAt(prevIdx); t != prevTerm {
		resp.ConflictTerm = t
		first := prevIdx
		for first-1 > r.base() {
			if pt, _ := r.termAt(first - 1); pt != t {
				break
			}
			first--
		}
		resp.ConflictIndex = first
		return resp
	}

	// Skip entries we already have; truncate at the first conflict.
	for i, e := range ents {
		t, ok := r.termAt(e.Index)
		if ok && t == e.Term {
			continue
		}
		if ok {
			r.log = r.log[:e.Index-r.base()]
			r.durableIndex = min(r.durableIndex, e.Index-1)
		}
		newEnts := ents[i:]
		r.log = append(r.log, newEnts...)
		r.persistEntries(newEnts)
		break
	}
	lastNew := prevIdx + uint64(len(ents))
	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = min(req.LeaderCommit, lastNew)
		r.applyCond.Broadcast()
	}
	term := r.term
	// Everything we acknowledge must be on disk first.
	r.syncLocked()
	if r.stopped || r.term != term {
		resp.Term = r.term
		return resp
	}
	resp.Success = true
	resp.MatchIndex = lastNew
	return resp
}

// HandleInstallSnapshot is the InstallSnapshot RPC handler.
func (r *Raft) HandleInstallSnapshot(req *raftpb.InstallSnapshotRequest) *raftpb.InstallSnapshotResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	resp := &raftpb.InstallSnapshotResponse{Term: r.term}
	if r.stopped || req.Term < r.term {
		return resp
	}
	if req.Term > r.term || r.role != follower || r.leaderID != int(req.LeaderId) {
		r.becomeFollower(req.Term, int(req.LeaderId))
	}
	resp.Term = r.term
	r.lastLeaderContact = time.Now()
	r.resetElectionTimer()
	idx, term := req.LastIncludedIndex, req.LastIncludedTerm
	if idx <= r.commitIndex {
		return resp // we already have everything the snapshot covers
	}
	var suffix []*raftpb.Entry
	if t, ok := r.termAt(idx); ok && t == term {
		suffix = slices.Clone(r.log[idx-r.base()+1:])
	}
	c := &raftpb.Compaction{Index: idx, Term: term}
	if err := r.cfg.Storage.Compact(c, r.hardState(), req.Data, suffix); err != nil {
		panic("raft: installing snapshot: " + err.Error())
	}
	r.log = append([]*raftpb.Entry{{Index: idx, Term: term}}, suffix...)
	r.snapshot = req.Data
	r.syncedSeq = r.lastSeq
	r.durableIndex = r.lastIndex()
	r.commitIndex = idx
	r.lastApplied = idx
	r.pendingSnap = &ApplyMsg{SnapshotValid: true, Snapshot: req.Data, SnapshotIndex: idx, SnapshotTerm: term}
	r.applyCond.Broadcast()
	return resp
}

// ---- client-facing API ----

// Start proposes cmd. It returns immediately; the command is committed if
// it later appears on ApplyCh at the returned index with the returned term.
func (r *Raft) Start(cmd []byte) (index, term uint64, isLeader bool) {
	if cmd == nil {
		cmd = []byte{} // nil is reserved for no-op entries
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped || r.role != leader {
		return 0, 0, false
	}
	e := &raftpb.Entry{Term: r.term, Index: r.lastIndex() + 1, Data: cmd}
	r.log = append(r.log, e)
	r.persistEntries([]*raftpb.Entry{e})
	r.proposed[e.Index] = time.Now()
	r.kickAll()
	r.kickSync()
	return e.Index, e.Term, true
}

// ReadIndex returns an index such that, once the state machine has applied
// it, a local read is linearizable. Concurrent calls share heartbeat rounds.
func (r *Raft) ReadIndex(ctx context.Context) (uint64, error) {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return 0, ErrStopped
	}
	if r.role != leader {
		r.mu.Unlock()
		return 0, ErrNotLeader
	}
	r.readSeq++
	w := &readWaiter{seq: r.readSeq, ch: make(chan uint64, 1)}
	if r.commitIndex >= r.termStart {
		w.index = r.commitIndex
	}
	r.readers = append(r.readers, w)
	r.kickAll()
	r.checkReads()
	r.mu.Unlock()

	select {
	case idx, ok := <-w.ch:
		if !ok {
			return 0, ErrNotLeader
		}
		return idx, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// checkReads releases ReadIndex waiters whose heartbeat round has been
// acknowledged by a majority.
func (r *Raft) checkReads() {
	if len(r.readers) == 0 || r.commitIndex < r.termStart {
		return
	}
	acks := make([]uint64, 0, len(r.cfg.Peers))
	acks = append(acks, r.readSeq) // the leader acknowledges itself
	for p := range r.kicks {
		acks = append(acks, r.readAcks[p])
	}
	slices.Sort(acks)
	quorum := acks[len(acks)-r.majority()]
	n := 0
	for _, w := range r.readers {
		if w.seq > quorum {
			break
		}
		idx := w.index
		if idx == 0 {
			idx = r.commitIndex
		}
		w.ch <- idx
		n++
	}
	r.readers = r.readers[n:]
}

func (r *Raft) failReaders() {
	for _, w := range r.readers {
		close(w.ch)
	}
	r.readers = nil
}

// Snapshot tells Raft that the state machine has captured everything up to
// index. If data is nil the state machine has made that state durable by
// itself; otherwise Raft stores data and serves it to lagging followers.
func (r *Raft) Snapshot(index uint64, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index <= r.base() {
		return nil
	}
	if index > r.lastApplied {
		return errors.New("raft: snapshot beyond applied index")
	}
	term, _ := r.termAt(index)
	remaining := slices.Clone(r.log[index-r.base()+1:])
	c := &raftpb.Compaction{Index: index, Term: term}
	if err := r.cfg.Storage.Compact(c, r.hardState(), data, remaining); err != nil {
		return err
	}
	r.log = append([]*raftpb.Entry{{Index: index, Term: term}}, remaining...)
	r.snapshot = data
	r.syncedSeq = r.lastSeq
	if li := r.lastIndex(); li > r.durableIndex {
		r.durableIndex = li
		if r.role == leader {
			r.advanceCommit()
		}
		r.applyCond.Broadcast()
	}
	return nil
}

// State reports the current term and whether this node believes it is leader.
func (r *Raft) State() (term uint64, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.term, r.role == leader
}

// Leader returns the ID of the last known leader, or None.
func (r *Raft) Leader() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}

// Status is a point-in-time view used for metrics and debugging.
type Status struct {
	ID          int
	Role        string
	Term        uint64
	Leader      int
	CommitIndex uint64
	LastApplied uint64
	LastIndex   uint64
	LogEntries  int
}

// Status returns a snapshot of internal state.
func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{
		ID:          r.me,
		Role:        r.role.String(),
		Term:        r.term,
		Leader:      r.leaderID,
		CommitIndex: r.commitIndex,
		LastApplied: r.lastApplied,
		LastIndex:   r.lastIndex(),
		LogEntries:  len(r.log) - 1,
	}
}
