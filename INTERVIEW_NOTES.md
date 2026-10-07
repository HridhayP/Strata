# Interview notes

Ten questions I expect about Strata, with answers tied to the code.

### 1. How do you make reads linearizable without putting them in the Raft log?

ReadIndex (`raft.ReadIndex`). The leader records its commit index, then confirms that a majority still recognizes it as leader for its current term, waits until the state machine has applied up to that index, and reads locally (`kv.Server.get`). A leader that was deposed in a partition can't get the majority confirmation, so it can't serve a stale value. There's one subtlety: a new leader doesn't know the true commit index until an entry from its own term commits. That's why reads wait for the term's no-op entry (`commitIndex >= termStart`). I tested this by mutation. With confirmation skipped, porcupine flagged 9 of 40 histories as non-linearizable.

### 2. Why do read confirmations use a separate "probe" RPC?

At first they rode on AppendEntries. The replicator only had one AppendEntries in flight per follower, and the follower fsyncs before replying, so a read waited behind a write's disk flush. Running only reads gave a read p50 of 1.05 ms. Adding 10% writes pushed it to 3.7 ms, the same as write latency. Now a per-peer `prober` goroutine sends `read_probe` AppendEntries. The follower only checks the term and replies, without touching its log (`HandleAppendEntries`). In the same 10 s experiment (64 clients, 90/10), read p50 dropped to 0.65 ms and throughput rose from 17K to 49K ops/s. The probe is safe because only "a majority acknowledges my term after the read arrived" is needed, not log agreement.

### 3. How are duplicate client requests handled?

Every write carries `(client_id, seq)`. Clerks are closed-loop, so sequence numbers per client increase. In the same LSM batch as the write, the state machine stores the last applied sequence per (shard, client) (`applyClient`, `dedupKey`). A retried write whose `seq` is at or below the stored value returns OK without applying again. The dedup table is keyed by shard and migrates with the shard's data. So a retry that lands on the new owner after a migration is still deduplicated.

### 4. How do the Raft log and the LSM avoid double-writing (two WALs)?

The LSM's own WAL is off in the KV service. The Raft log is the write-ahead log. Every applied batch also writes the applied Raft index into the LSM (`appliedKey`). After a crash, the LSM recovers the data its SSTables cover plus that index, and Raft starts replaying from there (`Config.AppliedIndex`). To compact the log, the server flushes the memtable, then calls `rf.Snapshot(idx, nil)`. `nil` tells Raft that the state machine already owns durability up to `idx`, so Raft only needs to drop the prefix. A follower that falls behind the compaction point gets a fresh scan of the LSM through `SnapshotFn`.

### 5. What does fsync batching (group commit) buy you, and how is it built?

`wal.Sync(seq)` is called concurrently. The first caller becomes the "sync leader": it flushes the buffer and fsyncs everything written so far. Callers that arrive in the meantime wait on a condition variable, and one fsync covers all of them. Raft does the same: a `syncer` goroutine makes the log durable, and the leader counts its own durable index toward the commit quorum. On this machine one fsync costs about 1 ms. With 16 concurrent appenders, batched appends cost 7.3 µs each versus 1.08 ms each fsynced individually. End-to-end numbers are in RESULTS.md.

### 6. How does sharding work and why bounded loads?

There are 64 fixed shards (`kv.KeyShard`). The controller maps each shard to a replica group with a consistent-hash ring (64 virtual nodes per group) plus "consistent hashing with bounded loads": a shard goes to the first group clockwise from it whose load is below `ceil(1.25 × average)` (`shard.Assign`). With only 64 shards, plain consistent hashing gave some groups several times the average. The cap keeps groups near even, while adding or removing a group still moves few shards. The controller is its own Raft group storing the configuration history (Join/Leave/Move/Query).

### 7. Walk through a live shard migration.

Groups move through configurations one at a time, and only once every shard has settled. When config N+1 is applied, a gained shard becomes `Pulling` and a lost shard becomes `Offering`, which freezes it. The new owner's leader pulls the frozen data (with its dedup entries) from any replica of the old owner. It installs that data through its own Raft log, which moves the shard to `Cleaning`, and it serves requests from then on. It then tells the old owner to delete its copy (through the old owner's Raft log), and marks the shard `Serving`. Every step is a Raft entry, so any replica can crash at any point and resume. Mutation test: skipping the pull was flagged in 10/10 sharded histories.

### 8. How do you test this, and what does the harness inject?

`transport/sim` is an in-memory network that can drop, delay and reorder messages and partition nodes. `harness.Run` starts a cluster on it, runs concurrent clients, and randomly partitions, crashes and restarts nodes. With `DiskRaft`, it uses real files and drops unsynced bytes on crash. Each operation's call and return times go into a history that porcupine checks against a sequential KV model. `cmd/strata-lincheck` runs thousands of these across scenarios and seeds in parallel; the counts are in RESULTS.md. Histories are capped at 100 ops per client. Uncapped runs produced 30K–330K-op histories, which porcupine couldn't check in reasonable time.

### 9. What's the difference between your two crash-recovery tests?

`wal.CrashCheck` and `lsm.CrashCheck` simulate power loss. Unsynced bytes are cut at a random point, sometimes followed by junk, and recovery has to keep every acknowledged record and nothing corrupt. The CRC framing makes a torn tail detectable. `strata-crashtest -mode cluster` kills real server processes (the leader, a minority, or all five) while writes are in flight, restarts them, and reads every acknowledged write back. A process kill doesn't lose the OS page cache. So that mode tests recovery logic (log replay, applied-index tracking, snapshot catch-up), not fsync placement. That's why both kinds exist.

### 10. What would you change for production?

- Batch proposals at the KV layer. Each write is currently its own Raft entry and LSM batch.
- Membership changes (joint consensus or single-server changes). Groups are static today.
- Leader leases for reads, which are cheaper than ReadIndex but depend on bounded clock drift.
- A segmented WAL. Compaction currently rewrites the Raft WAL under the Raft lock (about 6–10 ms here).
- Real nodes on separate disks. The local benchmark runs five replicas on one laptop SSD, so every node's fsyncs compete for the same device, which inflates write latency (single fsync about 1.07 ms alone, about 1.9 ms with five processes syncing).
