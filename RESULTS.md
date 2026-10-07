# Results

Every number here was measured on the machine below. Each one comes with the command that produced it, and the raw output is committed next to this file. Where a target was missed, the real number is reported.

## Summary vs targets

| Metric | Target | Measured | Hit? |
|---|---|---|---|
| Throughput, 5-node cluster, 90/10 read/write | 120K ops/s | **38.9K ops/s** (median of 3, 256 clients); best single run 42.0K | miss |
| p99 latency, same workload | 4 ms | **13.1 ms** (median of 3, 64 clients); best single run 6.1 ms | miss |
| Write throughput, fsync batching vs none | 3.2x | **25.2x** (12,428 vs 493 ops/s, 64 clients, median of 3) | hit |
| Linearizability-checked histories | 50K | **50,000 checked, 50,000 linearizable** (0 illegal, 0 checker timeouts, 17.25M ops total) | hit |
| Crash-recovery runs passed | 1,000+ | **3,000 / 3,000**: 1,000 WAL power-loss + 1,000 LSM power-loss + 1,000 real-process kills of a 5-node cluster | hit |

The cluster misses the throughput and latency targets on this hardware, and the reasons are measured below. All five replicas and the load generator share one 22-thread laptop CPU, which sampled at 68â€“99% utilization mid-run. They also share one NVMe SSD, where a single fsync takes 1.07 ms alone but about 1.9 ms when five processes sync at once. A Raft commit waits for fsyncs on the leader and on two followers.

## Hardware and software

- Intel Core Ultra 9 185H (16 cores, 22 threads), 31.6 GB RAM, laptop on AC power, Windows "Balanced" power plan
- Samsung MZAL81T0HDLB NVMe SSD, 1 TB
- Windows 11 Home (build 26200), Go 1.27.0
- Cluster: 5 `strata-server` processes on 127.0.0.1, one Raft group serving all shards, election timeout 300 ms, Raft log compaction every 50K entries
- Measured 2026-10-07

## 1. Throughput and latency (90% reads / 10% writes)

Command (each run starts a fresh cluster; 5 s warm-up, 30 s measured):

```
go build -o bin/ ./cmd/...
bash scripts/bench.sh 3 30s        # writes bench-results/*.json
python scripts/summarize.py        # the table below
```

The workload is closed-loop: each client issues one operation at a time over 10,000 preloaded keys with 100-byte values. Reads are linearizable, going through ReadIndex on the leader. Writes go through the Raft log and are fsynced on a majority.

| clients | median ops/s | minâ€“max ops/s | p50 ms | p99 ms | read p99 ms | write p99 ms | errors |
|---|---|---|---|---|---|---|---|
| 64 | 27,846 | 26,366â€“42,042 | 1.13 | 13.14 | 9.60 | 56.91 | 0, 0, 0 |
| 128 | 36,267 | 27,551â€“38,306 | 2.10 | 22.35 | 17.42 | 76.34 | 0, 0, 0 |
| 256 | 38,867 | 38,315â€“40,585 | 4.21 | 43.63 | 35.50 | 124.03 | 0, 0, 0 |

Per-run numbers:

| run | ops/s | p50 ms | p99 ms | read p50 ms | write p50 ms |
|---|---|---|---|---|---|
| c64 r1 | 42,042 | 1.06 | 6.13 | 1.06 | 4.60 |
| c64 r2 | 27,846 | 1.13 | 13.14 | 1.10 | 4.95 |
| c64 r3 | 26,366 | 1.11 | 18.05 | 1.09 | 4.98 |
| c128 r1 | 27,551 | 2.17 | 38.26 | 2.11 | 6.52 |
| c128 r2 | 36,267 | 2.10 | 22.35 | 1.81 | 6.22 |
| c128 r3 | 38,306 | 2.12 | 19.29 | 1.85 | 6.29 |
| c256 r1 | 38,867 | 4.21 | 43.63 | 3.81 | 9.21 |
| c256 r2 | 38,315 | 4.04 | 47.31 | 3.73 | 8.88 |
| c256 r3 | 40,585 | 3.96 | 36.82 | 3.67 | 9.00 |

**Reading the numbers.** p50 is stable across runs. Between identical runs, throughput swings by up to 1.6x and p99 by up to 3x. In the per-second timelines (`ops_per_second` in each JSON), c64 r2 runs at 9â€“40K ops/s from its first second, while r1 holds 40â€“50K for most of the run and then slows. That points to machine-level variation, not a stall inside one run. A mid-run sample showed the CPU at 68â€“99% utility and 153â€“169% of base clock, so boost clocks and thermals vary from run to run. Throughput levels off at about 39K ops/s from 128 to 256 clients while latency doubles, so the cluster is saturated there. Writes dominate the tail: in the median runs, write p99 is 3.5â€“6x read p99.

**Changes made while preparing these numbers** (in this repo's history):

- *Read probes.* Read confirmations used to ride on AppendEntries, so a read waited behind the follower's fsync. In a 10 s experiment (64 clients), 100% reads gave a read p50 of 1.05 ms, while 90/10 gave 3.70 ms, the same as write latency. With a separate `read_probe` RPC, 90/10 went from 17.2K to 48.9K ops/s and read p50 from 3.70 ms to 0.65 ms. That experiment was 10 s long and isn't part of the table above.
- *AppendEntries pipelining.* Tested and left off by default. In 10 s A/B runs it raised write-only throughput at 256 clients (23.2K vs 29.0K ops/s) but lowered it at 64 clients (14.2K vs 10.9K) and at 90/10 with 128 clients (59.7K vs 48.1K). Available as `-max-inflight N`.
- *Background memtable flush.* LSM flushes no longer hold the DB lock, so they no longer stall reads and applies.

**Unexplained tail.** Write-only runs occasionally show one burst of commits all delayed by about 200 ms (p999 18 ms in some runs, 190â€“265 ms in others). Instrumentation ruled out Raft log compaction (WAL rewrite took 6â€“10 ms), snapshot transfer (none sent), and GC (max pause about 1 ms). The cause isn't identified.

## 2. Write throughput with and without fsync batching

Same script, write-only (`-read-ratio 0`). The unbatched cluster runs with `-no-fsync-batching`, which makes the WAL fsync every record inside `Append`, one at a time. With batching, concurrent appenders share fsyncs (group commit) on every node.

| config | median ops/s | minâ€“max ops/s | p50 ms | p99 ms | client timeouts per run |
|---|---|---|---|---|---|
| batched, 64 clients | 12,428 | 11,682â€“12,827 | 4.28 | 14.84 | 0, 0, 0 |
| unbatched, 64 clients | 493 | 477â€“509 | 127.09 | 171.78 | 0, 0, 0 |
| batched, 256 clients | 20,117 | 17,879â€“25,046 | 10.60 | 55.76 | 0, 0, 0 |
| unbatched, 256 clients | 13 | 0â€“93 | 4,472 | 8,797 | 768, 746, 276 |

**Ratio: 12,428 / 493 = 25.2x at 64 clients**, the highest load where the unbatched cluster still completes every request. At 256 clients the unbatched cluster stalls. Requests queue past the client's 10 s timeout, clients retry, and the timelines show stretches of 10â€“30 s with zero completions. A ratio there would be meaningless, so it isn't reported.

WAL microbenchmark (16 parallel appenders, each Append followed by Sync):

```
go test -run XXX -bench AppendSync -benchtime 2s ./storage/wal
```

```
BenchmarkAppendSync/noBatch=false-22    148951     14938 ns/op
BenchmarkAppendSync/noBatch=false-22    158576     15324 ns/op
BenchmarkAppendSync/noBatch=false-22    208742     12158 ns/op
BenchmarkAppendSync/noBatch=true-22       1806   1229042 ns/op
BenchmarkAppendSync/noBatch=true-22       2064   1202568 ns/op
BenchmarkAppendSync/noBatch=true-22       1786   1206171 ns/op
```

With group commit, a durable append costs 12–15 µs per caller. Without it, each costs 1.2 ms, one fsync each, so batching is 80–100x faster on a single node with no replication. The end-to-end 25x above is smaller because a cluster write also pays for network round trips, Raft and the state machine. Raw output: `bench-results/wal-appendsync.txt`.

## 3. Linearizability-checked histories

Command:

```
./bin/strata-lincheck -n 50000 -workers 22 -seed 100000 -checkpoint 2m -out tmp/lincheck-50k.json
```

Each history is a fresh simulated cluster with 5 concurrent clients doing Get/Put/Append (50/20/30) on a small keyspace. Faults run for a 1 s phase (2 s for sharded runs), then the network heals so pending operations finish. Porcupine then checks the history against a sequential KV model, with a 30 s timeout. Seeds 100000–149999 rotate through the scenarios; every 10th seed runs a sharded cluster (3-node controller plus three 3-node groups) with live shard migrations.

| scenario | histories | linearizable | illegal | checker timeout | errors | avg ops per history |
|---|---|---|---|---|---|---|
| reliable | 5,000 | 5,000 | 0 | 0 | 0 | 500 |
| unreliable (drop, delay, reorder) | 6,250 | 6,250 | 0 | 0 | 0 | 241 |
| partitions | 5,000 | 5,000 | 0 | 0 | 0 | 483 |
| crashes (in-memory Raft state) | 6,250 | 6,250 | 0 | 0 | 0 | 473 |
| disk crashes (real files, unsynced bytes dropped) | 6,250 | 6,250 | 0 | 0 | 0 | 352 |
| all faults + snapshots every 50 entries | 5,000 | 5,000 | 0 | 0 | 0 | 179 |
| all faults + pipelined replication | 5,000 | 5,000 | 0 | 0 | 0 | 181 |
| unreliable + partitions + pipelined | 6,250 | 6,250 | 0 | 0 | 0 | 221 |
| sharded, migrations | 2,500 | 2,500 | 0 | 0 | 0 | 500 |
| sharded, crashes + unreliable | 2,500 | 2,500 | 0 | 0 | 0 | 500 |
| **total** | **50,000** | **50,000** | **0** | **0** | **0** | **345** |

Wall time: 3,654 s with 22 workers. Raw output: `bench-results/lincheck-50k.{txt,json}`.

**Does the checker catch bugs?** It does. Two deliberate bugs were run through the harness while building it: serving reads without ReadIndex was flagged in 9 of 40 histories, and skipping the shard pull during migration was flagged in 10 of 10 sharded histories. `TestCheckerRejectsStaleRead` keeps a minimal stale-read history in the unit tests.

**Limitations.** Histories are short (about 345 operations on average) because porcupine's search grows exponentially with concurrency. Uncapped runs produced histories of 30K–330K operations that it couldn't finish checking. Faults come from a simulated network inside one process, and real processes are covered by section 4.

## 4. Crash-recovery runs

Three kinds of crash, 1,000 runs each, all with `strata-crashtest`. Each run fails if any acknowledged write is missing or wrong after recovery.

| mode | what crashes | runs | passed | failed | wall time |
|---|---|---|---|---|---|
| `wal` | WAL simulated power loss | 1,000 | 1,000 | 0 | 64 s |
| `lsm` | LSM (with its WAL) simulated power loss | 1,000 | 1,000 | 0 | 403 s |
| `cluster` | real `strata-server` processes, hard-killed | 1,000 | 1,000 | 0 | 2,249 s |

```
./bin/strata-crashtest -mode wal     -runs 1000 -seed 1 -out bench-results/crash-wal.json
./bin/strata-crashtest -mode lsm     -runs 1000 -seed 1 -out bench-results/crash-lsm.json
./bin/strata-crashtest -mode cluster -runs 1000 -seed 1 -dir tmp/crash-cluster -out bench-results/crash-cluster.json
```

- **wal:** each run appends 1–300 records and fsyncs a random quarter of them, then simulates a crash. Everything after the last fsync is cut at a random byte offset, and random junk is sometimes appended. On reopen, every fsynced record must be there, intact, in order, with nothing invented.
- **lsm:** 4 concurrent writers do 20–120 Puts each through group-committed WAL fsyncs, with memtables of 1–8 KiB so flushes and compactions happen mid-run. Then the store crashes with a torn log tail. Every acknowledged value must survive, and no key may hold a value the test didn't expect.
- **cluster:** a 5-process cluster (Raft log compaction every 2,000 entries, so the log is compacted many times during the run) takes writes from 8 concurrent writers. Each iteration hard-kills (`TerminateProcess`) the leader (337 iterations), a random minority of 1–2 nodes (323), or all five (340), while writes are in flight. It restarts them and reads back, through linearizable Gets, every write acknowledged in that iteration (1,257,432 in total) plus 200 randomly chosen older writes per iteration. Each process started 488–518 times over the run (counted from the node logs).

A process kill leaves the OS page cache intact, so `cluster` mode tests recovery logic: log replay, applied-index tracking, LSM recovery, elections and snapshot catch-up. The loss of unsynced data is covered by `wal` and `lsm`, and by the 6,250 disk-crash linearizability histories in section 3.

## Discarded run

The first pass of the benchmark matrix (raw files kept locally, not committed) had a harness bug. `local-cluster.sh stop` returned before the old servers exited, so the next run's cluster could start while ports were still held. Three unbatched-256 runs read 0 ops/s, and throughput fell with every rep. The script now waits for processes to exit and for a leader before each run, and every number above comes from the second pass. As it turned out, the unbatched-256 stall reproduced even with the fix (section 2).
