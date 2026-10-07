# Strata build plan

Working plan for this repo. The checklist is the source of truth for "where did we leave off".

## Spec

Fault-tolerant sharded distributed key-value store in Go.

- Raft from scratch: leader election, log replication, persistence, snapshotting/log compaction. Node-to-node RPC over gRPC (protobuf).
- KV service on top with linearizable Get/Put/Append (client request IDs for dedup).
- Sharding: consistent hashing across replica groups, shard controller, live shard migration.
- Storage: write-ahead log with fsync batching + a simple LSM-tree (memtable, SSTables, compaction). Crash-recovery tests.
- Fault-injection harness: simulated network partitions, node crashes/restarts, message delay/reordering. Record operation histories and verify linearizability with porcupine.
- Benchmarks: load generator measuring ops/sec and p50/p99 latency on a local 5-node cluster (90/10 read/write). Write throughput with vs without fsync batching. Count of crash-recovery runs passed. Count of linearizability-checked histories.
- Dockerfile + docker-compose for a 5-node cluster, k8s manifests, Prometheus metrics (commit latency, leader changes, replication lag).
- Resume targets to compare against (report real numbers, hit or miss): 120K ops/sec, 4ms p99, 3.2x write throughput, 50K histories, 1,000+ crash-recovery runs.

Global rules: real code, passing unit tests, CI (build + test), only measured numbers in RESULTS.md, incremental commits, push at milestones, README with Mermaid diagram + design decisions, INTERVIEW_NOTES.md with 10 Q&As.

## Layout

| Package | Role |
|---|---|
| `proto/` | protobuf definitions (Raft RPCs, KV client API, controller API) |
| `storage/wal` | append-only record log, CRC-framed, group-commit fsync |
| `storage/lsm` | memtable (skiplist) + SSTables + size-tiered compaction + manifest |
| `raft` | Raft consensus; pluggable `Transport`; log persisted through `storage/wal` |
| `transport/sim` | in-memory network for tests: partitions, drops, delay, reordering |
| `transport/grpcx` | gRPC transport for real clusters |
| `kv` | replicated state machine on LSM; dedup table; ReadIndex reads |
| `shard` | consistent-hash ring, shard controller, shard migration |
| `harness` | fault-injection cluster + porcupine linearizability checking |
| `cmd/strata-server`, `cmd/strata-bench`, `cmd/strata-crashtest` | binaries |

## Milestones

- [x] M1 WAL with group commit + crash tests
- [x] M2 Raft: election, replication, persistence, snapshots; sim transport tests
- [x] M3 LSM engine + crash tests
- [x] M4 KV service (linearizable, dedup, ReadIndex) + porcupine harness
- [x] M5 Sharding: ring, controller, live migration
- [x] M6 gRPC transport, server binary, Prometheus metrics
- [x] M7 Benchmarks run, RESULTS.md
- [x] M8 Docker, compose, k8s, CI, README, INTERVIEW_NOTES
