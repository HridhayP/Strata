// Command strata-server runs one Strata node. A node can host a replica of
// the shard controller and replicas of one or more KV groups, all served on
// a single gRPC port, plus a Prometheus /metrics endpoint.
//
// Unsharded (one KV group serving every shard, no controller):
//
//	strata-server -id 1 -nodes 1=localhost:7001,2=localhost:7002,3=localhost:7003 \
//	    -groups 1=1,2,3 -data ./data/1
//
// Sharded (controller on nodes 1-3, two KV groups):
//
//	strata-server -id 1 -nodes ... -ctrl 1,2,3 -groups 1=1,2,3;2=4,5,6 -bootstrap
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/HridhayP/strata/kv"
	"github.com/HridhayP/strata/metrics"
	"github.com/HridhayP/strata/raft"
	"github.com/HridhayP/strata/shard"
	"github.com/HridhayP/strata/storage/wal"
	"github.com/HridhayP/strata/transport/grpcx"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

func main() {
	var (
		id          = flag.Int("id", 0, "this node's ID")
		nodesFlag   = flag.String("nodes", "", "node address book: id=host:port,...")
		listen      = flag.String("listen", "", "gRPC listen address (default: this node's address book entry)")
		ctrlFlag    = flag.String("ctrl", "", "node IDs of the controller group (empty: unsharded)")
		groupsFlag  = flag.String("groups", "", "KV groups: gid=id,id,...;gid=...")
		dataDir     = flag.String("data", "data", "data directory")
		metricsAddr = flag.String("metrics", "", "Prometheus/status HTTP address, e.g. :9100")
		bootstrap   = flag.Bool("bootstrap", false, "join all configured groups at the controller once it is up")
		noBatch     = flag.Bool("no-fsync-batching", false, "fsync every WAL append individually (benchmark baseline)")
		election    = flag.Duration("election-timeout", 300*time.Millisecond, "Raft election timeout")
		maxLog      = flag.Int("max-raft-log", 50000, "log entries before compaction")
	)
	flag.Parse()

	nodes, err := parseNodes(*nodesFlag)
	if err != nil {
		log.Fatal(err)
	}
	ctrlIDs, err := parseIDs(*ctrlFlag)
	if err != nil {
		log.Fatal(err)
	}
	groups, err := parseGroups(*groupsFlag)
	if err != nil {
		log.Fatal(err)
	}
	addr := *listen
	if addr == "" {
		a, ok := nodes[*id]
		if !ok {
			log.Fatalf("node %d is not in -nodes", *id)
		}
		// Listen on all interfaces at the configured port.
		_, port, _ := net.SplitHostPort(a)
		addr = ":" + port
	}

	pool := grpcx.NewPool(nodes)
	host := grpcx.NewHost()
	transport := &grpcx.RaftTransport{Pool: pool}
	walOpts := wal.Options{NoBatch: *noBatch}
	sharded := len(ctrlIDs) > 0

	var stops []func()
	var statuses []func() any

	if slices.Contains(ctrlIDs, *id) {
		st, err := raft.OpenDiskStorage(filepath.Join(*dataDir, "ctrl"), walOpts)
		if err != nil {
			log.Fatal(err)
		}
		c, err := shard.NewController(shard.ControllerConfig{
			ID: *id, Peers: ctrlIDs, Group: grpcx.CtrlGroup, Storage: st, Transport: transport,
			ElectionTimeout: *election,
		})
		if err != nil {
			log.Fatal(err)
		}
		host.SetController(c)
		metrics.RegisterCollector(grpcx.CtrlGroup, c.Raft(), nil)
		stops = append(stops, func() { c.Stop(); st.Close() })
		statuses = append(statuses, func() any { return c.Raft().Status() })
		log.Printf("node %d: controller replica (peers %v)", *id, ctrlIDs)
	}

	var ctrlClerk *shard.CtrlClerk
	if sharded {
		ctrlClerk = shard.NewCtrlClerk(ctrlIDs, grpcx.CtrlCaller{Pool: pool})
	}
	gids := make([]int64, 0, len(groups))
	for gid := range groups {
		gids = append(gids, gid)
	}
	slices.Sort(gids)
	for _, gid := range gids {
		members := groups[gid]
		if !slices.Contains(members, *id) {
			continue
		}
		name := kv.GroupName(gid)
		dir := filepath.Join(*dataDir, name)
		st, err := raft.OpenDiskStorage(filepath.Join(dir, "raft"), walOpts)
		if err != nil {
			log.Fatal(err)
		}
		cfg := kv.Config{
			ID: *id, Peers: members, Group: name, Dir: filepath.Join(dir, "lsm"),
			RaftStorage: st, Transport: transport,
			ElectionTimeout: *election, MaxRaftLog: *maxLog,
			Observer: metrics.NewObserver(name),
		}
		if sharded {
			cfg.GID = gid
			cfg.Ctrl = ctrlClerk
			cfg.PeerCaller = grpcx.KVCaller{Pool: pool}
		}
		s, err := kv.NewServer(cfg)
		if err != nil {
			log.Fatal(err)
		}
		host.AddKV(name, s)
		metrics.RegisterCollector(name, s.Raft(), s.DB())
		stops = append(stops, func() { s.Stop(false); st.Close() })
		statuses = append(statuses, func() any { return s.Raft().Status() })
		log.Printf("node %d: %s replica (peers %v, sharded=%v)", *id, name, members, sharded)
	}
	if len(stops) == 0 {
		log.Fatalf("node %d belongs to no group", *id)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(256<<20), grpc.MaxSendMsgSize(256<<20))
	host.Register(gs)
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Printf("grpc: %v", err)
		}
	}()
	log.Printf("node %d: serving gRPC on %s", *id, addr)

	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
			var out []any
			for _, f := range statuses {
				out = append(out, f())
			}
			json.NewEncoder(w).Encode(out)
		})
		go func() { log.Print(http.ListenAndServe(*metricsAddr, mux)) }()
	}

	if *bootstrap && sharded {
		go bootstrapGroups(ctrlClerk, groups)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("node %d: shutting down", *id)
	gs.Stop()
	for _, stop := range stops {
		stop()
	}
	pool.Close()
}

// bootstrapGroups joins every configured group that the latest config lacks.
func bootstrapGroups(ck *shard.CtrlClerk, groups map[int64][]int) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cfg, err := ck.Query(ctx, -1)
	if err != nil {
		log.Printf("bootstrap: %v", err)
		return
	}
	missing := map[int64][]int32{}
	for gid, members := range groups {
		if _, ok := cfg.Groups[gid]; !ok {
			for _, m := range members {
				missing[gid] = append(missing[gid], int32(m))
			}
		}
	}
	if len(missing) == 0 {
		return
	}
	if err := ck.Join(ctx, missing); err != nil {
		log.Printf("bootstrap: join: %v", err)
		return
	}
	log.Printf("bootstrap: joined groups %v", keys(missing))
}

func keys(m map[int64][]int32) []int64 {
	var out []int64
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func parseNodes(s string) (map[int]string, error) {
	out := map[int]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("bad -nodes entry %q", part)
		}
		id, err := strconv.Atoi(k)
		if err != nil {
			return nil, fmt.Errorf("bad node id %q", k)
		}
		out[id] = v
	}
	return out, nil
}

func parseIDs(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("bad id %q", part)
		}
		out = append(out, id)
	}
	return out, nil
}

func parseGroups(s string) (map[int64][]int, error) {
	out := map[int64][]int{}
	for _, part := range strings.Split(s, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		g, members, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("bad -groups entry %q", part)
		}
		gid, err := strconv.ParseInt(g, 10, 64)
		if err != nil || gid <= 0 {
			return nil, fmt.Errorf("bad gid %q", g)
		}
		ids, err := parseIDs(members)
		if err != nil {
			return nil, err
		}
		out[gid] = ids
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("-groups is required")
	}
	return out, nil
}
