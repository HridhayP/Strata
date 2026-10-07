// Command strata-bench is a closed-loop load generator. Each of -clients
// goroutines issues one operation at a time (reads with probability
// -read-ratio, otherwise Puts) against a running cluster and records every
// operation's latency. It reports throughput and latency percentiles.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HridhayP/strata/kv"
	"github.com/HridhayP/strata/shard"
	"github.com/HridhayP/strata/transport/grpcx"
)

type client interface {
	Get(ctx context.Context, key string) (string, bool, error)
	Put(ctx context.Context, key, value string) error
}

// Report is printed as JSON at the end of a run.
type Report struct {
	Clients    int     `json:"clients"`
	Duration   float64 `json:"duration_s"`
	ReadRatio  float64 `json:"read_ratio"`
	Keys       int     `json:"keys"`
	ValueSize  int     `json:"value_size"`
	Ops        int     `json:"ops"`
	Reads      int     `json:"reads"`
	Writes     int     `json:"writes"`
	Errors     int64   `json:"errors"`
	OpsPerSec  float64 `json:"ops_per_sec"`
	P50Ms      float64 `json:"p50_ms"`
	P99Ms      float64 `json:"p99_ms"`
	P999Ms     float64 `json:"p999_ms"`
	ReadP50Ms  float64 `json:"read_p50_ms"`
	ReadP99Ms  float64 `json:"read_p99_ms"`
	WriteP50Ms float64 `json:"write_p50_ms"`
	WriteP99Ms float64 `json:"write_p99_ms"`
}

func main() {
	var (
		nodesFlag = flag.String("nodes", "", "node address book: id=host:port,...")
		servers   = flag.String("servers", "", "unsharded: IDs of the KV group's servers")
		group     = flag.Int64("group", 1, "unsharded: group ID")
		ctrl      = flag.String("ctrl", "", "sharded: controller node IDs")
		clients   = flag.Int("clients", 64, "concurrent closed-loop clients")
		duration  = flag.Duration("duration", 30*time.Second, "measurement duration")
		warmup    = flag.Duration("warmup", 5*time.Second, "warm-up before measuring")
		readRatio = flag.Float64("read-ratio", 0.9, "fraction of operations that are reads")
		keys      = flag.Int("keys", 10000, "keyspace size")
		valueSize = flag.Int("value-size", 100, "bytes per value")
		preload   = flag.Bool("preload", true, "write every key once before the run")
		out       = flag.String("out", "", "also write the JSON report to this file")
	)
	flag.Parse()

	nodes := map[int]string{}
	for _, part := range strings.Split(*nodesFlag, ",") {
		if k, v, ok := strings.Cut(strings.TrimSpace(part), "="); ok {
			id, err := strconv.Atoi(k)
			if err != nil {
				log.Fatalf("bad node %q", part)
			}
			nodes[id] = v
		}
	}
	pool := grpcx.NewPool(nodes)
	defer pool.Close()

	newClient := func() client {
		if *ctrl != "" {
			return shard.NewClerk(shard.NewCtrlClerk(ids(*ctrl), grpcx.CtrlCaller{Pool: pool}), grpcx.KVCaller{Pool: pool})
		}
		return kv.NewClerk(ids(*servers), kv.GroupName(*group), grpcx.KVCaller{Pool: pool})
	}
	value := strings.Repeat("v", *valueSize)
	key := func(i int) string { return fmt.Sprintf("key%08d", i) }

	if *preload {
		log.Printf("preloading %d keys", *keys)
		var wg sync.WaitGroup
		var next atomic.Int64
		for c := 0; c < *clients; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cl := newClient()
				for {
					i := int(next.Add(1)) - 1
					if i >= *keys {
						return
					}
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					if err := cl.Put(ctx, key(i), value); err != nil {
						log.Fatalf("preload: %v", err)
					}
					cancel()
				}
			}()
		}
		wg.Wait()
	}

	type sample struct {
		lat  time.Duration
		read bool
	}
	var (
		measuring atomic.Bool
		stop      atomic.Bool
		errs      atomic.Int64
		wg        sync.WaitGroup
		mu        sync.Mutex
		all       []sample
	)
	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			cl := newClient()
			rng := rand.New(rand.NewPCG(uint64(c), 42))
			var local []sample
			for !stop.Load() {
				k := key(rng.IntN(*keys))
				read := rng.Float64() < *readRatio
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				t0 := time.Now()
				var err error
				if read {
					_, _, err = cl.Get(ctx, k)
				} else {
					err = cl.Put(ctx, k, value)
				}
				lat := time.Since(t0)
				cancel()
				if !measuring.Load() {
					continue
				}
				if err != nil {
					errs.Add(1)
					continue
				}
				local = append(local, sample{lat, read})
			}
			mu.Lock()
			all = append(all, local...)
			mu.Unlock()
		}(c)
	}
	log.Printf("warming up for %v", *warmup)
	time.Sleep(*warmup)
	measuring.Store(true)
	start := time.Now()
	log.Printf("measuring for %v with %d clients", *duration, *clients)
	time.Sleep(*duration)
	measuring.Store(false)
	elapsed := time.Since(start)
	stop.Store(true)
	wg.Wait()

	var lat, rlat, wlat []time.Duration
	for _, s := range all {
		lat = append(lat, s.lat)
		if s.read {
			rlat = append(rlat, s.lat)
		} else {
			wlat = append(wlat, s.lat)
		}
	}
	r := Report{
		Clients: *clients, Duration: elapsed.Seconds(), ReadRatio: *readRatio, Keys: *keys, ValueSize: *valueSize,
		Ops: len(lat), Reads: len(rlat), Writes: len(wlat), Errors: errs.Load(),
		OpsPerSec: float64(len(lat)) / elapsed.Seconds(),
		P50Ms:     pct(lat, 0.50), P99Ms: pct(lat, 0.99), P999Ms: pct(lat, 0.999),
		ReadP50Ms: pct(rlat, 0.50), ReadP99Ms: pct(rlat, 0.99),
		WriteP50Ms: pct(wlat, 0.50), WriteP99Ms: pct(wlat, 0.99),
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		os.WriteFile(*out, append(b, '\n'), 0o644)
	}
}

// pct returns the q-quantile in milliseconds (nearest-rank).
func pct(d []time.Duration, q float64) float64 {
	if len(d) == 0 {
		return 0
	}
	slices.Sort(d)
	i := int(q*float64(len(d))+0.5) - 1
	i = max(0, min(i, len(d)-1))
	return float64(d[i].Microseconds()) / 1000
}

func ids(s string) []int {
	var out []int
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			id, err := strconv.Atoi(p)
			if err != nil {
				log.Fatalf("bad id %q", p)
			}
			out = append(out, id)
		}
	}
	return out
}
