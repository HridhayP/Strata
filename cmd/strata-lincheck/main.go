// Command strata-lincheck runs many randomized fault-injection histories
// against simulated Strata clusters and checks each one for linearizability
// with porcupine. It rotates through fault scenarios by seed, runs -workers
// histories in parallel, and prints per-scenario counts of Ok, Illegal and
// Unknown (checker timeout) results.
//
// Every failure is logged with its seed and scenario so it can be replayed:
//
//	strata-lincheck -n 1 -seed <seed>
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/HridhayP/strata/harness"
	"github.com/anishathalye/porcupine"
)

type scenario struct {
	name    string
	sharded bool
	run     func(seed uint64, d time.Duration) (*harness.Result, error)
}

func unsharded(name string, o harness.Options) scenario {
	return scenario{name: name, run: func(seed uint64, d time.Duration) (*harness.Result, error) {
		o := o
		o.Seed, o.Duration = seed, d
		return harness.Run(o)
	}}
}

func sharded(name string, o harness.ShardedOptions) scenario {
	return scenario{name: name, sharded: true, run: func(seed uint64, d time.Duration) (*harness.Result, error) {
		o := o
		o.Seed, o.Duration = seed, 2*d // migrations need longer runs
		return harness.RunSharded(o)
	}}
}

var unshardedScenarios = []scenario{
	unsharded("reliable", harness.Options{}),
	unsharded("unreliable", harness.Options{Unreliable: true}),
	unsharded("partitions", harness.Options{Partitions: true}),
	unsharded("crashes", harness.Options{Crashes: true}),
	unsharded("all-faults+snapshots", harness.Options{Unreliable: true, Partitions: true, Crashes: true, MaxRaftLog: 50}),
	unsharded("disk-crashes", harness.Options{Crashes: true, DiskRaft: true, MaxRaftLog: 100}),
	unsharded("all-faults+pipelined", harness.Options{Unreliable: true, Partitions: true, Crashes: true, MaxRaftLog: 50, MaxInflight: 4}),
	unsharded("unreliable+partitions+pipelined", harness.Options{Unreliable: true, Partitions: true, MaxInflight: 4}),
}

var shardedScenarios = []scenario{
	sharded("sharded-migrations", harness.ShardedOptions{}),
	sharded("sharded-crashes+unreliable", harness.ShardedOptions{Crashes: true, Unreliable: true, MaxRaftLog: 100}),
}

// pick maps a seed to a scenario: every shardedEvery'th seed is sharded.
func pick(seed uint64, shardedEvery int) scenario {
	if shardedEvery > 0 && seed%uint64(shardedEvery) == 0 {
		return shardedScenarios[(seed/uint64(shardedEvery))%uint64(len(shardedScenarios))]
	}
	return unshardedScenarios[seed%uint64(len(unshardedScenarios))]
}

type counts struct {
	Runs    int `json:"runs"`
	Ok      int `json:"ok"`
	Illegal int `json:"illegal"`
	Unknown int `json:"unknown"`
	Errors  int `json:"errors"`
	Stuck   int `json:"stuck"`
	Ops     int `json:"ops"`
	MinOps  int `json:"min_ops"`
}

func (c *counts) add(res *harness.Result, err error) {
	c.Runs++
	if err != nil {
		c.Errors++
		return
	}
	switch res.Linearizable {
	case porcupine.Ok:
		c.Ok++
	case porcupine.Illegal:
		c.Illegal++
	default:
		c.Unknown++
	}
	if res.Stuck {
		c.Stuck++
	}
	c.Ops += res.Ops
	if c.Runs == 1 || res.Ops < c.MinOps {
		c.MinOps = res.Ops
	}
}

// Report is printed as JSON at the end (and written to -out periodically).
type Report struct {
	Histories  int                `json:"histories"`
	Workers    int                `json:"workers"`
	Duration   string             `json:"fault_phase"`
	FirstSeed  uint64             `json:"first_seed"`
	WallTime   float64            `json:"wall_time_s"`
	Total      counts             `json:"total"`
	ByScenario map[string]*counts `json:"by_scenario"`
	Failures   []string           `json:"failures"`
}

func main() {
	var (
		n            = flag.Int("n", 1000, "histories to run")
		workers      = flag.Int("workers", runtime.NumCPU()/2, "histories run in parallel")
		seed         = flag.Uint64("seed", 1, "first seed; history i uses seed+i")
		duration     = flag.Duration("duration", time.Second, "fault phase per history (sharded runs use twice this)")
		shardedEvery = flag.Int("sharded-every", 10, "every Nth seed runs a sharded cluster (0: never)")
		out          = flag.String("out", "", "write the JSON report here (also every -checkpoint)")
		checkpoint   = flag.Duration("checkpoint", time.Minute, "progress and report interval")
	)
	flag.Parse()

	rep := &Report{
		Histories: *n, Workers: *workers, Duration: duration.String(), FirstSeed: *seed,
		ByScenario: map[string]*counts{},
	}
	var mu sync.Mutex
	start := time.Now()
	write := func() {
		rep.WallTime = time.Since(start).Seconds()
		b, _ := json.MarshalIndent(rep, "", "  ")
		if *out != "" {
			if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
				log.Printf("writing report: %v", err)
			}
		}
	}

	seeds := make(chan uint64)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range seeds {
				sc := pick(s, *shardedEvery)
				res, err := sc.run(s, *duration)
				mu.Lock()
				rep.Total.add(res, err)
				c := rep.ByScenario[sc.name]
				if c == nil {
					c = &counts{}
					rep.ByScenario[sc.name] = c
				}
				c.add(res, err)
				switch {
				case err != nil:
					rep.Failures = append(rep.Failures, fmt.Sprintf("seed=%d scenario=%s error=%v", s, sc.name, err))
				case res.Linearizable != porcupine.Ok:
					rep.Failures = append(rep.Failures, fmt.Sprintf("seed=%d scenario=%s result=%s ops=%d", s, sc.name, res.Linearizable, res.Ops))
				}
				if err != nil || res.Linearizable != porcupine.Ok {
					log.Print(rep.Failures[len(rep.Failures)-1])
				}
				mu.Unlock()
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		t := time.NewTicker(*checkpoint)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
			}
			mu.Lock()
			log.Printf("%d/%d histories: ok=%d illegal=%d unknown=%d errors=%d (%.0fs)",
				rep.Total.Runs, *n, rep.Total.Ok, rep.Total.Illegal, rep.Total.Unknown, rep.Total.Errors, time.Since(start).Seconds())
			write()
			mu.Unlock()
		}
	}()

	for i := 0; i < *n; i++ {
		seeds <- *seed + uint64(i)
	}
	close(seeds)
	wg.Wait()
	close(done)

	mu.Lock()
	defer mu.Unlock()
	write()
	names := make([]string, 0, len(rep.ByScenario))
	for k := range rep.ByScenario {
		names = append(names, k)
	}
	slices.Sort(names)
	fmt.Printf("%-34s %7s %7s %7s %7s %7s %9s\n", "scenario", "runs", "ok", "illegal", "unknown", "errors", "avg ops")
	for _, k := range append(names, "TOTAL") {
		c := &rep.Total
		if k != "TOTAL" {
			c = rep.ByScenario[k]
		}
		fmt.Printf("%-34s %7d %7d %7d %7d %7d %9.0f\n", k, c.Runs, c.Ok, c.Illegal, c.Unknown, c.Errors, float64(c.Ops)/float64(max(c.Runs, 1)))
	}
	fmt.Printf("wall time %.0fs with %d workers\n", rep.WallTime, *workers)
	if rep.Total.Illegal > 0 || rep.Total.Errors > 0 {
		os.Exit(1)
	}
}
