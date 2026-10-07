// Command strata-crashtest counts crash-recovery runs that preserve every
// acknowledged write. It has three modes:
//
//   - wal: randomized power-loss runs of the write-ahead log. Unsynced bytes
//     are cut at a random point and junk may follow (wal.CrashCheck).
//   - lsm: randomized crash runs of the LSM engine with its WAL, with a torn
//     log tail (lsm.CrashCheck).
//   - cluster: real strata-server processes. Each iteration runs writers
//     against a 5-node cluster, hard-kills the leader, a random minority, or
//     every node while writes are in flight, restarts them, and reads back
//     every acknowledged write through linearizable Gets.
//
// A process kill leaves the OS page cache intact, so cluster mode checks
// recovery logic (log replay, applied-index tracking, LSM recovery, snapshot
// catch-up) rather than fsync placement; the wal and lsm modes cover the
// loss of unsynced data.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/HridhayP/strata/kv"
	"github.com/HridhayP/strata/storage/lsm"
	"github.com/HridhayP/strata/storage/wal"
	"github.com/HridhayP/strata/transport/grpcx"
)

// Report is printed as JSON at the end.
type Report struct {
	Mode     string         `json:"mode"`
	Runs     int            `json:"runs"`
	Passed   int            `json:"passed"`
	Failed   int            `json:"failed"`
	Seed     int64          `json:"seed"`
	WallTime float64        `json:"wall_time_s"`
	Kills    map[string]int `json:"kills,omitempty"`
	Checked  int            `json:"acked_writes_checked,omitempty"`
	Failures []string       `json:"failures,omitempty"`
}

func main() {
	var (
		mode     = flag.String("mode", "wal", "wal, lsm or cluster")
		runs     = flag.Int("runs", 1000, "crash-recovery runs")
		seed     = flag.Int64("seed", 1, "random seed")
		dir      = flag.String("dir", "", "working directory (default: a temp dir, removed afterwards)")
		out      = flag.String("out", "", "also write the JSON report here")
		server   = flag.String("server", "bin/strata-server", "cluster: strata-server binary")
		nodes    = flag.Int("nodes", 5, "cluster: number of nodes")
		basePort = flag.Int("base-port", 7200, "cluster: node i listens on base-port+i, metrics on base-port+100+i")
		writers  = flag.Int("writers", 8, "cluster: concurrent writers per iteration")
		maxLog   = flag.Int("max-raft-log", 2000, "cluster: Raft log entries before compaction")
	)
	flag.Parse()

	work := *dir
	if work == "" {
		d, err := os.MkdirTemp("", "strata-crashtest-")
		if err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(d)
		work = d
	}
	rep := &Report{Mode: *mode, Seed: *seed}
	start := time.Now()
	rng := rand.New(rand.NewSource(*seed))
	record := func(i int, err error) {
		rep.Runs++
		if err == nil {
			rep.Passed++
			return
		}
		rep.Failed++
		msg := fmt.Sprintf("run %d: %v", i, err)
		rep.Failures = append(rep.Failures, msg)
		log.Print(msg)
	}
	progress := func(i int) {
		if (i+1)%100 == 0 {
			log.Printf("%d/%d runs, %d failed (%.0fs)", i+1, *runs, rep.Failed, time.Since(start).Seconds())
		}
	}

	switch *mode {
	case "wal":
		for i := 0; i < *runs; i++ {
			record(i, wal.CrashCheck(work, rng))
			progress(i)
		}
	case "lsm":
		for i := 0; i < *runs; i++ {
			record(i, lsm.CrashCheck(filepath.Join(work, "db"), rng))
			progress(i)
		}
	case "cluster":
		srv := *server
		if _, err := os.Stat(srv); err != nil {
			srv += ".exe"
		}
		c := newCluster(srv, work, *nodes, *basePort, *maxLog)
		defer c.stopAll()
		rep.Kills = map[string]int{}
		if err := c.startAll(); err != nil {
			log.Fatal(err)
		}
		var history []string // keys acked in earlier iterations
		for i := 0; i < *runs; i++ {
			kind, keys, err := c.iteration(i, rng, *writers, history)
			rep.Kills[kind]++
			rep.Checked += len(keys)
			record(i, err)
			history = append(history, keys...)
			progress(i)
		}
	default:
		log.Fatalf("unknown -mode %q", *mode)
	}

	rep.WallTime = time.Since(start).Seconds()
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		os.WriteFile(*out, append(b, '\n'), 0o644)
	}
	if rep.Failed > 0 {
		os.Exit(1)
	}
}

type cluster struct {
	bin, dir string
	n        int
	basePort int
	maxLog   int
	addrs    map[int]string
	procs    map[int]*proc
	pool     *grpcx.Pool
	ids      []int
}

func newCluster(bin, dir string, n, basePort, maxLog int) *cluster {
	c := &cluster{bin: bin, dir: dir, n: n, basePort: basePort, maxLog: maxLog,
		addrs: map[int]string{}, procs: map[int]*proc{}}
	for i := 1; i <= n; i++ {
		c.addrs[i] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
		c.ids = append(c.ids, i)
	}
	c.pool = grpcx.NewPool(c.addrs)
	return c
}

type proc struct {
	cmd    *exec.Cmd
	exited chan struct{}
}

func (c *cluster) start(id int) error {
	var book, members []string
	for _, i := range c.ids {
		book = append(book, fmt.Sprintf("%d=%s", i, c.addrs[i]))
		members = append(members, fmt.Sprint(i))
	}
	logf, err := os.OpenFile(filepath.Join(c.dir, fmt.Sprintf("node%d.log", id)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(c.bin,
		"-id", fmt.Sprint(id),
		"-nodes", strings.Join(book, ","),
		"-groups", "1="+strings.Join(members, ","),
		"-data", filepath.Join(c.dir, "data", fmt.Sprint(id)),
		"-metrics", fmt.Sprintf("127.0.0.1:%d", c.basePort+100+id),
		"-max-raft-log", fmt.Sprint(c.maxLog),
	)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		logf.Close()
		return err
	}
	p := &proc{cmd: cmd, exited: make(chan struct{})}
	go func() { cmd.Wait(); logf.Close(); close(p.exited) }()
	c.procs[id] = p
	return nil
}

func (c *cluster) startAll() error {
	for _, id := range c.ids {
		if err := c.start(id); err != nil {
			return err
		}
	}
	return nil
}

// kill terminates a node abruptly (TerminateProcess on Windows, SIGKILL
// elsewhere), so it gets no chance to flush or close anything. It returns
// once the process is gone and its files and port are released.
func (c *cluster) kill(id int) {
	if p := c.procs[id]; p != nil {
		p.cmd.Process.Kill()
		<-p.exited
		delete(c.procs, id)
	}
}

func (c *cluster) stopAll() {
	for id := range c.procs {
		c.kill(id)
	}
	c.pool.Close()
}

// leader asks each live node's /status endpoint who it thinks leads.
func (c *cluster) leader() int {
	cl := http.Client{Timeout: 500 * time.Millisecond}
	for id := range c.procs {
		resp, err := cl.Get(fmt.Sprintf("http://127.0.0.1:%d/status", c.basePort+100+id))
		if err != nil {
			continue
		}
		var st []struct{ Role string }
		json.NewDecoder(resp.Body).Decode(&st)
		resp.Body.Close()
		if len(st) > 0 && st[0].Role == "leader" {
			return id
		}
	}
	return 0
}

func (c *cluster) clerk() *kv.Clerk {
	return kv.NewClerk(c.ids, kv.GroupName(1), grpcx.KVCaller{Pool: c.pool})
}

// iteration writes under load, kills nodes mid-write, restarts them and
// verifies every acknowledged write. It returns the kill kind and the keys
// acknowledged in this iteration.
func (c *cluster) iteration(it int, rng *rand.Rand, writers int, history []string) (string, []string, error) {
	var (
		mu    sync.Mutex
		acked []string
		wg    sync.WaitGroup
	)
	stop := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ck := c.clerk()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("it%06d-w%02d-%06d", it, w, n)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				err := ck.Put(ctx, k, "v-"+k)
				cancel()
				if err == nil {
					mu.Lock()
					acked = append(acked, k)
					mu.Unlock()
				}
			}
		}(w)
	}

	// Let writes flow, then kill while they are still in flight.
	time.Sleep(time.Duration(200+rng.Intn(800)) * time.Millisecond)
	var victims []int
	kind := []string{"leader", "minority", "all"}[rng.Intn(3)]
	switch kind {
	case "leader":
		if l := c.leader(); l != 0 {
			victims = []int{l}
		} else {
			victims = []int{c.ids[rng.Intn(c.n)]}
		}
	case "minority":
		perm := rng.Perm(c.n)
		for _, p := range perm[:1+rng.Intn((c.n-1)/2)] {
			victims = append(victims, c.ids[p])
		}
	case "all":
		victims = append(victims, c.ids...)
	}
	for _, v := range victims {
		c.kill(v)
	}
	// Survivors keep serving (or not, after a full kill) for a moment.
	time.Sleep(time.Duration(100+rng.Intn(400)) * time.Millisecond)
	close(stop)
	wg.Wait()
	for _, v := range victims {
		if err := c.start(v); err != nil {
			return kind, acked, err
		}
	}

	// Verify: everything acked this iteration, plus a sample of older keys.
	check := append([]string(nil), acked...)
	for i := 0; i < 200 && len(history) > 0; i++ {
		check = append(check, history[rng.Intn(len(history))])
	}
	ck := c.clerk()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, k := range check {
		v, ok, err := ck.Get(ctx, k)
		if err != nil {
			return kind, acked, fmt.Errorf("reading %s after %s kill: %w", k, kind, err)
		}
		if !ok || v != "v-"+k {
			return kind, acked, fmt.Errorf("acked write %s lost after %s kill (got %q, present=%v)", k, kind, v, ok)
		}
	}
	if len(acked) == 0 && kind != "all" {
		return kind, acked, errors.New("no writes were acknowledged before the kill")
	}
	return kind, acked, nil
}
