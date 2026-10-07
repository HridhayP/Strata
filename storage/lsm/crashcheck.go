package lsm

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
)

// CrashCheck performs random writes into a fresh database in dir with the
// WAL enabled, crashes with a random torn WAL tail, reopens, and returns an
// error unless every acknowledged write survived and nothing unexpected
// appeared. The unit tests and cmd/strata-crashtest both run it.
func CrashCheck(dir string, rng *rand.Rand) error {
	os.RemoveAll(dir)
	opts := Options{WAL: true, MemtableSize: 1 << 10 << rng.Intn(4), CompactionTrigger: 3}
	db, err := Open(dir, opts)
	if err != nil {
		return err
	}
	// Four writers on disjoint key ranges share WAL fsyncs (group commit).
	// Each Put returns only after its record is durable, so its value is
	// what must survive unless the same writer overwrote it later.
	acked := map[string]string{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var werr error
	n := 20 + rng.Intn(100)
	for g := 0; g < 4; g++ {
		seed := rng.Int63()
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < n; i++ {
				k := string(checkKey(g*15 + r.Intn(15)))
				v := fmt.Sprint("v", g, "-", i)
				if err := db.Put([]byte(k), []byte(v)); err != nil {
					mu.Lock()
					werr = err
					mu.Unlock()
					return
				}
				mu.Lock()
				acked[k] = v
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	if werr != nil {
		return werr
	}
	walPath := filepath.Join(dir, "lsm.wal")
	db.CrashClose()
	// Torn tail: append garbage that recovery must ignore.
	if rng.Intn(2) == 0 {
		f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0)
		if err == nil {
			junk := make([]byte, 1+rng.Intn(30))
			rng.Read(junk)
			f.Write(junk)
			f.Close()
		}
	}
	db, err = Open(dir, opts)
	if err != nil {
		return err
	}
	defer db.Close()
	for i := 0; i < 60; i++ {
		k := checkKey(i)
		v, ok, err := db.Get(k)
		if err != nil {
			return err
		}
		want, wantOK := acked[string(k)]
		if ok != wantOK || string(v) != want {
			return fmt.Errorf("%s = %q,%v after crash, want %q,%v", k, v, ok, want, wantOK)
		}
	}
	return nil
}

func checkKey(i int) []byte { return []byte(fmt.Sprintf("key-%05d", i)) }
