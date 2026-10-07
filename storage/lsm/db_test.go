package lsm

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func key(i int) []byte { return []byte(fmt.Sprintf("key-%05d", i)) }

func open(t *testing.T, dir string, opts Options) *DB {
	t.Helper()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// checkModel compares every key in the model (and absent keys) and a full
// scan against the database.
func checkModel(t *testing.T, db *DB, model map[string]string, keyspace int) {
	t.Helper()
	for i := 0; i < keyspace; i++ {
		k := key(i)
		v, ok, err := db.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		want, wantOK := model[string(k)]
		if ok != wantOK || (ok && string(v) != want) {
			t.Fatalf("Get(%s) = %q,%v want %q,%v", k, v, ok, want, wantOK)
		}
	}
	var keys []string
	for k := range model {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var got []string
	err := db.Scan([]byte("key-"), func(k, v []byte) bool {
		if string(v) != model[string(k)] {
			t.Fatalf("Scan %s = %q want %q", k, v, model[string(k)])
		}
		got = append(got, string(k))
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != fmt.Sprint(keys) {
		t.Fatalf("scan returned %d keys, want %d", len(got), len(keys))
	}
}

func TestRandomOpsAgainstModel(t *testing.T) {
	db := open(t, t.TempDir(), Options{MemtableSize: 8 << 10, CompactionTrigger: 3})
	model := map[string]string{}
	rng := rand.New(rand.NewSource(7))
	const keyspace = 500
	for i := 0; i < 20000; i++ {
		k := key(rng.Intn(keyspace))
		if rng.Intn(5) == 0 {
			if err := db.Delete(k); err != nil {
				t.Fatal(err)
			}
			delete(model, string(k))
		} else {
			v := fmt.Sprintf("v%d-%s", i, bytes.Repeat([]byte{'z'}, rng.Intn(40)))
			if err := db.Put(k, []byte(v)); err != nil {
				t.Fatal(err)
			}
			model[string(k)] = v
		}
		if i%5000 == 4999 {
			checkModel(t, db, model, keyspace)
		}
	}
	st := db.Stats()
	if st.Flushes == 0 || st.Compactions == 0 {
		t.Fatalf("expected flushes and compactions, got %+v", st)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	checkModel(t, db, model, keyspace)
	if st := db.Stats(); st.Tables > 1 {
		t.Fatalf("full compaction left %d tables", st.Tables)
	}
}

func TestReopenWithoutWALKeepsFlushedData(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{MemtableSize: 4 << 10})
	model := map[string]string{}
	for i := 0; i < 1000; i++ {
		v := fmt.Sprint("val", i)
		db.Put(key(i), []byte(v))
		model[string(key(i))] = v
	}
	if err := db.Close(); err != nil { // Close flushes
		t.Fatal(err)
	}
	db = open(t, dir, Options{MemtableSize: 4 << 10})
	checkModel(t, db, model, 1000)
}

func TestScanPrefixAndOverrides(t *testing.T) {
	db := open(t, t.TempDir(), Options{})
	db.Put([]byte("a/1"), []byte("old"))
	db.Put([]byte("b/1"), []byte("x"))
	db.Flush()
	db.Put([]byte("a/1"), []byte("new"))
	db.Put([]byte("a/2"), []byte("two"))
	db.Flush()
	db.Delete([]byte("a/2"))
	db.Put([]byte("a/3"), []byte("three"))
	var got []string
	db.Scan([]byte("a/"), func(k, v []byte) bool {
		got = append(got, string(k)+"="+string(v))
		return true
	})
	if want := "[a/1=new a/3=three]"; fmt.Sprint(got) != want {
		t.Fatalf("scan = %v, want %s", got, want)
	}
	// Early stop.
	n := 0
	db.Scan(nil, func(k, v []byte) bool { n++; return false })
	if n != 1 {
		t.Fatalf("scan did not stop: %d", n)
	}
}

func TestBloomFilterRejectsMostMisses(t *testing.T) {
	bf := newBloom(10000)
	for i := 0; i < 10000; i++ {
		bf.add(key(i))
	}
	for i := 0; i < 10000; i++ {
		if !bf.mayContain(key(i)) {
			t.Fatalf("false negative for %s", key(i))
		}
	}
	fp := 0
	for i := 10000; i < 20000; i++ {
		if bf.mayContain(key(i)) {
			fp++
		}
	}
	if rate := float64(fp) / 10000; rate > 0.03 {
		t.Fatalf("false-positive rate %.3f too high", rate)
	}
}

func TestConcurrentReadersDuringCompaction(t *testing.T) {
	db := open(t, t.TempDir(), Options{MemtableSize: 2 << 10, CompactionTrigger: 2})
	for i := 0; i < 2000; i++ {
		db.Put(key(i%300), []byte(fmt.Sprint(i)))
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := 0; i < 300; i++ {
					if _, ok, err := db.Get(key(i)); err != nil || !ok {
						t.Errorf("Get(%d) ok=%v err=%v", i, ok, err)
						return
					}
				}
			}
		}()
	}
	for i := 2000; i < 6000; i++ {
		db.Put(key(i%300), []byte(fmt.Sprint(i)))
	}
	close(stop)
	wg.Wait()
	db.Compact()
	// Obsolete table files must be gone once readers released them.
	ents, _ := os.ReadDir(db.dir)
	ssts := 0
	for _, e := range ents {
		if filepath.Ext(e.Name()) == ".sst" {
			ssts++
		}
	}
	if st := db.Stats(); ssts != st.Tables {
		t.Fatalf("%d sst files on disk but %d live tables", ssts, st.Tables)
	}
}

func TestCorruptBlockIsReported(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})
	for i := 0; i < 100; i++ {
		db.Put(key(i), []byte("v"))
	}
	db.Close()
	path := filepath.Join(dir, "000001.sst")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[10] ^= 0xff
	os.WriteFile(path, data, 0o644)
	db = open(t, dir, Options{})
	if _, _, err := db.Get(key(0)); err == nil {
		t.Fatal("expected corruption error")
	}
}

// crashRun performs random writes with the WAL enabled, crashes with a
// random torn WAL tail, reopens, and checks that every acknowledged write
// survived and nothing unexpected appeared.
func crashRun(dir string, rng *rand.Rand) error {
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
				k := string(key(g*15 + r.Intn(15)))
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
		k := key(i)
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

func TestCrashRecoveryRandomized(t *testing.T) {
	runs := 100
	if testing.Short() {
		runs = 20
	}
	base := t.TempDir()
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < runs; i++ {
		if err := crashRun(filepath.Join(base, "db"), rng); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	db, _ := Open(b.TempDir(), Options{})
	defer db.Close()
	for i := 0; i < 100000; i++ {
		db.Put(key(i), bytes.Repeat([]byte{'v'}, 100))
	}
	db.Flush()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			db.Get(key(rng.Intn(100000)))
		}
	})
}

// TestReadsDuringBackgroundFlush has readers check that a key's value never
// goes backwards or disappears while flushes and compactions run underneath.
func TestReadsDuringBackgroundFlush(t *testing.T) {
	db := open(t, t.TempDir(), Options{MemtableSize: 4 << 10, CompactionTrigger: 3})
	const keys = 50
	const rounds = 400
	for k := 0; k < keys; k++ {
		db.Put(key(k), []byte(fmt.Sprintf("%06d", 0)))
	}
	var wg sync.WaitGroup
	done := make(chan struct{})
	errs := make(chan error, 4)
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			last := make([]string, keys)
			for {
				select {
				case <-done:
					return
				default:
				}
				for k := 0; k < keys; k++ {
					v, ok, err := db.Get(key(k))
					if err != nil || !ok || string(v) < last[k] {
						errs <- fmt.Errorf("key %d: got %q ok=%v err=%v after %q", k, v, ok, err, last[k])
						return
					}
					last[k] = string(v)
				}
			}
		}()
	}
	for i := 1; i <= rounds; i++ {
		for k := 0; k < keys; k++ {
			if err := db.Put(key(k), []byte(fmt.Sprintf("%06d", i))); err != nil {
				t.Fatal(err)
			}
		}
		// Fresh keys grow the memtable so it keeps flushing.
		for j := 0; j < 5; j++ {
			db.Put(key(1000+i*5+j), bytes.Repeat([]byte{'f'}, 100))
		}
	}
	close(done)
	wg.Wait()
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	if st := db.Stats(); st.Flushes < 10 || st.Compactions == 0 {
		t.Fatalf("expected many flushes and a compaction, got %+v", st)
	}
}
