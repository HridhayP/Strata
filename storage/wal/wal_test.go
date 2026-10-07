package wal

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func rec(i int) Record {
	return Record{Type: byte(i % 7), Data: []byte(fmt.Sprintf("record-%06d-%s", i, bytes.Repeat([]byte{'x'}, i%50)))}
}

func mustOpen(t *testing.T, path string, opts Options) (*WAL, []Record) {
	t.Helper()
	w, recs, err := Open(path, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, recs
}

func checkPrefix(t *testing.T, got []Record, n int) {
	t.Helper()
	if len(got) != n {
		t.Fatalf("recovered %d records, want %d", len(got), n)
	}
	for i, r := range got {
		want := rec(i)
		if r.Type != want.Type || !bytes.Equal(r.Data, want.Data) {
			t.Fatalf("record %d mismatch: got %q want %q", i, r.Data, want.Data)
		}
	}
}

func TestAppendReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, recs := mustOpen(t, path, Options{})
	if len(recs) != 0 {
		t.Fatalf("fresh log has %d records", len(recs))
	}
	var seq uint64
	for i := 0; i < 100; i++ {
		s, err := w.Append(rec(i).Type, rec(i).Data)
		if err != nil {
			t.Fatal(err)
		}
		if s != uint64(i+1) {
			t.Fatalf("seq %d, want %d", s, i+1)
		}
		seq = s
	}
	if err := w.Sync(seq); err != nil {
		t.Fatal(err)
	}
	if w.DurableSize() != w.Size() {
		t.Fatalf("durable %d != size %d after sync", w.DurableSize(), w.Size())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, recs = mustOpen(t, path, Options{})
	defer w.Close()
	checkPrefix(t, recs, 100)

	// Appending after reopen continues the log.
	seq, err := w.Append(rec(100).Type, rec(100).Data)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(seq); err != nil {
		t.Fatal(err)
	}
	w.Close()
	_, recs = mustOpen(t, path, Options{})
	checkPrefix(t, recs, 101)
}

func TestTornTailIsTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, _ := mustOpen(t, path, Options{})
	for i := 0; i < 10; i++ {
		w.Append(rec(i).Type, rec(i).Data)
	}
	w.Close()
	full, _ := os.Stat(path)

	// Chop the file in the middle of the last record.
	if err := os.Truncate(path, full.Size()-3); err != nil {
		t.Fatal(err)
	}
	w, recs := mustOpen(t, path, Options{})
	checkPrefix(t, recs, 9)
	// The torn bytes must be gone so new records are readable.
	s, _ := w.Append(rec(9).Type, rec(9).Data)
	w.Sync(s)
	w.Close()
	_, recs = mustOpen(t, path, Options{})
	checkPrefix(t, recs, 10)
}

func TestCorruptRecordStopsReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, _ := mustOpen(t, path, Options{})
	var offsets []int64
	for i := 0; i < 10; i++ {
		offsets = append(offsets, w.Size())
		w.Append(rec(i).Type, rec(i).Data)
	}
	w.Close()
	data, _ := os.ReadFile(path)
	data[offsets[6]+headerSize+2] ^= 0xff // flip a payload byte of record 6
	os.WriteFile(path, data, 0o644)
	_, recs := mustOpen(t, path, Options{})
	checkPrefix(t, recs, 6)
}

func TestGroupCommitCoalescesFsyncs(t *testing.T) {
	for _, noBatch := range []bool{false, true} {
		t.Run(fmt.Sprintf("noBatch=%v", noBatch), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal.log")
			w, _ := mustOpen(t, path, Options{NoBatch: noBatch})
			const writers, per = 16, 25
			var wg sync.WaitGroup
			for g := 0; g < writers; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < per; i++ {
						seq, err := w.Append(1, []byte(fmt.Sprintf("g%d-%d", g, i)))
						if err != nil {
							t.Error(err)
							return
						}
						if err := w.Sync(seq); err != nil {
							t.Error(err)
							return
						}
					}
				}(g)
			}
			wg.Wait()
			st := w.Stats()
			w.Close()
			if st.Records != writers*per {
				t.Fatalf("records %d", st.Records)
			}
			if noBatch && st.Fsyncs != st.Records {
				t.Fatalf("no-batch mode did %d fsyncs for %d records", st.Fsyncs, st.Records)
			}
			if !noBatch && st.Fsyncs >= st.Records {
				t.Fatalf("group commit did not coalesce: %d fsyncs for %d records", st.Fsyncs, st.Records)
			}
			_, recs := mustOpen(t, path, Options{})
			if len(recs) != writers*per {
				t.Fatalf("recovered %d", len(recs))
			}
		})
	}
}

func TestRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, _ := mustOpen(t, path, Options{})
	for i := 0; i < 50; i++ {
		w.Append(9, []byte("old"))
	}
	keep := []Record{rec(0), rec(1), rec(2)}
	if err := w.Rewrite(keep); err != nil {
		t.Fatal(err)
	}
	s, _ := w.Append(rec(3).Type, rec(3).Data)
	w.Sync(s)
	w.Close()
	_, recs := mustOpen(t, path, Options{})
	checkPrefix(t, recs, 4)
}

func TestCrashRecoveryRandomized(t *testing.T) {
	runs := 200
	if testing.Short() {
		runs = 30
	}
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < runs; i++ {
		if err := CrashCheck(dir, rng); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}
}

func BenchmarkAppendSync(b *testing.B) {
	for _, noBatch := range []bool{false, true} {
		b.Run(fmt.Sprintf("noBatch=%v", noBatch), func(b *testing.B) {
			w, _, err := Open(filepath.Join(b.TempDir(), "wal.log"), Options{NoBatch: noBatch})
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()
			payload := bytes.Repeat([]byte{'v'}, 128)
			b.SetParallelism(16)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					seq, err := w.Append(1, payload)
					if err != nil {
						b.Fatal(err)
					}
					if err := w.Sync(seq); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
