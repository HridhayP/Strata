package wal

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
)

// CrashCheck performs one randomized power-loss experiment in dir and
// returns an error if recovery lost an acknowledged record or produced
// garbage. It appends records, fsyncs some of them, crashes, keeps a random
// amount of the unsynced tail (optionally followed by junk), and reopens.
// The unit tests and cmd/strata-crashtest both run it.
func CrashCheck(dir string, rng *rand.Rand) error {
	path := filepath.Join(dir, "wal.log")
	os.Remove(path)
	w, _, err := Open(path, Options{})
	if err != nil {
		return err
	}
	n := 1 + rng.Intn(300)
	acked := 0
	for i := 0; i < n; i++ {
		seq, err := w.Append(checkRec(i).Type, checkRec(i).Data)
		if err != nil {
			return err
		}
		if rng.Intn(4) == 0 {
			if err := w.Sync(seq); err != nil {
				return err
			}
			acked = i + 1
		}
	}
	durable := w.DurableSize()
	w.CrashClose()

	// Power loss: anything after the durable prefix may or may not survive,
	// and the surviving part may end in garbage.
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	cut := durable
	if st.Size() > durable {
		cut += rng.Int63n(st.Size() - durable + 1)
	}
	if err := os.Truncate(path, cut); err != nil {
		return err
	}
	if rng.Intn(2) == 0 {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			return err
		}
		junk := make([]byte, rng.Intn(64))
		rng.Read(junk)
		f.Write(junk)
		f.Close()
	}

	w, recs, err := Open(path, Options{})
	if err != nil {
		return err
	}
	defer w.Close()
	if len(recs) < acked {
		return fmt.Errorf("lost acknowledged records: recovered %d, acked %d", len(recs), acked)
	}
	if len(recs) > n {
		return fmt.Errorf("recovered %d records but only %d were written", len(recs), n)
	}
	for i, r := range recs {
		want := checkRec(i)
		if r.Type != want.Type || !bytes.Equal(r.Data, want.Data) {
			return fmt.Errorf("record %d corrupted", i)
		}
	}
	return nil
}

// checkRec is the i'th record CrashCheck writes.
func checkRec(i int) Record {
	return Record{Type: byte(i % 7), Data: []byte(fmt.Sprintf("record-%06d-%s", i, bytes.Repeat([]byte{'x'}, i%50)))}
}
