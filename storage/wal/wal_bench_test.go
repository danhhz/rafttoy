package wal_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/nvanbenschoten/rafttoy/storage/engine"
	"github.com/nvanbenschoten/rafttoy/storage/wal"
	"github.com/nvanbenschoten/rafttoy/util"
	"github.com/nvanbenschoten/rafttoy/workload"
	"go.etcd.io/etcd/raft/raftpb"
)

func benchmarkWalConfig(b *testing.B, batchSize, bytes int, w wal.Wal) {
	workers := workload.NewWorkers(workload.Config{
		KeyPrefix: engine.MinDataKey,
		KeyLen:    len(engine.MinDataKey) + 8,
		ValLen:    bytes,
		Workers:   1,
		Proposals: b.N,
	})
	worker := workers[0]

	var a util.ByteAllocator
	entries := make([]raftpb.Entry, 0, batchSize)
	appendEntries := func() {
		if len(entries) > 0 {
			w.Append(entries)
			entries = entries[:0]
		}
	}

	b.ResetTimer()
	var index uint64
	for prop := worker.NextProposal(); prop != nil; prop = worker.NextProposal() {
		index++
		e := raftpb.Entry{
			Index: index,
			Type:  raftpb.EntryNormal,
		}
		// prop is only valid until NextProposal is called again, so copy it.
		a, e.Data = a.Copy(prop)
		entries = append(entries, e)
		if len(entries) == batchSize {
			appendEntries()
		}
	}
	// Flush out the remaining partial batch.
	appendEntries()

	b.StopTimer()
	b.SetBytes(int64(bytes))
}

func benchmarkWal(b *testing.B, walFn func(root string) wal.Wal) {
	util.RunFor(b, "batch", 1, 3, 4, func(b *testing.B, batchSize int) {
		util.RunFor(b, "bytes", 1, 3, 5, func(b *testing.B, bytes int) {
			dir, err := ioutil.TempDir(``, ``)
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(dir)
			w := walFn(dir)
			defer w.CloseWal()
			benchmarkWalConfig(b, batchSize, bytes, w)
		})
	})
}

func benchmarkMultiWalConfig(b *testing.B, batchSize, bytes, groups int, mw wal.MultiWal) {
	workers := workload.NewWorkers(workload.Config{
		KeyPrefix: engine.MinDataKey,
		KeyLen:    len(engine.MinDataKey) + 8,
		ValLen:    bytes,
		Workers:   groups,
		Proposals: b.N,
	})

	wals := make([]wal.Wal, len(workers))
	for i := range wals {
		wals[i] = mw.WalForGroup(uint64(i))
	}
	defer func() {
		for i := range wals {
			wals[i].CloseWal()
		}
	}()

	b.ResetTimer()
	var wg sync.WaitGroup
	for i := range workers {
		worker, w := workers[i], wals[i]

		var a util.ByteAllocator
		entries := make([]raftpb.Entry, 0, batchSize)
		appendEntries := func() {
			if len(entries) > 0 {
				w.Append(entries)
				entries = entries[:0]
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			var index uint64
			for prop := worker.NextProposal(); prop != nil; prop = worker.NextProposal() {
				index++
				e := raftpb.Entry{
					Index: index,
					Type:  raftpb.EntryNormal,
				}
				// prop is only valid until NextProposal is called again, so copy it.
				a, e.Data = a.Copy(prop)
				entries = append(entries, e)
				if len(entries) == batchSize {
					appendEntries()
				}
			}
			// Flush out the remaining partial batch.
			appendEntries()
		}()
	}
	wg.Wait()
	b.StopTimer()
	b.SetBytes(int64(bytes))
}

func benchmarkMultiWal(b *testing.B, walFn func(root string) wal.MultiWal) {
	benches := []struct {
		batch, bytes, groups int
	}{
		{batch: 100, bytes: 1 << 10, groups: 1},
		{batch: 100, bytes: 1 << 10, groups: 10},
		{batch: 100, bytes: 1 << 10, groups: 100},
		{batch: 100, bytes: 1 << 10, groups: 1000},
	}
	for _, bench := range benches {
		name := fmt.Sprintf("batch=%d/bytes=%d/groups=%d", bench.batch, bench.bytes, bench.groups)
		b.Run(name, func(b *testing.B) {
			dir, err := ioutil.TempDir(``, ``)
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(dir)
			w := walFn(dir)
			defer w.CloseMultiWal()
			benchmarkMultiWalConfig(b, bench.batch, bench.bytes, bench.groups, w)
		})
	}
}

type etcdMultiWalAdaptor string

func (w etcdMultiWalAdaptor) WalForGroup(i uint64) wal.Wal {
	dir := filepath.Join(string(w), strconv.Itoa(int(i)))
	return wal.NewEtcdWal(dir)
}
func (w etcdMultiWalAdaptor) CloseMultiWal() {}

func BenchmarkEtcd(b *testing.B) {
	benchmarkWal(b, wal.NewEtcdWal)
	b.Run(`multi`, func(b *testing.B) {
		benchmarkMultiWal(b, func(root string) wal.MultiWal {
			return etcdMultiWalAdaptor(root)
		})
	})
}

func BenchmarkPebble(b *testing.B) {
	benchmarkWal(b, func(root string) wal.Wal {
		return engine.NewPebble(root, false /* disableWal */).(wal.Wal)
	})
	// TODO(dan): Implement MultiWal using pebble and benchmark it.
}

func BenchmarkFlat(b *testing.B) {
	benchmarkWal(b, func(root string) wal.Wal {
		return wal.NewFlatWal(root).WalForGroup(0)
	})
	b.Run(`multi`, func(b *testing.B) {
		benchmarkMultiWal(b, func(root string) wal.MultiWal {
			return wal.NewFlatWal(root)
		})
	})
}
