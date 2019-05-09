package wal

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/raftpb"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Llongfile)
}

type flatWal struct {
	dir     string
	bufPool sync.Pool

	// Order of lock acquisition: flushMu must be before writeMu

	writeMu struct {
		sync.Mutex
		currentIndex walIndex

		useBuf1 bool
		buf0    bytes.Buffer
		buf1    bytes.Buffer
	}

	flushMu struct {
		sync.Mutex
		syncedIndex walIndex
		currentFile *os.File
	}
}

var _ MultiWal = (*flatWal)(nil)

// NewFlatWal creates a new write-ahead log using some nonsense I dreamed up
// after thinking about Kafka.
func NewFlatWal(root string) MultiWal {
	mw := &flatWal{
		dir:     root,
		bufPool: sync.Pool{New: func() interface{} { return &bytes.Buffer{} }},
	}

	f, err := os.Create(filepath.Join(mw.dir, `000.wal`))
	if err != nil {
		log.Fatal(err)
	}
	const segmentSize = 1 << 30 // 1 GiB
	if err := f.Truncate(segmentSize); err != nil {
		log.Fatal(err)
	}
	// TODO(dan): Warm up the file by writing to all the blocks and syncing.
	// Otherwise fdatasync may have to write the metadata.
	mw.flushMu.currentFile = f

	return mw
}

type walIndex uint64

func (mw *flatWal) WalForGroup(group uint64) Wal {
	return &flatWalGroup{
		mw:    mw,
		group: group,
	}
}

func (mw *flatWal) CloseMultiWal() {
	mw.flushMu.Lock()
	mw.writeMu.Lock()
	// Intentionally don't unlock.

	if mw.flushMu.currentFile != nil {
		mw.flushMu.currentFile.Close()
	}
	if err := os.RemoveAll(mw.dir); err != nil {
		log.Fatal(err)
	}
}

// write copies the data in the given reader to disk and returns a token that
// can be used to block on syncing it to disk or to quickly retrieve it later.
func (mw *flatWal) write(r io.Reader) (walIndex, error) {
	mw.writeMu.Lock()
	defer mw.writeMu.Unlock()

	buf := &mw.writeMu.buf0
	if mw.writeMu.useBuf1 {
		buf = &mw.writeMu.buf1
	}

	written, err := io.Copy(buf, r)
	if err != nil {
		return 0, err
	}
	writeIndex := mw.writeMu.currentIndex
	mw.writeMu.currentIndex = writeIndex + walIndex(written)
	return writeIndex, nil
}

// sync blocks until everything written to the group through at least the given
// index has been durably written to disk.
func (mw *flatWal) sync(group uint64, i walIndex) error {
	mw.flushMu.Lock()
	defer mw.flushMu.Unlock()
	if mw.flushMu.syncedIndex >= i {
		// log.Printf("group %d hit the happy path %d vs %d", group, i, mw.flushMu.syncedIndex)
		return nil
	}

	mw.writeMu.Lock()
	writeIndex := mw.writeMu.currentIndex
	buf := &mw.writeMu.buf0
	if mw.writeMu.useBuf1 {
		buf = &mw.writeMu.buf1
	}
	mw.writeMu.Unlock()

	// TODO(dan): Support moving on to another file when this one fills up. It's
	// faster to reuse them (for the same fdatasync reason we warm up them in the
	// constructor), so leave the file in a pool. Then when we need to reuse it,
	// hard link to the new name. If we're aggressive about truncating, this will
	// mean we can flip flop between two continually warm files.

	// TODO(dan): Probably want to do the write and the flush in separate
	// goroutines. We may even want more than one goroutine for the writes, each
	// going to a separate file, to take advantage of internal disk parallelism.

	// log.Printf("group %d starting write and flush", group)
	start := time.Now()
	written, err := io.Copy(mw.flushMu.currentFile, buf)
	if err != nil {
		return err
	}
	_, _ = start, written
	// log.Printf("group %d wrote %d bytes in %s", group, written, time.Since(start))
	start = time.Now()
	// TODO(dan): fdatasync
	if err := mw.flushMu.currentFile.Sync(); err != nil {
		return err
	}
	// log.Printf("group % 3d synced % 9d bytes in %s", group, written, time.Since(start))
	mw.flushMu.syncedIndex = writeIndex
	// log.Printf("group %d was looking for %d and synced to %d", group, i, mw.flushMu.syncedIndex)

	return nil
}

type flatWalGroup struct {
	mw    *flatWal
	group uint64

	mu struct {
		sync.RWMutex
		term       uint64
		lastIndex  uint64
		firstIndex uint64
	}
}

var _ Wal = (*flatWalGroup)(nil)

func (w *flatWalGroup) Append(entries []raftpb.Entry) {
	if len(entries) == 0 {
		return
	}
	lastEntry := entries[len(entries)-1]

	buf := w.mw.bufPool.Get().(*bytes.Buffer)
	defer w.mw.bufPool.Put(buf)
	buf.Reset()
	if err := serializeEntries(buf, entries); err != nil {
		log.Fatal(err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.mu.term = lastEntry.Term
	w.mu.lastIndex = lastEntry.Index
	walIndex, err := w.mw.write(buf)
	if err != nil {
		log.Fatal(err)
	}
	if err := w.mw.sync(w.group, walIndex); err != nil {
		log.Fatal(err)
	}
	// TODO(dan): Save this (term, index) -> walIndex in a cache somewhere,
	// periodically persisting it to disk so that we don't have to recreate it
	// from scratch to recover from crashes.
}

func (w *flatWalGroup) Entries(lo, hi uint64) []raftpb.Entry {
	panic(`WIP`)
}

func (w *flatWalGroup) Truncate() {
	// TODO(dan): Implement Truncate. Doesn't really matter until we worry about
	// recovery. For now, no-op.
}

func (w *flatWalGroup) Term(i uint64) uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.mu.lastIndex
}

func (w *flatWalGroup) LastIndex() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.mu.lastIndex
}

func (w *flatWalGroup) FirstIndex() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.mu.firstIndex
}

func (w *flatWalGroup) CloseWal() {
	w.mu.Lock()
	// Intentionally don't unlock
}

func serializeEntries(w io.Writer, entries []raftpb.Entry) error {
	var scratch [8]byte

	for i := range entries {
		// TODO(dan): Hand roll an alloc-less marshaller for this proto b/c why not.
		entryBytes, err := entries[i].Marshal()
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(scratch[:], uint64(len(entryBytes)))
		if _, err := w.Write(scratch[:]); err != nil {
			return nil
		}
		if _, err := w.Write(entryBytes); err != nil {
			return err
		}
	}
	return nil
}
