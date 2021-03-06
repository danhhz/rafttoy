package pipeline

import (
	"log"
	"sync"
	"time"

	"github.com/nvanbenschoten/rafttoy/metric"
	"github.com/nvanbenschoten/rafttoy/proposal"
	"github.com/nvanbenschoten/rafttoy/storage"
	"github.com/nvanbenschoten/rafttoy/storage/engine"
	"github.com/nvanbenschoten/rafttoy/transport"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// Pipeline represents an implementation of a Raft proposal pipeline. It manages
// the interactions between a Raft "raw node" and the various components that
// the Raft "raw node" needs to coordinate with.
type Pipeline interface {
	Init(int32, sync.Locker, *raft.RawNode, storage.Storage, transport.Transport, *proposal.Tracker)
	Start()
	Pause()
	Resume(int32, *raft.RawNode)
	Stop()
	RunOnce()
}

func measurePipelineLat() func() {
	if !metric.Enabled() {
		return func() {}
	}
	start := time.Now()
	return func() {
		lat := time.Since(start)
		metric.PipelineLatencyHistogram.Update(int64(lat / time.Microsecond))
	}
}

func saveToDisk(s storage.Storage, ents []raftpb.Entry, st raftpb.HardState, sync bool) {
	if len(ents) > 0 {
		metric.AppendBatchSizesHistogram.Update(int64(len(ents)))
	}
	if as, ok := s.(storage.AtomicStorage); ok {
		as.AppendAndSetHardState(ents, st, sync)
	} else {
		if len(ents) > 0 {
			s.Append(ents)
		}
		if !raft.IsEmptyHardState(st) {
			// This isn't exactly correct, but it's close enough.
			syncHS := sync && len(ents) == 0
			s.SetHardState(st, syncHS)
		}
	}
}

func sendMessages(t transport.Transport, epoch int32, msgs []raftpb.Message) {
	if len(msgs) > 0 {
		t.Send(epoch, msgs)
	}
}

// Stolen from cockroachdb/cockroach/pkg/storage/replica_raft.go.
func splitMsgApps(msgs []raftpb.Message) (msgApps, otherMsgs []raftpb.Message) {
	splitIdx := 0
	for i, msg := range msgs {
		if msg.Type == raftpb.MsgApp {
			msgs[i], msgs[splitIdx] = msgs[splitIdx], msgs[i]
			splitIdx++
		}
	}
	return msgs[:splitIdx], msgs[splitIdx:]
}

func processSnapshot(sn raftpb.Snapshot) {
	if !raft.IsEmptySnap(sn) {
		log.Fatalf("unhandled snapshot %v", sn)
	}
}

func applyToStore(
	n *raft.RawNode, s storage.Storage, pt *proposal.Tracker, l sync.Locker, ents []raftpb.Entry, ack bool,
) {
	if len(ents) == 0 {
		return
	}
	metric.ApplyBatchSizesHistogram.Update(int64(len(ents)))
	if be, ok := s.(engine.BatchingEngine); ok {
		// Apply all entries at once then ack all entries at once.
		st := 0
		for i := range ents {
			ent := &ents[i]
			switch ent.Type {
			case raftpb.EntryNormal:
			case raftpb.EntryConfChange:
				// Flush the previous batch.
				be.ApplyEntries(ents[st:i])
				st = i + 1

				var cc raftpb.ConfChange
				cc.Unmarshal(ent.Data)
				l.Lock()
				n.ApplyConfChange(cc)
				l.Unlock()
			default:
				panic("unexpected")
			}
		}
		be.ApplyEntries(ents[st:])

		if ack {
			for i := range ents {
				ent := &ents[i]
				switch ent.Type {
				case raftpb.EntryNormal:
					if len(ent.Data) == 0 {
						continue
					}
					ec := proposal.EncProposal(ent.Data)
					pt.Finish(ec.GetID(), true)
				case raftpb.EntryConfChange:
				default:
					panic("unexpected")
				}
			}
		}
	} else {
		// Apply and ack entries, one at a time.
		for i := range ents {
			ent := &ents[i]
			switch ent.Type {
			case raftpb.EntryNormal:
				if len(ent.Data) == 0 {
					continue
				}
				s.ApplyEntry(*ent)
				if ack {
					ec := proposal.EncProposal(ent.Data)
					pt.Finish(ec.GetID(), true)
				}
			case raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				cc.Unmarshal(ent.Data)
				l.Lock()
				n.ApplyConfChange(cc)
				l.Unlock()
			default:
				panic("unexpected")
			}
		}
	}
}
