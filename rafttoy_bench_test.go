package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/nvanbenschoten/rafttoy/metric"
	"github.com/nvanbenschoten/rafttoy/peer"
	"github.com/nvanbenschoten/rafttoy/storage/engine"
	"github.com/nvanbenschoten/rafttoy/util"
	"github.com/nvanbenschoten/rafttoy/workload"
	"go.etcd.io/etcd/raft"
	"golang.org/x/sync/errgroup"
)

// epoch represents a single iteration of a `go bench` loop. Tracking it and
// threading it through Raft messages allows a single instance of `go bench`
// to coordinate Raft state machine resets across a cluster of Go processes.
var epoch int32

func newEpoch() int32 {
	e := epoch
	epoch++
	return e
}

func TestMain(m *testing.M) {
	defer metric.Enable(*recordMetrics)()
	m.Run()
}

func BenchmarkRaft(b *testing.B) {
	util.RunFor(b, "conc", 1, 1, 15, func(b *testing.B, conc int) {
		util.RunFor(b, "bytes", 1, 2, 8, func(b *testing.B, bytes int) {
			benchmarkRaft(b, conc, bytes)
		})
	})
}

func benchmarkRaft(b *testing.B, conc, bytes int) {
	cfg := cfgFromFlags()
	cfg.Epoch = newEpoch()
	p := newPeer(cfg)
	go p.Run()
	defer p.Stop()

	// Wait for the initial leader election to complete.
	becomeLeader(p)

	workers := workload.NewWorkers(workload.Config{
		KeyPrefix: engine.MinDataKey,
		KeyLen:    len(engine.MinDataKey) + 8,
		ValLen:    bytes,
		Workers:   conc,
		Proposals: b.N,
	})

	b.ResetTimer()
	b.SetBytes(int64(bytes))
	var g errgroup.Group
	defer g.Wait()
	for i := range workers {
		worker := workers[i]
		g.Go(func() error {
			c := make(chan bool, 1)
			for prop := worker.NextProposal(); prop != nil; prop = worker.NextProposal() {
				if !p.ProposeWith(prop, c) {
					return errors.New("proposal failed")
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkLocalRaft(b *testing.B) {
	const nodes = 3
	util.RunFor(b, "conc", 1, 1, 15, func(b *testing.B, conc int) {
		util.RunFor(b, "bytes", 1, 2, 8, func(b *testing.B, bytes int) {
			benchmarkLocalRaft(b, conc, bytes, nodes)
		})
	})
}

func benchmarkLocalRaft(b *testing.B, conc, bytes int, nodes int) {
	cfg := peer.Config{
		Epoch:     newEpoch(),
		Peers:     make([]raft.Peer, nodes),
		PeerAddrs: make(map[uint64]string, nodes),
	}
	for i := 0; i < nodes; i++ {
		pID := uint64(i + 1)
		cfg.Peers[i].ID = pID
		cfg.PeerAddrs[pID] = fmt.Sprintf(`127.0.0.1:%d`, 1234+i)
	}

	peers := make([]*peer.Peer, nodes)
	for i := range peers {
		peerCfg := cfg
		peerCfg.ID = uint64(i + 1)
		peerCfg.SelfAddr = peerCfg.PeerAddrs[peerCfg.ID]
		peers[i] = newPeer(peerCfg)
		go peers[i].Run()
		defer peers[i].Stop()
	}

	// Wait for the initial leader election to complete.
	leader := peers[0]
	becomeLeader(leader)

	workers := workload.NewWorkers(workload.Config{
		KeyPrefix: engine.MinDataKey,
		KeyLen:    len(engine.MinDataKey) + 8,
		ValLen:    bytes,
		Workers:   conc,
		Proposals: b.N,
	})

	b.ResetTimer()
	b.SetBytes(int64(bytes))
	// util.AllocProfileDiff(b, "/tmp/mem.before", "/tmp/mem.after", func() {
		var g errgroup.Group
		defer g.Wait()
		for i := range workers {
			i := i
			worker := workers[i]
			g.Go(func() error {
				c := make(chan bool, 1)
				for prop := worker.NextProposal(); prop != nil; prop = worker.NextProposal() {
					// fmt.Printf("propose from %d %d len=%d\n", i, prop.GetID(), len(prop))
					if !leader.ProposeWith(prop, c) {
						return errors.New("proposal failed")
					}
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			b.Fatal(err)
		}
	// })
}
