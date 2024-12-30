package main

import (
	"fmt"
	"raft-example/node"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/raft"
)

var defaultConfigs = []node.Config{
	{
		LocalID:  "node1",
		BindAddr: "127.0.0.1:8081",
		RaftDir:  "../data/raft1",
		MetaDir:  "../data/meta1",
	},
	{
		LocalID:  "node2",
		BindAddr: "127.0.0.1:8082",
		RaftDir:  "../data/raft2",
		MetaDir:  "../data/meta2",
	},
	{
		LocalID:  "node3",
		BindAddr: "127.0.0.1:8083",
		RaftDir:  "../data/raft3",
		MetaDir:  "../data/meta3",
	},
}

func main() {
	s := metrics.NewInmemSink(10*time.Second, 300*time.Second)
	cfg := metrics.DefaultConfig("raft.test")
	cfg.EnableHostname = false
	metrics.NewGlobal(cfg, s)

	fmt.Printf("Hello, World!\n")

	nodes := make([]*node.Node, len(defaultConfigs))
	for i, config := range defaultConfigs {
		node, err := node.NewNode(config.LocalID, config.BindAddr, config.RaftDir, config.MetaDir)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}

		defer node.Close()
		nodes[i] = node

		hasState, err := raft.HasExistingState(node.LogStore(), node.StableStore(), node.SnapshotStore())
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}

		if hasState {
			continue
		}

		if err := node.BootstrapCluster(defaultConfigs); err != nil {
			fmt.Printf("Error: index: %d %v\n", i, err)
			return
		}

	}

	leaderNode := findLeader(nodes)

	ss := make(chan struct{}, 100000)
	for i := 0; i < 10000; i++ {
		ss <- struct{}{}
		go func() {
			defer func() {
				<-ss
			}()
			if i%100000 == 0 {
				fmt.Printf("put batch: %d\n", i)
			}

			if err := leaderNode.Put(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
				fmt.Printf("Error: %v\n", err)
				return
			}
		}()
	}

	f := func() {
		for i := 100; i < 10000; i++ {
			value, err := nodes[2].Get(fmt.Sprintf("key-%d", i))
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			fmt.Printf("key: %s value: %s\n", fmt.Sprintf("key-%d", i), value)
		}
	}

	for {
		f()
		time.Sleep(10 * time.Second)
	}
}

func findLeader(nodes []*node.Node) *node.Node {
	for {
		for _, node := range nodes {
			fmt.Printf("Find Leader: %s\n", node.LeaderAddr())
			if node.LeaderAddr() == node.BindAddr() {
				fmt.Printf("Find leader: %s\n", node.BindAddr())
				return node
			}
		}

		time.Sleep(10 * time.Second)
	}
}
