package node

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type Node struct {
	bindAddr      string
	raft          *raft.Raft
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore
	rockesDB      *pebble.DB
}

type Config struct {
	LocalID  string
	BindAddr string
	RaftDir  string
	MetaDir  string
}

// func RaftConfig(conf *Config) *raft.Config {
// 	rc := raft.DefaultConfig()
// 	rc.LocalID = raft.ServerID(conf.LocalID)
// 	rc.BindAddr
// }

const (
	retainSnapshotCount = 100
	raftTimeout         = 10 * time.Second
)

func NewNode(localID, raftBind, raftDir, metaDir string) (*Node, error) {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)
	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", raftBind)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(raftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.

	boltDB, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(raftDir, "raft.db"),
	})
	if err != nil {
		return nil, fmt.Errorf("new bbolt store: %s", err)
	}

	db, err := pebble.Open(metaDir, nil)
	if err != nil {
		return nil, fmt.Errorf("open pebble db: %s", err)
	}

	// Instantiate the Raft systems.
	fsm := &fsm{addr: raftBind, db: db}
	fmt.Printf("Starting Node: %s\n", localID)
	ra, err := raft.NewRaft(config, fsm, boltDB, boltDB, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %s", err)
	}

	// configuration := raft.Configuration{
	// 	Servers: []raft.Server{
	// 		{
	// 			ID:      config.LocalID,
	// 			Address: transport.LocalAddr(),
	// 		},
	// 	},
	// }
	// ra.BootstrapCluster(configuration)

	return &Node{
		raft:          ra,
		bindAddr:      raftBind,
		logStore:      boltDB,
		stableStore:   boltDB,
		snapshotStore: snapshots,
		rockesDB:      db,
	}, nil
}

func (n *Node) BootstrapCluster(confs []Config) error {
	servers := make([]raft.Server, len(confs))
	for i, conf := range confs {
		servers[i] = raft.Server{
			ID:      raft.ServerID(conf.LocalID),
			Address: raft.ServerAddress(conf.BindAddr),
		}
	}

	configuration := raft.Configuration{
		Servers: servers,
	}

	if err := n.raft.BootstrapCluster(configuration).Error(); err != nil {
		return fmt.Errorf("bootstrap cluster: %s", err)
	}

	n.raft.State()

	return nil
}

func (n *Node) LogStore() raft.LogStore {
	return n.logStore
}

func (n *Node) StableStore() raft.StableStore {
	return n.stableStore
}

func (n *Node) SnapshotStore() raft.SnapshotStore {
	return n.snapshotStore
}

func (n *Node) LeaderAddr() string {
	return string(n.raft.Leader())
}

func (n *Node) BindAddr() string {
	return n.bindAddr
}

func (n *Node) Close() error {
	if err := n.rockesDB.Close(); err != nil {
		return err
	}

	return nil
}

type Store interface {
	Put(key, value string) error
	Get(key string) (string, error)
}

// node implements the Store interface
// Put actually sends a command to the Raft cluster
func (n *Node) Put(key, value string) error {
	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	return n.raft.Apply(b, raftTimeout).Error()
}

func (n *Node) Get(key string) (string, error) {
	value, closer, err := n.rockesDB.Get([]byte(key))
	if err != nil {
		return "", err
	}

	if err := closer.Close(); err != nil {
		fmt.Printf("Error: %v", err)
	}

	return string(value), nil
}
