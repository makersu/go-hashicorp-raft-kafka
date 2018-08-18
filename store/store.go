package store

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/makersu/go-raft-kafka/config"
)

type Store struct {
	RaftDir  string
	RaftAddr string

	raft *raft.Raft // The consensus mechanism
	// fsm  *fsm

	logger *log.Logger
}

// TODO: rename
// func NewStore(raftDir, raftBindAddr string) (*Store, error) {
func NewStore(nodeConfig *config.NodeConfig) (*Store, error) {
	// fsm, err := NewFSM(raftdir)
	// if err != nil {
	// 	return nil, err
	// }

	store := &Store{
		logger: log.New(os.Stderr, "[store] ", log.LstdFlags),
		// fsm:      fsm,
		RaftDir:  nodeConfig.RaftDir,
		RaftAddr: nodeConfig.RaftAddr,
	}

	// TODO
	bootstrap := nodeConfig.JoinAddr == ""

	err := store.Open(bootstrap, nodeConfig.NodeID)
	if err != nil {
		store.logger.Println(err.Error())
		// panic(err.Error())
		return nil, err
	}

	return store, nil
}

func (store *Store) Open(bootstrap bool, localNodeID string) error {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(localNodeID)
	raftConfig.SnapshotThreshold = 10

	// store.logger.Println("store.RaftBindAddr:", store.RaftAddr)
	transport, err := NewRaftTCPTransport(store.RaftAddr, os.Stderr)
	if err != nil {
		return err
	}
	// store.logger.Println("transport:", transport)

	fsm := NewNodeFSM()

	//TODO: refactoring to extract
	if err := os.MkdirAll(store.RaftDir, 0700); err != nil {
		return err
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(store.RaftDir, "raft-log.db"))
	if err != nil {
		return err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(store.RaftDir, "raft-stable.db"))
	if err != nil {
		return err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(store.RaftDir, 2, os.Stderr)
	if err != nil {
		return err
	}

	// raft system
	// func NewRaft(conf *Config, fsm FSM, logs LogStore, stable StableStore, snaps SnapshotStore, trans Transport) (*Raft, error) {
	raftNode, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return err
	}

	store.raft = raftNode

	if bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		store.raft.BootstrapCluster(configuration)
	} else {
		// if err := join(joinAddr, raftAddr, nodeID); err != nil {
		// 	log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
		// }
	}

	return nil
}

func NewRaftTCPTransport(raftBindAddr string, log io.Writer) (*raft.NetworkTransport, error) {

	address, err := net.ResolveTCPAddr("tcp", raftBindAddr)
	if err != nil {
		return nil, err
	}

	// fmt.Println("address:", address)
	// fmt.Println("address.String():", address.String())

	transport, err := raft.NewTCPTransport(address.String(), address, 3, 10*time.Second, log)
	if err != nil {
		return nil, err
	}

	return transport, nil
}

func checkRaftDir(dir string) {

	// Ensure Raft storage exists.
	raftDir := dir
	if raftDir == "" {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}
	os.MkdirAll(raftDir, 0700)
}

// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) Join(nodeID, addr string) error {
	s.logger.Printf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				s.logger.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}
