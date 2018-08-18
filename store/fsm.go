package store

/**
	// FSM provides an interface that can be implemented by
	// clients to make use of the replicated log.
	type FSM interface {
	// Apply log is invoked once a log entry is committed.
	// It returns a value which will be made available in the
	// ApplyFuture returned by Raft.Apply method if that
	// method was called on the same Raft node as the FSM.
	Apply(*Log) interface{}

	// Snapshot is used to support log compaction. This call should
	// return an FSMSnapshot which can be used to save a point-in-time
	// snapshot of the FSM. Apply and Snapshot are not called in multiple
	// threads, but Apply will be called concurrently with Persist. This means
	// the FSM should be implemented in a fashion that allows for concurrent
	// updates while a snapshot is happening.
	Snapshot() (FSMSnapshot, error)

	// Restore is used to restore an FSM from a snapshot. It is not called
	// concurrently with any other command. The FSM must discard all previous
	// state.
	Restore(io.ReadCloser) error
}
**/

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/hashicorp/raft"
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// Node State Machine
type NodeFSM struct {
	// db DB
	mutex sync.Mutex
	m     map[string]string // The key-value store for the system.

	logger *log.Logger
}

func NewNodeFSM() *NodeFSM {
	return &NodeFSM{
		logger: log.New(os.Stderr, "[fsm] ", log.LstdFlags),
	}
}

// Apply applies a Raft log entry to the key-value store.
func (fsm *NodeFSM) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	// fsm.logger.Println("c", c)

	switch c.Op {
	case "set":
		return fsm.applySet(c.Key, c.Value)
	// case "delete":
	// return f.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

func (fsm *NodeFSM) applySet(key, value string) interface{} {
	fsm.logger.Printf("apply %s to %s\n", key, value)

	fsm.mutex.Lock()
	defer fsm.mutex.Unlock()

	fsm.m[key] = value
	return nil
}

// Snapshot returns a snapshot of the key-value store.
func (f *NodeFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	// Clone the map.
	o := make(map[string]string)
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *NodeFSM) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}
