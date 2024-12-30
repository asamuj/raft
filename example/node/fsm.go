package node

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
)

type fsm struct {
	addr string
	db   *pebble.DB
}

// Apply is called once a log entry is committed by a majority of the cluster.
//
// Apply should apply the log to the FSM. Apply must be deterministic and
// produce the same result on all peers in the cluster.
//
// The returned value is returned to the client as the ApplyFuture.Response.
func (f *fsm) Apply(log *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(log.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	case "delete":
		return f.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

func (f *fsm) applySet(key, value string) error {
	fmt.Printf("bind addr: %s, set key: %s value: %s\n", f.addr, key, value)
	return f.db.Set([]byte(key), []byte(value), nil)
}

func (f *fsm) applyDelete(key string) error {

	return f.db.Delete([]byte(key), nil)
}

// Snapshot returns an FSMSnapshot used to: support log compaction, to
// restore the FSM to a previous state, or to bring out-of-date followers up
// to a recent log index.
//
// The Snapshot implementation should return quickly, because Apply can not
// be called while Snapshot is running. Generally this means Snapshot should
// only capture a pointer to the state, and any expensive IO should happen
// as part of FSMSnapshot.Persist.
//
// Apply and Snapshot are always called from the same thread, but Apply will
// be called concurrently with FSMSnapshot.Persist. This means the FSM should
// be implemented to allow for concurrent updates while a snapshot is happening.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{db: f.db}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (f *fsm) Restore(snapshot io.ReadCloser) error {
	return nil
}

type fsmSnapshot struct {
	db *pebble.DB
}

// Persist is used to write a snapshot of the FSM to the sink. The FSM should
// write the snapshot to the sink and then call sink.Close(). If the FSM fails
// to write a snapshot, it should return an error.
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

// Release is used to release any resources acquired during the Persist call.
func (f *fsmSnapshot) Release() {
}
