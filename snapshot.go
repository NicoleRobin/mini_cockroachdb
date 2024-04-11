package main

import (
	"github.com/hashicorp/raft"
)

type snapshotNoop struct{}

func (sn snapshotNoop) Persist(sink raft.SnapshotSink) error {
	return sink.Cancel()
}

func (sn snapshotNoop) Release() {}
