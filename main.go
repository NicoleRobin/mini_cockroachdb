package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

func setupRaft(dir, nodeId, raftAddress string, pf *pgFsm) (*raft.Raft, error) {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	store, err := raftboltdb.NewBoltStore(path.Join(dir, "bolt"))
	if err != nil {
		return nil, err
	}

	snapshots, err := raft.NewFileSnapshotStore(path.Join(dir, "snapshot"), 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return nil, fmt.Errorf("could not resolve address: %s", err)
	}

	transport, err := raft.NewTCPTransport(raftAddress, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create tcp transport: %s", err)
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(nodeId)

	r, err := raft.NewRaft(raftCfg, pf, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("could not create raft instance: %s", err)
	}

	r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(nodeId),
				Address: transport.LocalAddr(),
			},
		},
	})

	return r, nil
}

func main() {
	if err := Execute(); err != nil {
		log.Printf("Execute() failed, err: %s\n", err)
		os.Exit(-1)
	}
}
