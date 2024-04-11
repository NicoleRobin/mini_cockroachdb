package main

import (
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	pgquery "github.com/pganalyze/pg_query_go/v2"
)

type pgFsm struct {
	pe *pgEngine
}

func (pf *pgFsm) Apply(log *raft.Log) interface{} {
	switch log.Type {
	case raft.LogCommand:
		ast, err := pgquery.Parse(string(log.Data))
		if err != nil {
			panic(fmt.Errorf("could not parse payload: %s", err))
		}

		err = pf.pe.execute(ast)
		if err != nil {
			panic(err)
		}
	default:
		panic(fmt.Errorf("unknown raft log type: %#v", log.Type))
	}
	return nil
}

func (pf *pgFsm) Snapshot() (raft.FSMSnapshot, error) {
	return snapshotNoop{}, nil
}

func (pf *pgFsm) Restore(rc io.ReadCloser) error {
	return fmt.Errorf("nothing to restore")
}
