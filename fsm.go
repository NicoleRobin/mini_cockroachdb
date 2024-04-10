package mini_cockroachdb

import (
	"context"
	"fmt"

	"github.com/hashicorp/raft"
	pgquery "github.com/pganalyze/pg_query_go/v2"
)

type pgFsm struct {
	pe *pgEngine
}

func (pf *pgFsm) Apply(ctx context.Context, log *raft.Log) interface{} {
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
