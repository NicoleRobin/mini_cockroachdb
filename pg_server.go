package mini_cockroachdb

import (
	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
	"log"
	"net"
)

type pgConn struct {
	conn net.Conn
	db   *bolt.DB
	r    *raft.Raft
}

func (pc pgConn) handle() {
	pgc := pgproto3.NewBackend(pgproto3.NewChunkReader(pc.conn))
	defer pc.conn.Close()

}
func runPgServer(port string, db *bolt.Db, r *raft.Raft) {
	ln, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}

		pc := pgConn{conn, db, r}
		go pc.handle()
	}
}
