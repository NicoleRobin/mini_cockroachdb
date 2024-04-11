package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/jackc/pgproto3/v2"
	pgquery "github.com/pganalyze/pg_query_go/v2"
	bolt "go.etcd.io/bbolt"
)

type pgConn struct {
	conn net.Conn
	db   *bolt.DB
	r    *raft.Raft
}

func (pc pgConn) handle() error {
	pgc := pgproto3.NewBackend(pgproto3.NewChunkReader(pc.conn), pc.conn)
	defer pc.conn.Close()

	err := pc.handleStartupMessage(pgc)
	if err != nil {
		log.Println(err)
		return err
	}

	for {
		err := pc.handleMessage(pgc)
		if err != nil {
			log.Println(err)
			return err
		}
	}
}

func (pc pgConn) handleStartupMessage(pgc *pgproto3.Backend) error {
	startupMessage, err := pgc.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("error receiving startup message: %s", err)
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf, _ := (&pgproto3.AuthenticationOk{}).Encode(nil)
		buf, _ = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = pc.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %s", err)
		}
		return nil
	case *pgproto3.SSLRequest:
		_, err = pc.conn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("error sending deny SSL request: %s", err)
		}
	default:
		return fmt.Errorf("unknown startup message: %#v", startupMessage)
	}
	return nil
}

func (pc pgConn) handleMessage(pgc *pgproto3.Backend) error {
	msg, err := pgc.Receive()
	if err != nil {
		return fmt.Errorf("error receiving message: %s", err)
	}
	switch t := msg.(type) {
	case *pgproto3.Query:
		stmts, err := pgquery.Parse(t.String)
		if err != nil {
			return fmt.Errorf("error parsing query: %s", err)
		}
		if len(stmts.GetStmts()) > 1 {
			return fmt.Errorf("only make one request at a time")
		}
		stmt := stmts.GetStmts()[0]

		// handle selects here
		s := stmt.GetStmt().GetSelectStmt()
		if s != nil {
			pe := newPgEngine(pc.db)
			res, err := pe.executeSelect(s)
			if err != nil {
				return err
			}
			log.Printf("pe.executeSelect() success, res:%+v", res)
		}

		// otherwise it's DDL/DML, raftify
		future := pc.r.Apply([]byte(t.String), 500*time.Microsecond)
		if err := future.Error(); err != nil {
			return fmt.Errorf("could not apply: %s", err)
		}
		e := future.Response()
		if e != nil {
			return fmt.Errorf("could not apply (internal): %s", e)
		}
		err = pc.done(nil, strings.ToUpper(strings.Split(t.String, " ")[0])+" ok")
		if err != nil {
			return fmt.Errorf("pc.done() failed, err: %w", err)
		}
	case *pgproto3.Terminate:
		return nil
	default:
		return fmt.Errorf("received message other than Query from client: %s", msg)
	}
	return nil
}

func (pc pgConn) done(buf []byte, msg string) error {
	var err error
	buf, err = (&pgproto3.CommandComplete{CommandTag: []byte(msg)}).Encode(buf)
	if err != nil {
		return fmt.Errorf("pgproto3.CommandComplete.Encode() failed, err: %w", err)
	}
	buf, err = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	if err != nil {
		return fmt.Errorf("pgproto3.ReadyForQuery.Encode() failed, err: %w", err)
	}
	_, err = pc.conn.Write(buf)
	if err != nil {
		log.Printf("failed to write query reponse: %s", err)
	}
	return nil
}

var dataTypeOIDMap = map[string]uint32{
	"text":            25,
	"pg_catalog.int4": 23,
}

func (pc pgConn) writePgResult(res *pgResult) error {
	rd := &pgproto3.RowDescription{}
	for i, field := range res.fieldNames {
		rd.Fields = append(rd.Fields, pgproto3.FieldDescription{
			Name:        []byte(field),
			DataTypeOID: dataTypeOIDMap[res.filedTypes[i]],
		})
	}
	buf, err := rd.Encode(nil)
	if err != nil {
		log.Printf("rd.Encode() failed, err: %s\n", err)
		return fmt.Errorf("rd.Encode() failed, err: %w", err)
	}

	for _, row := range res.rows {
		dr := &pgproto3.DataRow{}
		for _, value := range row {
			bs, err := json.Marshal(value)
			if err != nil {
				log.Printf("json.Marshal() failed, err: %s\n", err)
				return fmt.Errorf("json.Marshal() failed, err: %w", err)
			}
			dr.Values = append(dr.Values, bs)
		}
		buf, err = dr.Encode(buf)
		if err != nil {
			log.Printf("dr.Encode() failed, err: %s\n", err)
			return fmt.Errorf("dr.Encode() failed, err: %w", err)
		}
	}
	return pc.done(buf, fmt.Sprintf("SELECT %d", len(res.rows)))
}

func runPgServer(port int, db *bolt.DB, r *raft.Raft) {
	ln, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}

		pc := pgConn{conn, db, r}
		go func() {
			err := pc.handle()
			if err != nil {
				log.Printf("pc.handle() failed, err: %s", err)
			}
		}()
	}
}
