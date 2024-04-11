package mini_cockroachdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"
	pgquery "github.com/pganalyze/pg_query_go/v2"
	bolt "go.etcd.io/bbolt"
)

type tableDefinition struct {
	Name        string
	ColumnNames []string
	ColumnTypes []string
}

type pgEngine struct {
	db         *bolt.DB
	bucketName []byte
}

func newPgEngine(db *bolt.DB) *pgEngine {
	return &pgEngine{
		db:         db,
		bucketName: []byte("data"),
	}
}

func (pe *pgEngine) execute(tree *pgquery.ParseResult) error {
	for _, stmt := range tree.GetStmts() {
		n := stmt.GetStmt()
		if c := n.GetCreateStmt(); c != nil {
			return pe.executeCreate(c)
		}

		if c := n.GetInsertStmt(); c != nil {
			return pe.executeInsert(c)
		}

		if c := n.GetSelectStmt(); c != nil {
			results, err := pe.executeSelect(c)
			if err != nil {
				return fmt.Errorf("pe.executeSelect() failed, err: %w", err)
			}
			log.Printf("pe.executeSelect() success, results: %+v", results)
		}

		return fmt.Errorf("unknown statement type: %s", stmt)
	}
	return nil
}

func (pe *pgEngine) executeCreate(stmt *pgquery.CreateStmt) error {
	tbl := tableDefinition{}
	tbl.Name = stmt.Relation.Relname

	for _, c := range stmt.TableElts {
		cd := c.GetColumnRef()

		tbl.ColumnNames = append(tbl.ColumnNames, cd.String())

		// name is namespaced. so 'int' is pg_catalog.int4. 'bigint' is pg_catalog.int8
		var columnType string
		for _, n := range cd.Fields {
			if columnType != "" {
				columnType += "."
			}
			columnType += n.GetString_().Str
		}
		tbl.ColumnTypes = append(tbl.ColumnTypes, columnType)
	}
	return nil
}

func (pe *pgEngine) executeInsert(stmt *pgquery.InsertStmt) error {
	tblName := stmt.Relation.Relname

	slct := stmt.GetSelectStmt().GetSelectStmt()
	for _, values := range slct.ValuesLists {
		var rowData []any
		for _, value := range values.GetList().Items {
			if c := value.GetAConst(); c != nil {
				if s := c.Val.GetString_(); s != nil {
					rowData = append(rowData, s.Str)
					continue
				}

				if i := c.Val.GetInteger(); i != nil {
					rowData = append(rowData, i.Ival)
					continue
				}
			}

			return fmt.Errorf("unknown value type: %s", value)
		}

		rowBytes, err := json.Marshal(rowData)
		if err != nil {
			return fmt.Errorf("could not marshal row: %s", err)
		}

		id := uuid.New().String()
		err = pe.db.Update(func(tx *bolt.Tx) error {
			bkt, err := tx.CreateBucketIfNotExists(pe.bucketName)
			if err != nil {
				return err
			}

			return bkt.Put([]byte("rows_"+tblName+"_"+id), rowBytes)
		})
		if err != nil {
			return fmt.Errorf("could not store row: %s", err)
		}
	}
	return nil
}

type pgResult struct {
	fieldNames []string
	filedTypes []string
	rows       [][]any
}

func (pe *pgEngine) executeSelect(stmt *pgquery.SelectStmt) (*pgResult, error) {
	tblName := stmt.FromClause[0].GetRangeVar().Relname
	tbl, err := pe.getTableDefinition(tblName)
	if err != nil {
		return nil, err
	}

	results := &pgResult{}
	for _, c := range stmt.TargetList {
		fieldName := c.GetResTarget().Val.GetColumnRef().Fields[0].GetString_().Str
		results.fieldNames = append(results.fieldNames, fieldName)

		fieldType := ""
		for i, cn := range tbl.ColumnNames {
			if cn == fieldName {
				fieldType = tbl.ColumnTypes[i]
			}
		}

		if fieldType == "" {
			return nil, fmt.Errorf("unknown field: %s", fieldName)
		}

		results.filedTypes = append(results.filedTypes, fieldType)
	}

	prefix := []byte("row_" + tblName + "_")
	err = pe.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(pe.bucketName).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var row []any
			err = json.Unmarshal(v, &row)
			if err != nil {
				return fmt.Errorf("unable to unmarshal row: %s", err)
			}

			var targetRows []any
			for _, target := range results.fieldNames {
				for i, field := range tbl.ColumnNames {
					if target == field {
						targetRows = append(targetRows, row[i])
					}
				}
			}
			results.rows = append(results.rows, targetRows)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (pe *pgEngine) getTableDefinition(name string) (*tableDefinition, error) {
	var tbl tableDefinition

	err := pe.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pe.bucketName)
		if bkt == nil {
			return fmt.Errorf("table does not exist")
		}

		valBytes := bkt.Get([]byte("tables_" + name))
		err := json.Unmarshal(valBytes, &tbl)
		if err != nil {
			return fmt.Errorf("could not unmarshal table: %s", err)
		}
		return nil
	})

	return &tbl, err
}

func (pe *pgEngine) delete() error {
	return pe.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pe.bucketName)
		if bkt != nil {
			return tx.DeleteBucket(pe.bucketName)
		}

		return nil
	})
}
