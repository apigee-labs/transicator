/*
Copyright 2016 The Transicator Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/apigee-labs/transicator/common"
)

type tableInfo struct {
	allColumns   []string
	primaryKeys  []string
	otherColumns []string
	insert       *sql.Stmt
	update       *sql.Stmt
	delete       *sql.Stmt
}

func scanTableInfo(db *sql.DB) (tables map[string]*tableInfo, err error) {
	var rows *sql.Rows
	rows, err = db.Query(`
    select tableName, columnName, primaryKey from _transicator_tables
  `)

	if err == nil {
		tables = make(map[string]*tableInfo)
		defer rows.Close()

		for rows.Next() {
			var tn, cn string
			var primary bool
			err = rows.Scan(&tn, &cn, &primary)
			if err != nil {
				return
			}
			tps := strings.Split(tn, ".")
			tn = tps[len(tps)-1]

			table := tables[tn]
			if table == nil {
				table = &tableInfo{}
				tables[tn] = table
			}
			table.allColumns = append(table.allColumns, cn)
			if primary {
				table.primaryKeys = append(table.primaryKeys, cn)
			} else {
				table.otherColumns = append(table.otherColumns, cn)
			}
		}
	}

	if err == nil {
		for tn, table := range tables {
			err = makeTableSQL(db, tn, table)
			if err != nil {
				return
			}
		}
	}
	return
}

func makeTableSQL(db *sql.DB, tn string, table *tableInfo) (err error) {
	buf := &bytes.Buffer{}
	buf.WriteString("insert into ")
	buf.WriteString(tn)
	buf.WriteString(" (")
	buf.WriteString(strings.Join(table.allColumns, ", "))
	buf.WriteString(") values(?")
	buf.WriteString(strings.Repeat(", ?", len(table.allColumns)-1))
	buf.WriteString(")")
	fmt.Printf("Insert: %s\n", buf.String())

	table.insert, err = db.Prepare(buf.String())
	if err != nil {
		return
	}

	buf = &bytes.Buffer{}
	buf.WriteString("update ")
	buf.WriteString(tn)
	buf.WriteString(" set ")
	for i, cn := range table.otherColumns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(cn)
		buf.WriteString(" = ?")
	}
	writePrimaryWhere(table, buf)
	fmt.Printf("Update: %s\n", buf.String())

	table.update, err = db.Prepare(buf.String())
	if err != nil {
		return
	}

	buf = &bytes.Buffer{}
	buf.WriteString("delete from ")
	buf.WriteString(tn)
	writePrimaryWhere(table, buf)
	fmt.Printf("Delete: %s\n", buf.String())

	table.delete, err = db.Prepare(buf.String())

	return
}

func writePrimaryWhere(table *tableInfo, buf *bytes.Buffer) {
	buf.WriteString(" where ")
	for i, pk := range table.primaryKeys {
		if i > 0 {
			buf.WriteString(" and ")
		}
		buf.WriteString(pk)
		buf.WriteString(" = ?")
	}
}

func (t *tableInfo) applyInsert(row common.Row) error {
	var args []interface{}

	// Insert SQL takes all columns in a row
	for _, cn := range t.allColumns {
		args = append(args, argOrNil(row, cn))
	}

	_, err := t.insert.Exec(args...)
	return err
}

func (t *tableInfo) applyUpdate(row common.Row) error {
	var args []interface{}

	// Update SQL takes all non-primary key cols, then primary key
	for _, cn := range t.otherColumns {
		args = append(args, argOrNil(row, cn))
	}
	for _, cn := range t.primaryKeys {
		args = append(args, argOrNil(row, cn))
	}

	_, err := t.update.Exec(args...)
	return err
}

func (t *tableInfo) applyDelete(row common.Row) error {
	var args []interface{}

	// Delete just uses primary keys
	for _, cn := range t.primaryKeys {
		args = append(args, argOrNil(row, cn))
	}

	_, err := t.delete.Exec(args...)
	return err
}

func argOrNil(row common.Row, n string) interface{} {
	if row[n] == nil {
		return nil
	}
	return row[n].Value
}
