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
package snapshotserver

import "database/sql"

// A pgColumn describes a single column in a table
type pgColumn struct {
	name       string
	typid      int
	primaryKey bool
}

// A pgTable describes a table
type pgTable struct {
	schema      string
	name        string
	columns     []pgColumn
	primaryKeys []string
	hasSelector bool
}

// enumeratePgTables returns a list of information about each table
// and also about its columns.
func enumeratePgTables(tx *sql.Tx) (map[string]*pgTable, error) {

	// A map makes this algorithm easier to understand although
	// theoretically we can do without it.
	tm, err := mapPgTables(tx)
	if err != nil {
		return nil, err
	}

	ps, err := tx.Prepare(`
	select a.attname
	from pg_namespace n, pg_class c, pg_index i, pg_attribute a
	where n.oid = c.relnamespace and c.oid = i.indrelid and a.attrelid = c.oid
	and c.relkind='r'
	and a.attnum = any(i.indkey)
	and a.attnum > 0
	and n.nspname = $1
	and c.relname = $2
	order by a.attnum
	`)
	if err != nil {
		return nil, err
	}
	defer ps.Close()

	for _, t := range tm {
		err = mapPrimaryKeys(ps, t)
		if err != nil {
			return nil, err
		}
	}
	return tm, nil
}

func mapPgTables(tx *sql.Tx) (map[string]*pgTable, error) {
	rows, err := tx.Query(`
	select n.nspname, c.relname, a.attname, a.atttypid
	from pg_namespace n, pg_class c, pg_attribute a
	where n.oid = c.relnamespace and a.attrelid = c.oid
	and c.relkind = 'r'
	and n.nspname not in ('information_schema', 'pg_catalog')
	and a.attnum > 0
	order by n.nspname, c.relname, a.attnum
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tm := make(map[string]*pgTable)

	for rows.Next() {
		var schema, name, attname string
		var typid int

		err = rows.Scan(&schema, &name, &attname, &typid)
		if err != nil {
			return nil, err
		}

		id := schema + "." + name
		tab := tm[id]
		if tab == nil {
			tab = &pgTable{
				schema: schema,
				name:   name,
			}
			tm[id] = tab
		}

		col := pgColumn{
			name:  attname,
			typid: typid,
		}
		tab.columns = append(tab.columns, col)

		if attname == selectorColumn {
			tab.hasSelector = true
		}
	}
	return tm, nil
}

func mapPrimaryKeys(s *sql.Stmt, t *pgTable) error {
	rows, err := s.Query(t.schema, t.name)
	if err != nil {
		return err
	}

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return err
		}
		t.primaryKeys = append(t.primaryKeys, name)

		for i, c := range t.columns {
			if c.name == name {
				c.primaryKey = true
				t.columns[i] = c
			}
		}
	}
	return nil
}
