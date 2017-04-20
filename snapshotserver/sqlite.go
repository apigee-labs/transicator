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

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"

	"time"

	log "github.com/Sirupsen/logrus"
	sqlite "github.com/mattn/go-sqlite3"
)

var sqliteTimestampFormat = sqlite.SQLiteTimestampFormats[0]

/*
WriteSqliteSnapshot is responsible for generating the SQLite format of
the snapshot, and also for streaming the file back to the
caller.
*/
func WriteSqliteSnapshot(scopes []string, db *sql.DB, w http.ResponseWriter, r *http.Request) error {

	// Create temporary directory for the temporary DB
	dirName, err := ioutil.TempDir(tempSnapshotDir, tempSnapshotPrefix)
	if err != nil {
		sendAPIError(serverError, err.Error(), w, r)
		return err
	}
	defer func() {
		os.RemoveAll(dirName)
	}()

	// Open and verify the DB
	dbFileName := path.Join(dirName, tempSnapshotName)
	tdb, err := createDatabase(dbFileName)
	if err != nil {
		sendAPIError(http.StatusInternalServerError, err.Error(), w, r)
		return err
	}
	defer tdb.Close()

	// Because of previous config, this puts us in "repeatable read" mode
	pgTx, err := db.Begin()
	if err != nil {
		sendAPIError(serverError, err.Error(), w, r)
		return err
	}
	defer pgTx.Commit()

	tables, err := enumeratePgTables(pgTx)
	if err != nil {
		sendAPIError(http.StatusInternalServerError, err.Error(), w, r)
		return err
	}

	err = writeMetadata(pgTx, tdb, tables)
	if err != nil {
		sendAPIError(http.StatusInternalServerError, err.Error(), w, r)
		return err
	}
	// put tx id into header
	row := pgTx.QueryRow("select txid_current_snapshot()")
	var txId string
	err = row.Scan(&txId)
	if err == nil {
		w.Header().Set("Transicator-Snapshot-TXID", txId)
	}

	// For each table, update the DB
	for tid, pgTable := range tables {
		if pgTable.hasSelector {
			err = makeSqliteTable(tdb, pgTable)
			if err == nil {
				err = copyData(pgTx, tdb, scopes, pgTable)
			}
			if err != nil {
				sendAPIError(serverError, err.Error(), w, r)
				return err
			}
		} else {
			log.Debugf("Skipping table %s which has no selector", tid)
		}
	}

	pgTx.Commit()

	// Close the database. Checkpoint the WAL so that the result
	// is a single file, with no additional .wal or .shm file.
	_, err = tdb.Exec("pragma wal_checkpoint(TRUNCATE)")
	if err != nil {
		sendAPIError(serverError, err.Error(), w, r)
		return err
	}
	tdb.Close()

	// Stream the result to the client
	return streamFile(dbFileName, w)
}

func writeMetadata(pgTx *sql.Tx, tdb *sql.DB, tables map[string]*pgTable) error {
	_, err := tdb.Exec(`
		create table _transicator_metadata
		(key varchar primary key,
		 value varchar)
	 `)
	if err == nil {
		_, err = tdb.Exec(`
			create table _transicator_tables
			(tableName varchar not null,
			 columnName varchar not null,
			 typid integer,
			 primaryKey bool)
		`)
	}

	if err == nil {
		row := pgTx.QueryRow("select txid_current_snapshot()")
		var snap string
		err = row.Scan(&snap)
		if err == nil {
			_, err = tdb.Exec(`
			insert into _transicator_metadata (key, value) values('snapshot', ?)
			`, snap)
		}
	}

	var st *sql.Stmt
	if err == nil {
		st, err = tdb.Prepare(`
			insert into _transicator_tables
			(tableName, columnName, typid, primaryKey)
			values (?, ?, ?, ?)
		`)
		defer st.Close()
	}

	if err == nil {
		for _, table := range tables {
			for _, col := range table.columns {
				_, err = st.Exec(table.schema+"_"+table.name, col.name, col.typid, col.primaryKey)
				if err != nil {
					return err
				}
			}
		}
	}

	return err
}

func createDatabase(fileName string) (*sql.DB, error) {
	log.Debugf("Opening temporary SQLite database in %s", fileName)
	tdb, err := sql.Open("sqlite3", fileName)
	if err == nil {
		err = tdb.Ping()
	}
	if err != nil {
		return nil, err
	}

	_, err = tdb.Exec("pragma journal_mode = WAL")
	if err != nil {
		tdb.Close()
		return nil, err
	}
	return tdb, err
}

func makeSqliteTable(tdb *sql.DB, t *pgTable) error {
	sql := makeSqliteTableSQL(t)
	log.Debugf("New SQLite table: %s", sql)
	_, err := tdb.Exec(sql)
	return err
}

// makeSqliteTableSQL turns the parsed table description from Postgres
// into a "create table" statement that works in SQLite.
func makeSqliteTableSQL(t *pgTable) string {
	s := &bytes.Buffer{}

	s.WriteString(fmt.Sprintf("create table %s (", t.schema+"_"+t.name))
	first := true

	for _, col := range t.columns {
		if first {
			first = false
		} else {
			s.WriteString(",")
		}
		s.WriteString(fmt.Sprintf("%s %s", col.name, convertPgType(col.typid)))
	}
	if len(t.primaryKeys) > 0 {
		s.WriteString(", primary key (")
		for pki, pk := range t.primaryKeys {
			if pki > 0 {
				s.WriteString(",")
			}
			s.WriteString(pk)
		}
		s.WriteString(")")
	}
	s.WriteString(")")
	return s.String()
}

func copyData(pgTx *sql.Tx, tdb *sql.DB, scopes []string, pgTable *pgTable) error {

	sql := fmt.Sprintf("select * from %s.%s where %s in %s",
		pgTable.schema, pgTable.name, selectorColumn, GetTenants(scopes))
	log.Debugf("Postgres query: %s", sql)

	pgRows, err := pgTx.Query(sql)
	if err != nil {
		return err
	}
	defer pgRows.Close()

	// Use column names from the query and not from the table definition.
	// That way we are sure that we are getting them in the right order.
	colNames, colTypes, err := parseColumnNames(pgRows)
	if err != nil {
		return err
	}

	sql = makeInsertSQL(pgTable, colNames)
	log.Debugf("Sqlite insert: %s", sql)

	stmt, err := tdb.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for pgRows.Next() {
		cols := make([]interface{}, len(colNames))
		for i := range cols {
			switch pgToSqliteType(int(colTypes[i])) {
			case sqlInteger:
				cols[i] = new(*int64)
			case sqlReal:
				cols[i] = new(*float64)
			case sqlBlob:
				cols[i] = new([]byte)
			case sqlText:
				cols[i] = new(*string)
			case sqlTimestamp:
				cols[i] = new(*time.Time)
			default:
				panic(fmt.Sprintf("Invalid type code %d", pgToSqliteType(int(colTypes[i]))))
			}
		}
		err = pgRows.Scan(cols...)
		if err != nil {
			log.Errorf("Postgres scan error %s. cols = %s", err, debugColTypes(cols))
			return err
		}

		patchColTypes(cols)

		_, err := stmt.Exec(cols...)
		if err != nil {
			log.Errorf("SQLite insert error %s. SQL = %s. cols = %s", err, sql, debugColTypes(cols))
			return err
		}
	}

	return nil
}

func debugColTypes(cols []interface{}) string {
	s := &bytes.Buffer{}
	for i, c := range cols {
		if i > 0 {
			s.WriteString(",")
		}
		s.WriteString(fmt.Sprintf("%T", c))
	}
	return s.String()
}

// patchColTypes looks for column types that are different between PG
// and SQLite.
func patchColTypes(cols []interface{}) {
	for i, c := range cols {
		switch c.(type) {
		case **time.Time:
			ts := *(c.(**time.Time))
			if ts == nil {
				cols[i] = ""
			} else {
				cols[i] = ts.UTC().Format(sqliteTimestampFormat)
			}
		}
	}
}

func makeInsertSQL(pgTable *pgTable, colNames []string) string {
	s := &bytes.Buffer{}
	s.WriteString(fmt.Sprintf("insert into %s (", pgTable.schema+"_"+pgTable.name))

	for i, cn := range colNames {
		if i > 0 {
			s.WriteString(",")
		}
		s.WriteString(cn)
	}

	s.WriteString(") values(")

	for i := range colNames {
		if i > 0 {
			s.WriteString(",")
		}
		s.WriteString("?")
	}
	s.WriteString(")")

	return s.String()
}

func getSHA256Checksum(srcFile string) (error, string) {
	inFileHash, err := os.Open(srcFile)
	if err != nil {
		return err, ""
	}
	defer inFileHash.Close()
	hasher := sha256.New()
	_, err = io.Copy(hasher, inFileHash)
	if err != nil {
		return err, ""
	}
	return nil, hex.EncodeToString(hasher.Sum(nil))
}

func streamFile(srcFile string, w http.ResponseWriter) error {

	err, shasum := getSHA256Checksum(srcFile)
	if err != nil {
		return err
	}
	w.Header().Set("SHA256Sum", shasum)
	w.Header().Set("Content-Type", sqlMediaType)

	inFile, err := os.Open(srcFile)
	if err != nil {
		return err
	}
	defer inFile.Close()

	_, err = io.Copy(w, inFile)
	if err != nil {
		return err
	}

	return err
}
