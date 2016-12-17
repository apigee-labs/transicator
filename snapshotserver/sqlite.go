package snapshotserver

import (
	"database/sql"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"

	"bytes"
	"fmt"

	log "github.com/Sirupsen/logrus"
	_ "github.com/mattn/go-sqlite3"
)

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

	// Close the database
	_, err = tdb.Exec("pragma wal_checkpoint(TRUNCATE)")
	if err != nil {
		sendAPIError(serverError, err.Error(), w, r)
		return err
	}
	tdb.Close()

	// Stream the result to the client
	return streamFile(dbFileName, w)
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

	s.WriteString(fmt.Sprintf("create table %s.%s (", t.schema, t.name))
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

	sql := fmt.Sprintf("select * from %s where %s in %s",
		pgTable, selectorColumn, GetTenants(scopes))
	log.Debugf("Postgres query: %s", sql)

	pgRows, err := pgTx.Query(sql)
	if err != nil {
		return err
	}
	defer pgRows.Close()

	// Use column names from the query and not from the table definition.
	// That way we are sure that we are getting them in the right order.
	colNames, _, err := parseColumnNames(pgRows)
	if err != nil {
		return err
	}

	sql = makeInsertSql(pgTable, colNames)
	log.Debugf("Sqlite insert: %s", sql)

	stmt, err := tdb.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for pgRows.Next() {
		cols := make([]interface{}, len(colNames))
		for i := range cols {
			cols[i] = new(interface{})
		}
		err = pgRows.Scan(cols...)
		if err != nil {
			return err
		}

		_, err := stmt.Exec(cols)
		if err != nil {
			return err
		}
	}

	return nil
}

func makeInsertSql(pgTable *pgTable, colNames []string) string {
	s := &bytes.Buffer{}
	s.WriteString(fmt.Sprintf("insert into %s.%s (", pgTable.schema, pgTable.name))

	for i, cn := range colNames {
		if i > 0 {
			s.WriteString(",")
		}
		s.WriteString(cn)
	}

	s.WriteString(" values(")

	for i := range colNames {
		if i > 0 {
			s.WriteString(",")
		}
		s.WriteString("?")
	}
	s.WriteString(")")

	return s.String()
}

func streamFile(srcFile string, w http.ResponseWriter) error {
	inFile, err := os.Open(srcFile)
	if err != nil {
		return err
	}
	defer inFile.Close()

	w.Header().Set("Content-Type", sqlMediaType)

	_, err = io.Copy(w, inFile)
	return err
}
