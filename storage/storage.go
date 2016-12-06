package storage

import (
	"sort"

	"database/sql"

	"fmt"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/apigee-labs/transicator/common"
	_ "github.com/mattn/go-sqlite3"
)

/*
A DB is a handle to a database.
*/
type DB struct {
	baseFile  string
	db        *sql.DB
	insert    *sql.Stmt
	readRange *sql.Stmt
	readFirst *sql.Stmt
	readLast  *sql.Stmt
}

type readResult struct {
	lsn   uint64
	index uint32
	data  []byte
}
type readResults []readResult

/*
OpenDB opens a RocksDB database and makes it available for reads and writes.
Opened databases should be closed when done.

The "baseFile" parameter refers to the name of a directory where RocksDB can
store its data. RocksDB will create many files inside this directory. To create
an empty database, make sure that it is empty.
*/
func OpenDB(baseFile string) (*DB, error) {

	success := false

	st, err := os.Stat(baseFile)
	if err != nil {
		err = os.Mkdir(baseFile, 0775)
		if err != nil {
			return nil, err
		}
	} else if !st.IsDir() {
		return nil, fmt.Errorf("Database location %s is not a directory")
	}

	url := fmt.Sprintf("%s/transicator", baseFile)
	log.Infof("Opening SQLite DB at %s\n", url)
	db, err := sql.Open("sqlite3", url)
	if err != nil {
		return nil, err
	}

	defer func() {
		if !success {
			db.Close()
		}
	}()

	_, err = db.Exec(createTableSQL)
	if err != nil {
		return nil, err
	}

	stor := &DB{
		baseFile: baseFile,
		db:       db,
	}

	stor.insert, err = db.Prepare(insertSQL)
	if err == nil {
		stor.readRange, err = db.Prepare(readRangeSQL)
	}
	if err == nil {
		stor.readFirst, err = db.Prepare(readFirstSQL)
	}
	if err == nil {
		stor.readLast, err = db.Prepare(readLastSQL)
	}
	if err != nil {
		return nil, err
	}

	success = true

	return stor, nil
}

/*
GetDataPath returns the path of the data directory.
*/
func (s *DB) GetDataPath() string {
	return s.baseFile
}

/*
Close closes the database cleanly.
*/
func (s *DB) Close() {
	log.Infof("Closed DB in %s", s.baseFile)
	s.db.Close()
}

/*
Delete deletes all the files used by the database.
*/
func (s *DB) Delete() error {
	return os.RemoveAll(s.baseFile)
}

/*
PutEntry writes an entry to the database indexed by scope, lsn, and index in order
*/
func (s *DB) PutEntry(scope string, lsn uint64, index uint32, data []byte) error {
	_, err := s.insert.Exec(scope, lsn, index, data)
	return err
}

/*
GetEntry returns what was written by PutEntry.
*/
func (s *DB) GetEntry(scope string, lsn uint64, index uint32) ([]byte, error) {
	row := s.db.QueryRow("select data from transicator_entries where scope = ? and lsn = ? and ix = ?",
		scope, lsn, index)
	var data []byte
	err := row.Scan(&data)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err == nil {
		return data, nil
	}
	return nil, err
}

/*
GetEntries returns entries in sequence number order for the given
scope. The first entry returned will be the first entry that matches the specified
startLSN and startIndex. No more than "limit" entries will be returned.
To retrieve the very next entry after an entry, simply increment the index
by 1.
*/
func (s *DB) GetEntries(scope string, startLSN uint64,
	startIndex uint32, limit int, filter func([]byte) bool) ([][]byte, error) {

	var tx *sql.Tx
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	rr, err := s.readOneRange(scope, startLSN, startIndex, limit,
		tx, filter)
	if err != nil {
		return nil, err
	}

	var results [][]byte
	for _, r := range rr {
		results = append(results, r.data)
	}
	return results, nil
}

/*
GetMultiEntries returns entries in sequence number order a list of scopes.
It also returns the sequences of the first and last records in the DB.
The first entry returned will be the first entry that matches the specified
startLSN and startIndex. No more than "limit" entries will be returned.
To retrieve the very next entry after an entry, simply increment the index
by 1. This method uses a snapshot to guarantee consistency even if data is
being inserted to the database -- as long as the data is being inserted
in LSN order!
The array returned is the array of entries (again, in "sequence" order).
*/
func (s *DB) GetMultiEntries(
	scopes []string,
	startLSN uint64, startIndex uint32,
	limit int, filter func([]byte) bool) (final [][]byte, firstSeq common.Sequence, lastSeq common.Sequence, err error) {

	var tx *sql.Tx
	tx, err = s.db.Begin()
	if err != nil {
		return
	}
	defer tx.Commit()

	firstSeq, lastSeq, err = s.readLimits(tx)
	if err != nil {
		return
	}

	// Read range for each scope
	var results readResults
	for _, scope := range scopes {
		var rr readResults
		rr, err = s.readOneRange(scope, startLSN, startIndex, limit, tx, filter)
		if err != nil {
			return
		}
		results = append(results, rr...)
	}

	// Sort and then take limit
	sort.Sort(results)

	for count := 0; count < len(results) && count < limit; count++ {
		final = append(final, results[count].data)
	}
	return
}

/*
GetLimits returns the first and last sequences in the database.
*/
func (s *DB) GetLimits() (firstSeq, lastSeq common.Sequence, err error) {
	var tx *sql.Tx
	tx, err = s.db.Begin()
	if err != nil {
		return
	}
	defer tx.Commit()

	return s.readLimits(tx)
}

/*
PurgeEntries deletes everything from the database for which "filter" returns
true. It always returns the number of records that were actually deleted
(which could be zero). If there is an error during the purge process,
then a non-nil error will be returned. Be aware that this operation may
take a long time, so it is important to run it in a separate goroutine.
*/
func (s *DB) PurgeEntries(filter func([]byte) bool) (purgeCount uint64, err error) {
	var rows *sql.Rows
	rows, err = s.db.Query("select scope, lsn, ix, data from transicator_entries")
	if err != nil {
		return
	}
	defer rows.Close()

	var cleanStmt *sql.Stmt
	cleanStmt, err = s.db.Prepare("delete from transicator_entries where scope = ? and lsn = ? and ix = ?")
	if err != nil {
		return
	}
	defer cleanStmt.Close()

	for rows.Next() {
		var scope string
		var lsn uint64
		var ix uint32
		var data []byte
		err = rows.Scan(&scope, &lsn, &ix, &data)
		if err != nil {
			return
		}

		if filter(data) {
			var res sql.Result
			res, err = cleanStmt.Exec(scope, lsn, ix)
			if err != nil {
				return
			}
			rc, _ := res.RowsAffected()
			if rc != 1 {
				err = fmt.Errorf("Expected to delete 1 row, deleted %d\n", rc)
				return
			}
			purgeCount++
		}
	}
	return
}

func (s *DB) readOneRange(scope string, startLSN uint64,
	startIndex uint32, limit int, tx *sql.Tx,
	filter func([]byte) bool) (results readResults, err error) {

	var rows *sql.Rows
	rrs := tx.Stmt(s.readRange)
	defer rrs.Close()
	rows, err = rrs.Query(scope, startLSN, startIndex, limit)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var lsn uint64
		var index uint32
		var data []byte
		err = rows.Scan(&lsn, &index, &data)
		if err != nil {
			return
		}
		result := readResult{
			lsn:   lsn,
			index: index,
			data:  data,
		}
		results = append(results, result)
	}

	return results, nil
}

func (s *DB) readLimits(tx *sql.Tx) (firstSeq, lastSeq common.Sequence, err error) {
	rfs := tx.Stmt(s.readFirst)
	defer rfs.Close()

	row := rfs.QueryRow()

	var lsn uint64
	var ix uint32
	err = row.Scan(&lsn, &ix)
	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		return
	}
	firstSeq = common.MakeSequence(lsn, ix)

	rls := tx.Stmt(s.readLast)
	defer rls.Close()

	row = rls.QueryRow()

	err = row.Scan(&lsn, &ix)
	if err == sql.ErrNoRows {
		err = nil
		lastSeq = firstSeq
		return
	} else if err != nil {
		return
	}
	lastSeq = common.MakeSequence(lsn, ix)
	return
}

// Needed to sort read results by LSN and index

func (r readResults) Len() int {
	return len(r)
}

func (r readResults) Less(i, j int) bool {
	if r[i].lsn < r[j].lsn {
		return true
	}
	if r[i].lsn == r[j].lsn && r[i].index < r[j].index {
		return true
	}
	return false
}

func (r readResults) Swap(i, j int) {
	tmp := r[i]
	r[i] = r[j]
	r[j] = tmp
}
