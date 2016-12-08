// +build !rocksdb

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

package storage

import (
	"sort"

	"database/sql"

	"fmt"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/apigee-labs/transicator/common"
	// Ensure that the sqlite driver is loaded
	_ "github.com/mattn/go-sqlite3"
)

/*
An SQL is a handle to a database.
*/
type SQL struct {
	baseFile    string
	db          *sql.DB
	insert      *sql.Stmt
	readRange   *sql.Stmt
	readFirst   *sql.Stmt
	readLast    *sql.Stmt
	purgeByTime *sql.Stmt
}

type readResult struct {
	lsn   uint64
	index uint32
	data  []byte
}
type readResults []readResult

/*
Open opens a SQLite database and makes it available for reads and writes.
Opened databases should be closed when done.

The "baseFile" parameter refers to the name of a directory where RocksDB can
store its data. SQLite will create a few inside this directory. To create
an empty database, make sure that it is empty.
*/
func Open(baseFile string) (*SQL, error) {

	success := false

	st, err := os.Stat(baseFile)
	if err != nil {
		err = os.Mkdir(baseFile, 0775)
		if err != nil {
			return nil, err
		}
	} else if !st.IsDir() {
		return nil, fmt.Errorf("Database location %s is not a directory", baseFile)
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

	stor := &SQL{
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
	if err == nil {
		stor.purgeByTime, err = db.Prepare(purgeByTimeSQL)
	}
	if err != nil {
		return nil, err
	}

	success = true

	return stor, nil
}

/*
Close closes the database cleanly.
*/
func (s *SQL) Close() {
	log.Infof("Closed DB in %s", s.baseFile)
	s.db.Close()
}

/*
Delete deletes all the files used by the database.
*/
func (s *SQL) Delete() error {
	return os.RemoveAll(s.baseFile)
}

/*
Put writes an entry to the database indexed by scope, lsn, and index in order
*/
func (s *SQL) Put(scope string, lsn uint64, index uint32, data []byte) error {
	_, err := s.insert.Exec(scope, lsn, index, time.Now().UnixNano(), data)
	return err
}

/*
PutBatch writes a whole bunch.
*/
func (s *SQL) PutBatch(entries []Entry) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	is := tx.Stmt(s.insert)
	defer is.Close()

	for _, entry := range entries {
		_, err = is.Exec(
			entry.Scope, entry.LSN, entry.Index,
			time.Now().UnixNano(), entry.Data)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

/*
Get returns what was written by PutEntry. It's mainly used for testing.
*/
func (s *SQL) Get(scope string, lsn uint64, index uint32) ([]byte, error) {
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
Scan returns entries in sequence number order a list of scopes.
It also returns the sequences of the first and last records in the DB.
The first entry returned will be the first entry that matches the specified
startLSN and startIndex. No more than "limit" entries will be returned.
To retrieve the very next entry after an entry, simply increment the index
by 1. This method uses a snapshot to guarantee consistency even if data is
being inserted to the database -- as long as the data is being inserted
in LSN order!
The array returned is the array of entries (again, in "sequence" order).
*/
func (s *SQL) Scan(
	scopes []string,
	startLSN uint64, startIndex uint32,
	limit int, filter func([]byte) bool) (final [][]byte, firstSeq common.Sequence, lastSeq common.Sequence, err error) {

	// By doing this in a transaction we should get snapshot-level consistency
	var tx *sql.Tx
	tx, err = s.db.Begin()
	if err != nil {
		return
	}
	defer tx.Commit()

	// Read first and last sequences
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
	tx.Commit()

	// Sort and then take limit
	sort.Sort(results)

	for count := 0; count < len(results) && count < limit; count++ {
		final = append(final, results[count].data)
	}
	return
}

/*
Purge removes all entries older than the specified time.
*/
func (s *SQL) Purge(oldest time.Time) (uint64, error) {
	res, err := s.purgeByTime.Exec(oldest.UnixNano())
	if err != nil {
		return 0, err
	}
	ra, _ := res.RowsAffected()
	return uint64(ra), nil
}

func (s *SQL) readOneRange(scope string, startLSN uint64,
	startIndex uint32, limit int, tx *sql.Tx,
	filter func([]byte) bool) (results readResults, err error) {

	var rows *sql.Rows
	rrs := tx.Stmt(s.readRange)
	defer rrs.Close()
	rows, err = rrs.Query(scope, startLSN, startLSN, startIndex)
	if err != nil {
		return
	}
	defer rows.Close()

	count := 0
	for count < limit && rows.Next() {
		var lsn uint64
		var index uint32
		var data []byte
		err = rows.Scan(&lsn, &index, &data)
		if err != nil {
			return
		}
		if filter == nil || filter(data) {
			result := readResult{
				lsn:   lsn,
				index: index,
				data:  data,
			}
			results = append(results, result)
			count++
		}
	}

	return results, nil
}

func (s *SQL) readLimits(tx *sql.Tx) (firstSeq, lastSeq common.Sequence, err error) {
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
