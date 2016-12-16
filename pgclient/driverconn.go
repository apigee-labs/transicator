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
package pgclient

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

var registered = &sync.Once{}

var _ = RegisterDriver()

/*
RegisterDriver ensures that the driver has been registered with the
runtime. It's OK to call it more than once.
*/
func RegisterDriver() bool {
	registered.Do(func() {
		sql.Register("transicator", &PgDriver{})
	})
	return true
}

// PgDriver implements the standard driver interface
type PgDriver struct {
	isolationLevel      string
	extendedColumnNames bool
	readTimeout         time.Duration
}

// Open takes a Postgres URL as used elsewhere in this package
func (d *PgDriver) Open(url string) (driver.Conn, error) {
	pgc, err := Connect(url)
	if err != nil {
		return nil, err
	}
	if d.isolationLevel != "" {
		_, err = pgc.SimpleExec(
			fmt.Sprintf("set session default_transaction_isolation =  '%s'", d.isolationLevel))
		if err != nil {
			return nil, err
		}
		// TODO call AddrError.Timeout() to see if the error is a timeout
		// then cancel and go back and retry the read.
		// We should get a "cancelled" error!
	}
	pgc.setReadTimeout(d.readTimeout)
	return &PgDriverConn{
		driver: d,
		conn:   pgc,
	}, nil
}

/*
SetIsolationLevel ensures that all connections opened by this driver have
the specified isolation level. It will only affect connections opened
after it was called, so callers should call it before executing any
transactions
*/
func (d *PgDriver) SetIsolationLevel(level string) {
	d.isolationLevel = level
}

/*
SetExtendedColumnNames enables a mode in which the column names returned
from the "Rows" interface will return the format "name:type" where "type"
is the integer postgres type ID.
*/
func (d *PgDriver) SetExtendedColumnNames(extended bool) {
	d.extendedColumnNames = extended
}

/*
SetReadTimeout enables a timeout on all "read" operations from the database.
This effectively bounds the amount of time that the database can spend
on any single SQL operation.
*/
func (d *PgDriver) SetReadTimeout(t time.Duration) {
	d.readTimeout = t
}

// PgDriverConn implements Conn
type PgDriverConn struct {
	conn      *PgConnection
	driver    *PgDriver
	stmtIndex int
}

// Prepare creates a statement. Right now it just saves the SQL.
func (c *PgDriverConn) Prepare(query string) (driver.Stmt, error) {
	c.stmtIndex++
	stmtName := fmt.Sprintf("transicator-%x", c.stmtIndex)
	return makeStatement(stmtName, query, c)
}

// Exec is the fast path for SQL that the caller did not individually prepare.
// If there are no parameters, it uses the "simple
// query protocol" which works with fewer messages to the database.
// Otherwise, it uses the default (unnamed) prepared statement.
func (c *PgDriverConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if len(args) == 0 {
		rowCount, err := c.conn.SimpleExec(query)
		if err == nil {
			return driver.RowsAffected(rowCount), nil
		}
		return nil, err
	}

	log.Debug("Making unnamed statement")
	stmt, err := makeStatement("", query, c)
	if err != nil {
		return nil, err
	}

	ni := stmt.NumInput()
	if len(args) != ni {
		return nil, fmt.Errorf("Number of arguments does not match required %d", ni)
	}

	// Now just execute it normally
	return stmt.Exec(args)
}

// Query works like a normal prepared query but it uses the unnamed prepared
// statement to save a message. We don't use the simple query protocol
// here because it does not allow us to stream the rows
func (c *PgDriverConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	log.Debug("Making unnamed statement")
	stmt, err := makeStatement("", query, c)
	if err != nil {
		return nil, err
	}

	ni := stmt.NumInput()
	if len(args) != ni {
		return nil, fmt.Errorf("Number of arguments does not match required %d", ni)
	}

	// Now just execute it normally
	return stmt.Query(args)
}

// Close closes the connection
func (c *PgDriverConn) Close() error {
	c.conn.Close()
	return nil
}

// Begin just runs the SQL "begin" statement
func (c *PgDriverConn) Begin() (driver.Tx, error) {
	_, err := c.conn.SimpleExec("begin")
	if err != nil {
		return nil, err
	}
	return &PgTransaction{
		conn: c.conn,
	}, nil
}

// PgTransaction is a simple wrapper for transaction SQL
type PgTransaction struct {
	conn *PgConnection
}

// Commit just runs the "commit" statement
func (t *PgTransaction) Commit() error {
	_, err := t.conn.SimpleExec("commit")
	return err
}

// Rollback just runs the "rollback" statement
func (t *PgTransaction) Rollback() error {
	_, err := t.conn.SimpleExec("rollback")
	return err
}

// PgRows implements the Rows interface. All the rows are saved up before
// we return this, so "Next" does no I/O.
type PgRows struct {
	stmt       *PgStmt
	rows       [][][]byte
	curRow     int
	fetchedAll bool
}

// Columns returns the column names. The statement will already have them
// described before this structure is created.
func (r *PgRows) Columns() []string {
	cns := make([]string, len(r.stmt.columns))
	for i, col := range r.stmt.columns {
		if r.stmt.driver.extendedColumnNames {
			cns[i] = fmt.Sprintf("%s:%d", col.Name, col.Type)
		} else {
			cns[i] = col.Name
		}
	}
	return cns
}

// Next iterates through the rows. If no rows have been fetched yet (either
// because we came to the end of a batch, or if we have not called
// Execute yet) then we fetch some rows.
func (r *PgRows) Next(dest []driver.Value) error {
	if r.curRow >= len(r.rows) {
		if r.fetchedAll {
			return io.EOF
		}

		done, newRows, _, err := r.stmt.execute(fetchRowCount)
		if err != nil {
			r.stmt.syncAndWait()
			r.fetchedAll = true
			return err
		}
		r.rows = newRows
		r.curRow = 0
		r.fetchedAll = done

		if len(newRows) == 0 {
			return io.EOF
		}
	}

	row := r.rows[r.curRow]
	r.curRow++

	for i, col := range row {
		dest[i] = convertColumnValue(r.stmt.columns[i].Type, col)
	}

	return nil
}

// Close syncs the connection so that it can be made ready for the next
// time we need to use it. We can do this even if we didn't fetch
// all the rows.
func (r *PgRows) Close() error {
	return r.stmt.syncAndWait()
}
