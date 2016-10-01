package pgclient

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"time"
)

const (
	pgTimeFormat = "2006-01-02 15:04:05-07"
)

var _ = registerDriver()

func registerDriver() bool {
	sql.Register("transicator", &PgDriver{})
	return true
}

// PgDriver implements the standard driver interface
type PgDriver struct {
	isolationLevel string
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
	}
	return &PgDriverConn{
		driver: d,
		conn:   pgc,
	}, nil
}

// SetIsolationLevel ensures that all connections opened by this driver have
// the specified isolation level. It will only affect connections opened
// after it was called, so callers should call it before executing any
// transactions
func (d *PgDriver) SetIsolationLevel(level string) {
	d.isolationLevel = level
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
	return makeStatement(stmtName, query, c.conn)
}

// Exec is the fast path for sql with no parameters. It uses the "simple
// query protocol" which works with fewer messages to the database.
// We chose not to implement "Query" as well because the simple query protocol
// does not allow us to return a large result set one set of rows at a time,
// so we'd run out of memory.
func (c *PgDriverConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}

	rowCount, err := c.conn.SimpleExec(query)
	if err == nil {
		return driver.RowsAffected(rowCount), nil
	}
	return nil, err
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
	for i := range r.stmt.columns {
		cns[i] = r.stmt.columns[i].Name
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
		if r.stmt.columns[i].Type.isTimestamp() {
			tm, err := time.Parse(pgTimeFormat, string(col))
			if err == nil {
				dest[i] = tm
			} else {
				fmt.Printf("Error: %s\n", err)
				dest[i] = []byte(fmt.Sprintf("Invalid timestamp %s", string(col)))
			}
		} else {
			dest[i] = col
		}
	}

	return nil
}

// Close syncs the connection so that it can be made ready for the next
// time we need to use it. We can do this even if we didn't fetch
// all the rows.
func (r *PgRows) Close() error {
	return r.stmt.syncAndWait()
}
