package pgclient

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
)

var _ = registerDriver()

func registerDriver() bool {
	sql.Register("transicator", &PgDriver{})
	return true
}

// PgDriver implements the standard driver interface
type PgDriver struct {
}

// Open takes a Postgres URL as used elsewhere in this package
func (d *PgDriver) Open(url string) (driver.Conn, error) {
	pgc, err := Connect(url)
	if err != nil {
		return nil, err
	}
	return &PgDriverConn{
		conn: pgc,
	}, nil
}

// PgDriverConn implements Conn
type PgDriverConn struct {
	conn      *PgConnection
	stmtIndex int
}

// Prepare creates a statement. Right now it just saves the SQL.
func (c *PgDriverConn) Prepare(query string) (driver.Stmt, error) {
	c.stmtIndex++
	stmtName := fmt.Sprintf("transicator-%x", c.stmtIndex)
	return makeStatement(stmtName, query, c.conn)
}

// Query is the fast path for queries with no parameters
func (c *PgDriverConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}

	rawCols, rawRows, err := c.conn.SimpleQuery(query)
	if err == nil {
		return &PgRows{
			cols:   rawCols,
			rows:   rawRows,
			curRow: 0,
		}, nil
	}
	return nil, err
}

// Exec is the fast path for sql with no parameters
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
	cols   []ColumnInfo
	rows   [][]string
	curRow int
}

// Columns returns the column names
func (r *PgRows) Columns() []string {
	cns := make([]string, len(r.cols))
	for i := range r.cols {
		cns[i] = r.cols[i].Name
	}
	return cns
}

// Next iterates through the rows
func (r *PgRows) Next(dest []driver.Value) error {
	if r.curRow >= len(r.rows) {
		return io.EOF
	}

	row := r.rows[r.curRow]
	r.curRow++

	for i := range row {
		dest[i] = []byte(row[i])
	}

	return nil
}

// Close does nothing.
func (r *PgRows) Close() error {
	return nil
}
