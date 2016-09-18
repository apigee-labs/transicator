package pgclient

import (
	"database/sql"
	"database/sql/driver"
	"errors"
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
	conn *PgConnection
}

// Prepare creates a statement. Right now it just saves the SQL.
func (c *PgDriverConn) Prepare(query string) (driver.Stmt, error) {
	return &PgStmt{
		conn: c.conn,
		sql:  query,
	}, nil
}

// Close closes the connection
func (c *PgDriverConn) Close() error {
	c.conn.Close()
	return nil
}

// Begin just runs the SQL "begin" statement
func (c *PgDriverConn) Begin() (driver.Tx, error) {
	_, _, err := c.conn.SimpleQuery("begin")
	if err != nil {
		return nil, err
	}
	return &PgTransaction{
		conn: c.conn,
	}, nil
}

// PgStmt implements the Stmt interface. It does only "simple" queries now
type PgStmt struct {
	sql  string
	conn *PgConnection
}

// Close does nothing right now
func (s *PgStmt) Close() error {
	return nil
}

// NumInput does nothing special right now
func (s *PgStmt) NumInput() int {
	return -1
}

// Exec executes the SQL immediately
func (s *PgStmt) Exec(args []driver.Value) (driver.Result, error) {
	if len(args) > 0 {
		return nil, errors.New("Input values not yet supported by the driver")
	}

	_, _, err := s.conn.SimpleQuery(s.sql)
	if err == nil {
		return &PgResult{}, nil
	}
	return nil, err
}

// Query executes the SQL immediately and remembers the rows to return
func (s *PgStmt) Query(args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, errors.New("Input values not yet supported by the driver")
	}

	cols, rows, err := s.conn.SimpleQuery(s.sql)
	if err == nil {
		return &PgRows{
			cols:   cols,
			rows:   rows,
			curRow: 0,
		}, nil
	}
	return nil, err
}

// PgTransaction is a simple wrapper for transaction SQL
type PgTransaction struct {
	conn *PgConnection
}

// Commit just runs the "commit" statement
func (t *PgTransaction) Commit() error {
	_, _, err := t.conn.SimpleQuery("commit")
	return err
}

// Rollback just runs the "rollback" statement
func (t *PgTransaction) Rollback() error {
	_, _, err := t.conn.SimpleQuery("rollback")
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

// PgResult implements the Result interface
type PgResult struct {
}

// LastInsertId does nothing right now
func (r *PgResult) LastInsertId() (int64, error) {
	return 0, nil
}

// RowsAffected does nothing right now
func (r *PgResult) RowsAffected() (int64, error) {
	return 0, nil
}
