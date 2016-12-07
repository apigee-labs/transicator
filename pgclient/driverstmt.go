/*
Copyright 2016 Google Inc.

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
	"database/sql/driver"
	"errors"
	"fmt"

	log "github.com/Sirupsen/logrus"
)

const (
	fetchRowCount = 100
)

// PgStmt implements the Stmt interface. It does only "simple" queries now
type PgStmt struct {
	name      string
	conn      *PgConnection
	driver    *PgDriver
	described bool
	fields    []PgType
	columns   []ColumnInfo
}

// makeStatewment sends a Parse message to the database to have a bunch of
// SQL evaluated. It may result in an error, in which case we will
// send a Sync on the connection and go back to being ready for the next query.
func makeStatement(name, sql string, c *PgDriverConn) (*PgStmt, error) {
	parseMsg := NewOutputMessage(Parse)
	parseMsg.WriteString(name)
	parseMsg.WriteString(sql)
	parseMsg.WriteInt16(0)

	log.Debugf("Parse statement %s with sql \"%s\"", name, sql)
	err := c.conn.WriteMessage(parseMsg)
	if err != nil {
		return nil, err
	}
	c.conn.sendFlush()
	resp, err := c.conn.readStandardMessage()
	if err != nil {
		return nil, err
	}

	stmt := &PgStmt{
		name:   name,
		conn:   c.conn,
		driver: c.driver,
	}

	switch resp.Type() {
	case ParseComplete:
		return stmt, nil
	case ErrorResponse:
		stmt.syncAndWait()
		return nil, ParseError(resp)
	default:
		stmt.syncAndWait()
		return nil, fmt.Errorf("Unexpected database response: %d", resp.Type())
	}
}

// describe ensures that Describe was called on the statement
// After a nil error return, the "numInputs" and "columns"
// fields may be used. If it has not been called, then a Describe message is
// sent and the result is interpreted.
func (s *PgStmt) describe() error {
	if s.described {
		return nil
	}

	describeMsg := NewOutputMessage(DescribeMsg)
	describeMsg.WriteByte('S')
	describeMsg.WriteString(s.name)

	log.Debugf("Describe statement %s", s.name)
	err := s.conn.WriteMessage(describeMsg)
	if err != nil {
		return err
	}
	s.conn.sendFlush()
	resp, err := s.conn.readStandardMessage()
	if err != nil {
		return err
	}

	// First response will always be a ParameterDescription
	switch resp.Type() {
	case ParameterDescription:
		var fields []PgType
		fields, err = ParseParameterDescription(resp)
		if err != nil {
			s.syncAndWait()
			return err
		}
		s.fields = fields
	case ErrorResponse:
		s.syncAndWait()
		return ParseError(resp)
	default:
		s.syncAndWait()
		return fmt.Errorf("Invalid response: %d", resp.Type())
	}

	resp, err = s.conn.readStandardMessage()
	if err != nil {
		return err
	}

	// SecondResponse will be a RowDescription or NoData
	switch resp.Type() {
	case RowDescription:
		rowDesc, err := ParseRowDescription(resp)
		if err != nil {
			return err
		}
		s.columns = rowDesc
		s.described = true
		return nil
	case NoData:
		s.described = true
		return nil
	case ErrorResponse:
		s.syncAndWait()
		return ParseError(resp)
	default:
		s.syncAndWait()
		return fmt.Errorf("Invalid response: %d", resp.Type())
	}
}

// Close removes knowledge of the statement from the server by sending
// a Close message.
func (s *PgStmt) Close() error {
	closeMsg := NewOutputMessage(Close)
	closeMsg.WriteByte('S')
	closeMsg.WriteString(s.name)

	log.Debugf("Close statement %s", s.name)
	err := s.conn.WriteMessage(closeMsg)
	if err != nil {
		return err
	}
	s.conn.sendFlush()
	resp, err := s.conn.readStandardMessage()
	if err != nil {
		return err
	}

	switch resp.Type() {
	case CloseComplete:
		return nil
	case ErrorResponse:
		return ParseError(resp)
	default:
		return fmt.Errorf("Invalid response: %d", resp.Type())
	}
}

// NumInput uses a "describe" message to get information about the inputs,
// but only if we haven't described the statement
func (s *PgStmt) NumInput() int {
	err := s.describe()
	if err == nil {
		return len(s.fields)
	}
	return -1
}

// Exec uses the "default" Postgres portal to Bind and then Execute,
// and retrieve all the rows.
func (s *PgStmt) Exec(args []driver.Value) (driver.Result, error) {
	err := s.bind(args)
	if err != nil {
		s.syncAndWait()
		return nil, err
	}

	// Execute the query but discard all the rows -- just read until we get
	// CommandComplete. But execute in a loop so we don't end up with all rows
	// in memory.
	done := false
	var rowCount int64
	for !done && err == nil {
		done, _, rowCount, err = s.execute(fetchRowCount)
	}

	s.syncAndWait()

	return driver.RowsAffected(rowCount), err
}

// Query uses the "default" Postgres portal to Bind. However we will defer
// Execute until we get all the rows.
func (s *PgStmt) Query(args []driver.Value) (driver.Rows, error) {
	err := s.bind(args)
	if err != nil {
		s.syncAndWait()
		return nil, err
	}

	// Ensure that we have column names before executing. Should only happen
	// once per statement.
	err = s.describe()
	if err != nil {
		s.syncAndWait()
		return nil, err
	}

	return &PgRows{
		stmt:   s,
		curRow: 0,
	}, nil
}

// bind sends a Bind message to bind the SQL for this statement to the default
// output portal so that we can begin executing a query. It returns an error
// and syncs the connection if the bind fails.
func (s *PgStmt) bind(args []driver.Value) error {
	bindMsg := NewOutputMessage(Bind)
	bindMsg.WriteString("") // Default portal
	bindMsg.WriteString(s.name)

	// Denote whether each argument is going to be sent in binary or string format
	bindMsg.WriteInt16(int16(len(s.fields)))
	for _, field := range s.fields {
		if field.isBinary() {
			bindMsg.WriteInt16(1)
		} else {
			bindMsg.WriteInt16(0)
		}
	}

	// Send each argument converted into a string or binary depending.
	bindMsg.WriteInt16(int16(len(args)))
	for i, arg := range args {
		if arg == nil {
			bindMsg.WriteInt32(-1)
		} else {
			bindVal, err := convertParameterValue(s.fields[i], arg)
			if err != nil {
				s.syncAndWait()
				return err
			}
			bindMsg.WriteInt32(int32(len(bindVal)))
			bindMsg.WriteBytes(bindVal)
		}
	}

	// Denote which result columns we want sent as a string versus binary
	bindMsg.WriteInt16(int16(len(s.columns)))
	for _, col := range s.columns {
		if col.Type.isBinary() {
			bindMsg.WriteInt16(1)
		} else {
			bindMsg.WriteInt16(0)
		}
	}

	log.Debugf("Bind statement %s", s.name)
	err := s.conn.WriteMessage(bindMsg)
	if err != nil {
		return driver.ErrBadConn
	}
	s.conn.sendFlush()
	resp, err := s.conn.readStandardMessage()
	if err != nil {
		return err
	}

	switch resp.Type() {
	case BindComplete:
		return nil
	case ErrorResponse:
		return ParseError(resp)
	default:
		return fmt.Errorf("Invalid response: %d", resp.Type())
	}
}

// execute executes the currently-bound statement (which means that it must
// always be called after a "bind"). It will fetch up to the number of rows
// specified in "maxRows" (zero means forever).
// It returns true in the first parameter if all the rows were fetched.
// The second parameter returns the rows retrieved, and the third returns
// the number of rows affected by the query, but only if the first parameter
// was true.
func (s *PgStmt) execute(maxRows int32) (bool, [][][]byte, int64, error) {
	execMsg := NewOutputMessage(Execute)
	execMsg.WriteString("")
	execMsg.WriteInt32(maxRows)

	log.Debugf("Execute up to %d rows", maxRows)
	err := s.conn.WriteMessage(execMsg)
	if err != nil {
		// Probably means that the connection is broken
		return true, nil, 0, driver.ErrBadConn
	}
	s.conn.sendFlush()

	var cmdErr error
	var rows [][][]byte
	var rowCount int64
	done := false
	complete := false

	// Loop until we get a message indicating that we have finished executing.
	for !done {
		im, err := s.conn.readStandardMessage()
		if err != nil {
			return true, nil, 0, err
		}

		switch im.Type() {
		case CommandComplete:
			// Command complete. No more rows coming.
			rowCount, err = ParseCommandComplete(im)
			done = true
			complete = true
		case CopyInResponse, CopyOutResponse:
			// Copy in/out response -- not yet supported
			cmdErr = errors.New("COPY operations not supported by this client")
			done = true
		case DataRow:
			row, err := ParseDataRow(im)
			if err != nil {
				cmdErr = err
				done = true
			} else {
				rows = append(rows, row)
			}
		case EmptyQueryResponse:
			// Empty query response. Nothing to do really.
			done = true
			complete = true
		case PortalSuspended:
			// There will be more rows returned after this
			done = true
		case ErrorResponse:
			cmdErr = ParseError(im)
			done = true
			complete = true
		default:
			cmdErr = fmt.Errorf("Invalid server response %d", im.Type())
			done = true
			complete = true
		}
	}

	log.Debugf("Returning with complete = %v rows = %d rowsAffected = %d",
		complete, len(rows), rowCount)
	return complete, rows, rowCount, cmdErr
}

// syncAndWait sends a "Sync" command and waits until it gets a ReadyForQuery
// message. It returns any ErrorResponse that it gets, and also returns any
// connection error. It must be called any time that we want to end the
// extended query protocol and return to being ready for a new
// query. If we do not do this then the connection hangs.
func (s *PgStmt) syncAndWait() error {
	syncMsg := NewOutputMessage(Sync)
	s.conn.WriteMessage(syncMsg)

	var errResp error
	for {
		im, err := s.conn.readStandardMessage()
		if err != nil {
			// Tell the SQL stuff that we probably can't continue with this connection
			return err
		}
		switch im.Type() {
		case ReadyForQuery:
			return errResp
		case ErrorResponse:
			errResp = ParseError(im)
		default:
			log.Debug("Ignoring unexpected message %s", im.Type())
		}
	}
}
