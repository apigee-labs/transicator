package pgclient

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
)

const (
	fetchRowCount = 100
)

// PgStmt implements the Stmt interface. It does only "simple" queries now
type PgStmt struct {
	name      string
	conn      *PgConnection
	described bool
	numInputs int
	columns   []ColumnInfo
}

// makeStatewment sends a Parse message to the database to have a bunch of
// SQL evaluated. It may result in an error, in which case we will
// send a Sync on the connection and go back to being ready for the next query.
func makeStatement(name, sql string, conn *PgConnection) (*PgStmt, error) {
	parseMsg := NewOutputMessage(Parse)
	parseMsg.WriteString(name)
	parseMsg.WriteString(sql)
	parseMsg.WriteInt16(0)

	log.Debugf("Parse statement %s with sql \"%s\"", name, sql)
	err := conn.WriteMessage(parseMsg)
	if err != nil {
		return nil, err
	}
	conn.sendFlush()
	resp, err := conn.readStandardMessage()
	if err != nil {
		return nil, err
	}

	stmt := &PgStmt{
		name: name,
		conn: conn,
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

	switch resp.Type() {
	case ParameterDescription:
		var ni int16
		ni, err = resp.ReadInt16()
		if err != nil {
			return err
		}
		s.numInputs = int(ni)
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

// NumInput uses a "describe" message to get information about the inputs.
func (s *PgStmt) NumInput() int {
	err := s.describe()
	if err == nil {
		return s.numInputs
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
	bindMsg.WriteInt16(0) // Zero format codes (everything a string)
	bindMsg.WriteInt16(int16(len(args)))

	// Send each argument converted into a string.
	for _, arg := range args {
		if arg == nil {
			bindMsg.WriteInt32(-1)
		} else {
			bindVal := convertParameterValue(arg)
			bindMsg.WriteInt32(int32(len(bindVal)))
			bindMsg.WriteBytes(bindVal)
		}
	}

	bindMsg.WriteInt16(0) // All result columns can be a string

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
func (s *PgStmt) execute(maxRows int32) (bool, [][]string, int64, error) {
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
	var rows [][]string
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

// convertParameterValue is used to convert input values to a string so
// that they may be passed on to SQL.
func convertParameterValue(v driver.Value) []byte {
	switch v.(type) {
	case int64:
		return []byte(strconv.FormatInt(v.(int64), 10))
	case float64:
		return []byte(strconv.FormatFloat(v.(float64), 'f', -1, 64))
	case bool:
		return []byte(strconv.FormatBool(v.(bool)))
	case string:
		return []byte(v.(string))
	case time.Time:
		return []byte(v.(time.Time).Format(time.RFC1123Z))
	case []byte:
		// TODO this is probably wrong
		return v.([]byte)
	default:
		// The "database/sql" package promises to only pass us data
		// in the types listed above.
		panic("Invalid value type passed to SQL driver")
	}
}
