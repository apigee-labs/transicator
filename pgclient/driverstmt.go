package pgclient

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
)

// PgStmt implements the Stmt interface. It does only "simple" queries now
type PgStmt struct {
	name      string
	conn      *PgConnection
	described bool
	numInputs int
	columns   []ColumnInfo
}

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
	resp, err := conn.ReadMessage()
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
		return nil, fmt.Errorf("Invalid response: %d", resp.Type())
	}
}

// describe ensures that Describe was called on the statement
// After a nil error return, the "numInputs" and "columns"
// fields may be used
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
	resp, err := s.conn.ReadMessage()
	if err != nil {
		return err
	}

	switch resp.Type() {
	case ParameterDescription:
		ni, err := resp.ReadInt16()
		if err != nil {
			return err
		}
		s.numInputs = int(ni)
	case ErrorResponse:
		return ParseError(resp)
	default:
		return fmt.Errorf("Invalid response: %d", resp.Type())
	}

	resp, err = s.conn.ReadMessage()
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
		return ParseError(resp)
	default:
		return fmt.Errorf("Invalid response: %d", resp.Type())
	}
}

// Close removes knowledge of the statement from the server.
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
	resp, err := s.conn.ReadMessage()
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

// NumInput uses a "describe" message to get information about the inputs
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
	_, rowCount, err := s.execute(args)
	if err == nil {
		return driver.RowsAffected(rowCount), nil
	}
	return nil, err
}

// Query uses the "default" Postgres portal to Bind and then Execute,
// and retrieve all the rows.
func (s *PgStmt) Query(args []driver.Value) (driver.Rows, error) {
	rowVals, _, err := s.execute(args)
	if err == nil {
		return &PgRows{
			cols:   s.columns,
			rows:   rowVals,
			curRow: 0,
		}, nil
	}
	return nil, err
}

func (s *PgStmt) execute(args []driver.Value) ([][]string, int64, error) {
	bindMsg := NewOutputMessage(Bind)
	bindMsg.WriteString("") // Default portal
	bindMsg.WriteString(s.name)
	bindMsg.WriteInt16(0) // Zero format codes (everything a string)
	bindMsg.WriteInt16(int16(len(args)))

	for _, arg := range args {
		if arg == nil {
			bindMsg.WriteInt32(-1)
		} else {
			bindVal := convertParameterValue(arg)
			bindMsg.WriteInt32(int32(len(bindVal)))
			bindMsg.WriteBytes(bindVal)
		}
	}

	bindMsg.WriteInt16(0) // All results can be a string

	log.Debugf("Bind statement %s", s.name)
	err := s.conn.WriteMessage(bindMsg)
	if err != nil {
		return nil, 0, driver.ErrBadConn
	}
	s.conn.sendFlush()
	resp, err := s.conn.ReadMessage()
	if err != nil {
		return nil, 0, driver.ErrBadConn
	}

	switch resp.Type() {
	case BindComplete:
		// Keep on going
	case ErrorResponse:
		s.syncAndWait()
		return nil, 0, ParseError(resp)
	default:
		s.syncAndWait()
		return nil, 0, fmt.Errorf("Invalid response: %d", resp.Type())
	}

	execMsg := NewOutputMessage(Execute)
	execMsg.WriteString("")
	execMsg.WriteInt32(0)

	log.Debug("Execute")
	err = s.conn.WriteMessage(execMsg)
	if err != nil {
		// Probably means that the connection is broken
		return nil, 0, driver.ErrBadConn
	}
	s.conn.sendFlush()

	var cmdErr error
	var rows [][]string
	var rowCount int64
	done := false

	// Loop until we get a ReadyForQuery message, or until we get an error
	// reading messages at all.
	for !done {
		im, err := s.conn.ReadMessage()
		if err != nil {
			return nil, 0, driver.ErrBadConn
		}

		switch im.Type() {
		case CommandComplete:
			// Command complete. Could return what we did.
			rowCount, err = ParseCommandComplete(im)
			done = true
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
		case NoticeResponse:
			msg, err := ParseNotice(im)
			if err == nil {
				log.Info(msg)
			}
		case ErrorResponse:
			cmdErr = ParseError(resp)
			done = true
		default:
			cmdErr = fmt.Errorf("Invalid server response %d", im.Type())
			done = true
		}
	}

	// Always "Sync" after the end
	syncErr := s.syncAndWait()
	if cmdErr != nil {
		return rows, rowCount, cmdErr
	}
	return rows, rowCount, syncErr
}

// syncAndWait sends a "Sync" command and waits until it gets a ReadyForQuery
// message. It returns any ErrorResponse that it gets, and also returns any
// connection error.
func (s *PgStmt) syncAndWait() error {
	syncMsg := NewOutputMessage(Sync)
	s.conn.WriteMessage(syncMsg)

	var errResp error
	for {
		im, err := s.conn.ReadMessage()
		if err != nil {
			// Tell the SQL stuff that we probably can't continue with this connection
			return driver.ErrBadConn
		}
		switch im.Type() {
		case ReadyForQuery:
			return errResp
		case ErrorResponse:
			errResp = ParseError(im)
		case NoticeResponse:
			msg, err := ParseNotice(im)
			if err == nil {
				log.Info(msg)
			}
		default:
			log.Debug("Ignoring unexpected message %s", im.Type())
		}
	}
}

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
		panic("Invalid value type passed to SQL driver")
	}
}
