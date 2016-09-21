package pgclient

import (
	"errors"
	"fmt"

	log "github.com/Sirupsen/logrus"
)

/*
A ColumnInfo describes a single row in a query result.
*/
type ColumnInfo struct {
	Name   string
	Type   int32
	Binary bool
}

/*
SimpleQuery executes a query as a string, and returns a list of rows.
It does not do any kind of preparation. The first parameter returned is an
array of descriptors for each row. The second is an array of rows.
*/
func (c *PgConnection) SimpleQuery(query string) ([]ColumnInfo, [][]string, error) {
	cols, rows, _, err := c.exec(query)
	return cols, rows, err
}

/*
SimpleExec executes a query as a string, and returns a row count.
It does not do any kind of preparation. The first parameter returned is an
array of descriptors for each row. The second is an array of rows.
*/
func (c *PgConnection) SimpleExec(query string) (int64, error) {
	_, _, rowCount, err := c.exec(query)
	return rowCount, err
}

func (c *PgConnection) exec(query string) ([]ColumnInfo, [][]string, int64, error) {
	log.Debugf("Query: %s", query)
	qm := NewOutputMessage(Query)
	qm.WriteString(query)
	err := c.WriteMessage(qm)
	if err != nil {
		return nil, nil, 0, err
	}

	var rowDesc []ColumnInfo
	var rows [][]string
	var rowCount int64
	var cmdErr error

	// Loop until we get a ReadyForQuery message, or until we get an error
	// reading messages at all.
	for {
		im, err := c.ReadMessage()
		if err != nil {
			return nil, nil, 0, err
		}

		switch im.Type() {
		case CommandComplete:
			// Command complete. Could return what we did.
			rowCount, err = ParseCommandComplete(im)
		case CopyInResponse, CopyOutResponse:
			// Copy in/out response -- not yet supported
			cmdErr = errors.New("COPY operations not supported by this client")
		case RowDescription:
			rowDesc, err = ParseRowDescription(im)
			if err != nil {
				cmdErr = err
			}
		case DataRow:
			row, err := ParseDataRow(im)
			if err != nil {
				cmdErr = err
			} else {
				rows = append(rows, row)
			}
		case EmptyQueryResponse:
			// Empty query response. Nothing to do really.
		case NoticeResponse:
			msg, err := ParseNotice(im)
			if err == nil {
				log.Info(msg)
			}
		case ErrorResponse:
			// Still have to keep processing until we get ReadyForQuery
			cmdErr = ParseError(im)
		case ReadyForQuery:
			return rowDesc, rows, rowCount, cmdErr
		default:
			cmdErr = fmt.Errorf("Invalid server response %v", im.Type())
		}
	}
}
