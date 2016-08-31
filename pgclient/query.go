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
	log.Debugf("Query: %s", query)
	qm := NewOutputMessage(Query)
	qm.WriteString(query)
	err := c.WriteMessage(qm)
	if err != nil {
		return nil, nil, err
	}

	var rowDesc []ColumnInfo
	var rows [][]string
	var cmdErr error

	// Loop until we get a ReadyForQuery message, or until we get an error
	// reading messages at all.
	for {
		im, err := c.ReadMessage()
		if err != nil {
			return nil, nil, err
		}

		switch im.Type() {
		case CommandComplete:
			// Command complete. Could return what we did.
			msg, err := ParseCommandComplete(im)
			if err == nil {
				log.Debug(msg)
			}
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
			return rowDesc, rows, cmdErr
		default:
			cmdErr = fmt.Errorf("Invalid server response %v", im.Type())
		}
	}
}
