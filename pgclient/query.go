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
	// Name of the column
	Name string
	// Postgres type OID
	Type PgType
	// Was the column represented in binary form in the result row?
	Binary bool
}

/*
SimpleQuery executes a query as a string, and returns a list of rows.
It does not do any kind of preparation. The first parameter returned is an
array of descriptors for each row. The second is an array of rows.
*/
func (c *PgConnection) SimpleQuery(query string) ([]ColumnInfo, [][]string, error) {
	cols, rawRows, _, err := c.exec(query)
	var rows [][]string
	if err == nil {
		// Convert rows into an array of strings
		for _, rawRow := range rawRows {
			var cols []string
			for _, rawCol := range rawRow {
				cols = append(cols, string(rawCol))
			}
			rows = append(rows, cols)
		}
	}
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

func (c *PgConnection) exec(query string) ([]ColumnInfo, [][][]byte, int64, error) {
	log.Debugf("Query: %s", query)
	qm := NewOutputMessage(Query)
	qm.WriteString(query)
	err := c.WriteMessage(qm)
	if err != nil {
		return nil, nil, 0, err
	}

	var rowDesc []ColumnInfo
	var rows [][][]byte
	var rowCount int64
	var cmdErr error

	// Loop until we get a ReadyForQuery message, or until we get an error
	// reading messages at all.
	for {
		im, err := c.readStandardMessage()
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
		case ReadyForQuery:
			return rowDesc, rows, rowCount, cmdErr
		case ErrorResponse:
			// Need to record error and keep reading until we get ReadyForQuery
			cmdErr = ParseError(im)
		default:
			cmdErr = fmt.Errorf("Invalid server response %s", im.Type())
		}
	}
}
