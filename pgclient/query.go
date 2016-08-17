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
	qm := NewOutputMessage('Q')
	qm.WriteString(query)
	err := c.writeMessage(qm)
	if err != nil {
		return nil, nil, err
	}

	var rowDesc []ColumnInfo
	var rows [][]string

	for {
		im, err := c.readMessage()
		if err != nil {
			return nil, nil, err
		}

		switch im.Type() {
		case 'C':
			// Command complete. Could return what we did.
		case 'G', 'H':
			// Copy in/out response -- not yet supported
			return nil, nil, errors.New("COPY operations not supported by this client")
		case 'T':
			rowDesc, err = parseRowDescription(im)
			if err != nil {
				return nil, nil, err
			}
		case 'D':
			row, err := parseDataRow(im)
			if err != nil {
				return nil, nil, err
			}
			rows = append(rows, row)
		case 'I':
			// Empty query response. Nothing to do really.
		case 'N':
			msg, err := parseNotice(im)
			if err == nil {
				log.Info(msg)
			}
		case 'E':
			return nil, nil, parseError(im)
		case 'Z':
			return rowDesc, rows, nil
		default:
			return nil, nil, fmt.Errorf("Invalid server response %v", im.Type())
		}
	}
}
