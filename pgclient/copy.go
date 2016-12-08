/*
Copyright 2016 The Transicator Authors

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
	"fmt"
	"io"

	log "github.com/Sirupsen/logrus"
)

// CopyFormat is the format of the copy.
type CopyFormat string

// Various types of copy formats
const (
	CopyFormatText    CopyFormat = "text"
	CopyFormatBinary             = "binary"
	CopyFormatCsv                = "csv"
	CopyFormatUnknown            = "unknown"
)

// CopyResponseInfo describes the format of the copy response
type CopyResponseInfo struct {
	format  CopyFormat
	numCol  int
	colFmts []int
}

// GetCopyFormat translates a numeric format code from the server to a format
func GetCopyFormat(i int) CopyFormat {
	switch i {
	case 0:
		// postgres also seems to return 0 for CSV format
		return CopyFormatText
	case 1:
		return CopyFormatBinary
	default:
		return CopyFormatUnknown
	}
}

// CopyTo performs the actual copy on the connection
func (c *PgConnection) CopyTo(wr io.Writer, query string, cf CopyFormat) (io.Writer, error) {
	var cmd string
	if cf == CopyFormatText {
		// postgres server complains about 'WITH text'
		cmd = fmt.Sprintf("COPY (%s) TO STDOUT", query)
	} else {
		// postgres server doesn't seem to support 'WITH FORMAT [csv|binary]'
		cmd = fmt.Sprintf("COPY (%s) TO STDOUT WITH %s", query, cf)
	}
	log.Infof("CopyTo cmd: %s", cmd)
	copyMsg := NewOutputMessage(Query)
	copyMsg.WriteString(cmd)
	err := c.WriteMessage(copyMsg)
	if err != nil {
		return nil, err
	}

	for {
		m, err := c.ReadMessage()
		if err != nil {
			return nil, err
		}

		var msg string
		switch m.Type() {
		case ErrorResponse:
			return nil, ParseError(m)

		case NoticeResponse:
			msg, _ = ParseNotice(m)

		case ParameterStatus:
			msg, _ = ParseParameterStatus(m)

		case CopyOutResponse:
			info, _ := ParseCopyOutResponse(m)
			msg = fmt.Sprintf("%+v", info)

		case CopyData:
			_, err = wr.Write(m.ReadRemaining())
			if err != nil {
				return nil, err
			}

		case CommandComplete:
			ParseCommandComplete(m)

		case CopyDone:
			// no more data, so wait for ReadyForQuery

		case ReadyForQuery:
			// all done
			return wr, nil

		default:
			return nil, fmt.Errorf("Unknown message type from server: %d", m.Type())
		}
		if msg != "" {
			log.Infof("Info from server: %s", msg)
		}
	}
}
