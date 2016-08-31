package pgclient

import (
	"errors"
	"fmt"
	"io"

	log "github.com/Sirupsen/logrus"
)

type CopyFormat string

const (
	CopyFormatText    CopyFormat = "text"
	CopyFormatBinary             = "binary"
	CopyFormatCsv                = "csv"
	CopyFormatUnknown            = "unknown"
)

type CopyResponseInfo struct {
	format  CopyFormat
	numCol  int
	colFmts []int
}

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
			msg, _ = ParseCommandComplete(m)

		case CopyDone:
			// no more data, so wait for ReadyForQuery

		case ReadyForQuery:
			// all done
			return wr, nil

		default:
			return nil, errors.New("Unknown message type from server: " + m.Type().String())
		}
		if msg != "" {
			log.Infof("Info from server: %s", msg)
		}
	}
}
