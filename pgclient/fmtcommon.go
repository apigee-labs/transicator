package pgclient

import (
	"bytes"
	"errors"
	"regexp"
	"strconv"

	log "github.com/Sirupsen/logrus"
)

/*
ParseError looks at an input message that represents an error and returns
a Go error.
*/
func ParseError(m *InputMessage) error {
	if m.Type() != ErrorResponse {
		return errors.New("Message type is not Error")
	}
	msg, err := ParseNotice(m)
	if err == nil {
		log.Debugf("Received error: %s", msg)
		return errors.New(msg)
	}
	return err
}

/*
ParseNotice looks at an input message that's an error or a "notice" message
(both have the same format) and returns the text.
*/
func ParseNotice(m *InputMessage) (string, error) {
	if m.Type() != NoticeResponse && m.Type() != ErrorResponse {
		return "", errors.New("Mismatched message type")
	}

	msg := &bytes.Buffer{}
	for {
		code, err := m.ReadByte()
		if err != nil {
			return "", errors.New("Invalid message format")
		}

		if code == 0 {
			return msg.String(), nil
		}
		txt, err := m.ReadString()
		if err != nil {
			return "", errors.New("Invalid message format")
		}
		msg.WriteString(txt)
		msg.WriteString(" ")
	}
}

// ParseCopyOutResponse parses the response from the CopyData message
func ParseCopyOutResponse(m *InputMessage) (info *CopyResponseInfo, err error) {
	if m.Type() != CopyOutResponse {
		err = errors.New("Message type is not CopyOutResponse")
		return
	}

	i8, err := m.ReadInt8()
	if err != nil {
		return
	}
	log.Debugf("copy format (raw): %d", i8)
	copyFmt := GetCopyFormat(int(i8))

	i16, err := m.ReadInt16()
	if err != nil {
		return
	}
	numCol := int(i16)
	var colFmts = make([]int, numCol)
	for i := 0; i < numCol; i++ {
		var f int16
		f, err = m.ReadInt16()
		if err != nil {
			return
		}
		colFmts[i] = int(f)
	}
	info = &CopyResponseInfo{
		format:  copyFmt,
		numCol:  numCol,
		colFmts: colFmts,
	}
	return info, nil
}

/*
ParseCopyData looks at a CopyData message and then parses it again as
another message.
*/
func ParseCopyData(m *InputMessage) (*InputMessage, error) {
	if m.Type() != CopyData {
		return nil, errors.New("Mismatched message type")
	}

	buf := m.buf.Bytes()
	typeByte := buf[0]
	return NewInputMessage(PgInputType(typeByte), buf[1:]), nil
}

// ParseRowDescription looks at a RowDescription message and parses it
func ParseRowDescription(m *InputMessage) ([]ColumnInfo, error) {
	if m.Type() != RowDescription {
		return nil, errors.New("Message type is not Row Description")
	}

	var cols []ColumnInfo
	numFields, err := m.ReadInt16()
	if err != nil {
		return nil, err
	}

	nf := int(numFields)
	log.Debugf("Row description has %d fields", nf)
	for i := 0; i < nf; i++ {
		col := ColumnInfo{}
		col.Name, _ = m.ReadString()
		m.ReadInt32() // Table OID
		m.ReadInt16() // Attribute number
		ct, _ := m.ReadInt32()
		col.Type = PgType(ct)
		m.ReadInt16() // Type size
		m.ReadInt32() // Type modifier
		fmtCode, _ := m.ReadInt16()
		col.Binary = (fmtCode == 1)
		cols = append(cols, col)
	}

	return cols, nil
}

// ParseParameterDescription looks at a Parameter description and returns
// a list of types.
func ParseParameterDescription(m *InputMessage) ([]PgType, error) {
	if m.Type() != ParameterDescription {
		return nil, errors.New("Message type is not ParameterDescription")
	}

	var types []PgType
	numCols, err := m.ReadInt16()
	if err != nil {
		return nil, err
	}

	nf := int(numCols)
	for i := 0; i < nf; i++ {
		ct, err := m.ReadInt32()
		if err != nil {
			return nil, err
		}
		types = append(types, PgType(ct))
	}

	return types, nil
}

// ParseDataRow turns a single DataRow message to a list of buffers
func ParseDataRow(m *InputMessage) ([][]byte, error) {
	if m.Type() != DataRow {
		return nil, errors.New("Message type is not DataRow")
	}

	var fields [][]byte

	numFields, err := m.ReadInt16()
	if err != nil {
		return nil, err
	}

	nf := int(numFields)
	log.Debugf("Row has %d columns", nf)
	for i := 0; i < nf; i++ {
		len, _ := m.ReadInt32()
		if len > 0 {
			buf, _ := m.ReadBytes(int(len))
			fields = append(fields, buf)
		} else {
			fields = append(fields, nil)
		}
	}
	return fields, nil
}

// ParseCommandComplete parses the CommandComplete message and returns
// the row count that it contains
func ParseCommandComplete(m *InputMessage) (int64, error) {
	if m.Type() != CommandComplete {
		return 0, errors.New("Message type is not CommandComplete")
	}

	s, err := m.ReadString()
	if err != nil {
		return 0, err
	}
	log.Debugf("CommandComplete %s", s)

	return parseRowCount(s), nil
}

var insertCompleteRe = regexp.MustCompile("^INSERT [0-9]+ ([0-9]+)$")
var otherCompleteRe = regexp.MustCompile("^[A-Z]+ ([0-9]+)$")

func parseRowCount(completeMsg string) int64 {
	match := insertCompleteRe.FindStringSubmatch(completeMsg)
	if match == nil {
		match = otherCompleteRe.FindStringSubmatch(completeMsg)
	}
	if match == nil {
		return 0
	}

	ret, err := strconv.ParseInt(match[1], 10, 64)
	if err == nil {
		return ret
	}
	return 0
}

// ParseParameterStatus parses the ParameterStatus message
func ParseParameterStatus(m *InputMessage) (string, error) {
	if m.Type() != ParameterStatus {
		return "", errors.New("Message type is not ParameterStatus")
	}

	s, err := m.ReadString()
	if err != nil {
		return "", err
	}

	s2, err := m.ReadString()
	if err != nil {
		return "", err
	}

	return s + " " + s2, nil
}
