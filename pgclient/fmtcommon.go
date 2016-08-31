package pgclient

import (
	"bytes"
	"errors"

	log "github.com/Sirupsen/logrus"
)

/*
ParseError looks at an input message that represents an error and returns
a Go error.
*/
func ParseError(m *InputMessage) error {
	if m.Type() != 'E' {
		return errors.New("Message type is not Error")
	}
	msg, err := ParseNotice(m)
	if err == nil {
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
	return NewInputMessage(PgMessageType(typeByte), buf[1:]), nil
}

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
		col.Type, _ = m.ReadInt32()
		m.ReadInt16() // Type size
		m.ReadInt32() // Type modifier
		fmtCode, _ := m.ReadInt16()
		col.Binary = (fmtCode == 1)
		cols = append(cols, col)
	}

	return cols, nil
}

func ParseDataRow(m *InputMessage) ([]string, error) {
	if m.Type() != DataRow {
		return nil, errors.New("Message type is not DataRow")
	}

	var fields []string

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
			fields = append(fields, string(buf))
		} else {
			fields = append(fields, "")
		}
	}
	return fields, nil
}

func ParseCommandComplete(m *InputMessage) (string, error) {
	if m.Type() != CommandComplete {
		return "", errors.New("Message type is not CommandComplete")
	}

	s, err := m.ReadString()
	if err != nil {
		return "", err
	}

	log.Debugf("CommandComplete %s", s)
	return s, nil
}

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

	log.Debugf("ParameterStatus %s %s", s, s2)
	return s + " " + s2, nil
}
