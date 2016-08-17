package pgclient

import (
	"bytes"
	"errors"
)

func parseError(m *InputMessage) error {
	if m.Type() != 'E' {
		return errors.New("Message type is not Error")
	}
	msg, err := parseNotice(m)
	if err == nil {
		return errors.New(msg)
	}
	return err
}

func parseNotice(m *InputMessage) (string, error) {
	if m.Type() != 'N' && m.Type() != 'E' {
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

func parseRowDescription(m *InputMessage) ([]ColumnInfo, error) {
	if m.Type() != 'T' {
		return nil, errors.New("Message type is not Row Description")
	}

	var cols []ColumnInfo
	numFields, err := m.ReadInt16()
	if err != nil {
		return nil, err
	}

	nf := int(numFields)
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

func parseDataRow(m *InputMessage) ([]string, error) {
	if m.Type() != 'D' {
		return nil, errors.New("Message type is not Row Description")
	}

	var fields []string

	numFields, err := m.ReadInt16()
	if err != nil {
		return nil, err
	}

	nf := int(numFields)
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
