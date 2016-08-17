package pgclient

import (
	"bytes"
	"encoding/binary"
)

var networkByteOrder = binary.BigEndian

/*
An OutputMessage represents a single message that is going to be sent to
the Postgres server. It will be prepended with a type byte and a four-byte
length.
*/
type OutputMessage struct {
	buf     *bytes.Buffer
	msgType byte
	hasType bool
}

/*
NewOutputMessage constructs a new message with the given type byte
*/
func NewOutputMessage(msgType byte) *OutputMessage {
	return &OutputMessage{
		buf:     &bytes.Buffer{},
		msgType: msgType,
		hasType: true,
	}
}

/*
NewStartupMessage constructs a startup message, which has no type byte
*/
func NewStartupMessage() *OutputMessage {
	return &OutputMessage{
		buf:     &bytes.Buffer{},
		hasType: false,
	}
}

/*
Type returns the message type byte from the message that was passed in to
the "NewOutputMessage" function.
*/
func (m *OutputMessage) Type() byte {
	return m.msgType
}

/*
WriteInt32 writes a single "int32" to the output.
*/
func (m *OutputMessage) WriteInt32(i int32) {
	err := binary.Write(m.buf, networkByteOrder, &i)
	if err != nil {
		panic(err.Error())
	}
}

/*
WriteString writes a single "string" to the output.
*/
func (m *OutputMessage) WriteString(s string) {
	_, err := m.buf.WriteString(s)
	if err != nil {
		panic(err.Error())
	}
	err = m.buf.WriteByte(0)
	if err != nil {
		panic(err.Error())
	}
}

/*
Encode returns a byte slice that represents the entire message, including
the header byte and length.
*/
func (m *OutputMessage) Encode() []byte {
	var hdr *bytes.Buffer

	if m.hasType {
		hdr = bytes.NewBuffer(make([]byte, 5))
		hdr.Reset()
		hdr.WriteByte(m.msgType)
	} else {
		hdr = bytes.NewBuffer(make([]byte, 4))
		hdr.Reset()
	}
	bufLen := int32(m.buf.Len() + 4)
	binary.Write(hdr, networkByteOrder, &bufLen)

	return append(hdr.Bytes(), m.buf.Bytes()...)
}

/*
An InputMessage represents a message read from the server. It's understood
that we already read the type byte and also the four-byte length, and that
we are being given a slice to the data of the appropriate length for the
message.
*/
type InputMessage struct {
	buf     *bytes.Buffer
	msgType byte
}

/*
NewInputMessage generates a new input message from the specified byte array,
which must be the correct length for the message.
*/
func NewInputMessage(msgType byte, b []byte) *InputMessage {
	return &InputMessage{
		buf:     bytes.NewBuffer(b),
		msgType: msgType,
	}
}

/*
Type returns the message type byte from the message that was passed in to
the "NewInputMessage" function.
*/
func (m *InputMessage) Type() byte {
	return m.msgType
}

/*
ReadInt32 reads a single int32 from the message and returns it.
*/
func (m *InputMessage) ReadInt32() (int32, error) {
	var i int32
	err := binary.Read(m.buf, networkByteOrder, &i)
	if err == nil {
		return i, nil
	}
	return 0, err
}

/*
ReadInt16 reads a single int16 from the message and returns it.
*/
func (m *InputMessage) ReadInt16() (int16, error) {
	var i int16
	err := binary.Read(m.buf, networkByteOrder, &i)
	if err == nil {
		return i, nil
	}
	return 0, err
}

/*
ReadString reads a single null-terminated string form the message and
returns it.
*/
func (m *InputMessage) ReadString() (string, error) {
	asc, err := m.buf.ReadBytes(0)
	if err != nil {
		return "", err
	}
	return string(asc[:len(asc)-1]), nil
}

/*
ReadByte reads a single byte.
*/
func (m *InputMessage) ReadByte() (byte, error) {
	b, err := m.buf.ReadByte()
	if err != nil {
		return 0, err
	}
	return b, nil
}

/*
ReadBytes reads a count of bytes.
*/
func (m *InputMessage) ReadBytes(n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := m.buf.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}
