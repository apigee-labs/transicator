/*
Copyright 2016 Google Inc.

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
	msgType PgOutputType
	hasType bool
}

/*
NewOutputMessage constructs a new message with the given type byte
*/
func NewOutputMessage(msgType PgOutputType) *OutputMessage {
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
func (m *OutputMessage) Type() PgOutputType {
	return m.msgType
}

/*
WriteInt64 writes a single "int64" to the output.
*/
func (m *OutputMessage) WriteInt64(i int64) {
	err := binary.Write(m.buf, networkByteOrder, &i)
	if err != nil {
		panic(err.Error())
	}
}

/*
WriteUint64 writes a single "uint64" to the output.
*/
func (m *OutputMessage) WriteUint64(i uint64) {
	err := binary.Write(m.buf, networkByteOrder, &i)
	if err != nil {
		panic(err.Error())
	}
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
WriteInt16 writes a single "int16" to the output.
*/
func (m *OutputMessage) WriteInt16(i int16) {
	err := binary.Write(m.buf, networkByteOrder, &i)
	if err != nil {
		panic(err.Error())
	}
}

/*
WriteByte writes a single byte to the output.
*/
func (m *OutputMessage) WriteByte(b byte) error {
	return m.buf.WriteByte(b)
}

/*
WriteBytes writes a bunch of bytes to the output.
*/
func (m *OutputMessage) WriteBytes(b []byte) error {
	_, err := m.buf.Write(b)
	return err
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
		hdr.WriteByte(byte(m.msgType))
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
	msgType PgInputType
}

/*
NewInputMessage generates a new input message from the specified byte array,
which must be the correct length for the message.
*/
func NewInputMessage(msgType PgInputType, b []byte) *InputMessage {
	return &InputMessage{
		buf:     bytes.NewBuffer(b),
		msgType: msgType,
	}
}

/*
Type returns the message type byte from the message that was passed in to
the "NewInputMessage" function.
*/
func (m *InputMessage) Type() PgInputType {
	return m.msgType
}

/*
ReadInt64 reads a single int64 from the message and returns it.
*/
func (m *InputMessage) ReadInt64() (int64, error) {
	var i int64
	err := binary.Read(m.buf, networkByteOrder, &i)
	if err == nil {
		return i, nil
	}
	return 0, err
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
ReadInt8 reads a single int8 from the message and returns it.
*/
func (m *InputMessage) ReadInt8() (int8, error) {
	var i int8
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

/*
ReadRemaining reads everything that is left in the message.
*/
func (m *InputMessage) ReadRemaining() []byte {
	return m.buf.Bytes()
}
