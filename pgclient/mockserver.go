/*
Copyright 2017 The Transicator Authors

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
	"fmt"
	"io"
	"net"

	log "github.com/Sirupsen/logrus"
)

const (
	mockUserName     = "mock"
	mockDatabaseName = "turtle"
)

type mockState int

const (
	mockIdle mockState = 1
)

/*
A MockServer is a server that implements much of the Postgres wire protocol.
It is used for unit testing of the postgres client, especially where
we are testing the many different SSL connection options.
*/
type MockServer struct {
	listener net.Listener
}

/*
NewMockServer starts a new server in the current process, listening on the
specified port.
*/
func NewMockServer(port int) (s *MockServer, err error) {
	var listener net.Listener
	listener, err = net.ListenTCP("tcp", &net.TCPAddr{
		Port: port,
	})
	if err != nil {
		return
	}

	s = &MockServer{
		listener: listener,
	}
	go s.acceptLoop()

	return
}

/*
Address returns the listen address in host:port format.
*/
func (m *MockServer) Address() string {
	return m.listener.Addr().String()
}

/*
Stop stops the server listening for new connections.
*/
func (m *MockServer) Stop() {
	m.listener.Close()
}

func (m *MockServer) acceptLoop() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			return
		}
		go m.connectLoop(conn)
	}
}

func (m *MockServer) connectLoop(c net.Conn) {
	defer c.Close()

	startup, err := readMockMessage(c, true)
	if err != nil {
		log.Errorf("Error reading startup message: %s\n", err)
		return
	}

	protoVersion, err := startup.ReadInt32()
	if err != nil {
		log.Error("Can't read protocol version")
		return
	}
	if protoVersion != protocolVersion {
		sendError(c, fmt.Sprintf("Invalid read protocol version %d\n", protoVersion))
		return
	}

	var paramName, paramVal string
	for {
		paramName, err = startup.ReadString()
		if err != nil {
			return
		}
		if paramName == "" {
			break
		}
		paramVal, err = startup.ReadString()
		if err != nil {
			return
		}

		if paramName == "user" {
			if paramVal != mockUserName {
				sendError(c, fmt.Sprintf("Invalid user name %s", paramVal))
				return
			}
		}
		if paramName == "database" {
			if paramVal != mockDatabaseName {
				sendError(c, fmt.Sprintf("Invalid database name %s\n", paramVal))
				return
			}
		}
	}

	out := NewOutputMessage(AuthenticationResponse)
	out.WriteInt32(0)
	c.Write(out.Encode())
	sendReady(c)

	m.readLoop(c)
}

func (m *MockServer) readLoop(c net.Conn) {
	state := mockIdle

	for {
		msg, err := readMockMessage(c, false)
		if err != nil {
			return
		}

		switch state {
		case mockIdle:
			m.readIdle(c, msg)
		}
	}
}

func (m *MockServer) readIdle(c net.Conn, msg *InputMessage) {
	switch msg.Type() {
	case Query:
		sendError(c, "Invalid SQL")
		sendReady(c)
	default:
		sendError(c, fmt.Sprintf("Invalid message %s", msg.Type()))
		sendReady(c)
	}
}

func readMockMessage(c net.Conn, isStartup bool) (msg *InputMessage, err error) {
	var hdr []byte
	if isStartup {
		hdr = make([]byte, 4)
	} else {
		hdr = make([]byte, 5)
	}

	_, err = io.ReadFull(c, hdr)
	if err != nil {
		return
	}

	hdrBuf := bytes.NewBuffer(hdr)
	var msgType PgMessageType

	if !isStartup {
		var msgTypeVal byte
		msgTypeVal, err = hdrBuf.ReadByte()
		if err != nil {
			return
		}
		msgType = PgMessageType(msgTypeVal)
	}

	var msgLen int32
	err = binary.Read(hdrBuf, networkByteOrder, &msgLen)
	if err != nil {
		return
	}

	if msgLen < 4 {
		err = fmt.Errorf("Invalid message length %d", msgLen)
		return
	}

	bodBuf := make([]byte, msgLen-4)
	_, err = io.ReadFull(c, bodBuf)
	if err != nil {
		return
	}

	msg = NewInputMessage(msgType, bodBuf)
	return
}

func sendError(c net.Conn, msg string) {
	out := NewOutputMessage(ErrorResponse)
	out.WriteByte('S')
	out.WriteString("FATAL")
	out.WriteByte('M')
	out.WriteString(msg)
	c.Write(out.Encode())
}

func sendReady(c net.Conn) {
	out := NewOutputMessage(ReadyForQuery)
	out.WriteByte('I')
	c.Write(out.Encode())
}
