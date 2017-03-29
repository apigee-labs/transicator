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
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"regexp"

	log "github.com/Sirupsen/logrus"
)

const (
	mockUserName     = "mock"
	mockPassword     = "mocketty"
	mockDatabaseName = "turtle"
)

type mockState int

const (
	mockIdle mockState = 1
)

// MockAuth specifies what type of authentication the server supports
type MockAuth int

// Different auth types
const (
	MockTrust MockAuth = 0
	MockClear MockAuth = 1
	MockMD5   MockAuth = 2
)

var insertRE = regexp.MustCompile("insert into mock values \\('([\\w]+)', '([\\w]+)'\\)")

/*
A MockServer is a server that implements a little bit of the Postgres wire
protocol. We can use it for testing of the wire protocol client. In particular,
we can use it to test the myriad of password authentication and TLS options
without having to start and stop a real Postgres server in the test suite.
*/
type MockServer struct {
	listener  net.Listener
	mockTable map[string]string
	authType  MockAuth
	tlsConfig *tls.Config
	forceTLS  bool
}

/*
NewMockServer starts a new server in the current process, listening on the
specified port.
*/
func NewMockServer() *MockServer {
	return &MockServer{
		mockTable: make(map[string]string),
		authType:  MockTrust,
	}
}

/*
SetAuthType sets what kind of password authentication to require
*/
func (m *MockServer) SetAuthType(auth MockAuth) {
	m.authType = auth
}

/*
SetTLSInfo sets the cert and key file and makes it possible for the server
to support TLS.
*/
func (m *MockServer) SetTLSInfo(certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	m.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	return nil
}

/*
SetForceTLS sets up the server to reject any non-TLS clients.
*/
func (m *MockServer) SetForceTLS() {
	m.forceTLS = true
}

/*
Start listening for stuff.
*/
func (m *MockServer) Start(port int) (err error) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{
		Port: port,
	})

	if err != nil {
		return err
	}

	m.listener = listener
	go m.acceptLoop()
	return nil
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
	if m.listener != nil {
		m.listener.Close()
	}
}

/*
acceptLoop sits and accepts new connections.
*/
func (m *MockServer) acceptLoop() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			return
		}
		go m.connectLoop(conn)
	}
}

/*
connectLoop responds to a new connection by handling the Postgres
authentication and startup protocol.
*/
func (m *MockServer) connectLoop(c net.Conn) {
	defer c.Close()

	startup, err := readMockMessage(c, true)
	if err != nil {
		log.Errorf("Error reading startup message: %s\n", err)
		return
	}

	protoVersion, _ := startup.ReadInt32()

	if protoVersion == sslMagicNumber {
		// SSL startup attempt. Respond with "S" or "N"
		if m.tlsConfig == nil {
			c.Write([]byte{'N'})
		} else {
			// Respond with the appropriate byte and upgrade to TLS
			c.Write([]byte{'S'})
			c = tls.Server(c, m.tlsConfig)
		}

		// Look for a new startup packet now.
		startup, err = readMockMessage(c, true)
		if err != nil {
			// This might happen if TLS handshake failed, which is a valid test case
			return
		}
		protoVersion, _ = startup.ReadInt32()
	} else if m.forceTLS {
		// We will just close any connection that doesn't ask for TLS
		return
	}

	if protoVersion != protocolVersion {
		sendError(c, fmt.Sprintf("Invalid read protocol version %d\n", protoVersion))
		return
	}

	var paramName, paramVal string
	for {
		paramName, _ = startup.ReadString()
		if paramName == "" {
			break
		}
		paramVal, _ = startup.ReadString()

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

	authOK := m.authLoop(c)

	if authOK {
		sendAuthResponse(c, 0)
		sendReady(c)
		m.readLoop(c)
	}
}

/*
authLoop runs the various Postgres authentication options.
*/
func (m *MockServer) authLoop(c net.Conn) bool {
	switch m.authType {
	case MockTrust:
		return true

	case MockClear:
		sendAuthResponse(c, 3)
		return m.readPassword(c, mockPassword)

	case MockMD5:
		salt := make([]byte, 4)
		rand.Read(salt)
		out := NewServerOutputMessage(AuthenticationResponse)
		out.WriteInt32(5)
		out.WriteBytes(salt)
		c.Write(out.Encode())
		return m.readPassword(c, passwordMD5(mockUserName, mockPassword, salt))

	default:
		return false
	}
}

func (m *MockServer) readPassword(c net.Conn, expected string) bool {
	msg, _ := readMockMessage(c, false)
	if msg.ServerType() != PasswordMessage {
		sendError(c, "Expected PasswordMessage")
		return false
	}

	pwd, _ := msg.ReadString()
	if pwd == expected {
		return true
	}
	sendError(c, "Invalid password")
	return false
}

/*
readLoop now reads and parses SQL commands until it's time to shut the
connection down.
*/
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
	switch msg.ServerType() {
	case Query:
		sql, _ := msg.ReadString()
		match := insertRE.FindStringSubmatch(sql)
		if match != nil {
			m.mockTable[match[1]] = match[2]
			out := NewServerOutputMessage(CommandComplete)
			out.WriteString("INSERT 1")
			c.Write(out.Encode())
			sendReady(c)

		} else {
			sendError(c, fmt.Sprintf("Invalid SQL \"%s\"", sql))
			sendReady(c)
		}

	default:
		sendError(c, fmt.Sprintf("Invalid message %s", msg.ServerType()))
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
	var msgType PgOutputType

	if !isStartup {
		var msgTypeVal byte
		msgTypeVal, err = hdrBuf.ReadByte()
		if err != nil {
			return
		}
		msgType = PgOutputType(msgTypeVal)
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

	msg = NewServerInputMessage(msgType, bodBuf)
	return
}

func sendError(c net.Conn, msg string) {
	out := NewServerOutputMessage(ErrorResponse)
	out.WriteByte('S')
	out.WriteString("FATAL")
	out.WriteByte('M')
	out.WriteString(msg)
	out.WriteByte(0)
	c.Write(out.Encode())
}

func sendAuthResponse(c net.Conn, code int32) {
	out := NewServerOutputMessage(AuthenticationResponse)
	out.WriteInt32(code)
	c.Write(out.Encode())
}

func sendReady(c net.Conn) {
	out := NewServerOutputMessage(ReadyForQuery)
	out.WriteByte('I')
	c.Write(out.Encode())
}
