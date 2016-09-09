package pgclient

import (
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"

	log "github.com/Sirupsen/logrus"
)

const (
	// Postgres protocol version. We only support the latest as of 9.5.
	protocolVersion = (3 << 16)
	// Magic number needed to initate SSL stuff
	sslMagicNumber = 80877103
)

/*
PgMessageType is the one-byte type of the postgres message.
*/
type PgMessageType byte

// Various types of messages that represent one-byte message types.
const (
	AuthenticationResponse PgMessageType = 'R'
	BackEndKeyData                       = 'K'
	ParameterStatus                      = 'S'
	NoticeResponse                       = 'N'
	ErrorResponse                        = 'E'
	Query                                = 'Q'
	ReadyForQuery                        = 'Z'
	CommandComplete                      = 'C'
	CopyInResponse                       = 'G'
	CopyOutResponse                      = 'H'
	CopyBothResponse                     = 'W'
	RowDescription                       = 'T'
	DataRow                              = 'D'
	EmptyQueryResponse                   = 'I'
	Terminate                            = 'X'
	CopyData                             = 'd'
	CopyDone                             = 'c'
	WALData                              = 'w'
	SenderKeepalive                      = 'k'
	StandbyStatusUpdate                  = 'r'
	HotStandbyFeedback                   = 'h'
	PasswordMessage                      = 'p'
)

func (t PgMessageType) String() string {
	// Unfortunately "stringer" doesn't seem to be able to generate this
	switch t {
	case AuthenticationResponse:
		return "AuthenticationResponse"
	case BackEndKeyData:
		return "BackEndKeyData"
	case ParameterStatus:
		return "ParameterStatus"
	case NoticeResponse:
		return "NoticeResponse"
	case ErrorResponse:
		return "ErrorResponse"
	case Query:
		return "Query"
	case ReadyForQuery:
		return "ReadyForQuery"
	case CommandComplete:
		return "CommandComplete"
	case CopyInResponse:
		return "CopyInResponse"
	case CopyOutResponse:
		return "CopyOutResponse"
	case CopyBothResponse:
		return "CopyBothResponse"
	case CopyDone:
		return "CopyDone"
	case RowDescription:
		return "RowDescription"
	case DataRow:
		return "DataRow"
	case EmptyQueryResponse:
		return "EmptyQueryResponse"
	case Terminate:
		return "Terminate"
	case CopyData:
		return "CopyData"
	case WALData:
		return "WALData"
	case SenderKeepalive:
		return "SenderKeepalive"
	case StandbyStatusUpdate:
		return "StandbyStatusUpdate"
	case HotStandbyFeedback:
		return "HotStandbyFeedback"
	case PasswordMessage:
		return "PasswordMessage"
	default:
		return fmt.Sprintf("PgMessageType(%d)", t)
	}
}

/*
A PgConnection represents a connection to the database.
*/
type PgConnection struct {
	conn net.Conn
}

/*
Connect to the database. "host" must be a "host:port" pair, "user" and "database"
must contain the appropriate user name and database name, and "opts" contains
any other keys and values to send to the database.

The connect string works the same way as "psql," or the JDBC driver:

postgres://[user[:password]@]hostname[:port]/[database]?ssl=[true|false]&param=val&param=val
*/
func Connect(connect string) (*PgConnection, error) {
	ci, err := parseConnectString(connect)
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", ci.host, ci.port))
	if err != nil {
		return nil, err
	}
	c := &PgConnection{
		conn: conn,
	}

	success := false
	defer func() {
		if !success {
			conn.Close()
		}
	}()

	if ci.ssl {
		err = c.startSSL(ci)
		if err != nil {
			return nil, err
		}
	}

	// Send "Startup" message
	err = c.sendStartup(ci)
	if err != nil {
		return nil, err
	}

	// Loop here later for challenge-response
	err = c.authenticationLoop(ci)
	if err != nil {
		return nil, err
	}

	// Finish up with receiving parameters and all that
	err = c.finishConnect(ci)
	if err == nil {
		success = true
	}
	return c, err
}

func (c *PgConnection) startSSL(ci *connectInfo) error {
	log.Debug("Starting SSL on connection")
	sslStartup := NewStartupMessage()
	sslStartup.WriteInt32(sslMagicNumber)

	err := c.WriteMessage(sslStartup)
	if err != nil {
		return err
	}

	// Just read one byte to see if we support SSL:
	sslStatus := make([]byte, 1)
	_, err = io.ReadFull(c.conn, sslStatus)
	if err != nil {
		log.Debug("Error on read")
		return err
	}

	log.Debugf("Got back %v", sslStatus[0])
	switch sslStatus[0] {
	case 'S':
		return c.sslHandshake(ci)
	case 'N':
		return errors.New("Server does not support SSL")
	default:
		return fmt.Errorf("Invalid SSL handshake from server: %v", sslStatus[0])
	}
}

func (c *PgConnection) sslHandshake(ci *connectInfo) error {
	// Always do TLS without verifying the server
	tlsConfig := &tls.Config{
		ServerName:         ci.host,
		InsecureSkipVerify: true,
	}

	tlsConn := tls.Client(c.conn, tlsConfig)
	err := tlsConn.Handshake()
	if err != nil {
		return err
	}

	cs := tlsConn.ConnectionState()
	log.Debugf("TLS state: %v", cs)
	//log.Debugf("TLS status: handshake = %v version = %d cipher = %d protocol = %s",
	//	cs.HandshakeComplete, cs.Version, cs.CipherSuite, cs.NegotiatedProtocol)
	c.conn = tlsConn
	return nil
}

func (c *PgConnection) sendStartup(ci *connectInfo) error {
	startup := NewStartupMessage()
	startup.WriteInt32(protocolVersion)
	startup.WriteString("user")
	startup.WriteString(ci.user)
	startup.WriteString("database")
	startup.WriteString(ci.database)

	for k, v := range ci.options {
		startup.WriteString(k)
		startup.WriteString(v)
	}

	startup.WriteString("")

	return c.WriteMessage(startup)
}

func (c *PgConnection) authenticationLoop(ci *connectInfo) error {
	authDone := false
	for !authDone {
		im, err := c.ReadMessage()
		if err != nil {
			return err
		}

		if im.Type() == ErrorResponse {
			return ParseError(im)
		} else if im.Type() != AuthenticationResponse {
			return fmt.Errorf("Invalid response from server: %v", im.Type())
		}

		authResp, err := im.ReadInt32()
		if err != nil {
			return err
		}
		switch authResp {
		case 0:
			authDone = true
		case 3:
			pm := NewOutputMessage(PasswordMessage)
			pm.WriteString(ci.creds)
			c.WriteMessage(pm)
		case 5:
			salt, _ := im.ReadBytes(4)
			pm := NewOutputMessage(PasswordMessage)
			pm.WriteString(passwordMD5(ci.user, ci.creds, salt))
			c.WriteMessage(pm)
		default:
			return fmt.Errorf("Invalid authentication response: %d", authResp)
		}
	}
	return nil
}

func (c *PgConnection) finishConnect(ci *connectInfo) error {
	// Loop to wait for "ready" status
	for {
		im, err := c.ReadMessage()
		if err != nil {
			return err
		}

		switch im.Type() {
		case BackEndKeyData:
			// Back end key data -- ignore
		case ParameterStatus:
			// Database parameter -- no need for now
			//paramName, _ := im.ReadString()
			//paramVal, _ := im.ReadString()
			//log.Debugf("%s = %s", paramName, paramVal)
		case NoticeResponse:
			msg, _ := ParseNotice(im)
			log.Info(msg)
		case ErrorResponse:
			return ParseError(im)
		case ReadyForQuery:
			// Ready for query!
			return nil
		default:
			return fmt.Errorf("Invalid database response %v", im.Type())
		}
	}
}

/*
Close does what you think it does.
*/
func (c *PgConnection) Close() {
	if c.conn != nil {
		tm := NewOutputMessage(Terminate)
		// Ignore error because what would we do if we couldn't send?
		c.WriteMessage(tm)
		log.Debug("Closing TCP connection")
		c.conn.Close()
	}
}

/*
WriteMessage sends the specified message to the server, and does not wait
to see what comes back.
*/
func (c *PgConnection) WriteMessage(m *OutputMessage) error {
	buf := m.Encode()
	log.Debugf("Sending message type %s length %d", m.Type(), len(buf))
	_, err := c.conn.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

/*
ReadMessage reads a single message from the socket, and decodes its type byte
and length.
*/
func (c *PgConnection) ReadMessage() (*InputMessage, error) {
	hdr := make([]byte, 5)
	_, err := io.ReadFull(c.conn, hdr)
	if err != nil {
		return nil, err
	}

	hdrBuf := bytes.NewBuffer(hdr)
	msgTypeVal, err := hdrBuf.ReadByte()
	if err != nil {
		return nil, err
	}
	msgType := PgMessageType(msgTypeVal)

	var msgLen int32
	err = binary.Read(hdrBuf, networkByteOrder, &msgLen)
	if err != nil {
		return nil, err
	}
	//log.Debugf("Got message type %s length %d", msgType, msgLen)

	if msgLen < 4 {
		return nil, fmt.Errorf("Invalid message length %d", msgLen)
	}

	bodBuf := make([]byte, msgLen-4)
	_, err = io.ReadFull(c.conn, bodBuf)
	if err != nil {
		return nil, err
	}

	im := NewInputMessage(msgType, bodBuf)
	return im, nil
}

/*
passwordMD5 generates an MD5 password using the same algorithm
as Postgres.
*/
func passwordMD5(user, pass string, salt []byte) string {
	up := user + pass
	md1 := md5.Sum([]byte(up))
	md1S := hex.EncodeToString(md1[:]) + string(salt)
	md2 := md5.Sum([]byte(md1S))
	return "md5" + hex.EncodeToString(md2[:])
}
