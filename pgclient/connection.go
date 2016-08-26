package pgclient

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"

	log "github.com/Sirupsen/logrus"
)

const (
	protocolVersion = (3 << 16)
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
Connect to the databsae. "host" must be a "host:port" pair, "user" and "database"
must contain the appropriate user name and database name, and "opts" contains
any other keys and values to send to the database.

The connect string works the same way as "psql":

postgres://[user[:password]@]hostname[:port]/[database]?param=val&param=val
*/
func Connect(connect string) (*PgConnection, error) {
	ci, err := parseConnectString(connect)
	if err != nil {
		return nil, err
	}

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

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", ci.host, ci.port))
	if err != nil {
		return nil, err
	}

	success := false
	defer func() {
		if !success {
			conn.Close()
		}
	}()

	c := &PgConnection{
		conn: conn,
	}

	err = c.WriteMessage(startup)
	if err != nil {
		return nil, err
	}

	// Loop here later for challenge-response
	authDone := false
	for !authDone {
		im, err := c.ReadMessage()
		if err != nil {
			return nil, err
		}

		if im.Type() == ErrorResponse {
			return nil, ParseError(im)
		} else if im.Type() != AuthenticationResponse {
			return nil, fmt.Errorf("Invalid response from server: %v", im.Type())
		}

		authResp, err := im.ReadInt32()
		if err != nil {
			return nil, err
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
			return nil, fmt.Errorf("Invalid authentication response: %d", authResp)
		}
	}

	// Loop to wait for "ready" status
	for {
		im, err := c.ReadMessage()
		if err != nil {
			return nil, err
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
			return nil, ParseError(im)
		case ReadyForQuery:
			// Ready for query!
			success = true
			return c, nil
		default:
			return nil, fmt.Errorf("Invalid database response %v", im.Type())
		}
	}
}

/*
Close does what you think it does.
*/
func (c *PgConnection) Close() {
	if c.conn != nil {
		tm := NewOutputMessage(Terminate)
		c.WriteMessage(tm)
		log.Debug("Closing TCP connection")
		c.conn.Close()
		c.conn = nil
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
