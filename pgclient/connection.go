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
	protocolVersion = (3 << 16)
)

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
*/
func Connect(host, user, database string, opts map[string]string) (*PgConnection, error) {
	startup := NewStartupMessage()
	startup.WriteInt32(protocolVersion)
	startup.WriteString("user")
	startup.WriteString(user)
	startup.WriteString("database")
	startup.WriteString(database)

	for k, v := range opts {
		startup.WriteString(k)
		startup.WriteString(v)
	}

	startup.WriteString("")

	conn, err := net.Dial("tcp", host)
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

	err = c.writeMessage(startup)
	if err != nil {
		return nil, err
	}

	// Loop here later for challenge-response
	authDone := false
	for !authDone {
		im, err := c.readMessage()
		if err != nil {
			return nil, err
		}
		if im.Type() != 'R' {
			return nil, fmt.Errorf("Invalid response from server: %v", im.Type())
		}

		authResp, err := im.ReadInt32()
		if err != nil {
			return nil, err
		}
		switch authResp {
		case 0:
			authDone = true
		default:
			return nil, fmt.Errorf("Invalid authentication response: %d", authResp)
		}
	}

	// Loop to wait for "ready" status
	for {
		im, err := c.readMessage()
		if err != nil {
			return nil, err
		}

		switch im.Type() {
		case 'K':
			// Back end key data -- ignore
		case 'S':
			// Parameter status -- ignore
		case 'N':
			msg, _ := parseNotice(im)
			log.Info(msg)
		case 'E':
			return nil, parseError(im)
		case 'Z':
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
		log.Debug("Closing TCP connection")
		c.conn.Close()
	}
}

func (c *PgConnection) writeMessage(m *OutputMessage) error {
	buf := m.Encode()
	log.Debugf("Sending message type %v length %d", m.Type(), len(buf))
	_, err := c.conn.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (c *PgConnection) readMessage() (*InputMessage, error) {
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
	msgType := byte(msgTypeVal)

	var msgLen int32
	err = binary.Read(hdrBuf, networkByteOrder, &msgLen)
	if err != nil {
		return nil, err
	}
	log.Debugf("Got message type %v length %d", msgType, msgLen)

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
