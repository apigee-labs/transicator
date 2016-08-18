package replication

import (
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/apigee-internal/transicator/pgclient"
)

const (
	// January 1, 2000, midnight UTC, in microseconds from Unix epoch
	epoch2000 = 946717749000000000
)

/*
A Change represents a single change that has been replicated from the server.
*/
type Change struct {
	LSN   int64
	Data  string
	Error error
}

/*
A Replicator is a client for the logical replication protocol.
*/
type Replicator struct {
	conn       *pgclient.PgConnection
	slotName   string
	changeChan chan Change
	updateChan chan int64
	stopChan   chan bool
}

/*
Start replication and return a new Replicator object that may be used to
read it. The fourth argument is the name of the replication slot to create
on the server.
*/
func Start(host, user, database, sn string) (*Replicator, error) {
	slotName := strings.ToLower(sn)
	conn, err := pgclient.Connect(host, user, database,
		map[string]string{"replication": "database"})
	if err != nil {
		return nil, err
	}

	repl := &Replicator{
		conn:       conn,
		slotName:   slotName,
		changeChan: make(chan Change, 100),
		updateChan: make(chan int64, 100),
		stopChan:   make(chan bool, 1),
	}

	log.Debug("Starting replication...")
	err = repl.connect()
	if err != nil {
		conn.Close()
		return nil, err
	}
	log.Debug("Replication started.")

	// Start the main loop that will listen for commands and stop requests
	go repl.replLoop()
	// And start another loop that will read from the socket
	go repl.readLoop()

	return repl, nil
}

/*
Changes returns a channel that can be used to wait for changes. If an error
is returned, then no more changes will be forthcoming.
*/
func (r *Replicator) Changes() <-chan Change {
	return r.changeChan
}

/*
Stop stops the replication process and closes the channel.
*/
func (r *Replicator) Stop() {
	r.stopChan <- true
}

/*
Acknowledge acknowledges to the server that we have committed a change.
Acknowledged changes will not be re-delivered via the replication
protocol. Postgres LSN order and logical decoding order are not the
same. It is important to periodically acknowledge LSNs, but it is also
important that we do not do so until we are sure that all previous LSNs
are stable.
*/
func (r *Replicator) Acknowledge(lsn int64) {
	r.updateChan <- lsn
}

/*
connect to the database and either get replication started, or
return an error.
*/
func (r *Replicator) connect() error {
	slotCreated := false
	startMsg := pgclient.NewOutputMessage(pgclient.Query)
	startMsg.WriteString(fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL 0/0", r.slotName))
	err := r.conn.WriteMessage(startMsg)
	if err != nil {
		return err
	}

	// Read until we get a CopyBothResponse
	for {
		m, err := r.conn.ReadMessage()
		if err != nil {
			return err
		}

		switch m.Type() {
		case pgclient.ErrorResponse:
			if !slotCreated {
				// First error might because slot is not created, so create it.
				consumeTillReady(r.conn)

				log.Debugf("Creating new replication slot %s", r.slotName)
				_, _, err = r.conn.SimpleQuery(fmt.Sprintf(
					"CREATE_REPLICATION_SLOT %s LOGICAL transicator_output", r.slotName))
				if err != nil {
					return err
				}
				slotCreated = true

				// Re-send start replication command.
				err := r.conn.WriteMessage(startMsg)
				if err != nil {
					return err
				}
			} else {
				return pgclient.ParseError(m)
			}

		case pgclient.NoticeResponse:
			msg, _ := pgclient.ParseNotice(m)
			log.Infof("Info from server: %s", msg)

		case pgclient.CopyBothResponse:
			// We'll use this as a signal to move on
			parseCopyBoth(m)
			return nil

		default:
			return fmt.Errorf("Unknown message from server: %s", m.Type())
		}
	}
}

/*
This channel will read data from the clients and write to the server.
*/
func (r *Replicator) replLoop() {
	for {
		select {
		case newLSN := <-r.updateChan:
			// Send an update to the server
			r.updateLSN(newLSN)
		case <-r.stopChan:
			// This will send a terminate and also stop
			log.Debug("Stopping replication")
			r.conn.Close()
			return
		}
	}
}

/*
This channel will read from the server and hand out what it gets.
*/
func (r *Replicator) readLoop() {
	log.Debug("Starting to read from replication connection")
	shouldClose := false
	for !shouldClose {
		m, err := r.conn.ReadMessage()
		if err != nil {
			log.Warningf("Error reading from server: %s", err)
			errChange := Change{
				Error: err,
			}
			r.changeChan <- errChange
			shouldClose = true
			break
		}

		switch m.Type() {
		case pgclient.NoticeResponse:
			msg, _ := pgclient.ParseNotice(m)
			log.Infof("Info from server: %s", msg)

		case pgclient.ErrorResponse:
			err = pgclient.ParseError(m)
			log.Warningf("Server returned an error: %s", err)
			errChange := Change{
				Error: err,
			}
			r.changeChan <- errChange
			shouldClose = true

		case pgclient.CopyData:
			cm, err := pgclient.ParseCopyData(m)
			if err != nil {
				log.Warningf("Received invalid CopyData message: %s", err)
			} else {
				r.handleCopyData(cm)
			}

		default:
			log.Warningf("Server received an unknown message %s", m.Type())
		}
	}

	r.conn.Close()
}

func parseCopyBoth(m *pgclient.InputMessage) {
	isBinary, _ := m.ReadInt8()
	log.Debugf("Is binary = %d", isBinary)
	nfi, _ := m.ReadInt16()
	numFields := int(nfi)

	for i := 0; i < numFields; i++ {
		isBinary, _ := m.ReadInt16()
		log.Debugf("Column %d: binary = %d", i, isBinary)
	}
}

func (r *Replicator) handleCopyData(m *pgclient.InputMessage) {
	switch m.Type() {
	case pgclient.WALData:
		r.handleWALData(m)
	case pgclient.SenderKeepalive:
		r.handleKeepalive(m)
	default:
		log.Warningf("Received unknown WAL message %s", m.Type())
	}
}

func (r *Replicator) handleWALData(m *pgclient.InputMessage) {
	m.ReadInt64() // StartWAL
	endWAL, _ := m.ReadInt64()
	m.ReadInt64() // Timestamp
	buf := m.ReadRemaining()

	c := Change{
		LSN:  endWAL,
		Data: string(buf),
	}

	r.changeChan <- c
}

func (r *Replicator) handleKeepalive(m *pgclient.InputMessage) {
	m.ReadInt64() // end WAL
	m.ReadInt64() // Timestamp
	replyNow, _ := m.ReadByte()
	log.Infof("Reply now? %d", replyNow)
	// TODO send a message to the other channel to do something
}

func (r *Replicator) updateLSN(lsn int64) {
	om := pgclient.NewOutputMessage(pgclient.CopyData)
	om.WriteByte(pgclient.StandbyStatusUpdate)
	om.WriteInt64(lsn)                               // last written to disk
	om.WriteInt64(lsn)                               // last flushed to disk
	om.WriteInt64(lsn)                               // last applied
	om.WriteInt64(time.Now().UnixNano() - epoch2000) // Timestamp!
	om.WriteByte(0)

	r.conn.WriteMessage(om)
}

func consumeTillReady(c *pgclient.PgConnection) error {
	for {
		m, err := c.ReadMessage()
		if err != nil {
			return err
		}

		if m.Type() == pgclient.ReadyForQuery {
			return nil
		}
	}
}
