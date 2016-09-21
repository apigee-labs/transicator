package replication

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/30x/transicator/common"
	"github.com/30x/transicator/pgclient"
	log "github.com/Sirupsen/logrus"
)

const (
	// January 1, 2000, midnight UTC, in microseconds from Unix epoch
	epoch2000 = 946717749000000000
	// Make sure that we reply to the server at least this often
	heartbeatTimeout = 10 * time.Second
	// Make sure that we always reconnect eventually
	maxReconnectDelay     = time.Minute
	initialReconnectDelay = 100 * time.Millisecond
)

type replCommand int

const (
	stopCmd replCommand = iota
	disconnectedCmd
	shutdownCmd
	acknowedgeCmd
)

//go:generate stringer -type State replication.go

/*
State represents whether the replicator is connected or reconnecting.
*/
type State int32

// Values of replication state
const (
	Connecting State = iota
	Running
	Stopped
)

/*
A Replicator is a client for the logical replication protocol.
*/
type Replicator struct {
	slotName      string
	connectString string
	state         int32
	changeChan    chan *common.Change
	updateChan    chan int64
	cmdChan       chan replCommand
}

/*
Start replication and return a new Replicator object that may be used to
read it. "connect" is a postgres URL to be passed to the "pgclient" module.
"sn" is the name of the replication slot to read from. This slot will be
created if it does not already exist.
*/
func Start(connect, sn string) (*Replicator, error) {
	slotName := strings.ToLower(sn)
	connectString := connect + "?replication=database"

	repl := &Replicator{
		slotName:      slotName,
		connectString: connectString,
		state:         int32(Connecting),
		changeChan:    make(chan *common.Change, 100),
		updateChan:    make(chan int64, 100),
		cmdChan:       make(chan replCommand, 1),
	}

	// The main loop will handle connecting and all events.
	go repl.replLoop()

	return repl, nil
}

/*
DropSlot deletes the logical replication slot created by "Start".
"connect" is a postgres URL to be passed to the "pgclient" module.
"sn" is the name of the replication slot to drop.
*/
func DropSlot(connect, sn string) error {
	slotName := strings.ToLower(sn)
	// TODO what if there are already queries?
	conn, err := pgclient.Connect(connect)
	if err != nil {
		return err
	}
	defer conn.Close()

	sql := fmt.Sprintf("select * from pg_drop_replication_slot('%s')", slotName)
	_, _, err = conn.SimpleQuery(sql)
	return err
}

/*
Changes returns a channel that can be used to wait for changes. If an error
is returned, then no more changes will be forthcoming.
*/
func (r *Replicator) Changes() <-chan *common.Change {
	return r.changeChan
}

/*
Stop stops the replication process and closes the channel.
*/
func (r *Replicator) Stop() {
	r.cmdChan <- stopCmd
}

/*
State returns the current state of the replication.
*/
func (r *Replicator) State() State {
	return State(atomic.LoadInt32(&r.state))
}

/*
setState atomically updates the state
*/
func (r *Replicator) setState(s State) {
	atomic.StoreInt32(&r.state, int32(s))
}

/*
Acknowledge acknowledges to the server that we have committed a change, and
will result in a message being sent back to the database to the same
effect. It is important to periodically acknowledge changes so that the
database does not have to maintain its transaction log forever.
However, changes that happened before the specified LSN might still be
delivered on a reconnect, so it is important that consumers of this class
be prepared to handle and ignore duplicates.
*/
func (r *Replicator) Acknowledge(lsn int64) {
	r.updateChan <- lsn
}

/*
connect to the database and either get replication started, or
return an error.
*/
func (r *Replicator) connect() (*pgclient.PgConnection, error) {
	log.Debugf("Replication connecting to Postgres using %s", r.connectString)
	success := false
	conn, err := pgclient.Connect(r.connectString)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			conn.Close()
		}
	}()

	slotCreated := false
	startMsg := pgclient.NewOutputMessage(pgclient.Query)
	startMsg.WriteString(fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL 0/0", r.slotName))
	err = conn.WriteMessage(startMsg)
	if err != nil {
		return nil, err
	}

	// Read until we get a CopyBothResponse
	for {
		m, err := conn.ReadMessage()
		if err != nil {
			return nil, err
		}

		switch m.Type() {
		case pgclient.ErrorResponse:
			if !slotCreated {
				// First error might because slot is not created, so create it.
				consumeTillReady(conn)

				log.Debugf("Creating new replication slot %s", r.slotName)
				_, _, err = conn.SimpleQuery(fmt.Sprintf(
					"CREATE_REPLICATION_SLOT %s LOGICAL transicator_output", r.slotName))
				if err != nil {
					return nil, err
				}
				slotCreated = true

				// Re-send start replication command.
				err = conn.WriteMessage(startMsg)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, pgclient.ParseError(m)
			}

		case pgclient.NoticeResponse:
			msg, _ := pgclient.ParseNotice(m)
			log.Infof("Info from server: %s", msg)

		case pgclient.CopyBothResponse:
			// We'll use this as a signal to move on
			parseCopyBoth(m)
			success = true
			return conn, nil

		default:
			return nil, fmt.Errorf("Unknown message from server: %s", m.Type())
		}
	}
}

/*
This is the main loop. It handles connecting and reconnecting, and then
receives commands from the client and passes them on as appropriate.
*/
func (r *Replicator) replLoop() {
	var highLSN int64
	var connected bool
	var connection *pgclient.PgConnection
	var err error
	connectDelay := initialReconnectDelay

	hbTimer := time.NewTimer(heartbeatTimeout)
	defer hbTimer.Stop()

	// First time through -- connect right away
	log.Debug("Starting replLoop")
	connectTimer := time.NewTimer(0)
	defer connectTimer.Stop()

	for {
		select {
		// We were not connected, so attempt a reconnect.
		case <-connectTimer.C:
			connection, err = r.connect()
			if err == nil {
				connected = true
				r.setState(Running)
				go r.readLoop(connection)
			} else {
				connectDelay *= 2
				if connectDelay > maxReconnectDelay {
					connectDelay = maxReconnectDelay
				}
				log.Warningf("Error connecting to Postgres. Retrying in %v: %s",
					connectDelay, err)
				connectTimer.Reset(connectDelay)
			}

		// Client called "Acknowledge" to ask us to update the high LSN
		case newLSN := <-r.updateChan:
			log.Debugf("Got updated LSN %d", newLSN)
			if newLSN > highLSN {
				highLSN = newLSN
				// Send an update to the server
				if connected {
					r.updateLSN(highLSN, connection)
				}
				hbTimer.Reset(heartbeatTimeout)
			}

		// Periodic timeout to keep replication connection alive.
		case <-hbTimer.C:
			log.Debug("Heartbeat timer expired")
			if connected {
				r.updateLSN(highLSN, connection)
				hbTimer.Reset(heartbeatTimeout)
			}

		case cmd := <-r.cmdChan:
			log.Debugf("Got command %d", cmd)
			switch cmd {
			// Read loop exiting due to connection failure.
			case disconnectedCmd:
				connected = false
				log.Warning("Disconnected from Postgres.")
				r.setState(Connecting)
				connection.Close()
				connectDelay = initialReconnectDelay
				connectTimer.Reset(connectDelay)

			// Server asked us to acknowledge right now
			case acknowedgeCmd:
				if connected {
					r.updateLSN(highLSN, connection)
				}

			// We think that the server wants us to disconnect
			case shutdownCmd:
				if connected {
					cd := pgclient.NewOutputMessage(pgclient.CopyDoneOut)
					connection.WriteMessage(cd)
					r.cmdChan <- disconnectedCmd
				}

			// Client called Stop
			case stopCmd:
				log.Debug("Stopping replication")
				if connected {
					connection.Close()
				}
				r.setState(Stopped)
				return
			}
		}
	}
}

/*
This is the goroutine that is responsible for reading the replication output
from the database and passing it along to clients.
*/
func (r *Replicator) readLoop(connection *pgclient.PgConnection) {
	log.Debug("Starting to read from replication connection")

	for {
		m, err := connection.ReadMessage()
		if err != nil {
			log.Warningf("Error reading from server: %s", err)
			errChange := &common.Change{
				Error: err,
			}
			r.changeChan <- errChange
			r.cmdChan <- disconnectedCmd
			return
		}
		log.Debugf("Received message type %s", m.Type())

		switch m.Type() {
		case pgclient.NoticeResponse:
			msg, _ := pgclient.ParseNotice(m)
			log.Infof("Info from server: %s", msg)

		case pgclient.ErrorResponse:
			err = pgclient.ParseError(m)
			log.Warningf("Server returned an error: %s", err)
			errChange := &common.Change{
				Error: err,
			}
			r.changeChan <- errChange
			r.cmdChan <- disconnectedCmd
			return

		case pgclient.CopyData:
			cm, err := pgclient.ParseCopyData(m)
			if err != nil {
				log.Warningf("Received invalid CopyData message: %s", err)
			} else {
				shouldExit := r.handleCopyData(cm)
				if shouldExit {
					r.cmdChan <- shutdownCmd
					return
				}
			}

		default:
			log.Warningf("Server received an unknown message %s", m.Type())
		}
	}
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

func (r *Replicator) handleCopyData(m *pgclient.InputMessage) bool {
	switch m.Type() {
	case pgclient.WALData:
		r.handleWALData(m)
		return false
	case pgclient.SenderKeepalive:
		return r.handleKeepalive(m)
	default:
		log.Warningf("Received unknown WAL message %s", m.Type())
		return false
	}
}

func (r *Replicator) handleWALData(m *pgclient.InputMessage) {
	m.ReadInt64() // StartWAL
	m.ReadInt64() // end WAL
	m.ReadInt64() // Timestamp
	buf := m.ReadRemaining()

	c, err := common.UnmarshalChange(buf)
	if err == nil {
		r.changeChan <- c
	} else {
		log.Warningf("Received invalid change %s: %s", string(buf), err)
	}
}

func (r *Replicator) handleKeepalive(m *pgclient.InputMessage) bool {
	m.ReadInt64() // end WAL
	m.ReadInt64() // Timestamp
	replyNow, _ := m.ReadByte()
	log.Debugf("Got heartbeat. Reply now = %d", replyNow)
	if replyNow != 0 {
		// Postgres 9.5 does this on a graceful shutdown, and never exits unless
		// we use this as a trigger to stop replication and exit.
		// That is not what the documentation says -- the documentation says that
		// we should just send a heartbeat right away. But if we do that, then
		// we end up in a heartbeat loop and the database instance never exits.
		log.Info("Database requested immediate heartbeat response. Using this to trigger shutdown.")
		return true
	}
	return false
}

func (r *Replicator) updateLSN(highLSN int64, conn *pgclient.PgConnection) {
	log.Debugf("Updating server with last LSN %d", highLSN)
	om := pgclient.NewOutputMessage(pgclient.CopyDataOut)
	om.WriteByte(byte(pgclient.StandbyStatusUpdate))
	om.WriteInt64(highLSN)                           // last written to disk
	om.WriteInt64(highLSN)                           // last flushed to disk
	om.WriteInt64(highLSN)                           // last applied
	om.WriteInt64(time.Now().UnixNano() - epoch2000) // Timestamp!
	om.WriteByte(0)

	conn.WriteMessage(om)
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
