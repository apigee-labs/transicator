package replication

import (
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apigee-labs/transicator/common"
	"github.com/apigee-labs/transicator/pgclient"
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
	// Retry dropping the slot because sometimes it takes a few seconds
	// for it to be actually dropper
	dropRetries = 10
)

type replCommand int

const (
	stopCmd replCommand = iota
	stopAndDropCmd
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
	slotName         string
	rawConnectString string
	connectString    string
	filter           func(c *common.Change) bool
	state            int32
	stopWaiter       *sync.WaitGroup
	changeChan       chan *common.Change
	updateChan       chan uint64
	cmdChan          chan replCommand
	jsonMode         bool
}

/*
CreateReplicator creates a new Replicator that will use the specified
URL to reach Postgres, and the specified slot name. The user must
call Start in order to start it up. "connect" is a postgres URL to be
passed to the "pgclient" module.
"sn" is the name of the replication slot to read from. This slot will be
created if it does not already exist.
*/
func CreateReplicator(connect, sn string) (*Replicator, error) {
	slotName := strings.ToLower(sn)
	connectString := addParam(connect, "replication", "database")
	log.Debugf("Connecting to the database at \"%s\"", connectString)

	repl := &Replicator{
		slotName:         slotName,
		rawConnectString: connect,
		connectString:    connectString,
		state:            int32(Stopped),
		changeChan:       make(chan *common.Change, 100),
		updateChan:       make(chan uint64, 100),
		cmdChan:          make(chan replCommand, 1),
	}
	return repl, nil
}

/*
addParam takes a URL and adds a parameter, regardless of what's already in
there.
*/
func addParam(query, key, val string) string {
	// Add a value to the connect URL in such a way that we are flexible
	var err error
	var queryVals url.Values
	cs := strings.SplitN(query, "?", 2)
	if len(cs) == 1 {
		queryVals = url.Values(make(map[string][]string))
	} else if len(cs) == 2 {
		queryVals, err = url.ParseQuery(cs[1])
		if err != nil {
			return query
		}
	} else {
		panic(fmt.Sprintf("Invalid string splitting of \"%s\"", query))
	}

	queryVals.Set(key, val)
	return fmt.Sprintf("%s?%s", cs[0], queryVals.Encode())
}

/*
SetChangeFilter supplies a function that will be called before every change
is passed on to the channel. This makes it easier to write clients,
especially for tests. The specified filter function will run inside a
critical goroutine, so it must make its own decision without blocking.
A typical use case would be to look for a particular value of a field.
*/
func (r *Replicator) SetChangeFilter(f func(*common.Change) bool) {
	r.filter = f
}

/*
Start replication. Start will succeed even if the database cannot be
reached.
*/
func (r *Replicator) Start() {
	// The main loop will handle connecting and all events.
	if r.State() == Stopped {
		r.setState(Connecting)
		r.stopWaiter = &sync.WaitGroup{}
		r.stopWaiter.Add(1)
		go r.replLoop()
	}
}

/*
DropSlot deletes the logical replication slot created by "Start".
"connect" is a postgres URL to be passed to the "pgclient" module.
"sn" is the name of the replication slot to drop.
*/
func DropSlot(connect, sn string) error {
	slotName := strings.ToLower(sn)

	ddb, err := sql.Open("transicator", connect)
	if err != nil {
		return err
	}
	defer ddb.Close()

	_, err = ddb.Exec("select * from pg_drop_replication_slot($1)", slotName)
	return err
}

/*
Changes returns a channel that can be used to wait for changes. If an error
is returned, then no more changes will be forthcoming. There is only one
channel per Replicator -- any kind of "broadcast" needs to be handled by
the client.
*/
func (r *Replicator) Changes() <-chan *common.Change {
	return r.changeChan
}

/*
Stop stops the replication process and closes the channel. It does not remove
the replication slot -- for that, use "DropSlot" after the channel is closed.
This method does not return until replication has been stopped.
*/
func (r *Replicator) Stop() {
	if r.State() != Stopped {
		r.cmdChan <- stopCmd
		r.stopWaiter.Wait()
	}
}

/*
StopAndDrop stops the replication process and closes the channel, and then
removes the replication slot.
This method does not return until replication has been stopped.
*/
func (r *Replicator) StopAndDrop() {
	if r.State() != Stopped {
		r.cmdChan <- stopAndDropCmd
		r.stopWaiter.Wait()
	}
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
func (r *Replicator) Acknowledge(lsn uint64) {
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
	startSQL :=
		fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL 0/0 (protobuf)", r.slotName)
	log.Debugf("Sending SQL to start replication: %s\n", startSQL)
	startMsg.WriteString(startSQL)
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
				log.Debugf("Re-starting replication with %s", startSQL)
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
	var highLSN uint64
	var connected bool
	var stopping bool
	var dropping bool
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
				log.Infof("Connected to Postgres using replication slot \"%s\"", r.slotName)
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
				if stopping {
					// But it was on purpose...
					r.finishStop(dropping, connection)
					return
				}

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

			// Client called stop with "delete" flag
			case stopAndDropCmd:
				if connected {
					dropping = true
					stopping = true
					connection.Close()
				} else {
					r.finishStop(true, connection)
					return
				}

			// Client called Stop
			case stopCmd:
				if connected {
					stopping = true
					log.Infof("Closing connection to Postgres")
					connection.Close()
				} else {
					r.finishStop(false, connection)
					return
				}
			}
		}
	}
}

func (r *Replicator) finishStop(
	deleteSlot bool,
	connection *pgclient.PgConnection) {

	r.setState(Stopped)
	log.Infof("Stopped replicating from slot \"%s\"", r.slotName)

	if deleteSlot {
		log.Infof("Dropping replication slot \"%s\"", r.slotName)
		err := r.dropSlot()
		if err != nil {
			log.Warnf("Error dropping replication slot \"%s\": %s", r.slotName, err)
		}
	}

	r.stopWaiter.Done()
}

/*
dropSlot drops the replication slot using the replication protocol, in
the unlikely event that we are not able to use any other protocol.
This requires us to open a new connection in replication mode.
Since we do this right after closing the connection, give it a few
retries.
*/
func (r *Replicator) dropSlot() error {
	conn, err := pgclient.Connect(r.connectString)
	if err != nil {
		return err
	}
	defer conn.Close()

	for try := 0; try < dropRetries; try++ {
		_, err = conn.SimpleExec(fmt.Sprintf(
			"DROP_REPLICATION_SLOT %s", r.slotName))
		if err == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return err
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
			if err != io.EOF {
				log.Warningf("Error reading from server: %s", err)
			}
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

	var c *common.Change
	var err error

	if r.jsonMode {
		c, err = common.UnmarshalChange(buf)
	} else {
		c, err = common.UnmarshalChangeProto(buf)
		if err != nil {
			// Defensive code in case we have an old version of the output plugin
			// that does not understand the "protobuf" option.
			c, err = common.UnmarshalChange(buf)
			if err == nil {
				log.Warn("Error decoding protobuf -- looks like Postgres is sending JSON")
				r.jsonMode = true
			}
		}
	}

	if err == nil {
		if r.filter == nil || r.filter(c) {
			r.changeChan <- c
		}
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

func (r *Replicator) updateLSN(highLSN uint64, conn *pgclient.PgConnection) {
	log.Debugf("Updating server with last LSN %d", highLSN)
	om := pgclient.NewOutputMessage(pgclient.CopyDataOut)
	om.WriteByte(byte(pgclient.StandbyStatusUpdate))
	om.WriteUint64(highLSN)                          // last written to disk
	om.WriteUint64(highLSN)                          // last flushed to disk
	om.WriteUint64(highLSN)                          // last applied
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
