package replication

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/30x/transicator/pgclient"
	log "github.com/Sirupsen/logrus"
)

const (
	// January 1, 2000, midnight UTC, in microseconds from Unix epoch
	epoch2000 = 946717749000000000
	// Make sure that we reply to the server at least this often
	heartbeatTimeout = 10 * time.Second
)

/*
EncodedChange represents the JSON schema of a change produced via
logical replication.
*/
type EncodedChange struct {
	Table          string                 `json:"table"`
	Sequence       int64                  `json:"sequence"`       // LSN of the actual change
	CommitSequence int64                  `json:"commitSequence"` // LSN of the transaction commit (> Sequence)
	FirstSequence  int64                  `json:"firstSequence"`  // LSN of the first change in transaction (< CommitSequence)
	Index          int32                  `json:"index"`          // Position of change within transaction
	Txid           int32                  `json:"txid"`
	Operation      string                 `json:"operation"`     // insert, update, or delete
	New            map[string]interface{} `json:"new,omitempty"` // Fields in the new row for insert or update
	Old            map[string]interface{} `json:"old,omitempty"` // Fields in the old row for delete
}

/*
A Change represents a single change that has been replicated from the server.
*/
type Change struct {
	LSN   int64
	Data  string
	Error error
}

/*
Decode returns a decoded version of the JSON.
*/
func (c Change) Decode() (*EncodedChange, error) {
	var change EncodedChange
	err := json.Unmarshal([]byte(c.Data), &change)
	return &change, err
}

/*
A Replicator is a client for the logical replication protocol.
*/
type Replicator struct {
	conn       *pgclient.PgConnection
	lastLSN    int64
	slotName   string
	changeChan chan Change
	updateChan chan int64
	stopChan   chan bool
}

/*
Start replication and return a new Replicator object that may be used to
read it. "connect" is a postgres URL to be passed to the "pgclient" module.
"sn" is the name of the replication slot to read from. This slot will be
created if it does not already exist.
*/
func Start(connect, sn string) (*Replicator, error) {
	slotName := strings.ToLower(sn)
	// TODO what if there are already queries?
	conn, err := pgclient.Connect(connect + "?replication=database")
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
	hbTimer := time.NewTimer(heartbeatTimeout)
	defer hbTimer.Stop()

	for {
		select {
		case newLSN := <-r.updateChan:
			if newLSN > r.lastLSN {
				r.lastLSN = newLSN
				// Send an update to the server
				r.updateLSN()
				hbTimer.Reset(heartbeatTimeout)
			} else if newLSN <= 0 {
				// Use this to trigger a re-send
				r.updateLSN()
				hbTimer.Reset(heartbeatTimeout)
			}

		case <-hbTimer.C:
			r.updateLSN()
			hbTimer.Reset(heartbeatTimeout)

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
	log.Debugf("Got heartbeat. Reply now = %d", replyNow)
	if replyNow != 0 {
		// This will trigger an immediate heartbeat of current LSN
		r.Acknowledge(0)
	}
}

func (r *Replicator) updateLSN() {
	log.Debugf("Updating server with last LSN %d", r.lastLSN)
	om := pgclient.NewOutputMessage(pgclient.CopyData)
	om.WriteByte(pgclient.StandbyStatusUpdate)
	om.WriteInt64(r.lastLSN)                         // last written to disk
	om.WriteInt64(r.lastLSN)                         // last flushed to disk
	om.WriteInt64(r.lastLSN)                         // last applied
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
