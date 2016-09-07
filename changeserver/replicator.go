package main

import (
	"time"

	"github.com/30x/transicator/replication"
	log "github.com/Sirupsen/logrus"
)

const (
	acknowledgeDelay = 500 * time.Millisecond
)

func (s *server) runReplication(firstChange int64) {
	var lastAck int64
	var lastChange int64
	ackTimer := time.NewTimer(acknowledgeDelay)

	for {
		select {
		case change := <-s.repl.Changes():
			newChange := s.handleChange(change, firstChange)
			if newChange > lastChange {
				lastChange = newChange
			}

		case <-ackTimer.C:
			if lastChange > lastAck {
				s.repl.Acknowledge(lastChange)
				lastAck = lastChange
			}
			ackTimer.Reset(acknowledgeDelay)

		case stopped := <-s.stopChan:
			s.repl.Stop()
			ackTimer.Stop()
			stopped <- true
			return
		}
	}
}

func (s *server) handleChange(c replication.Change, firstChange int64) int64 {
	e, err := c.Decode()
	if err != nil {
		log.Errorf("Received an invalid change: %s", err)
		return 0
	}

	if e.Sequence > firstChange {
		scope := getScope(e)
		log.Debugf("Received change %d for scope %s", e.Sequence, scope)
		s.db.PutEntryAndMetadata(
			scope, e.CommitSequence, e.Index, []byte(c.Data),
			lastSequenceKey, e.Sequence)
		s.tracker.update(e.CommitSequence, scope)
	} else {
		log.Debugf("Ignoring change %d which we already processed", e.Sequence)
	}
	return e.Sequence
}

func getScope(e *replication.EncodedChange) string {
	if e.New != nil {
		if e.New["_scope"] == nil {
			return ""
		}
		return e.New["_scope"].(string)
	}
	if e.Old != nil {
		if e.Old["_scope"] == nil {
			return ""
		}
		return e.Old["_scope"].(string)
	}
	return ""
}
