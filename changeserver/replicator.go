package main

import (
	"time"

	"github.com/30x/transicator/common"
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

func (s *server) handleChange(c *common.Change, firstChange int64) int64 {
	// TODO Problem -- we need to save index too here
	if c.CommitSequence > firstChange {
		scope := getScope(c)
		log.Debugf("Received change %d for scope %s", c.ChangeSequence, scope)
		dataBuf := c.Marshal()
		s.db.PutEntryAndMetadata(
			scope, c.CommitSequence, c.CommitIndex, dataBuf,
			lastSequenceKey, c.CommitSequence)
		s.tracker.update(c.CommitSequence, scope)
	} else {
		log.Debugf("Ignoring change %d which we already processed", c.ChangeSequence)
	}
	return c.ChangeSequence
}

func getScope(c *common.Change) string {
	if c.NewRow != nil {
		if c.NewRow[scopeField] == nil {
			return ""
		}
		return c.NewRow[scopeField].Value
	}
	if c.OldRow != nil {
		if c.OldRow[scopeField] == nil {
			return ""
		}
		return c.OldRow[scopeField].Value
	}
	return ""
}
