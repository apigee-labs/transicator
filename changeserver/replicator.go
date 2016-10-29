package main

import (
	"sync/atomic"
	"time"

	"github.com/30x/transicator/common"
	log "github.com/Sirupsen/logrus"
)

const (
	acknowledgeDelay = 500 * time.Millisecond
)

func (s *server) runReplication(firstChange common.Sequence) {
	var lastAck uint64
	var lastChange common.Sequence
	ackTimer := time.NewTimer(acknowledgeDelay)

	for {
		select {
		case change := <-s.repl.Changes():
			newChange := s.handleChange(change, firstChange)
			if newChange.Compare(lastChange) > 0 {
				lastChange = newChange
			}

		case <-ackTimer.C:
			if lastChange.LSN > lastAck {
				lastAck = lastChange.LSN
				s.repl.Acknowledge(lastAck)
			}
			ackTimer.Reset(acknowledgeDelay)

		case stopped := <-s.stopChan:
			if atomic.LoadInt32(&s.dropSlot) == 0 {
				s.repl.Stop()
			} else {
				s.repl.StopAndDrop()
			}
			ackTimer.Stop()
			stopped <- true
			return
		}
	}
}

func (s *server) handleChange(c *common.Change, firstChange common.Sequence) common.Sequence {

	changeSeq := c.GetSequence()

	if changeSeq.Compare(firstChange) > 0 {
		scope := getScope(c)
		log.Debugf("Received change %d for scope %s", c.ChangeSequence, scope)
		dataBuf := encodeChangeProto(c)
		s.db.PutEntryAndMetadata(
			scope, c.CommitSequence, c.CommitIndex, dataBuf,
			lastSequenceKey, changeSeq.Bytes())
		s.tracker.update(changeSeq, scope)

	} else {
		log.Debugf("Ignoring change %s which we already processed", changeSeq)
	}
	return changeSeq
}

func getScope(c *common.Change) string {
	var scope string
	if c.NewRow != nil {
		c.NewRow.Get(scopeField, &scope)
	}
	if c.OldRow != nil {
		c.OldRow.Get(scopeField, &scope)
	}
	return scope
}
