/*
Copyright 2016 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"sync/atomic"
	"time"

	"github.com/apigee-labs/transicator/common"
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
		if c.Timestamp == 0 {
			c.Timestamp = time.Now().Unix()
		}
		dataBuf := encodeChangeProto(c)
		s.db.Put(
			scope, c.CommitSequence, c.CommitIndex, dataBuf)
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
