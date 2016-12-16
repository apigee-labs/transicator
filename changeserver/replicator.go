/*
Copyright 2016 The Transicator Authors

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

	log "github.com/Sirupsen/logrus"
	"github.com/apigee-labs/transicator/common"
	"github.com/apigee-labs/transicator/storage"
)

const (
	acknowledgeDelay = 500 * time.Millisecond
	maxBatchSize     = 100
)

func (s *server) runReplication(firstChange common.Sequence) {
	var lastAck uint64
	var lastChange common.Sequence
	var changeBatch []*common.Change

	// We will wake up periodically to send acknowledgements -- don't want to overload
	// this timer also lets us consolidate lots of changes into a batch.
	ackTimer := time.NewTicker(acknowledgeDelay)

	for {
		select {
		case change := <-s.repl.Changes():
			changeBatch = append(changeBatch, change)
			if len(changeBatch) >= maxBatchSize {
				newChange := s.handleChanges(changeBatch, firstChange)
				if newChange.Compare(lastChange) > 0 {
					lastChange = newChange
				}
				changeBatch = nil
			}

		case <-ackTimer.C:
			if len(changeBatch) > 0 {
				newChange := s.handleChanges(changeBatch, firstChange)
				if newChange.Compare(lastChange) > 0 {
					lastChange = newChange
				}
				changeBatch = nil
			}
			if lastChange.LSN > lastAck {
				lastAck = lastChange.LSN
				s.repl.Acknowledge(lastAck)
			}

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

func (s *server) handleChanges(cb []*common.Change, firstChange common.Sequence) common.Sequence {

	var entryBatch []storage.Entry
	var lastSeq common.Sequence

	for _, c := range cb {
		cs := c.GetSequence()
		if cs.Compare(firstChange) > 0 {
			if c.Timestamp == 0 {
				c.Timestamp = time.Now().Unix()
			}
			e := storage.Entry{
				Scope: getSelector(c),
				LSN:   c.CommitSequence,
				Index: c.CommitIndex,
				Data:  encodeChangeProto(c),
			}
			entryBatch = append(entryBatch, e)
			lastSeq = cs
		}
	}

	s.db.PutBatch(entryBatch)
	log.Debugf("Inserted a batch of %d changes", len(entryBatch))

	for _, e := range entryBatch {
		s.tracker.update(common.MakeSequence(e.LSN, e.Index), e.Scope)
	}

	return lastSeq
}

func getSelector(c *common.Change) string {
	var selector string
	if c.NewRow != nil {
		c.NewRow.Get(selectorColumn, &selector)
	}
	if c.OldRow != nil {
		c.OldRow.Get(selectorColumn, &selector)
	}
	return selector
}
