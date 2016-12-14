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

	"github.com/apigee-labs/transicator/common"
)

const (
	closed = iota
	newWaiter
	cancelWaiter
	update
)

type trackerUpdate struct {
	updateType int
	key        int32
	change     common.Sequence
	selector   string
	waiter     changeWaiter
}

type changeWaiter struct {
	change    common.Sequence
	selectors map[string]bool
	rc        chan common.Sequence
}

/*
A changeTracker allows clients to submit a change, and to wait for a change
to occur. The overall effect is like a condition variable, in that waiters
are notified when something changes. This work is done using a goroutine,
which is simpler and faster than the equivalent using a condition variable.
*/
type changeTracker struct {
	updateChan  chan trackerUpdate
	lastKey     int32
	waiters     map[int32]changeWaiter
	lastChanges map[string]common.Sequence
}

/*
CreateTracker creates a new change tracker with "lastChange" set to zero.
*/
func createTracker() *changeTracker {
	tracker := &changeTracker{
		updateChan: make(chan trackerUpdate, 100),
	}
	go tracker.run()
	return tracker
}

/*
Close stops the change tracker from delivering notifications.
*/
func (t *changeTracker) close() {
	u := trackerUpdate{
		updateType: closed,
	}
	t.updateChan <- u
}

/*
Update indicates that the current sequence has changed. Wake up any waiting
waiters and tell them about it.
*/
func (t *changeTracker) update(change common.Sequence, selector string) {
	u := trackerUpdate{
		updateType: update,
		change:     change,
		selector:   selector,
	}
	t.updateChan <- u
}

/*
Wait blocks the calling gorouting forever until the change tracker has reached a value at least as high as
"curChange." Return the current value when that happens.
*/
func (t *changeTracker) wait(curChange common.Sequence, selectors []string) common.Sequence {
	_, resultChan := t.doWait(curChange, selectors)
	return <-resultChan
}

/*
TimedWait blocks the current gorouting until either a new value higher than
"curChange" has been reached, or "maxWait" has been exceeded.
*/
func (t *changeTracker) timedWait(
	curChange common.Sequence, maxWait time.Duration, selectors []string) common.Sequence {
	key, resultChan := t.doWait(curChange, selectors)
	timer := time.NewTimer(maxWait)
	select {
	case result := <-resultChan:
		return result
	case <-timer.C:
		u := trackerUpdate{
			updateType: cancelWaiter,
			key:        key,
		}
		t.updateChan <- u
		return common.Sequence{}
	}
}

func (t *changeTracker) doWait(curChange common.Sequence, selectors []string) (int32, chan common.Sequence) {
	key := atomic.AddInt32(&t.lastKey, 1)
	resultChan := make(chan common.Sequence, 1)
	selectorMap := make(map[string]bool)
	for _, s := range selectors {
		selectorMap[s] = true
	}

	u := trackerUpdate{
		updateType: newWaiter,
		key:        key,
		waiter: changeWaiter{
			change:    curChange,
			selectors: selectorMap,
			rc:        resultChan,
		},
	}
	t.updateChan <- u
	return key, resultChan
}

/*
 * This is the goroutine. It receives updates for new waiters, and updates
 * for new sequences, and distributes them appropriately.
 */
func (t *changeTracker) run() {
	t.waiters = make(map[int32]changeWaiter)
	t.lastChanges = make(map[string]common.Sequence)

	running := true
	for running {
		up := <-t.updateChan
		switch up.updateType {
		case closed:
			running = false
		case update:
			t.handleUpdate(up)
		case cancelWaiter:
			t.handleCancel(up.key)
		case newWaiter:
			t.handleWaiter(up)
		}
	}

	// Close out all waiting waiters
	for _, w := range t.waiters {
		w.rc <- t.getMaxChange(w.selectors)
	}
}

func (t *changeTracker) handleUpdate(up trackerUpdate) {
	// Changes might come out of order
	if up.change.Compare(t.lastChanges[up.selector]) > 0 {
		t.lastChanges[up.selector] = up.change
	}

	for k, w := range t.waiters {
		if up.change.Compare(w.change) >= 0 && w.selectors[up.selector] {
			//log.Debugf("Waking up waiter waiting for change %d with change %d and tag %s",
			//	w.change, up.change, up.tag)
			w.rc <- up.change
			delete(t.waiters, k)
		}
	}
}

func (t *changeTracker) handleWaiter(u trackerUpdate) {
	maxAlready := t.getMaxChange(u.waiter.selectors)
	if maxAlready.Compare(u.waiter.change) >= 0 {
		u.waiter.rc <- maxAlready
	} else {
		t.waiters[u.key] = u.waiter
	}
}

func (t *changeTracker) handleCancel(key int32) {
	delete(t.waiters, key)
}

func (t *changeTracker) getMaxChange(selectors map[string]bool) common.Sequence {
	var max common.Sequence
	for selector := range selectors {
		if t.lastChanges[selector].Compare(max) > 0 {
			max = t.lastChanges[selector]
		}
	}
	return max
}
