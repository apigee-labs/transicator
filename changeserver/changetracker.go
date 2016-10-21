package main

import (
	"sync"
	"time"

	"github.com/30x/transicator/common"
)

type changeWaiter struct {
	change common.Sequence
	scopes map[string]bool
	rc     chan common.Sequence
}

/*
A changeTracker allows clients to submit a change, and to wait for a change
to occur. The overall effect is like a condition variable, in that waiters
are notified when something changes.
*/
type changeTracker struct {
	lastKey     int32
	waiters     map[int32]changeWaiter
	lastChanges map[string]common.Sequence
	latch       sync.Locker
	closed      bool
}

/*
CreateTracker creates a new change tracker with "lastChange" set to zero.
*/
func createTracker() *changeTracker {
	tracker := &changeTracker{
		latch:       &sync.Mutex{},
		waiters:     make(map[int32]changeWaiter),
		lastChanges: make(map[string]common.Sequence),
	}
	return tracker
}

/*
Close stops the change tracker from delivering notifications and wakes
up anyone who is left.
*/
func (t *changeTracker) close() {
	t.latch.Lock()
	defer t.latch.Unlock()

	t.closed = true
	for k, w := range t.waiters {
		w.rc <- common.Sequence{}
		delete(t.waiters, k)
	}
}

/*
Update indicates that the current sequence has changed. Wake up any waiting
waiters and tell them about it.
*/
func (t *changeTracker) update(change common.Sequence, scope string) {
	t.latch.Lock()
	defer t.latch.Unlock()

	t.lastChanges[scope] = change
	for k, w := range t.waiters {
		if change.Compare(w.change) >= 0 && w.scopes[scope] {
			//log.Debugf("Waking up waiter waiting for change %d with change %d and tag %s",
			//	w.change, up.change, up.tag)
			w.rc <- change
			delete(t.waiters, k)
		}
	}
}

/*
Wait blocks the calling gorouting forever until the change tracker has reached a value at least as high as
"curChange." Return the current value when that happens.
*/
func (t *changeTracker) wait(curChange common.Sequence, scopes []string) common.Sequence {
	_, resultChan := t.doWait(curChange, scopes)
	return <-resultChan
}

/*
TimedWait blocks the current gorouting until either a new value higher than
"curChange" has been reached, or "maxWait" has been exceeded.
*/
func (t *changeTracker) timedWait(
	curChange common.Sequence, maxWait time.Duration, scopes []string) common.Sequence {

	key, resultChan := t.doWait(curChange, scopes)
	if key < 0 {
		return <-resultChan
	}

	timer := time.NewTimer(maxWait)
	select {
	case result := <-resultChan:
		return result
	case <-timer.C:
		return t.cancelWaiter(key)
	}
}

func (t *changeTracker) doWait(curChange common.Sequence, scopes []string) (int32, chan common.Sequence) {
	t.latch.Lock()
	defer t.latch.Unlock()

	scopeMap := make(map[string]bool)
	for _, s := range scopes {
		scopeMap[s] = true
	}

	resultChan := make(chan common.Sequence, 1)
	maxAlready := t.getMaxChange(scopeMap)
	if maxAlready.Compare(curChange) >= 0 {
		resultChan <- maxAlready
		return -1, resultChan
	}

	waiter := changeWaiter{
		change: curChange,
		scopes: scopeMap,
		rc:     resultChan,
	}

	waitKey := t.lastKey
	t.lastKey++
	t.waiters[waitKey] = waiter

	return waitKey, resultChan
}

/*
cancelWaiter deletes a waiter who has timed out. But it also returns the
highest sequence for the scope because otherwise there is a race
condition.
*/
func (t *changeTracker) cancelWaiter(key int32) common.Sequence {
	t.latch.Lock()
	defer t.latch.Unlock()

	waiter := t.waiters[key]
	delete(t.waiters, key)
	return t.getMaxChange(waiter.scopes)
}

func (t *changeTracker) getMaxChange(scopes map[string]bool) common.Sequence {
	var max common.Sequence
	for scope := range scopes {
		if t.lastChanges[scope].Compare(max) > 0 {
			max = t.lastChanges[scope]
		}
	}
	return max
}
