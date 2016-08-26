package main

import (
	"sync/atomic"
	"time"
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
	change     int64
	tag        string
	waiter     changeWaiter
}

type changeWaiter struct {
	change int64
	tag    string
	rc     chan int64
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
	lastChanges map[string]int64
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
func (t *changeTracker) update(change int64, tag string) {
	u := trackerUpdate{
		updateType: update,
		change:     change,
		tag:        tag,
	}
	t.updateChan <- u
}

/*
Wait blocks the calling gorouting forever until the change tracker has reached a value at least as high as
"curChange." Return the current value when that happens.
*/
func (t *changeTracker) wait(curChange int64, tag string) int64 {
	_, resultChan := t.doWait(curChange, tag)
	return <-resultChan
}

/*
TimedWait blocks the current gorouting until either a new value higher than
"curChange" has been reached, or "maxWait" has been exceeded.
*/
func (t *changeTracker) timedWait(
	curChange int64, maxWait time.Duration, tag string) int64 {
	key, resultChan := t.doWait(curChange, tag)
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
		return 0
	}
}

func (t *changeTracker) doWait(curChange int64, tag string) (int32, chan int64) {
	key := atomic.AddInt32(&t.lastKey, 1)
	resultChan := make(chan int64, 1)
	u := trackerUpdate{
		updateType: newWaiter,
		key:        key,
		tag:        tag,
		waiter: changeWaiter{
			change: curChange,
			tag:    tag,
			rc:     resultChan,
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
	t.lastChanges = make(map[string]int64)

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
		w.rc <- t.lastChanges[w.tag]
	}
}

func (t *changeTracker) handleUpdate(up trackerUpdate) {
	t.lastChanges[up.tag] = up.change
	for k, w := range t.waiters {
		if up.change >= w.change && up.tag == w.tag {
			//log.Debugf("Waking up waiter waiting for change %d with change %d and tag %s",
			//	w.change, up.change, up.tag)
			w.rc <- up.change
			delete(t.waiters, k)
		}
	}
}

func (t *changeTracker) handleWaiter(u trackerUpdate) {
	if t.lastChanges[u.tag] >= u.waiter.change {
		u.waiter.rc <- t.lastChanges[u.tag]
	} else {
		t.waiters[u.key] = u.waiter
	}
}

func (t *changeTracker) handleCancel(key int32) {
	delete(t.waiters, key)
}
