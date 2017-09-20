// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package goscaffold

import (
	"math"
	"sync/atomic"
	"time"
)

/*
values for the command channel.
*/
const (
	startRequest = iota
	endRequest
	shutdown
)

/*
values for the shutdown state
*/
const (
	running    int32 = iota
	markedDown int32 = iota
	shutDown   int32 = iota
)

/*
The requestTracker keeps track of HTTP requests. In normal operations it
just counts. Once the server has been marked for shutdown, however, it
counts down to zero and returns a shutdown indication when that
happens.
*/
type requestTracker struct {
	// A value will be delivered to this channel when the server can stop.
	// If "shutdown" is never called then this will never happen.
	C              chan error
	shutdownWait   time.Duration
	shutdownState  int32
	shutdownReason *atomic.Value
	commandChan    chan int
}

/*
startRequestTracker creates a new tracker. shutdownWait defines the
maximum amount of time that we should wait for shutdown in case some
do not complete in a timely way.
*/
func startRequestTracker(shutdownWait time.Duration) *requestTracker {
	rt := &requestTracker{
		C:              make(chan error, 1),
		commandChan:    make(chan int, 100),
		shutdownState:  running,
		shutdownWait:   shutdownWait,
		shutdownReason: &atomic.Value{},
	}
	go rt.trackerLoop()
	return rt
}

/*
start indicates that a request started. It returns true if the request
should proceed, and false if the request should fail because the server is
shutting down.
*/
func (t *requestTracker) start() error {
	md := t.markedDown()
	if md == nil {
		t.commandChan <- startRequest
	}
	return md
}

/*
end indicates that a request ended. In order for this thing to work, the
caller needs to ensure that start and end are always paired.
*/
func (t *requestTracker) end() {
	t.commandChan <- endRequest
}

/*
markedDown returns nil if everything is good, and an error if the server
has been marked down. The error is the one that was sent to the
"Shutdown" method.
*/
func (t *requestTracker) markedDown() error {
	ss := atomic.LoadInt32(&t.shutdownState)
	if ss != running {
		reason := t.shutdownReason.Load().(*error)
		if reason == nil {
			return nil
		}
		return *reason
	}
	return nil
}

/*
shutdown indicates that the tracker should start counting down until
the number of running requests reaches zero. The "reason" will be returned
as the result of the "start" call.
*/
func (t *requestTracker) shutdown(reason error) {
	t.shutdownReason.Store(&reason)
	t.commandChan <- shutdown
}

func (t *requestTracker) markDown() {
	t.shutdownReason.Store(&ErrMarkedDown)
	atomic.StoreInt32(&t.shutdownState, markedDown)
}

func (t *requestTracker) sendStop(sent bool) bool {
	if !sent {
		reason := t.shutdownReason.Load().(*error)
		if reason == nil {
			return false
		}
		t.C <- *reason
	}
	return true
}

/*
trackerLoop runs all day and manages stuff.
*/
func (t *requestTracker) trackerLoop() {
	activeRequests := 0
	stopping := false
	sentStop := false
	graceTimer := time.NewTimer(time.Duration(math.MaxInt64))

	for !sentStop {
		select {
		case cmd := <-t.commandChan:
			switch cmd {
			case startRequest:
				activeRequests++
			case endRequest:
				activeRequests--
				if stopping && activeRequests == 0 {
					sentStop = t.sendStop(sentStop)
				}
			case shutdown:
				stopping = true
				atomic.StoreInt32(&t.shutdownState, shutDown)
				if activeRequests <= 0 {
					sentStop = t.sendStop(sentStop)
				} else {
					graceTimer.Reset(t.shutdownWait)
				}
			}
		case <-graceTimer.C:
			sentStop = t.sendStop(sentStop)
		}
	}
}
