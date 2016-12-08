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
	"sync"
	"testing"
	"time"

	"github.com/apigee-labs/transicator/common"
)

func BenchmarkTracker1(b *testing.B) {
	runTrackerBench(1, false, b)
}

func BenchmarkTracker10(b *testing.B) {
	runTrackerBench(10, false, b)
}

func BenchmarkTracker100(b *testing.B) {
	runTrackerBench(100, false, b)
}

func BenchmarkTrackerTimeout1(b *testing.B) {
	runTrackerBench(1, true, b)
}

func BenchmarkTrackerTimeout10(b *testing.B) {
	runTrackerBench(10, true, b)
}

func BenchmarkTrackerTimeout100(b *testing.B) {
	runTrackerBench(100, true, b)
}

func runTrackerBench(consumers int, useTimeout bool, b *testing.B) {
	tracker := createTracker()
	defer func() {
		b.StopTimer()
		tracker.close()
	}()

	scopes := []string{"bench"}

	wg := &sync.WaitGroup{}
	wg.Add(consumers)

	for c := 0; c < consumers; c++ {
		go func() {
			defer wg.Done()
			cur := 0
			for cur < b.N {
				seq := common.Sequence{
					LSN: uint64(cur),
				}
				var newSeq common.Sequence
				if useTimeout {
					newSeq = tracker.timedWait(seq, 5*time.Millisecond, scopes)
				} else {
					newSeq = tracker.wait(seq, scopes)
				}
				cur = int(newSeq.LSN)
			}
		}()
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		newSeq := common.Sequence{
			LSN: uint64(i),
		}
		tracker.update(newSeq, "bench")
	}
}
