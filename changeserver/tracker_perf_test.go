package main

import (
	"sync"
	"testing"
	"time"

	"github.com/30x/transicator/common"
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
