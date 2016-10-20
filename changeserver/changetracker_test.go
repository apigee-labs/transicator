package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/30x/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	startIndex         = 2
	maxWait            = 10 * time.Microsecond
	trackerTestTimeout = 60 * time.Second
	trackerCount       = 1000
)

var tracker *changeTracker

var _ = Describe("Change tracker", func() {
	BeforeEach(func() {
		tracker = createTracker()
		tracker.update(common.MakeSequence(uint64(startIndex), 0), "")
	})

	AfterEach(func() {
		tracker.close()
	})

	It("Behind", func() {
		behind := tracker.wait(common.MakeSequence(1, 0), []string{""})
		Expect(behind.LSN).Should(BeEquivalentTo(2))
	})

	It("Behind with scopes", func() {
		tracker.update(common.MakeSequence(3, 0), "foo")
		tracker.update(common.MakeSequence(4, 0), "bar")
		behind := tracker.wait(common.MakeSequence(1, 0), []string{"foo"})
		Expect(behind.LSN).Should(BeEquivalentTo(3))
		behind = tracker.wait(common.MakeSequence(1, 0), []string{"bar"})
		Expect(behind.LSN).Should(BeEquivalentTo(4))
		behind = tracker.wait(common.MakeSequence(1, 0), []string{"foo", "bar"})
		Expect(behind.LSN).Should(BeEquivalentTo(4))
		behind = tracker.wait(common.MakeSequence(1, 0), []string{"bar", "foo"})
		Expect(behind.LSN).Should(BeEquivalentTo(4))
	})

	It("Caught up", func() {
		behind := tracker.wait(common.MakeSequence(2, 0), []string{""})
		Expect(behind.LSN).Should(BeEquivalentTo(2))
	})

	It("Caught up with scopes", func() {
		tracker.update(common.MakeSequence(3, 0), "foo")
		tracker.update(common.MakeSequence(4, 0), "bar")
		time.Sleep(time.Millisecond * 250)

		behind := tracker.wait(common.MakeSequence(2, 0), []string{"foo"})
		Expect(behind.LSN).Should(BeEquivalentTo(3))
		behind = tracker.wait(common.MakeSequence(2, 0), []string{"bar"})
		Expect(behind.LSN).Should(BeEquivalentTo(4))
		behind = tracker.wait(common.MakeSequence(2, 0), []string{"foo", "bar"})
		Expect(behind.LSN).Should(BeEquivalentTo(4))
	})

	It("Timeout", func() {
		blocked := tracker.timedWait(common.MakeSequence(3, 0), 250*time.Millisecond, []string{""})
		Expect(blocked.LSN).Should(BeEquivalentTo(0))
	})

	It("Up to date", func() {
		doneChan := make(chan uint64, 1)

		go func() {
			new := tracker.wait(common.MakeSequence(3, 0), []string{""})
			doneChan <- new.LSN
		}()

		tracker.update(common.MakeSequence(3, 0), "")
		Eventually(doneChan).Should(Receive(BeEquivalentTo(3)))
	})

	It("Up to date with timeout", func() {
		doneChan := make(chan uint64, 1)

		go func() {
			new := tracker.timedWait(common.MakeSequence(3, 0), 2*time.Second, []string{""})
			doneChan <- new.LSN
		}()

		tracker.update(common.MakeSequence(3, 0), "")
		Eventually(doneChan).Should(Receive(BeEquivalentTo(3)))
	})

	It("Behind with tags", func() {
		doneChanFoo := make(chan uint64, 1)
		doneChanBar := make(chan uint64, 1)

		go func() {
			new := tracker.wait(common.MakeSequence(4, 0), []string{"foo"})
			doneChanFoo <- new.LSN
		}()

		go func() {
			new := tracker.timedWait(common.MakeSequence(4, 0), 2*time.Second, []string{"bar"})
			doneChanBar <- new.LSN
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.update(common.MakeSequence(4, 0), "foo")

		Eventually(doneChanFoo).Should(Receive(BeEquivalentTo(4)))
		Eventually(doneChanBar, 5*time.Second).Should(Receive(BeZero()))
	})

	It("Tag never updated", func() {
		blocked := tracker.timedWait(common.MakeSequence(3, 0), 250*time.Millisecond, []string{"baz"})
		Expect(blocked.LSN).Should(BeEquivalentTo(0))
	})

	It("Less up to date with timeout", func() {
		doneChan := make(chan uint64, 1)

		go func() {
			new := tracker.timedWait(common.MakeSequence(4, 0), 2*time.Second, []string{""})
			doneChan <- new.LSN
		}()

		tracker.update(common.MakeSequence(3, 0), "")
		tracker.update(common.MakeSequence(4, 0), "")
		Eventually(doneChan).Should(Receive(BeEquivalentTo(4)))
	})

	It("Up to date timeout", func() {
		doneChan := make(chan uint64, 1)

		go func() {
			new := tracker.timedWait(common.MakeSequence(3, 0), 250*time.Millisecond, []string{""})
			doneChan <- new.LSN
		}()

		time.Sleep(1 * time.Second)
		tracker.update(common.MakeSequence(3, 0), "")
		Eventually(doneChan).Should(Receive(BeEquivalentTo(0)))
	})

	It("Update", func() {
		doneChan := make(chan uint64, 1)

		go func() {
			new := tracker.wait(common.MakeSequence(4, 0), []string{""})
			doneChan <- new.LSN
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.update(common.MakeSequence(3, 0), "")
		time.Sleep(250 * time.Millisecond)
		tracker.update(common.MakeSequence(4, 0), "")
		Eventually(doneChan).Should(Receive(BeEquivalentTo(4)))
	})

	It("Update with scopes", func() {
		doneChan := make(chan uint64, 1)

		go func() {
			new := tracker.wait(common.MakeSequence(4, 0), []string{"baz"})
			doneChan <- new.LSN
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.update(common.MakeSequence(3, 0), "baz")
		time.Sleep(250 * time.Millisecond)
		tracker.update(common.MakeSequence(4, 0), "baz")
		Eventually(doneChan).Should(Receive(BeEquivalentTo(4)))
	})

	It("Update with two scopes", func() {
		doneChan := make(chan uint64, 1)

		go func() {
			new := tracker.wait(common.MakeSequence(4, 0), []string{"bar", "baz"})
			doneChan <- new.LSN
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.update(common.MakeSequence(3, 0), "foo")
		time.Sleep(250 * time.Millisecond)
		tracker.update(common.MakeSequence(4, 0), "baz")
		Eventually(doneChan).Should(Receive(BeEquivalentTo(4)))
	})

	It("Update twice", func() {
		doneChan := make(chan uint64, 1)
		doneChan2 := make(chan uint64, 1)

		go func() {
			new := tracker.wait(common.MakeSequence(4, 0), []string{""})
			doneChan <- new.LSN
		}()

		go func() {
			new2 := tracker.wait(common.MakeSequence(4, 0), []string{""})
			doneChan2 <- new2.LSN
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.update(common.MakeSequence(3, 0), "")
		time.Sleep(250 * time.Millisecond)
		tracker.update(common.MakeSequence(4, 0), "")
		got := <-doneChan
		Expect(got).Should(BeEquivalentTo(4))
		got = <-doneChan2
		Expect(got).Should(BeEquivalentTo(4))
	})

	It("Multi Update", func() {
		prematureDoneChan := make(chan uint64, 1)
		doneChan := make(chan uint64, 1)

		go func() {
			oldNew := tracker.wait(common.MakeSequence(10, 0), []string{""})
			prematureDoneChan <- oldNew.LSN
		}()

		go func() {
			new := tracker.wait(common.MakeSequence(4, 0), []string{""})
			doneChan <- new.LSN
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.update(common.MakeSequence(3, 0), "")
		time.Sleep(250 * time.Millisecond)
		tracker.update(common.MakeSequence(4, 0), "")

		// No loop -- we expect that the first case arrive before the second
		select {
		case gotVal := <-doneChan:
			Expect(gotVal).Should(BeEquivalentTo(4))
		case <-prematureDoneChan:
			Expect(true).Should(BeFalse())
		}
	})

	It("Close", func() {
		tracker = createTracker()
		tracker.update(common.MakeSequence(2, 0), "")
		done := make(chan uint64, 1)

		go func() {
			new := tracker.wait(common.MakeSequence(3, 0), []string{""})
			done <- new.LSN
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.close()

		Eventually(done).Should(Receive(BeEquivalentTo(2)))
	})

	It("Stress 1, 1", func() {
		trackerStress(1, 1, trackerCount)
	})

	It("Stress 100, 1", func() {
		trackerStress(100, 1, trackerCount)
	})

	It("Stress 1, 100", func() {
		trackerStress(1, 100, trackerCount)
	})

	It("Stress 1, 1000", func() {
		trackerStress(1, 1000, trackerCount)
	})

	It("Stress 100, 100", func() {
		trackerStress(100, 100, trackerCount)
	})

	It("Stress 100, 1000", func() {
		trackerStress(100, 1000, trackerCount)
	})
})

func trackerStress(producers, consumers int, max int64) {
	// Make a separate tracker here. The global pointer may get shared
	// by multiple tests otherwise as one shuts down while the next starts.
	stressTracker := createTracker()
	defer stressTracker.close()

	var start int64 = startIndex

	prodDone := make(chan bool, producers)
	consDone := make(chan bool, consumers)

	for i := 0; i <= producers; i++ {
		go func() {
			for atomic.LoadInt64(&start) < max {
				waitTime := rand.Int63n(int64(maxWait))
				time.Sleep(time.Duration(waitTime))
				val := atomic.AddInt64(&start, 1)
				seq := common.MakeSequence(uint64(val), 0)
				stressTracker.update(seq, "")
			}
			prodDone <- true
		}()
	}

	for i := 0; i <= consumers; i++ {
		go func() {
			var last int64 = startIndex
			for last < max {
				waitTime := rand.Int63n(int64(maxWait))
				seq := common.MakeSequence(uint64(last+1), 0)
				lastSeq := stressTracker.timedWait(seq, time.Duration(waitTime), []string{""})
				last = int64(lastSeq.LSN)
			}
			consDone <- true
		}()
	}

	prodCount := 0
	consCount := 0

	timeout := time.NewTimer(trackerTestTimeout)
	for prodCount < producers || consCount < consumers {
		select {
		case <-prodDone:
			prodCount++
		case <-consDone:
			consCount++
		case <-timeout.C:
			fmt.Fprintf(GinkgoWriter,
				"Test timed out after %d producers and %d consumers. last = %d\n",
				prodCount, consCount, atomic.LoadInt64(&start))
			stacks := make([]byte, 1000000)
			runtime.Stack(stacks, true)
			fmt.Fprintf(GinkgoWriter, string(stacks))
			Expect(false).Should(BeTrue())
			return
		}
	}
}
