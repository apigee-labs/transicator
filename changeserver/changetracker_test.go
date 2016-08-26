package main

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	startIndex         = 2
	maxWait            = 10 * time.Microsecond
	trackerTestTimeout = 10 * time.Second
	trackerCount       = 10000
)

var tracker *changeTracker

var _ = Describe("Change tracker", func() {
	BeforeEach(func() {
		tracker = createTracker()
		tracker.update(startIndex, "")
	})

	AfterEach(func() {
		tracker.close()
	})

	It("Behind", func() {
		behind := tracker.wait(1, "")
		Expect(behind).Should(BeEquivalentTo(2))
	})

	It("Caught up", func() {
		behind := tracker.wait(2, "")
		Expect(behind).Should(BeEquivalentTo(2))
	})

	It("Caught up with tags", func() {
		tracker.update(3, "foo")
		tracker.update(4, "bar")
		time.Sleep(time.Millisecond * 250)

		behind := tracker.wait(2, "foo")
		Expect(behind).Should(BeEquivalentTo(3))
		behind = tracker.wait(2, "bar")
		Expect(behind).Should(BeEquivalentTo(4))
	})

	It("Timeout", func() {
		blocked := tracker.timedWait(3, 250*time.Millisecond, "")
		Expect(blocked).Should(BeEquivalentTo(0))
	})

	It("Up to date", func() {
		doneChan := make(chan int64, 1)

		go func() {
			new := tracker.wait(3, "")
			doneChan <- new
		}()

		tracker.update(3, "")
		Eventually(doneChan).Should(Receive(BeEquivalentTo(3)))
	})

	It("Up to date with timeout", func() {
		doneChan := make(chan int64, 1)

		go func() {
			new := tracker.timedWait(3, 2*time.Second, "")
			doneChan <- new
		}()

		tracker.update(3, "")
		Eventually(doneChan).Should(Receive(BeEquivalentTo(3)))
	})

	It("Behind with tags", func() {
		doneChanFoo := make(chan int64, 1)
		doneChanBar := make(chan int64, 1)

		go func() {
			new := tracker.wait(4, "foo")
			doneChanFoo <- new
		}()

		go func() {
			new := tracker.timedWait(4, 2*time.Second, "bar")
			doneChanBar <- new
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.update(4, "foo")

		got := <-doneChanFoo
		Expect(got).Should(BeEquivalentTo(4))
		got = <-doneChanBar
		Expect(got).Should(BeZero())
	})

	It("Tag never updated", func() {
		blocked := tracker.timedWait(3, 250*time.Millisecond, "baz")
		Expect(blocked).Should(BeEquivalentTo(0))
	})

	It("Less up to date with timeout", func() {
		doneChan := make(chan int64, 1)

		go func() {
			new := tracker.timedWait(4, 2*time.Second, "")
			doneChan <- new
		}()

		tracker.update(3, "")
		tracker.update(4, "")
		Eventually(doneChan).Should(Receive(BeEquivalentTo(4)))
	})

	It("Up to date timeout", func() {
		doneChan := make(chan int64, 1)

		go func() {
			new := tracker.timedWait(3, 250*time.Millisecond, "")
			doneChan <- new
		}()

		time.Sleep(1 * time.Second)
		tracker.update(3, "")
		Eventually(doneChan).Should(Receive(BeEquivalentTo(0)))
	})

	It("Update", func() {
		doneChan := make(chan int64, 1)

		go func() {
			new := tracker.wait(4, "")
			doneChan <- new
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.update(3, "")
		time.Sleep(250 * time.Millisecond)
		tracker.update(4, "")
		Eventually(doneChan).Should(Receive(BeEquivalentTo(4)))
	})

	It("Update twice", func() {
		doneChan := make(chan int64, 1)
		doneChan2 := make(chan int64, 1)

		go func() {
			new := tracker.wait(4, "")
			doneChan <- new
		}()

		go func() {
			new2 := tracker.wait(4, "")
			doneChan2 <- new2
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.update(3, "")
		time.Sleep(250 * time.Millisecond)
		tracker.update(4, "")
		got := <-doneChan
		Expect(got).Should(BeEquivalentTo(4))
		got = <-doneChan2
		Expect(got).Should(BeEquivalentTo(4))
	})

	It("Multi Update", func() {
		prematureDoneChan := make(chan int64, 1)
		doneChan := make(chan int64, 1)

		go func() {
			oldNew := tracker.wait(10, "")
			prematureDoneChan <- oldNew
		}()

		go func() {
			new := tracker.wait(4, "")
			doneChan <- new
		}()

		time.Sleep(250 * time.Millisecond)
		tracker.update(3, "")
		time.Sleep(250 * time.Millisecond)
		tracker.update(4, "")

		// No loop -- we expect that the first case arrive before the second
		select {
		case gotVal := <-doneChan:
			Expect(gotVal).Should(BeEquivalentTo(4))
		case <-prematureDoneChan:
			Expect(true).Should(BeFalse())
		}
	})

	It("Close", func() {
		tracker := createTracker()
		tracker.update(2, "")
		done := make(chan int64, 1)

		go func() {
			new := tracker.wait(3, "")
			done <- new
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
	var start int64 = startIndex

	prodDone := make(chan bool)
	consDone := make(chan bool)

	for i := 0; i <= producers; i++ {
		go func() {
			for atomic.LoadInt64(&start) < max {
				waitTime := rand.Int63n(int64(maxWait))
				time.Sleep(time.Duration(waitTime))
				val := atomic.AddInt64(&start, 1)
				tracker.update(val, "")
			}
			prodDone <- true
		}()
	}

	for i := 0; i <= consumers; i++ {
		go func() {
			var last int64 = startIndex
			for last < max {
				waitTime := rand.Int63n(int64(maxWait))
				last = tracker.timedWait(last+1, time.Duration(waitTime), "")
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
				"Test timed out after %d producers and %d consumers\n",
				prodCount, consCount)
			Expect(false).Should(BeTrue())
			return
		}
	}
}
