package goscaffold

import (
	"errors"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tracker tests", func() {
	It("Basic tracker", func() {
		t := startRequestTracker(10 * time.Second)
		t.start()
		Consistently(t.C, 250*time.Millisecond).ShouldNot(Receive())
		t.shutdown(errors.New("Basic"))
		Consistently(t.C, 250*time.Millisecond).ShouldNot(Receive())
		t.end()
		Eventually(t.C).Should(Receive(MatchError("Basic")))
	})

	It("Tracker stop idle", func() {
		t := startRequestTracker(10 * time.Second)
		t.shutdown(errors.New("Stop"))
		Eventually(t.C).Should(Receive(MatchError("Stop")))
	})

	It("Tracker grace timeout", func() {
		t := startRequestTracker(time.Second)
		t.start()
		t.shutdown(errors.New("Stop"))
		Eventually(t.C, 2*time.Second).Should(Receive(MatchError("Stop")))
	})
})
