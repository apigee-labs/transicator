package replication

import (
	"fmt"
	"time"

	"github.com/30x/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Replicator tests", func() {
	It("Basic replication", func() {
		repl, err := Start(dbURL, "unittestslot")
		Expect(err).Should(Succeed())
		defer repl.Stop()
		// There may be duplicates, so always drain first
		drainReplication(repl)

		doExecute("insert into transicator_test (id) values ('basic replication')")

		var change *common.Change
		Eventually(repl.Changes()).Should(Receive(&change))
		Expect(change.NewRow["id"].Value).Should(Equal("basic replication"))
		Consistently(repl.Changes()).ShouldNot(Receive())

		doExecute("begin")
		doExecute("insert into transicator_test (id) values ('replication 1')")
		doExecute("insert into transicator_test (id) values ('replication 2')")
		doExecute("commit")

		Eventually(repl.Changes()).Should(Receive(&change))
		Expect(change.NewRow["id"].Value).Should(Equal("replication 1"))

		Eventually(repl.Changes()).Should(Receive(&change))
		Expect(change.NewRow["id"].Value).Should(Equal("replication 2"))
		Consistently(repl.Changes()).ShouldNot(Receive())
	})

	It("Cleanup", func() {
		err := DropSlot(dbURL, "unittestslot")
		Expect(err).Should(Succeed())
	})
})

func drainReplication(repl *Replicator) []*common.Change {
	// Just pull stuff until we get a bit of a delay
	var maxLSN int64
	var ret []*common.Change
	timedOut := false

	for !timedOut {
		timeout := time.After(1 * time.Second)
		select {
		case <-timeout:
			timedOut = true
		case change := <-repl.Changes():
			Expect(change.Error).Should(Succeed())
			if change.CommitSequence > maxLSN {
				maxLSN = change.CommitSequence
			}
			ret = append(ret, change)
		}
	}

	fmt.Fprintf(GinkgoWriter, "Acknowledging %d\n", maxLSN)
	repl.Acknowledge(maxLSN)
	return ret
}
