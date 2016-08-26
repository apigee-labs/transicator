package replication

import (
	"encoding/json"
	"fmt"
	"time"

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

		var change Change
		Eventually(repl.Changes()).Should(Receive(&change))
		enc := decodeChange(change)
		Expect(enc.New["id"]).Should(Equal("basic replication"))
		Consistently(repl.Changes()).ShouldNot(Receive())

		doExecute("begin")
		doExecute("insert into transicator_test (id) values ('replication 1')")
		doExecute("insert into transicator_test (id) values ('replication 2')")
		doExecute("commit")

		Eventually(repl.Changes()).Should(Receive(&change))
		enc = decodeChange(change)
		Expect(enc.New["id"]).Should(Equal("replication 1"))

		Eventually(repl.Changes()).Should(Receive(&change))
		enc = decodeChange(change)
		Expect(enc.New["id"]).Should(Equal("replication 2"))
		Consistently(repl.Changes()).ShouldNot(Receive())
	})

	It("Cleanup", func() {
		err := DropSlot(dbURL, "unittestslot")
		Expect(err).Should(Succeed())
	})
})

func drainReplication(repl *Replicator) {
	// Just pull stuff until we get a bit of a delay
	var maxLSN int64
	timedOut := false
	for !timedOut {
		timeout := time.After(1 * time.Second)
		select {
		case <-timeout:
			timedOut = true
		case change := <-repl.Changes():
			Expect(change.Error).Should(Succeed())
			fmt.Fprintf(GinkgoWriter, "LSN %d data %s\n", change.LSN, change.Data)
			if change.LSN > maxLSN {
				maxLSN = change.LSN
			}
		}
	}

	fmt.Fprintf(GinkgoWriter, "Acknowledging %d\n", maxLSN)
	repl.Acknowledge(maxLSN)
}

func decodeChange(c Change) *EncodedChange {
	var enc EncodedChange
	err := json.Unmarshal([]byte(c.Data), &enc)
	Expect(err).Should(Succeed())
	return &enc
}
