package replication

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Replicator tests", func() {
	It("Basic replication", func() {
		repl, err := CreateReplicator(dbURL, "unittestslot")
		Expect(err).Should(Succeed())
		repl.SetChangeFilter(filterChange)
		repl.Start()
		defer repl.Stop()
		// There may be duplicates, so always drain first
		drainReplication(repl)

		is, err := db.Prepare("insert into transicator_test (id, testid) values ($1, $2)")
		Expect(err).Should(Succeed())
		defer is.Close()

		_, err = is.Exec("basic replication", testID)
		Expect(err).Should(Succeed())

		var change *common.Change
		Eventually(repl.Changes()).Should(Receive(&change))
		Expect(change.NewRow["id"].Value).Should(Equal("basic replication"))
		Consistently(repl.Changes()).ShouldNot(Receive())

		tx, err := db.Begin()
		Expect(err).Should(Succeed())
		tis := tx.Stmt(is)

		_, err = tis.Exec("replication 1", testID)
		Expect(err).Should(Succeed())
		_, err = tis.Exec("replication 2", testID)
		Expect(err).Should(Succeed())

		err = tx.Commit()
		Expect(err).Should(Succeed())

		Eventually(repl.Changes()).Should(Receive(&change))
		Expect(change.NewRow["id"].Value).Should(Equal("replication 1"))

		Eventually(repl.Changes()).Should(Receive(&change))
		Expect(change.NewRow["id"].Value).Should(Equal("replication 2"))
		Consistently(repl.Changes()).ShouldNot(Receive())
	})

	It("Drop slot", func() {
		repl, err := CreateReplicator(dbURL, "droptestslot")
		Expect(err).Should(Succeed())
		repl.Start()
		// There may be duplicates, so always drain first
		drainReplication(repl)

		// Slot should be created pretty soon
		Eventually(func() string {
			row := db.QueryRow(
				"select slot_name from pg_replication_slots where slot_name = 'droptestslot'")
			var sn string
			err = row.Scan(&sn)
			Expect(err).Should(Succeed())
			return sn
		}).Should(Equal("droptestslot"))

		repl.StopAndDrop()

		// Slot should be destroyed pretty soon
		Eventually(func() error {
			row := db.QueryRow(
				"select slot_name from pg_replication_slots where slot_name = 'droptestslot'")
			var sn string
			return row.Scan(&sn)
		}, 5*time.Second).Should(MatchError(sql.ErrNoRows))
	})
})

func drainReplication(repl *Replicator) []*common.Change {
	// Just pull stuff until we get a bit of a delay
	var maxLSN uint64
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
