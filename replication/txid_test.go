package replication

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("TXID Tests", func() {
	It("Running some transactions", func() {
		repl, err := Start(dbURL, "unittestslot")
		Expect(err).Should(Succeed())
		defer repl.Stop()

		fmt.Fprintf(GinkgoWriter, "One at a time 1\n")
		_, _, err = dbConn.SimpleQuery("insert into txid_test values('1')")
		Expect(err).Should(Succeed())
		getSnapshot()

		fmt.Fprintf(GinkgoWriter, "One at a time 2\n")
		_, _, err = dbConn.SimpleQuery("insert into txid_test values('2')")
		Expect(err).Should(Succeed())
		getSnapshot()

		fmt.Fprintf(GinkgoWriter, "Out of order 3a\n")
		_, _, err = dbConn.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn.SimpleQuery("insert into txid_test values('3a')")
		Expect(err).Should(Succeed())
		getSnapshot()

		fmt.Fprintf(GinkgoWriter, "Out of order 3b\n")
		_, _, err = dbConn2.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("insert into txid_test values('3b')")
		Expect(err).Should(Succeed())
		getSnapshot()

		fmt.Fprintf(GinkgoWriter, "Commit 3b\n")
		_, _, err = dbConn2.SimpleQuery("commit")
		Expect(err).Should(Succeed())
		getSnapshot()

		fmt.Fprintf(GinkgoWriter, "Commit 3a\n")
		_, _, err = dbConn.SimpleQuery("commit")
		Expect(err).Should(Succeed())
		getSnapshot()

		drainReplication(repl)
	})
})

func getSnapshot() {
	_, _, err := dbConn3.SimpleQuery("begin isolation level repeatable read")
	Expect(err).Should(Succeed())
	_, rows, err := dbConn3.SimpleQuery("select * from txid_current_snapshot()")
	Expect(err).Should(Succeed())
	fmt.Fprintf(GinkgoWriter, "Snapshot: %v\n", rows)
	_, _, err = dbConn3.SimpleQuery("commit")
	Expect(err).Should(Succeed())
}
