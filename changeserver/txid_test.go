package main

import (
	"fmt"
	"strconv"

	"github.com/30x/transicator/replication"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("TXID Tests", func() {
	var lastIx int64 = 999
	var lastCommitSeq int64
	var lastTxid int32

	BeforeEach(func() {
		// Establish a baseline transaction so we know where to start
		lastIx++
		executeSQL(fmt.Sprintf("insert into changeserver_test values(%d, 'txid')", lastIx))
		Eventually(func() bool {
			changes := getChanges(fmt.Sprintf("%s/changes?tag=txid&since=%d",
				baseURL, lastCommitSeq))
			if len(changes) == 0 {
				return false
			}
			Expect(len(changes)).Should(BeNumerically(">=", 1))
			lastCommitSeq = changes[len(changes)-1].CommitSequence
			lastTxid = changes[len(changes)-1].Txid
			fmt.Fprintf(GinkgoWriter, "Txid = %d seq = %d\n", lastTxid, lastCommitSeq)
			return true
		}).Should(BeTrue())
	})

	/*
	   Do a query while three transactions are in progress. xmin and xmax should
	   equal the previous transaction ID plus one.
	*/
	It("Three transactions", func() {
		conn1 := connectDB()
		defer conn1.Close()
		conn2 := connectDB()
		defer conn2.Close()
		conn3 := connectDB()
		defer conn3.Close()

		conn1.SimpleQuery("begin")
		lastIx++
		conn1.SimpleQuery(fmt.Sprintf("insert into changeserver_test values(%d, 'txid')", lastIx))
		conn2.SimpleQuery("begin")
		lastIx++
		conn2.SimpleQuery(fmt.Sprintf("insert into changeserver_test values(%d, 'txid')", lastIx))
		conn3.SimpleQuery("begin")
		lastIx++
		conn3.SimpleQuery(fmt.Sprintf("insert into changeserver_test values(%d, 'txid')", lastIx))

		// Snapshot IDs should not be more than they were before we started txns
		xmin, xmax, xips := executeSQLTransaction("select * from changeserver_test where tag = 'txid'")
		Expect(xmin).Should(Equal(lastTxid + 1))
		Expect(xmax).Should(Equal(lastTxid + 1))
		Expect(xips).Should(BeEmpty())

		conn1.SimpleQuery("commit")
		conn2.SimpleQuery("commit")
		conn3.SimpleQuery("commit")

		var changes []replication.EncodedChange
		Eventually(func() int {
			changes = getChanges(fmt.Sprintf(
				"%s/changes?tag=txid&since=%d", baseURL, lastCommitSeq))
			return len(changes)
		}).Should(Equal(3))

		// All new txns should get higher txids
		for _, c := range changes {
			fmt.Fprintf(GinkgoWriter, "TXID %d: %v\n", c.Txid, c.New)
			Expect(c.Txid).Should(BeNumerically(">", lastTxid))
		}
	})

	/*
	   Do a query while three transactions are in progress. xmin and xmax should
	   equal the previous transaction ID plus one.
	*/
	It("Three transactions One", func() {
		conn1 := connectDB()
		defer conn1.Close()
		conn2 := connectDB()
		defer conn2.Close()
		conn3 := connectDB()
		defer conn3.Close()

		conn1.SimpleQuery("begin")
		lastIx++
		conn1.SimpleQuery(fmt.Sprintf("insert into changeserver_test values(%d, 'txid')", lastIx))
		conn2.SimpleQuery("begin")
		lastIx++
		conn2.SimpleQuery(fmt.Sprintf("insert into changeserver_test values(%d, 'txid')", lastIx))
		conn3.SimpleQuery("begin")
		lastIx++
		conn3.SimpleQuery(fmt.Sprintf("insert into changeserver_test values(%d, 'txid')", lastIx))

		_, id1Rows, _ := conn1.SimpleQuery("select * from txid_current(); commit")
		id1, _ := strconv.ParseInt(id1Rows[0][0], 10, 32)
		fmt.Fprintf(GinkgoWriter, "Committed %d\n", id1)

		// Snapshot IDs should be one higher than that commit.
		xmin, xmax, xips := executeSQLTransaction("select * from changeserver_test where tag = 'txid'")
		Expect(xmin).Should(BeEquivalentTo(id1 + 1))
		Expect(xmax).Should(BeEquivalentTo(id1 + 1))
		Expect(xips).Should(BeEmpty())

		conn2.SimpleQuery("commit")
		conn3.SimpleQuery("commit")

		var changes []replication.EncodedChange
		Eventually(func() int {
			changes = getChanges(fmt.Sprintf(
				"%s/changes?tag=txid&since=%d", baseURL, lastCommitSeq))
			return len(changes)
		}).Should(BeNumerically(">=", 3))

		// All new txns should get higher txids
		for _, c := range changes {
			fmt.Fprintf(GinkgoWriter, "TXID %d: %v\n", c.Txid, c.New)
			//Expect(c.Txid).Should(BeNumerically(">", lastTxid))
		}
	})

	It("Three transactions One OOO", func() {
		conn1 := connectDB()
		defer conn1.Close()
		conn2 := connectDB()
		defer conn2.Close()
		conn3 := connectDB()
		defer conn3.Close()

		conn1.SimpleQuery("begin")
		lastIx++
		conn1.SimpleQuery(fmt.Sprintf("insert into changeserver_test values(%d, 'txid')", lastIx))
		conn2.SimpleQuery("begin")
		lastIx++
		conn2.SimpleQuery(fmt.Sprintf("insert into changeserver_test values(%d, 'txid')", lastIx))
		conn3.SimpleQuery("begin")
		lastIx++
		conn3.SimpleQuery(fmt.Sprintf("insert into changeserver_test values(%d, 'txid')", lastIx))

		_, id1Rows, _ := conn3.SimpleQuery("select * from txid_current(); commit")
		id1, _ := strconv.ParseInt(id1Rows[0][0], 10, 32)
		fmt.Fprintf(GinkgoWriter, "Committed %d\n", id1)

		// Snapshot IDs should cover what's in range
		xmin, xmax, xips := executeSQLTransaction("select * from changeserver_test where tag = 'txid'")
		Expect(xmin).Should(BeNumerically("<", xmax))
		Expect(xmax).Should(BeEquivalentTo(id1 + 1))
		Expect(len(xips)).Should(Equal(2))

		conn2.SimpleQuery("commit")
		conn1.SimpleQuery("commit")

		var changes []replication.EncodedChange
		Eventually(func() int {
			changes = getChanges(fmt.Sprintf(
				"%s/changes?tag=txid&since=%d", baseURL, lastCommitSeq))
			return len(changes)
		}).Should(BeNumerically(">=", 3))

		// All new txns should get higher txids
		for _, c := range changes {
			fmt.Fprintf(GinkgoWriter, "TXID %d: %v\n", c.Txid, c.New)
			//Expect(c.Txid).Should(BeNumerically(">", lastTxid))
		}
	})

	It("Three transactions Two OOO", func() {
		conn1 := connectDB()
		defer conn1.Close()
		conn2 := connectDB()
		defer conn2.Close()
		conn3 := connectDB()
		defer conn3.Close()

		conn1.SimpleQuery("begin")
		lastIx++
		conn1.SimpleQuery(fmt.Sprintf("insert into changeserver_test values(%d, 'txid')", lastIx))
		conn2.SimpleQuery("begin")
		lastIx++
		conn2.SimpleQuery(fmt.Sprintf("insert into changeserver_test values(%d, 'txid')", lastIx))
		conn3.SimpleQuery("begin")
		lastIx++
		conn3.SimpleQuery(fmt.Sprintf("insert into changeserver_test values(%d, 'txid')", lastIx))

		_, id1Rows, _ := conn3.SimpleQuery("select * from txid_current(); commit")
		id1, _ := strconv.ParseInt(id1Rows[0][0], 10, 32)
		fmt.Fprintf(GinkgoWriter, "Committed %d\n", id1)

		// Snapshot IDs should cover what's in range
		xmin, xmax, xips := executeSQLTransaction("select * from changeserver_test where tag = 'txid'")
		Expect(xmin).Should(BeNumerically("<", xmax))
		Expect(xmax).Should(BeEquivalentTo(id1 + 1))
		Expect(len(xips)).Should(Equal(2))

		conn1.SimpleQuery("commit")
		conn2.SimpleQuery("commit")

		var changes []replication.EncodedChange
		Eventually(func() int {
			changes = getChanges(fmt.Sprintf(
				"%s/changes?tag=txid&since=%d", baseURL, lastCommitSeq))
			return len(changes)
		}).Should(BeNumerically(">=", 3))

		// All new txns should get higher txids
		for _, c := range changes {
			fmt.Fprintf(GinkgoWriter, "TXID %d: %v\n", c.Txid, c.New)
			//Expect(c.Txid).Should(BeNumerically(">", lastTxid))
		}
	})
})
