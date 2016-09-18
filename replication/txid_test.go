package replication

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/30x/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var snapRe = regexp.MustCompile("^([0-9]*):([0-9]*):(([0-9],?)+)?$")

var _ = Describe("TXID Tests", func() {
	It("Snapshot parse", func() {
		min, max, ips, err := parseSnap("1:1:")
		Expect(err).Should(Succeed())
		Expect(max).Should(BeEquivalentTo(1))
		Expect(min).Should(BeEquivalentTo(1))
		Expect(ips).Should(BeEmpty())

		min, max, ips, err = parseSnap("1:1:2")
		Expect(err).Should(Succeed())
		Expect(max).Should(BeEquivalentTo(1))
		Expect(min).Should(BeEquivalentTo(1))
		Expect(len(ips)).Should(Equal(1))
		Expect(ips[0]).Should(BeEquivalentTo(2))

		min, max, ips, err = parseSnap("1:1:2,3")
		Expect(err).Should(Succeed())
		Expect(max).Should(BeEquivalentTo(1))
		Expect(min).Should(BeEquivalentTo(1))
		Expect(len(ips)).Should(Equal(2))
		Expect(ips[0]).Should(BeEquivalentTo(2))
		Expect(ips[1]).Should(BeEquivalentTo(3))
	})

	It("Running some transactions", func() {
		repl, err := Start(dbURL, "unittestslot")
		Expect(err).Should(Succeed())
		defer repl.Stop()

		fmt.Fprintf(GinkgoWriter, "One at a time 1\n")
		_, _, err = dbConn.SimpleQuery("insert into txid_test values('1')")
		Expect(err).Should(Succeed())
		snap1 := getSnapshot()

		fmt.Fprintf(GinkgoWriter, "One at a time 2\n")
		_, _, err = dbConn.SimpleQuery("insert into txid_test values('2')")
		Expect(err).Should(Succeed())
		snap2 := getSnapshot()

		fmt.Fprintf(GinkgoWriter, "Out of order 3a\n")
		_, _, err = dbConn.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn.SimpleQuery("insert into txid_test values('3a')")
		Expect(err).Should(Succeed())
		snap3a := getSnapshot()

		fmt.Fprintf(GinkgoWriter, "Out of order 3b\n")
		_, _, err = dbConn2.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("insert into txid_test values('3b')")
		Expect(err).Should(Succeed())
		snap3b := getSnapshot()

		fmt.Fprintf(GinkgoWriter, "Commit 3b\n")
		_, _, err = dbConn2.SimpleQuery("commit")
		Expect(err).Should(Succeed())
		snap3b2 := getSnapshot()

		fmt.Fprintf(GinkgoWriter, "Commit 3a\n")
		_, _, err = dbConn.SimpleQuery("commit")
		Expect(err).Should(Succeed())
		snap3a2 := getSnapshot()

		fmt.Fprintf(GinkgoWriter, "Three in parallel\n")
		_, _, err = dbConn.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn3.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn.SimpleQuery("insert into txid_test values('4a')")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("insert into txid_test values('4b')")
		Expect(err).Should(Succeed())
		_, _, err = dbConn3.SimpleQuery("insert into txid_test values('4c')")
		Expect(err).Should(Succeed())
		snap4 := getSnapshot()
		_, _, err = dbConn.SimpleQuery("commit")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("commit")
		Expect(err).Should(Succeed())
		_, _, err = dbConn3.SimpleQuery("commit")
		Expect(err).Should(Succeed())

		fmt.Fprintf(GinkgoWriter, "Three in parallel one rolled back\n")
		_, _, err = dbConn.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn3.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn.SimpleQuery("insert into txid_test values('5a')")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("insert into txid_test values('5b')")
		Expect(err).Should(Succeed())
		_, _, err = dbConn3.SimpleQuery("insert into txid_test values('5c')")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("rollback")
		Expect(err).Should(Succeed())
		snap5 := getSnapshot()
		_, _, err = dbConn.SimpleQuery("commit")
		Expect(err).Should(Succeed())
		_, _, err = dbConn3.SimpleQuery("commit")
		Expect(err).Should(Succeed())

		fmt.Fprintf(GinkgoWriter, "Three in parallel one rolled back 2\n")
		_, _, err = dbConn.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn3.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn.SimpleQuery("insert into txid_test values('6a')")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("insert into txid_test values('6b')")
		Expect(err).Should(Succeed())
		_, _, err = dbConn3.SimpleQuery("insert into txid_test values('6c')")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("rollback")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("insert into txid_test values('6d')")
		Expect(err).Should(Succeed())
		snap6 := getSnapshot()
		_, _, err = dbConn.SimpleQuery("commit")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("commit")
		Expect(err).Should(Succeed())
		_, _, err = dbConn3.SimpleQuery("commit")
		Expect(err).Should(Succeed())

		fmt.Fprintf(GinkgoWriter, "Three in parallel one committed 2\n")
		_, _, err = dbConn.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn3.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn.SimpleQuery("insert into txid_test values('7a')")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("insert into txid_test values('7b')")
		Expect(err).Should(Succeed())
		_, _, err = dbConn3.SimpleQuery("insert into txid_test values('7c')")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("commit")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("begin")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("insert into txid_test values('7d')")
		Expect(err).Should(Succeed())
		snap7 := getSnapshot()
		_, _, err = dbConn.SimpleQuery("commit")
		Expect(err).Should(Succeed())
		_, _, err = dbConn2.SimpleQuery("commit")
		Expect(err).Should(Succeed())
		_, _, err = dbConn3.SimpleQuery("commit")
		Expect(err).Should(Succeed())

		changes := drainReplication(repl)

		fmt.Fprintf(GinkgoWriter, "Changes:\n")
		for i, change := range changes {
			fmt.Fprintf(GinkgoWriter, "%d Change %d Commit %d TXID %d Row = %s\n",
				i, change.ChangeSequence, change.CommitSequence, change.TransactionID,
				change.NewRow["id"].Value)
		}

		filtered := filterChanges(changes, snap1)
		fmt.Fprintf(GinkgoWriter, "Snap1: %s %v\n", snap1, filtered)
		filtered = filterChanges(changes, snap2)
		fmt.Fprintf(GinkgoWriter, "Snap2: %s %v\n", snap2, filtered)
		filtered = filterChanges(changes, snap3a)
		fmt.Fprintf(GinkgoWriter, "Snap3a: %s %v\n", snap3a, filtered)
		filtered = filterChanges(changes, snap3a2)
		fmt.Fprintf(GinkgoWriter, "Snap3a2: %s %v\n", snap3a2, filtered)
		filtered = filterChanges(changes, snap3b)
		fmt.Fprintf(GinkgoWriter, "Snap3b: %s %v\n", snap3b, filtered)
		filtered = filterChanges(changes, snap3b2)
		fmt.Fprintf(GinkgoWriter, "Snap3b2: %s %v\n", snap3b2, filtered)
		filtered = filterChanges(changes, snap4)
		fmt.Fprintf(GinkgoWriter, "Snap4: %s %v\n", snap4, filtered)
		filtered = filterChanges(changes, snap5)
		fmt.Fprintf(GinkgoWriter, "Snap5: %s %v\n", snap5, filtered)
		filtered = filterChanges(changes, snap6)
		fmt.Fprintf(GinkgoWriter, "Snap6: %s %v\n", snap6, filtered)
		filtered = filterChanges(changes, snap7)
		fmt.Fprintf(GinkgoWriter, "Snap7: %s %v\n", snap7, filtered)
	})
})

func getSnapshot() string {
	_, _, err := dbConn4.SimpleQuery("begin isolation level repeatable read")
	Expect(err).Should(Succeed())
	_, rows, err := dbConn4.SimpleQuery("select * from txid_current_snapshot()")
	Expect(err).Should(Succeed())
	_, _, err = dbConn4.SimpleQuery("commit")
	Expect(err).Should(Succeed())
	return rows[0][0]
}

func filterChanges(changes []*common.Change, snap string) []int {
	var ret []int

	for i, change := range changes {
		if !inSnap(snap, change.TransactionID) {
			ret = append(ret, i)
		}
	}
	return ret
}

/*
inSnapshot tells us whether a particular transaction's changes would
be visible in the specified snapshot.
*/
func inSnap(snap string, txid int64) bool {
	min, max, ips, err := parseSnap(snap)
	if err != nil {
		fmt.Fprintf(GinkgoWriter, "Invalid snapshot %s\n", snap)
		return false
	}

	if txid < min {
		return true
	}
	if txid >= max {
		return false
	}

	for _, ip := range ips {
		if txid == ip {
			return false
		}
	}

	return true
}

func parseSnap(snap string) (int64, int64, []int64, error) {
	pre := snapRe.FindStringSubmatch(snap)
	if pre == nil {
		return 0, 0, nil, errors.New("Invalid snapshot")
	}

	min, err := strconv.ParseInt(pre[1], 10, 64)
	if err != nil {
		return 0, 0, nil, err
	}
	max, err := strconv.ParseInt(pre[2], 10, 64)
	if err != nil {
		return 0, 0, nil, err
	}

	ipss := strings.Split(pre[3], ",")
	var ips []int64

	for _, s := range ipss {
		if s == "" {
			continue
		}
		ip, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, 0, nil, err
		}
		ips = append(ips, ip)
	}

	return min, max, ips, nil
}
