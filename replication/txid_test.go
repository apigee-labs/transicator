package replication

import (
	"database/sql"
	"fmt"

	"github.com/30x/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("TXID Tests", func() {
	It("Snapshot parse", func() {
		snap, err := MakeSnapshot("1:1:")
		Expect(err).Should(Succeed())
		Expect(snap.Xmax).Should(BeEquivalentTo(1))
		Expect(snap.Xmin).Should(BeEquivalentTo(1))
		Expect(snap.Xips).Should(BeEmpty())

		snap, err = MakeSnapshot("1:1:2")
		Expect(err).Should(Succeed())
		Expect(snap.Xmax).Should(BeEquivalentTo(1))
		Expect(snap.Xmin).Should(BeEquivalentTo(1))
		Expect(len(snap.Xips)).Should(Equal(1))
		Expect(snap.Xips[2]).Should(BeTrue())

		snap, err = MakeSnapshot("1:1:2,3")
		Expect(err).Should(Succeed())
		Expect(snap.Xmax).Should(BeEquivalentTo(1))
		Expect(snap.Xmin).Should(BeEquivalentTo(1))
		Expect(len(snap.Xips)).Should(Equal(2))
		Expect(snap.Xips[2]).Should(BeTrue())
		Expect(snap.Xips[3]).Should(BeTrue())
	})

	/*
		This test verifies that no matter what kind of transaction sequence we throw
		at the database, given a set of snapshot TXID values, we can resolve to a
		single place in the logical replication stream.
	*/
	It("Gather snapshots", func() {
		repl, err := Start(dbURL, "unittestslot")
		Expect(err).Should(Succeed())
		defer repl.Stop()

		var snaps []string

		snaps = append(snaps, execCommands([]string{
			"a", "insert into txid_test values('1')",
			"a", "commit",
			"b", "snapshot"}))

		snaps = append(snaps, execCommands([]string{
			"a", "insert into txid_test values('2')",
			"a", "commit",
			"b", "snapshot"}))

		snaps = append(snaps, execCommands([]string{
			"a", "insert into txid_test values('3a')",
			"b", "insert into txid_test values('3b')",
			"x", "snapshot",
			"b", "commit",
			"a", "commit"}))

		snaps = append(snaps, execCommands([]string{
			"a", "insert into txid_test values('4a')",
			"b", "insert into txid_test values('4b')",
			"c", "insert into txid_test values('4c')",
			"d", "insert into txid_test values('4d')",
			"b", "rollback",
			"x", "snapshot",
			"a", "commit",
			"c", "commit",
			"d", "commit"}))

		snaps = append(snaps, execCommands([]string{
			"a", "insert into txid_test values('5a')",
			"b", "insert into txid_test values('5b')",
			"c", "insert into txid_test values('5c')",
			"d", "insert into txid_test values('5d')",
			"e", "insert into txid_test values('5e')",
			"c", "commit",
			"x", "snapshot",
			"a", "commit",
			"b", "commit",
			"d", "commit",
			"e", "commit"}))

		snaps = append(snaps, execCommands([]string{
			"a", "insert into txid_test values('6a')",
			"b", "insert into txid_test values('6b')",
			"c", "insert into txid_test values('6c')",
			"d", "insert into txid_test values('6d')",
			"e", "insert into txid_test values('6e')",
			"f", "insert into txid_test values('6f')",
			"c", "commit",
			"a", "commit",
			"x", "snapshot",
			"b", "commit",
			"d", "commit",
			"f", "commit",
			"e", "commit"}))

		snaps = append(snaps, execCommands([]string{
			"a", "insert into txid_test values('7a')",
			"b", "insert into txid_test values('7b')",
			"c", "insert into txid_test values('7c')",
			"d", "insert into txid_test values('7d')",
			"e", "insert into txid_test values('7e')",
			"f", "insert into txid_test values('7f')",
			"c", "rollback",
			"a", "rollback",
			"d", "rollback",
			"f", "rollback",
			"x", "snapshot",
			"b", "commit",
			"e", "commit"}))

		changes := drainReplication(repl)

		fmt.Fprintf(GinkgoWriter, "Changes:\n")
		for i, change := range changes {
			fmt.Fprintf(GinkgoWriter, "%d Change %d Commit %d TXID %d Row = %v\n",
				i, change.ChangeSequence, change.CommitSequence, change.TransactionID,
				change.NewRow["id"])
		}

		for i, snap := range snaps {
			filtered := filterChanges(changes, snap)
			fmt.Fprintf(GinkgoWriter, "%d: %s: %v\n", i, snap, filtered)
		}
	})
})

func assertSequential(vals []int) {
	if len(vals) > 1 {
		for i := 1; i < len(vals); i++ {
			Expect(vals[i]).Should(Equal(vals[i-1] + 1))
		}
	}
}

func getSnapshot() string {
	tx, err := db.Begin()
	Expect(err).Should(Succeed())
	row := tx.QueryRow("select * from txid_current_snapshot()")

	var snap string
	err = row.Scan(&snap)
	Expect(err).Should(Succeed())

	err = tx.Commit()
	Expect(err).Should(Succeed())
	return snap
}

func filterChanges(changes []*common.Change, snap string) []int {
	var ret []int

	ss, err := MakeSnapshot(snap)
	Expect(err).Should(Succeed())

	for i, change := range changes {
		if !ss.Contains(uint32(change.TransactionID)) {
			ret = append(ret, i)
		}
	}
	return ret
}

func execCommands(commands []string) string {
	trans := make(map[string]*sql.Tx)
	var snap string
	var err error

	for i := 0; i < len(commands); i += 2 {
		txn := commands[i]
		sql := commands[i+1]

		tran := trans[txn]
		if tran == nil {
			tran, err = db.Begin()
			Expect(err).Should(Succeed())
			trans[txn] = tran
		}

		if sql == "commit" {
			err = tran.Commit()
			Expect(err).Should(Succeed())
		} else if sql == "rollback" {
			err = tran.Rollback()
			Expect(err).Should(Succeed())
		} else if sql == "snapshot" {
			row := tran.QueryRow("select * from txid_current_snapshot()")
			err = row.Scan(&snap)
			Expect(err).Should(Succeed())
		} else {
			_, err = tran.Exec(sql)
			Expect(err).Should(Succeed())
		}
	}
	return snap
}
