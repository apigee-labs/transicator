/*
Copyright 2016 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package replication

import (
	"database/sql"
	"fmt"

	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

/*
    ip_list   _actual latest commit txid of snapshot
          v  /
x x x x - - x - - -
        ^     ^
        xmin  xmax

xmin - Earliest transaction ID (txid) that is still active. All earlier
transactions will either be committed and visible, or rolled back and dead.

xmax - First as-yet-unassigned txid. All txids greater than or equal to this
are not yet started as of the time of the snapshot, and thus invisible.

xip_list - Active txids at the time of the snapshot. The list includes only
those active txids between xmin and xmax; there might be active txids higher
than xmax. A txid that is xmin <= txid < xmax and not in this list was already
completed at the time of the snapshot, and thus either visible or dead
according to its commit status. The list does not include txids of
subtransactions.
*/

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
		repl, err := CreateReplicator(dbURL, "txidtestslot")
		Expect(err).Should(Succeed())
		repl.SetChangeFilter(filterChange)
		repl.Start()
		defer repl.Stop()
		defer DropSlot(dbURL, "txidtestslot")

		var snaps []string

		snaps = append(snaps, execCommands([]string{
			"a", "1",
			"a", "commit",
			"b", "snapshot"}))

		snaps = append(snaps, execCommands([]string{
			"a", "2",
			"a", "commit",
			"b", "snapshot"}))

		snaps = append(snaps, execCommands([]string{
			"a", "3a",
			"b", "3b",
			"x", "snapshot",
			"b", "commit",
			"a", "commit"}))

		snaps = append(snaps, execCommands([]string{
			"a", "4a",
			"b", "4b",
			"c", "4c",
			"d", "4d",
			"b", "rollback",
			"x", "snapshot",
			"a", "commit",
			"c", "commit",
			"d", "commit"}))

		snaps = append(snaps, execCommands([]string{
			"a", "5a",
			"b", "5b",
			"c", "5c",
			"d", "5d",
			"e", "5e",
			"c", "commit",
			"x", "snapshot",
			"a", "commit",
			"b", "commit",
			"d", "commit",
			"e", "commit"}))

		snaps = append(snaps, execCommands([]string{
			"a", "6a",
			"b", "6b",
			"c", "6c",
			"d", "6d",
			"e", "6e",
			"f", "6f",
			"c", "commit",
			"a", "commit",
			"x", "snapshot",
			"b", "commit",
			"d", "commit",
			"f", "commit",
			"e", "commit"}))

		snaps = append(snaps, execCommands([]string{
			"a", "7a",
			"b", "7b",
			"c", "7c",
			"d", "7d",
			"e", "7e",
			"f", "7f",
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
		if !ss.Contains(change.TransactionID) {
			ret = append(ret, i)
		}
	}
	return ret
}

func execCommands(commands []string) string {
	trans := make(map[string]*sql.Tx)
	var snap string
	var err error
	insert, err := db.Prepare("insert into txid_test (id, testid) values($1, $2)")
	Expect(err).Should(Succeed())
	defer insert.Close()

	defer func() {
		// In case there is a problem with the input, roll back everything
		for _, t := range trans {
			t.Rollback()
		}
	}()

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
			tinsert := tran.Stmt(insert)
			_, err = tinsert.Exec(sql, testID)
			tinsert.Close()
			Expect(err).Should(Succeed())
		}
	}
	return snap
}
