package replication

import (
	"fmt"

	"github.com/30x/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Logical Decoding Tests", func() {
	BeforeEach(func() {
		doExecute(
			"select * from pg_create_logical_replication_slot(" +
				"'transicator_decoding_test', 'transicator_output')")
	})

	AfterEach(func() {
		doExecute(
			"select * from pg_drop_replication_slot('transicator_decoding_test')")
	})

	It("Basic insert", func() {
		doExecute(
			"insert into transicator_test (id) values ('basic insert')")
		changes := getChanges()
		Expect(len(changes)).Should(Equal(1))
		Expect(changes[0].CommitIndex).Should(BeEquivalentTo(0))
		Expect(changes[0].Operation).Should(Equal(1))
		Expect(changes[0].NewRow).ShouldNot(BeNil())
		Expect(changes[0].NewRow["id"].Value).Should(Equal("basic insert"))
	})

	It("Basic delete", func() {
		doExecute(
			"insert into transicator_test (id) values ('basic delete')")
		doExecute(
			"delete from transicator_test where id = 'basic delete'")

		changes := getChanges()
		Expect(len(changes)).Should(Equal(2))
		Expect(changes[0].CommitIndex).Should(BeEquivalentTo(0))
		Expect(changes[0].Operation).Should(Equal(1))
		Expect(changes[0].NewRow).ShouldNot(BeNil())
		Expect(changes[0].NewRow["id"].Value).Should(Equal("basic delete"))
		Expect(changes[1].CommitIndex).Should(BeEquivalentTo(0))
		Expect(changes[1].Operation).Should(Equal(3))
		Expect(changes[1].OldRow).ShouldNot(BeNil())
		Expect(changes[1].OldRow["id"].Value).Should(Equal("basic delete"))
		Expect(changes[1].CommitSequence).Should(BeNumerically(">", changes[0].CommitSequence))
	})

	It("Basic update", func() {
		doExecute(
			"insert into transicator_test (id, varchars) values ('basic update', 'one')")
		doExecute(
			"update transicator_test set varchars = 'two' where id = 'basic update'")

		changes := getChanges()
		Expect(len(changes)).Should(Equal(2))
		Expect(changes[0].CommitIndex).Should(BeEquivalentTo(0))
		Expect(changes[0].Operation).Should(Equal(1))
		Expect(changes[0].NewRow).ShouldNot(BeNil())
		Expect(changes[0].NewRow["id"].Value).Should(Equal("basic update"))
		Expect(changes[0].NewRow["varchars"].Value).Should(Equal("one"))

		Expect(changes[1].CommitIndex).Should(BeEquivalentTo(0))
		Expect(changes[1].Operation).Should(Equal(2))
		// TODO what table changes do we need to get old data?
		//Expect(changes[1].Old).ShouldNot(BeNil())
		//Expect(changes[1].Old["id"]).Should(Equal("basic update"))
		Expect(changes[1].NewRow).ShouldNot(BeNil())
		Expect(changes[1].NewRow["id"].Value).Should(Equal("basic update"))
		Expect(changes[1].NewRow["varchars"].Value).Should(Equal("two"))
		Expect(changes[1].CommitSequence).Should(BeNumerically(">", changes[0].CommitSequence))
	})

	It("Transactional insert", func() {
		doExecute("begin")
		doExecute(
			"insert into transicator_test (id, varchars) values ('basic tran 1', 'one')")
		doExecute(
			"insert into transicator_test (id, varchars) values ('basic tran 2', 'two')")
		doExecute("commit")

		changes := getChanges()
		Expect(len(changes)).Should(Equal(2))
		Expect(changes[0].CommitIndex).Should(BeEquivalentTo(0))
		Expect(changes[0].Operation).Should(Equal("insert"))
		Expect(changes[0].NewRow).ShouldNot(BeNil())
		Expect(changes[0].NewRow["id"].Value).Should(Equal("basic tran 1"))
		Expect(changes[0].NewRow["varchars"].Value).Should(Equal("one"))

		Expect(changes[1].CommitIndex).Should(BeEquivalentTo(1))
		Expect(changes[1].Operation).Should(Equal("insert"))
		Expect(changes[1].NewRow).ShouldNot(BeNil())
		Expect(changes[1].NewRow["id"].Value).Should(Equal("basic tran 2"))
		Expect(changes[1].NewRow["varchars"].Value).Should(Equal("two"))

		Expect(changes[1].CommitSequence).Should(Equal(changes[0].CommitSequence))
	})

	It("Transactional rollback", func() {
		doExecute("begin")
		doExecute(
			"insert into transicator_test (id, varchars) values ('rollback tran 1', 'one')")
		doExecute(
			"insert into transicator_test (id, varchars) values ('rollback tran 2', 'two')")
		doExecute("rollback")

		changes := getChanges()
		Expect(changes).Should(BeEmpty())
	})

	It("Datatypes", func() {
		doExecute(
			"insert into transicator_test values (" +
				"'datatypes', true, 'hello', 'world', 123, 456, 789, " +
				"3.14, 3.14159, " +
				"'1970-02-13', '04:05:06', '1970-02-13 04:05:06', '1970-02-13 04:05:06')")

		changes := getChanges()
		Expect(len(changes)).Should(Equal(1))
		Expect(changes[0].CommitIndex).Should(BeEquivalentTo(0))
		Expect(changes[0].Operation).Should(Equal("insert"))
		Expect(changes[0].NewRow).ShouldNot(BeNil())

		d := changes[0].NewRow
		Expect(d["id"].Value).Should(Equal("datatypes"))
		Expect(d["bool"].Value).Should(Equal(true))
		Expect(d["chars"].Value).Should(MatchRegexp("^hello.*"))
		Expect(d["varchars"].Value).Should(Equal("world"))
		Expect(d["int"].Value).Should(BeEquivalentTo(123))
		Expect(d["smallint"].Value).Should(BeEquivalentTo(456))
		Expect(d["bigint"].Value).Should(BeEquivalentTo(789))
		Expect(d["float"].Value).Should(BeEquivalentTo(3.14))
		Expect(d["double"].Value).Should(BeEquivalentTo(3.14159))
	})

	It("Interleaving", func() {
		doExecute("begin")
		doExecute("insert into transicator_test (id) values ('interleave 1')")
		// This is a separate connection, aka separate transaction
		doExecute2(
			"insert into transicator_test (id) values ('interleave 2')")
		doExecute("commit")

		// Ensure we got changes in commit order, not insertion order
		changes := getChanges()
		Expect(len(changes)).Should(Equal(2))
		Expect(changes[0].CommitIndex).Should(BeEquivalentTo(0))
		Expect(changes[0].Operation).Should(Equal("insert"))
		Expect(changes[0].NewRow).ShouldNot(BeNil())
		Expect(changes[0].NewRow["id"].Value).Should(Equal("interleave 2"))
		Expect(changes[1].CommitIndex).Should(BeEquivalentTo(0))
		Expect(changes[1].Operation).Should(Equal("insert"))
		Expect(changes[1].NewRow).ShouldNot(BeNil())
		Expect(changes[1].NewRow["id"].Value).Should(Equal("interleave 1"))
		Expect(changes[1].CommitSequence).Should(BeNumerically(">", changes[0].CommitSequence))
	})

	It("Interleaving 2", func() {
		doExecute("begin")
		doExecute("insert into transicator_test (id) values ('interleave 1-1')")
		// This is a separate connection, aka separate transaction
		doExecute2("begin")
		doExecute2("insert into transicator_test (id) values ('interleave 2-1')")
		doExecute("insert into transicator_test (id) values ('interleave 1-2')")
		doExecute2("insert into transicator_test (id) values ('interleave 2-2')")
		doExecute2("commit")
		doExecute("commit")

		// Ensure we got changes in commit order, not insertion order
		changes := getChanges()
		Expect(len(changes)).Should(Equal(4))
		Expect(changes[0].CommitIndex).Should(BeEquivalentTo(0))
		Expect(changes[0].NewRow["id"].Value).Should(Equal("interleave 2-1"))
		Expect(changes[1].CommitIndex).Should(BeEquivalentTo(1))
		Expect(changes[1].NewRow["id"].Value).Should(Equal("interleave 2-2"))
		Expect(changes[1].CommitSequence).Should(Equal(changes[0].CommitSequence))

		Expect(changes[2].CommitIndex).Should(BeEquivalentTo(0))
		Expect(changes[2].NewRow["id"].Value).Should(Equal("interleave 1-1"))
		Expect(changes[3].CommitIndex).Should(BeEquivalentTo(1))
		Expect(changes[3].NewRow["id"].Value).Should(Equal("interleave 1-2"))
		Expect(changes[2].CommitSequence).Should(Equal(changes[3].CommitSequence))
		Expect(changes[2].CommitSequence).Should(BeNumerically(">", changes[0].CommitSequence))
	})
})

func getChanges() []common.Change {
	schema, rows, err := dbConn.SimpleQuery(
		"select * from pg_logical_slot_get_changes('transicator_decoding_test', NULL, NULL)")
	Expect(err).Should(Succeed())
	Expect(schema[2].Name).Should(Equal("data"))

	var changes []common.Change
	for _, row := range rows {
		fmt.Fprintf(GinkgoWriter, "Decoding %s\n", row[2])
		change, err := common.UnmarshalChange([]byte(row[2]))
		Expect(err).Should(Succeed())
		changes = append(changes, *change)
	}
	return changes
}
