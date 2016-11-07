package replication

import (
	"fmt"
	"testing/quick"

	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Logical Decoding Tests", func() {
	BeforeEach(func() {
		doExecute(
			"select * from pg_create_logical_replication_slot(" +
				"'transicator_decoding_test', 'transicator_output')")
		doExecute(
			"select * from pg_create_logical_replication_slot(" +
				"'transicator_decoding_test_binary', 'transicator_output')")
	})

	AfterEach(func() {
		db.Exec(
			"select * from pg_drop_replication_slot($1)", "transicator_decoding_test")
		db.Exec(
			"select * from pg_drop_replication_slot($1)", "transicator_decoding_test_binary")
	})

	It("Basic insert", func() {
		verify := func(changes []common.Change) {
			Expect(len(changes)).Should(Equal(1))
			Expect(changes[0].CommitIndex).Should(BeEquivalentTo(0))
			Expect(changes[0].Operation).Should(BeEquivalentTo(1))
			Expect(changes[0].NewRow).ShouldNot(BeNil())
			Expect(changes[0].NewRow["id"].Value).Should(Equal("basic insert"))
		}

		_, err := db.Exec(
			"insert into transicator_test (id, testid) values ('basic insert', $1)", testID)
		Expect(err).Should(Succeed())
		changes := getChanges(false)
		verify(changes)

		changes = getBinaryChanges()
		verify(changes)
	})

	It("Basic delete", func() {
		verify := func(changes []common.Change) {
			Expect(len(changes)).Should(Equal(2))
			Expect(changes[0].CommitIndex).Should(BeEquivalentTo(0))
			Expect(changes[0].Operation).Should(BeEquivalentTo(1))
			Expect(changes[0].NewRow).ShouldNot(BeNil())
			Expect(changes[0].NewRow["id"].Value).Should(Equal("basic delete"))
			Expect(changes[1].CommitIndex).Should(BeEquivalentTo(0))
			Expect(changes[1].Operation).Should(BeEquivalentTo(3))
			Expect(changes[1].OldRow).ShouldNot(BeNil())
			Expect(changes[1].OldRow["id"].Value).Should(Equal("basic delete"))
			Expect(changes[1].CommitSequence).Should(BeNumerically(">", changes[0].CommitSequence))
		}

		_, err := db.Exec(
			"insert into transicator_test (id, testid) values ('basic delete', $1)", testID)
		Expect(err).Should(Succeed())
		doExecute(
			"delete from transicator_test where id = 'basic delete'")

		verify(getChanges(false))
		verify(getBinaryChanges())
	})

	It("Basic update", func() {
		verify := func(changes []common.Change) {
			Expect(len(changes)).Should(Equal(2))
			Expect(changes[0].CommitIndex).Should(BeEquivalentTo(0))
			Expect(changes[0].Operation).Should(BeEquivalentTo(1))
			Expect(changes[0].NewRow).ShouldNot(BeNil())
			Expect(changes[0].NewRow["id"].Value).Should(Equal("basic update"))
			Expect(changes[0].NewRow["varchars"].Value).Should(Equal("one"))

			Expect(changes[1].CommitIndex).Should(BeEquivalentTo(0))
			Expect(changes[1].Operation).Should(BeEquivalentTo(2))
			// TODO what table changes do we need to get old data?
			//Expect(changes[1].Old).ShouldNot(BeNil())
			//Expect(changes[1].Old["id"]).Should(Equal("basic update"))
			Expect(changes[1].NewRow).ShouldNot(BeNil())
			Expect(changes[1].NewRow["id"].Value).Should(Equal("basic update"))
			Expect(changes[1].NewRow["varchars"].Value).Should(Equal("two"))
			Expect(changes[1].CommitSequence).Should(BeNumerically(">", changes[0].CommitSequence))
		}

		_, err := db.Exec(
			"insert into transicator_test (id, testid, varchars) values ('basic update', $1, 'one')", testID)
		Expect(err).Should(Succeed())
		doExecute(
			"update transicator_test set varchars = 'two' where id = 'basic update'")

		verify(getChanges(false))
		verify(getBinaryChanges())
	})

	It("Transactional insert", func() {
		verify := func(changes []common.Change) {
			Expect(len(changes)).Should(Equal(2))
			Expect(changes[0].CommitIndex).Should(BeEquivalentTo(0))
			Expect(changes[0].Operation).Should(BeEquivalentTo(1))
			Expect(changes[0].NewRow).ShouldNot(BeNil())
			Expect(changes[0].NewRow["id"].Value).Should(Equal("basic tran 1"))
			Expect(changes[0].NewRow["varchars"].Value).Should(Equal("one"))

			Expect(changes[1].CommitIndex).Should(BeEquivalentTo(1))
			Expect(changes[1].Operation).Should(BeEquivalentTo(1))
			Expect(changes[1].NewRow).ShouldNot(BeNil())
			Expect(changes[1].NewRow["id"].Value).Should(Equal("basic tran 2"))
			Expect(changes[1].NewRow["varchars"].Value).Should(Equal("two"))

			Expect(changes[1].CommitSequence).Should(Equal(changes[0].CommitSequence))
		}

		tx, err := db.Begin()
		Expect(err).Should(Succeed())
		_, err = tx.Exec(
			"insert into transicator_test (id, testid, varchars) values ('basic tran 1', $1, 'one')",
			testID)
		Expect(err).Should(Succeed())
		_, err = tx.Exec(
			"insert into transicator_test (id, testid, varchars) values ('basic tran 2', $1, 'two')",
			testID)
		Expect(err).Should(Succeed())
		err = tx.Commit()
		Expect(err).Should(Succeed())

		verify(getChanges(false))
		verify(getBinaryChanges())
	})

	It("Transactional rollback", func() {
		tx, err := db.Begin()
		Expect(err).Should(Succeed())
		_, err = tx.Exec(
			"insert into transicator_test (id, testid, varchars) values ('rollback tran 1', $1, 'one')",
			testID)
		Expect(err).Should(Succeed())
		_, err = tx.Exec(
			"insert into transicator_test (id, testid, varchars) values ('rollback tran 2', $1, 'two')",
			testID)
		Expect(err).Should(Succeed())
		err = tx.Rollback()
		Expect(err).Should(Succeed())

		changes := getChanges(false)
		Expect(changes).Should(BeEmpty())
		changes = getBinaryChanges()
		Expect(changes).Should(BeEmpty())
	})

	It("Datatypes", func() {
		verify := func(changes []common.Change) {
			Expect(len(changes)).Should(Equal(1))
			Expect(changes[0].CommitIndex).Should(BeEquivalentTo(0))
			Expect(changes[0].Operation).Should(BeEquivalentTo(1))
			Expect(changes[0].NewRow).ShouldNot(BeNil())

			d := changes[0].NewRow
			var id string
			var bool bool
			var chars string
			var varchars string
			var integer int
			var smallint int32
			var bigint int64
			var float float32
			var double float64

			err := d.Get("id", &id)
			Expect(err).Should(Succeed())
			Expect(id).Should(Equal("datatypes"))
			err = d.Get("bool", &bool)
			Expect(err).Should(Succeed())
			Expect(bool).Should(BeTrue())
			err = d.Get("chars", &chars)
			Expect(err).Should(Succeed())
			Expect(chars).Should(MatchRegexp("^hello.*"))
			err = d.Get("varchars", &varchars)
			Expect(err).Should(Succeed())
			Expect(varchars).Should(Equal("world"))
			err = d.Get("int", &integer)
			Expect(err).Should(Succeed())
			Expect(integer).Should(BeEquivalentTo(123))
			err = d.Get("smallint", &smallint)
			Expect(err).Should(Succeed())
			Expect(smallint).Should(BeEquivalentTo(456))
			err = d.Get("bigint", &bigint)
			Expect(err).Should(Succeed())
			Expect(bigint).Should(BeEquivalentTo(789))
			err = d.Get("float", &float)
			Expect(err).Should(Succeed())
			Expect(float).Should(BeNumerically("~", 3.14, 1e-4))
			err = d.Get("double", &double)
			Expect(err).Should(Succeed())
			Expect(double).Should(BeEquivalentTo(3.14159))
		}

		_, err := db.Exec(`
			insert into transicator_test
			(id, testid, bool, chars, varchars, int, smallint, bigint, float, double, date,
			time, timestamp, timestampp)
		  values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
			"datatypes", testID, true, "hello", "world", 123, 456, 789,
			3.14, 3.14159,
			"1970-02-13", "04:05:06", "1970-02-13 04:05:06", "1970-02-13 04:05:06")
		Expect(err).Should(Succeed())

		verify(getChanges(false))
		verify(getBinaryChanges())
	})

	It("Binary data", func() {
		rawdata := []byte("Hello, World!")
		_, err :=
			db.Exec("insert into transicator_test (id, testid, rawdata) values($1, $2, $3)",
				"rawtest", testID, rawdata)
		Expect(err).Should(Succeed())

		// Ignore since binary will get screwed up
		getChanges(true)
		changes := getBinaryChanges()
		Expect(len(changes)).Should(Equal(1))
		nr := changes[0].NewRow
		var enddata []byte
		err = nr.Get("rawdata", &enddata)
		Expect(err).Should(Succeed())
		fmt.Fprintf(GinkgoWriter, "Start result: \"%s\"\n", string(rawdata))
		fmt.Fprintf(GinkgoWriter, "End result:   \"%s\"\n", string(enddata))
		Expect(enddata).Should(Equal(rawdata))
	})

	It("Interleaving", func() {
		verify := func(changes []common.Change) {
			Expect(len(changes)).Should(Equal(2))
			Expect(changes[0].CommitIndex).Should(BeEquivalentTo(0))
			Expect(changes[0].Operation).Should(BeEquivalentTo(1))
			Expect(changes[0].NewRow).ShouldNot(BeNil())
			Expect(changes[0].NewRow["id"].Value).Should(Equal("interleave 2"))
			Expect(changes[1].CommitIndex).Should(BeEquivalentTo(0))
			Expect(changes[1].Operation).Should(BeEquivalentTo(1))
			Expect(changes[1].NewRow).ShouldNot(BeNil())
			Expect(changes[1].NewRow["id"].Value).Should(Equal("interleave 1"))
			Expect(changes[1].CommitSequence).Should(BeNumerically(">", changes[0].CommitSequence))
		}

		tx, err := db.Begin()
		Expect(err).Should(Succeed())
		_, err = tx.Exec("insert into transicator_test (id, testid) values ('interleave 1', $1)",
			testID)
		Expect(err).Should(Succeed())
		// This is a separate connection, aka separate transaction
		_, err = db.Exec(
			"insert into transicator_test (id, testid) values ('interleave 2', $1)",
			testID)
		Expect(err).Should(Succeed())
		err = tx.Commit()
		Expect(err).Should(Succeed())

		// Ensure we got changes in commit order, not insertion order
		verify(getChanges(false))
		verify(getBinaryChanges())
	})

	It("Interleaving 2", func() {
		verify := func(changes []common.Change) {
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
		}

		tx1, err := db.Begin()
		Expect(err).Should(Succeed())
		_, err = tx1.Exec("insert into transicator_test (id, testid) values ('interleave 1-1', $1)", testID)
		Expect(err).Should(Succeed())

		tx2, err := db.Begin()
		Expect(err).Should(Succeed())
		_, err = tx2.Exec("insert into transicator_test (id, testid) values ('interleave 2-1', $1)", testID)
		Expect(err).Should(Succeed())
		_, err = tx1.Exec("insert into transicator_test (id, testid) values ('interleave 1-2', $1)", testID)
		Expect(err).Should(Succeed())
		_, err = tx2.Exec("insert into transicator_test (id, testid) values ('interleave 2-2', $1)", testID)
		Expect(err).Should(Succeed())
		err = tx2.Commit()
		Expect(err).Should(Succeed())
		err = tx1.Commit()
		Expect(err).Should(Succeed())

		// Ensure we got changes in commit order, not insertion order
		verify(getChanges(false))
		verify(getBinaryChanges())
	})

	It("Many strings", func() {
		i := 0
		ps, err := db.Prepare("insert into transicator_test (id, testid, varchars) values ($1, $2, $3)")
		Expect(err).Should(Succeed())
		defer ps.Close()

		err = quick.Check(func(val string) bool {
			_, err = ps.Exec(fmt.Sprintf("string-%d", i), testID, val)
			i++
			Expect(err).Should(Succeed())
			getChanges(true)
			changes := getBinaryChanges()
			Expect(len(changes)).Should(Equal(1))
			r := changes[0].NewRow
			var newVal string
			err = r.Get("varchars", &newVal)
			Expect(err).Should(Succeed())
			Expect(newVal).Should(Equal(val))
			return true
		}, nil)
		Expect(err).Should(Succeed())
	})

	It("Many arrays", func() {
		i := 0
		ps, err := db.Prepare("insert into transicator_test (id, testid, rawdata) values ($1, $2, $3)")
		Expect(err).Should(Succeed())
		defer ps.Close()

		err = quick.Check(func(val []byte) bool {
			_, err = ps.Exec(fmt.Sprintf("byte-%d", i), testID, val)
			i++
			Expect(err).Should(Succeed())
			getChanges(true)
			changes := getBinaryChanges()
			Expect(len(changes)).Should(Equal(1))
			r := changes[0].NewRow
			var newVal []byte
			err = r.Get("rawdata", &newVal)
			Expect(err).Should(Succeed())
			Expect(newVal).Should(Equal(val))
			return true
		}, nil)
		Expect(err).Should(Succeed())
	})

	It("Many numbers", func() {
		i := 0
		ps, err := db.Prepare(
			"insert into transicator_test (id, testid, bool, int, smallint, bigint) values ($1, $2, $3, $4, $5, $6)")
		Expect(err).Should(Succeed())
		defer ps.Close()

		err = quick.Check(func(bv bool, iv int32, sv int16, bi int64) bool {
			_, err = ps.Exec(fmt.Sprintf("nums-%d", i), testID, bv, iv, sv, bi)
			i++
			Expect(err).Should(Succeed())
			getChanges(true)
			changes := getBinaryChanges()
			Expect(len(changes)).Should(Equal(1))
			r := changes[0].NewRow
			var nbv bool
			var niv int32
			var nsv int16
			var nbi int64
			err = r.Get("bool", &nbv)
			Expect(err).Should(Succeed())
			Expect(nbv).Should(Equal(bv))
			err = r.Get("int", &niv)
			Expect(err).Should(Succeed())
			Expect(niv).Should(Equal(iv))
			err = r.Get("smallint", &nsv)
			Expect(err).Should(Succeed())
			Expect(nsv).Should(Equal(sv))
			err = r.Get("bigint", &nbi)
			Expect(err).Should(Succeed())
			Expect(nbi).Should(Equal(bi))
			return true
		}, nil)
		Expect(err).Should(Succeed())
	})
})

func getChanges(ignoreBody bool) []common.Change {
	rows, err := db.Query(
		"select * from pg_logical_slot_get_changes('transicator_decoding_test', NULL, NULL)")
	Expect(err).Should(Succeed())
	cols, err := rows.Columns()
	Expect(err).Should(Succeed())
	Expect(cols[2]).Should(Equal("data"))

	var changes []common.Change
	for rows.Next() {
		var ignore1 string
		var ignore2 string
		var changeDesc string
		err = rows.Scan(&ignore1, &ignore2, &changeDesc)
		Expect(err).Should(Succeed())
		if !ignoreBody {
			fmt.Fprintf(GinkgoWriter, "Decoding %s\n", changeDesc)
			change, err := common.UnmarshalChange([]byte(changeDesc))
			if err != nil {
				fmt.Printf("Error decoding JSON: %s\n", changeDesc)
			} else if filterChange(change) {
				changes = append(changes, *change)
			}
		}
	}
	return changes
}

func getBinaryChanges() []common.Change {
	rows, err := db.Query(
		"select * from pg_logical_slot_get_binary_changes('transicator_decoding_test_binary', NULL, NULL, 'protobuf', 'true')")
	Expect(err).Should(Succeed())
	cols, err := rows.Columns()
	Expect(err).Should(Succeed())
	Expect(cols[2]).Should(Equal("data"))

	var changes []common.Change
	for rows.Next() {
		var ignore1 string
		var ignore2 string
		var changeDesc []byte
		err = rows.Scan(&ignore1, &ignore2, &changeDesc)
		Expect(err).Should(Succeed())
		fmt.Fprintf(GinkgoWriter, "Decoding %d bytes\n", len(changeDesc))
		change, err := common.UnmarshalChangeProto(changeDesc)
		Expect(err).Should(Succeed())
		if filterChange(change) {
			changes = append(changes, *change)
		}
	}
	return changes
}
