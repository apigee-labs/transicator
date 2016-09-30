package pgclient

import (
	"bytes"
	"database/sql"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Driver tests", func() {
	var db *sql.DB

	BeforeEach(func() {
		if dbURL != "" {
			var err error
			db, err = sql.Open("transicator", dbURL)
			Expect(err).Should(Succeed())
		}
	})

	AfterEach(func() {
		if db != nil {
			db.Exec("truncate table client_test")
			db.Close()
		}
	})

	It("Basic connect", func() {
		if db == nil {
			return
		}

		err := db.Ping()
		Expect(err).Should(Succeed())
	})

	It("Basic SQL", func() {
		if db == nil {
			return
		}

		result, err := db.Exec("insert into client_test (id) values (1)")
		Expect(err).Should(Succeed())
		Expect(result.RowsAffected()).Should(BeEquivalentTo(1))

		rows, err := db.Query("select id from client_test")
		Expect(err).Should(Succeed())
		cols, err := rows.Columns()
		Expect(err).Should(Succeed())
		Expect(len(cols)).Should(Equal(1))
		Expect(cols[0]).Should(Equal("id"))

		hasNext := rows.Next()
		Expect(hasNext).Should(BeTrue())
		var rowVal int
		err = rows.Scan(&rowVal)
		Expect(err).Should(Succeed())
		Expect(rowVal).Should(Equal(1))
		hasNext = rows.Next()
		Expect(hasNext).Should(BeFalse())
		err = rows.Close()
		Expect(err).Should(Succeed())
	})

	// TODO need to fix date and time types.
	PIt("Insert with args", func() {
		if db == nil {
			return
		}

		ts := time.Now()
		hello := []byte("Hello!")
		_, err := db.Exec(`
			insert into client_test (id, int, double, timestamp, yesno, blob)
			values($1, $2, $3, $4, $5, $6)`, "one", 123, 3.14, ts, true, hello)
		Expect(err).Should(Succeed())

		rows, err := db.Query("select id, int, double, timestamp, yesno, blob from client_test")
		Expect(err).Should(Succeed())
		cols, err := rows.Columns()
		Expect(err).Should(Succeed())
		Expect(len(cols)).Should(Equal(6))
		Expect(cols[0]).Should(Equal("id"))
		Expect(cols[1]).Should(Equal("int"))
		Expect(cols[2]).Should(Equal("double"))
		Expect(cols[3]).Should(Equal("timestamp"))
		Expect(cols[4]).Should(Equal("yesno"))
		Expect(cols[5]).Should(Equal("blob"))

		hasNext := rows.Next()
		Expect(hasNext).Should(BeTrue())
		var id string
		var i int
		var d float64
		var nts time.Time
		var yesno bool
		var blob []byte
		err = rows.Scan(&id, &i, &d, &nts, &yesno, &blob)
		Expect(err).Should(Succeed())
		Expect(id).Should(Equal("one"))
		Expect(i).Should(Equal(123))
		Expect(d).Should(Equal(3.14))
		Expect(nts).Should(Equal(ts))
		Expect(yesno).Should(BeTrue())
		Expect(bytes.Equal(hello, blob)).Should(BeTrue())
		hasNext = rows.Next()
		Expect(hasNext).Should(BeFalse())
		err = rows.Close()
		Expect(err).Should(Succeed())
	})

	It("Select with args", func() {
		if db == nil {
			return
		}

		_, err := db.Exec(`
			insert into client_test (id, int, double)
			values($1, $2, $3)`, 1, 123, 3.14)
		Expect(err).Should(Succeed())

		rows, err := db.Query(`
			select id, int, double from client_test
			where id = $1`, 1)
		Expect(err).Should(Succeed())
		cols, err := rows.Columns()
		Expect(err).Should(Succeed())
		Expect(len(cols)).Should(Equal(3))
		Expect(cols[0]).Should(Equal("id"))
		Expect(cols[1]).Should(Equal("int"))
		Expect(cols[2]).Should(Equal("double"))

		hasNext := rows.Next()
		Expect(hasNext).Should(BeTrue())
		var id int
		var i int
		var d float64
		err = rows.Scan(&id, &i, &d)
		Expect(err).Should(Succeed())
		Expect(id).Should(Equal(1))
		Expect(i).Should(Equal(123))
		Expect(d).Should(Equal(3.14))
		hasNext = rows.Next()
		Expect(hasNext).Should(BeFalse())
		err = rows.Close()
		Expect(err).Should(Succeed())
	})

	It("Select multiple rows", func() {
		if db == nil {
			return
		}

		stmt, err := db.Prepare(`
		insert into client_test (id, int, double)
		values($1, $2, $3)`)
		Expect(err).Should(Succeed())

		for i := 0; i < 10; i++ {
			_, err = stmt.Exec(i, 123, 3.14)
			Expect(err).Should(Succeed())
		}

		err = stmt.Close()
		Expect(err).Should(Succeed())

		rows, err := db.Query("select id from client_test")
		Expect(err).Should(Succeed())

		rowCount := 0
		for rows.Next() {
			var id string
			err = rows.Scan(&id)
			Expect(err).Should(Succeed())
			Expect(id).Should(Equal(strconv.Itoa(rowCount)))
			rowCount++
		}

		Expect(rowCount).Should(Equal(10))
		err = rows.Close()
		Expect(err).Should(Succeed())
	})

	It("Transactions", func() {
		if db == nil {
			return
		}

		tx, err := db.Begin()
		Expect(err).Should(Succeed())

		_, err = tx.Exec(`
			insert into client_test (id)
			values($1)`, -1)
		Expect(err).Should(Succeed())

		err = tx.Rollback()
		Expect(err).Should(Succeed())

		tx, err = db.Begin()
		Expect(err).Should(Succeed())

		_, err = tx.Exec(`
					insert into client_test (id)
					values($1)`, 1)
		Expect(err).Should(Succeed())

		err = tx.Commit()
		Expect(err).Should(Succeed())

		rows, err := db.Query("select id from client_test")
		Expect(err).Should(Succeed())

		hasNext := rows.Next()
		Expect(hasNext).Should(BeTrue())
		var id int
		err = rows.Scan(&id)
		Expect(err).Should(Succeed())
		Expect(id).Should(Equal(1))
		hasNext = rows.Next()
		Expect(hasNext).Should(BeFalse())
		err = rows.Close()
		Expect(err).Should(Succeed())
	})

	It("Big query", func() {
		if db == nil {
			return
		}

		is, err := db.Prepare("insert into client_test (id, int) values ($1, $2)")
		Expect(err).Should(Succeed())
		defer is.Close()
		ss, err := db.Prepare("select id, int from client_test")
		Expect(err).Should(Succeed())
		defer ss.Close()

		var i int
		for i = 0; i < 999; i++ {
			_, err = is.Exec(strconv.Itoa(i), i)
			Expect(err).Should(Succeed())
		}

		// Fetch and ensure that we got all the rows
		rows, err := ss.Query()
		Expect(err).Should(Succeed())
		for i = 0; rows.Next(); i++ {
			var id string
			var val int
			err = rows.Scan(&id, &val)
			Expect(val).Should(Equal(i))
			Expect(id).Should(Equal(strconv.Itoa(i)))
		}
		Expect(i).Should(Equal(999))
		err = rows.Close()
		Expect(err).Should(Succeed())

		// Improperly use Exec and make sure that it works too
		_, err = ss.Exec()
		Expect(err).Should(Succeed())

		// Fetch but close the rows after only fetching a few
		rows, err = ss.Query()
		Expect(err).Should(Succeed())
		for i = 0; i < 123 && rows.Next(); i++ {
			var id string
			var val int
			err = rows.Scan(&id, &val)
			Expect(val).Should(Equal(i))
			Expect(id).Should(Equal(strconv.Itoa(i)))
		}
		Expect(i).Should(Equal(123))
		err = rows.Close()
		Expect(err).Should(Succeed())

		// Fetch but close the rows before doing anything
		rows, err = ss.Query()
		Expect(err).Should(Succeed())
		err = rows.Close()
		Expect(err).Should(Succeed())

		// Fetch using FetchOne which should do the same thing
		row := ss.QueryRow()
		var id string
		var val int
		err = row.Scan(&id, &val)
		Expect(err).Should(Succeed())
		Expect(id).Should(Equal("0"))
		Expect(val).Should(Equal(0))

		// Fetch one more time just for grins.
		rows, err = ss.Query()
		Expect(err).Should(Succeed())
		for i = 0; rows.Next(); i++ {
			var id string
			var val int
			err = rows.Scan(&id, &val)
			Expect(val).Should(Equal(i))
			Expect(id).Should(Equal(strconv.Itoa(i)))
		}
		Expect(i).Should(Equal(999))
		err = rows.Close()
		Expect(err).Should(Succeed())
	})

	It("Bad SQL", func() {
		if db == nil {
			return
		}

		_, err := db.Exec("this is not sql")
		Expect(err).ShouldNot(Succeed())
		// make sure error does not block connection
		row, err := db.Query("select * from client_test")
		Expect(err).Should(Succeed())
		row.Close()
		_, err = db.Query("select * from $1 where bar = %2", "foo", "baz")
		Expect(err).ShouldNot(Succeed())
		// make sure error does not block connection
		row, err = db.Query("select * from client_test")
		Expect(err).Should(Succeed())
		row.Close()
		_, err = db.Exec("This is not sql either")
		Expect(err).ShouldNot(Succeed())
		_, err = db.Query("select neither this")
		Expect(err).ShouldNot(Succeed())
	})

	It("Bad params", func() {
		if db == nil {
			return
		}

		_, err := db.Exec("insert into client_test (id) values($1)")
		Expect(err).ShouldNot(Succeed())
		_, err = db.Query("select id from client_test where id = $1")
		Expect(err).ShouldNot(Succeed())
		st, err := db.Prepare("insert into client_test (id) values($1)")
		Expect(err).Should(Succeed())
		defer st.Close()
		_, err = st.Exec()
		Expect(err).ShouldNot(Succeed())
		_, err = st.Exec("one", "two", "three")
		Expect(err).ShouldNot(Succeed())
	})
})
