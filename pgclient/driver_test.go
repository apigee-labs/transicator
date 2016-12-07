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
package pgclient

import (
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

	It("Empty", func() {
		if db == nil {
			return
		}

		_, err := db.Exec("")
		Expect(err).Should(Succeed())

		rows, err := db.Query("")
		Expect(err).Should(Succeed())
		Expect(rows.Next()).Should(BeFalse())
		err = rows.Close()
		Expect(err).Should(Succeed())
	})

	It("Select with args", func() {
		if db == nil {
			return
		}

		_, err := db.Exec(`
			insert into client_test (id, string, int, double)
			values($1, $2, $3, $4)`, 1, nil, 123, 3.14)
		Expect(err).Should(Succeed())

		rows, err := db.Query(`
			select id, string, int, double from client_test
			where id = $1`, 1)
		Expect(err).Should(Succeed())
		cols, err := rows.Columns()
		Expect(err).Should(Succeed())
		Expect(len(cols)).Should(Equal(4))
		Expect(cols[0]).Should(Equal("id"))
		Expect(cols[1]).Should(Equal("string"))
		Expect(cols[2]).Should(Equal("int"))
		Expect(cols[3]).Should(Equal("double"))

		hasNext := rows.Next()
		Expect(hasNext).Should(BeTrue())
		var id int
		var vc string
		var i int
		var d float64
		err = rows.Scan(&id, &vc, &i, &d)
		Expect(err).Should(Succeed())
		Expect(id).Should(Equal(1))
		Expect(vc).Should(BeEmpty())
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

	It("Isolation level", func() {
		if dbURL == "" {
			return
		}

		idb, err := sql.Open("transicator", dbURL)
		Expect(err).Should(Succeed())
		defer idb.Close()

		idb.Driver().(*PgDriver).SetIsolationLevel("serializable")

		tx, err := idb.Begin()
		Expect(err).Should(Succeed())
		_, err = db.Exec("insert into client_test (id, string) values ($1, $2)",
			1, "one")
		Expect(err).Should(Succeed())
		err = tx.Rollback()
		Expect(err).Should(Succeed())
	})

	It("Extended column names", func() {
		if dbURL == "" {
			return
		}

		idb, err := sql.Open("transicator", dbURL)
		Expect(err).Should(Succeed())
		defer idb.Close()

		idb.Driver().(*PgDriver).SetExtendedColumnNames(true)

		rows, err := idb.Query("select id, string, int from client_test")
		Expect(err).Should(Succeed())
		cols, err := rows.Columns()
		Expect(err).Should(Succeed())
		defer rows.Close()

		Expect(cols[0]).Should(Equal("id:23"))
		Expect(cols[1]).Should(Equal("string:1043"))
		Expect(cols[2]).Should(Equal("int:20"))
	})

	It("Query Timeout", func() {
		if dbURL == "" {
			return
		}

		tdb, err := sql.Open("transicator", dbURL)
		Expect(err).Should(Succeed())
		defer tdb.Close()
		tdb.Driver().(*PgDriver).SetReadTimeout(time.Second)

		_, err = tdb.Exec(`
			create table block_test(id text primary key)
		`)
		Expect(err).Should(Succeed())

		// Start a transaction that will get a shared lock on the table
		tx, err := tdb.Begin()
		Expect(err).Should(Succeed())
		_, err = tx.Exec("insert into block_test values('one')")
		Expect(err).Should(Succeed())

		// Try to drop the table. Expect this to fail with a timeout error
		doneChan := make(chan error)

		go func() {
			_, gerr := tdb.Exec("drop table block_test")
			doneChan <- gerr
		}()

		// We expect that thing to block with an error.
		// TODO verify what the error actually is
		Eventually(doneChan, 5*time.Second).Should(Receive())

		// Commit the transaction and now lock should be droppped
		err = tx.Commit()
		Expect(err).Should(Succeed())
		_, err = tdb.Exec("drop table block_test")
		Expect(err).Should(Succeed())
	})
})
