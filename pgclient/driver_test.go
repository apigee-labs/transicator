package pgclient

import (
	"database/sql"

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

		_, err := db.Exec("create table client_test (id varchar primary key)")
		Expect(err).Should(Succeed())
		defer func() {
			db.Exec("drop table client_test")
		}()

		_, err = db.Exec("insert into client_test values('one')")
		Expect(err).Should(Succeed())

		rows, err := db.Query("select * from client_test")
		Expect(err).Should(Succeed())
		cols, err := rows.Columns()
		Expect(err).Should(Succeed())
		Expect(len(cols)).Should(Equal(1))
		Expect(cols[0]).Should(Equal("id"))

		hasNext := rows.Next()
		Expect(hasNext).Should(BeTrue())
		var rowVal string
		err = rows.Scan(&rowVal)
		Expect(err).Should(Succeed())
		Expect(rowVal).Should(Equal("one"))
		hasNext = rows.Next()
		Expect(hasNext).Should(BeFalse())
		err = rows.Close()
		Expect(err).Should(Succeed())
	})
})
