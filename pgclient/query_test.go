package pgclient

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Query Tests", func() {
	var conn *PgConnection

	BeforeEach(func() {
		if dbHost != "" {
			var err error
			conn, err = Connect(fmt.Sprintf("postgres://postgres@%s/postgres", dbHost))
			Expect(err).Should(Succeed())
			Expect(conn).ShouldNot(BeNil())
		}
	})

	AfterEach(func() {
		if conn != nil {
			conn.Close()
		}
	})

	It("Basic Query", func() {
		if conn == nil {
			return
		}

		desc, rows, err := conn.SimpleQuery("select * from current_database()")
		Expect(err).Should(Succeed())
		Expect(len(desc)).Should(Equal(1))
		Expect(desc[0].Name).Should(Equal("current_database"))
		fmt.Printf("Row type: %d\n", desc[0].Type)

		Expect(len(rows)).Should(Equal(1))
		Expect(len(rows[0])).Should(Equal(1))
		Expect(rows[0][0]).Should(Equal("postgres"))
	})
})
