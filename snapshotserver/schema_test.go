package snapshotserver

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"fmt"
)

var _ = Describe("Schema tests", func() {
	It("Verify schema", func() {
		tx, err := db.Begin()
		Expect(err).Should(Succeed())
		defer tx.Commit()

		s, err := enumeratePgTables(tx)
		Expect(err).Should(Succeed())
		Expect(s["public.snapshot_test"]).ShouldNot(BeNil())
		Expect(s["not.found"]).Should(BeNil())

		st := s["public.snapshot_test"]
		Expect(st.schema).Should(Equal("public"))
		Expect(st.name).Should(Equal("snapshot_test"))
		Expect(st.columns[0].name).Should(Equal("id"))
		Expect(st.columns[0].typid).Should(Equal(1043))
		Expect(st.columns[13].name).Should(Equal("timestampp"))
		Expect(st.columns[13].typid).Should(Equal(1114))
		Expect(st.primaryKeys[0]).Should(Equal("id"))
		Expect(st.hasSelector).Should(BeTrue())
		fmt.Printf("SQL: %s\n", makeSqliteTableSQL(st))

		a := s["public.app"]
		Expect(a).ShouldNot(BeNil())
		Expect(a.columns[0].name).Should(Equal("org"))
		Expect(a.columns[7].name).Should(Equal("_change_selector"))
		Expect(a.primaryKeys[0]).Should(Equal("id"))
		Expect(a.primaryKeys[1]).Should(Equal("_change_selector"))
		fmt.Printf("SQL: %s\n", makeSqliteTableSQL(a))
	})
})
