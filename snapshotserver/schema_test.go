/*
Copyright 2016 The Transicator Authors

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
package snapshotserver

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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
		Expect(st.columns[14].name).Should(Equal("timestampp"))
		Expect(st.columns[14].typid).Should(Equal(1114))
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

	It("Check timestamp format", func() {
		// Just make sure that timestamp formatting works the way that we expect
		now := time.Now()
		fmt.Fprintf(GinkgoWriter, "Time is now %s\n", now)

		// Need to round down to millisecond granularity
		nowRoundUTC := roundTime(now).UTC()

		nowStr := now.UTC().Format(sqliteTimestampFormat)
		fmt.Fprintf(GinkgoWriter, "  rounded = %s\n", nowRoundUTC)
		fmt.Fprintf(GinkgoWriter, "          = %s\n", nowStr)

		nowParsed, err := time.Parse(sqliteTimestampFormat, nowStr)
		Expect(err).Should(Succeed())
		Expect(nowParsed).Should(Equal(nowRoundUTC))

		nowParsed, err = time.Parse(time.RFC3339Nano, nowStr)
		Expect(err).Should(Succeed())
		Expect(nowParsed).Should(Equal(nowRoundUTC))
	})
})

// roundTime rounds a timestamp down to millisecond granularity,
// because that's what SQLite will use.
func roundTime(t time.Time) time.Time {
	unixTime := (t.UnixNano() / 1000000) * 1000000
	return time.Unix(0, unixTime)
}
