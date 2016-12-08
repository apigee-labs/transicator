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
package pgclient

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Query Tests", func() {
	var conn *PgConnection

	BeforeEach(func() {
		if dbURL != "" {
			var err error
			conn, err = Connect(dbURL)
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
		fmt.Fprintf(GinkgoWriter, "Row type: %d\n", desc[0].Type)

		Expect(len(rows)).Should(Equal(1))
		Expect(len(rows[0])).Should(Equal(1))
		Expect(rows[0][0]).Should(Equal("postgres"))
	})
})
