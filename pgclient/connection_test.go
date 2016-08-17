package pgclient

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Connection Tests", func() {
	It("Basic Connect", func() {
		if dbHost == "" {
			return
		}
		conn, err := Connect(dbHost, "postgres", "postgres", nil)
		Expect(err).Should(Succeed())
		Expect(conn).ShouldNot(BeNil())
		conn.Close()
	})

	It("Connect to bad host", func() {
		_, err := Connect("localhost:99999", "postgres", "postgres", nil)
		Expect(err).ShouldNot(Succeed())
	})

	It("Connect to bad database", func() {
		if dbHost == "" {
			return
		}
		_, err := Connect(dbHost, "postgres", "NOTFOUND", nil)
		Expect(err).ShouldNot(Succeed())
		fmt.Fprintf(GinkgoWriter, "Error from database: %s\n", err)
	})
})
