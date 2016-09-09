package pgclient

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Connection Tests", func() {
	It("Basic Connect", func() {
		if dbURL == "" {
			return
		}
		conn, err := Connect(dbURL)
		Expect(err).Should(Succeed())
		Expect(conn).ShouldNot(BeNil())
		conn.Close()
	})

	It("Connect to bad host", func() {
		_, err := Connect("postgres://badhost:9999/postgres")
		Expect(err).ShouldNot(Succeed())
	})

	It("Connect to bad database", func() {
		if dbURL == "" {
			return
		}
		_, err := Connect("postgres://postgres@localhost/baddatabase")
		Expect(err).ShouldNot(Succeed())
		fmt.Fprintf(GinkgoWriter, "Error from database: %s\n", err)
	})

	PIt("Basic Connect with SSL", func() {
		if dbURL == "" {
			return
		}
		conn, err := Connect(dbURL + "?ssl=true")
		Expect(err).Should(Succeed())
		Expect(conn).ShouldNot(BeNil())
		conn.Close()
	})
})
