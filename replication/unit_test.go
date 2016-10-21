package replication

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Replication unit tests", func() {
	It("URL manipulation", func() {
		nu := addParam("http://localhost", "foo", "bar")
		Expect(nu).Should(Equal("http://localhost?foo=bar"))
		nu = addParam("http://localhost?", "foo", "bar")
		Expect(nu).Should(Equal("http://localhost?foo=bar"))
		nu = addParam("http://localhost?bar=baz", "foo", "bar")
		Expect(nu).Should(Equal("http://localhost?bar=baz&foo=bar"))
	})
})
