package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Miscellaneous tests", func() {
	It("Sanitize slot test", func() {
		Expect(sanitizeSlotName("foo")).Should(Equal("foo"))
		Expect(sanitizeSlotName("foo bar")).Should(Equal("foobar"))
		Expect(sanitizeSlotName("foo-bar")).Should(Equal("foo_bar"))
		Expect(sanitizeSlotName("foo_bar baz%^&*-123")).Should(Equal("foo_barbaz_123"))
		Expect(sanitizeSlotName(" *&^*&^!")).Should(Equal(""))
	})
})
