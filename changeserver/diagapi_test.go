package main

import (
	"fmt"
	"io/ioutil"
	"net/http"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Diag API tests", func() {
	It("Stack test", func() {
		url := fmt.Sprintf("%s/diagnostics/stack", baseURL)
		resp, err := http.Get(url)
		Expect(err).Should(Succeed())
		defer resp.Body.Close()

		bod, err := ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())
		Expect(len(bod)).ShouldNot(BeZero())
	})
})
