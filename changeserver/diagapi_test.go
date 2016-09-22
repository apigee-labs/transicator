package main

import (
	"fmt"
	"io/ioutil"
	"net/http"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Diag API tests", func() {
	It("Health OK", func() {
		url := fmt.Sprintf("%s/health", baseURL)

		resp, err := http.Get(url)
		Expect(err).Should(Succeed())
		bod, err := ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())
		resp.Body.Close()

		Expect(resp.StatusCode).Should(Equal(200))
		Expect(string(bod)).Should(Equal("OK"))
	})

	It("Mark health down", func() {
		url := fmt.Sprintf("%s/health", baseURL)
		resp, err := http.PostForm(url, map[string][]string{
			"up": []string{"false"},
		})
		Expect(err).Should(Succeed())
		Expect(resp.StatusCode).Should(Equal(200))

		resp, err = http.Get(url)
		Expect(err).Should(Succeed())
		bod, err := ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())
		resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(503))
		Expect(string(bod)).Should(Equal("Marked down"))

		resp, err = http.PostForm(url, map[string][]string{
			"up":     []string{"false"},
			"reason": []string{"verklempt"},
		})
		Expect(err).Should(Succeed())
		Expect(resp.StatusCode).Should(Equal(200))

		resp, err = http.Get(url)
		Expect(err).Should(Succeed())
		bod, err = ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())
		resp.Body.Close()
		Expect(resp.StatusCode).Should(Equal(503))
		Expect(string(bod)).Should(Equal("verklempt"))

		resp, err = http.PostForm(url, map[string][]string{
			"up": []string{"true"},
		})
		Expect(err).Should(Succeed())
		Expect(resp.StatusCode).Should(Equal(200))

		resp, err = http.Get(url)
		Expect(err).Should(Succeed())
		bod, err = ioutil.ReadAll(resp.Body)
		Expect(err).Should(Succeed())
		resp.Body.Close()

		Expect(resp.StatusCode).Should(Equal(200))
		Expect(string(bod)).Should(Equal("OK"))
	})

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
