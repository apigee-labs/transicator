package goscaffold

import (
	"net/http"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Accept header tests", func() {
	It("No nothing", func() {
		Expect(SelectMediaType(
			makeRequest(""), []string{})).Should(BeEmpty())
	})

	It("No header, one choice", func() {
		Expect(SelectMediaType(
			makeRequest(""),
			[]string{"text/plain"})).Should(Equal("text/plain"))
	})

	It("No header, two choices", func() {
		Expect(SelectMediaType(
			makeRequest(""),
			[]string{"text/plain", "text/xml"})).Should(Equal("text/plain"))
	})

	It("No header, two choices 2", func() {
		Expect(SelectMediaType(
			makeRequest(""),
			[]string{"application/json", "text/plain"})).Should(Equal("application/json"))
	})

	It("Accept all, two choices", func() {
		Expect(SelectMediaType(
			makeRequest("*/*"),
			[]string{"text/plain", "text/xml"})).Should(Equal("text/plain"))
	})

	It("Accept all, two choices 2", func() {
		Expect(SelectMediaType(
			makeRequest("*/*"),
			[]string{"application/json", "text/plain"})).Should(Equal("application/json"))
	})

	It("One Header, two choices", func() {
		Expect(SelectMediaType(
			makeRequest("application/json"),
			[]string{"text/plain", "application/json"})).Should(Equal("application/json"))
	})

	It("One Header, bad choices", func() {
		Expect(SelectMediaType(
			makeRequest("application/json"),
			[]string{"text/plain", "application/xml"})).Should(Equal(""))
	})

	It("audio/basic", func() {
		req := makeRequest("audio/*; q=0.2, audio/basic")
		Expect(SelectMediaType(req,
			[]string{"audio/mp3", "audio/basic"})).Should(Equal("audio/basic"))
		Expect(SelectMediaType(req,
			[]string{"audio/mp3", "text/plain"})).Should(Equal("audio/mp3"))
		Expect(SelectMediaType(req,
			[]string{"text/plain", "application/xml"})).Should(Equal(""))
	})

	It("text/plain", func() {
		req := makeRequest("text/plain; q=0.5, text/html, text/x-dvi; q=0.8, text/x-c")
		Expect(SelectMediaType(req,
			[]string{"text/html", "text/x-c"})).Should(Equal("text/html"))
		Expect(SelectMediaType(req,
			[]string{"text/x-c", "text/html"})).Should(Equal("text/html"))
		Expect(SelectMediaType(req,
			[]string{"text/x-dvi", "text/plain"})).Should(Equal("text/x-dvi"))
		Expect(SelectMediaType(req,
			[]string{"text/plain"})).Should(Equal("text/plain"))
		Expect(SelectMediaType(req,
			[]string{"application/json"})).Should(Equal(""))
	})

	It("text/plain 2", func() {
		req := makeRequest(
			`text/*;q=0.3, text/html;q=0.7, text/html;level=1,
		  text/html;level=2;q=0.4, */*;q=0.5`)
		Expect(SelectMediaType(req,
			[]string{"text/plain", "text/html;level=1", "text/html", "image/jpeg", "text/html;level=2", "foo/bar"})).
			Should(Equal("text/html;level=1"))
		Expect(SelectMediaType(req,
			[]string{"image/jpeg", "text/html", "text/plain", "text/html;level=2", "foo/bar"})).
			Should(Equal("text/html"))
		Expect(SelectMediaType(req,
			[]string{"image/jpeg", "text/plain", "text/html;level=2"})).
			Should(Equal("image/jpeg"))
		Expect(SelectMediaType(req,
			[]string{"text/plain", "text/html;level=2"})).
			Should(Equal("text/html;level=2"))
	})

	It("Weird Browser example", func() {
		req := makeRequest("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8,application/json")
		Expect(SelectMediaType(req,
			[]string{"application/xml", "text/xml", "text/html", "application/json"})).
			Should(Equal("text/html"))
		Expect(SelectMediaType(req,
			[]string{"application/xml", "application/json"})).
			Should(Equal("application/json"))
	})

	It("Firefox Example", func() {
		req := makeRequest("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
		Expect(SelectMediaType(req,
			[]string{"application/xml", "text/xml", "text/html", "text/plain", "application/json"})).
			Should(Equal("text/html"))
		Expect(SelectMediaType(req,
			[]string{"application/xml", "application/json"})).
			Should(Equal("application/xml"))
		Expect(SelectMediaType(req,
			[]string{"application/json"})).
			Should(Equal("application/json"))
	})

	It("Parse accept part", func() {
		ap := parseAcceptPart("text/plain", 0)
		Expect(ap.major).Should(Equal("text"))
		Expect(ap.minor).Should(Equal("plain"))
		Expect(ap.precedence).Should(BeEquivalentTo(1.0))
		ap = parseAcceptPart("text/plain;q=0.2", 0)
		Expect(ap.major).Should(Equal("text"))
		Expect(ap.minor).Should(Equal("plain"))
		Expect(ap.precedence).Should(BeNumerically("~", 0.2))
		ap = parseAcceptPart("text/plain; q=0.2", 0)
		Expect(ap.major).Should(Equal("text"))
		Expect(ap.minor).Should(Equal("plain"))
		Expect(ap.precedence).Should(BeNumerically("~", 0.2))
		ap = parseAcceptPart("text/plain;level=1", 0)
		Expect(ap.major).Should(Equal("text"))
		Expect(ap.minor).Should(Equal("plain;level=1"))
		Expect(ap.precedence).Should(BeEquivalentTo(1.0))
		ap = parseAcceptPart("text/plain;level=1;q=0.5", 0)
		Expect(ap.major).Should(Equal("text"))
		Expect(ap.minor).Should(Equal("plain;level=1"))
		Expect(ap.precedence).Should(BeNumerically("~", 0.5))
		ap = parseAcceptPart("text/plain;level=1;q=0.5;foo=bar", 0)
		Expect(ap.major).Should(Equal("text"))
		Expect(ap.minor).Should(Equal("plain;level=1;foo=bar"))
		Expect(ap.precedence).Should(BeNumerically("~", 0.5))
	})
})

func makeRequest(accept string) *http.Request {
	return &http.Request{
		Header: http.Header{
			"Accept": []string{accept},
		},
	}
}
