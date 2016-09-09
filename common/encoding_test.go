package common

import (
	"io/ioutil"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestEncoding(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Encoding suite")
}

var _ = Describe("Encoding Tests", func() {
	It("Parse snapshot", func() {
		s := readFile("./testfiles/snapshot.json")
		ss, err := UnmarshalSnapshot(s)
		Expect(err).Should(Succeed())

		ms := ss.Marshal()
		Expect(ms).Should(MatchJSON(s))
	})

	It("Parse change list", func() {
		s := readFile("./testfiles/changelist.json")
		ss, err := UnmarshalChangeList(s)
		Expect(err).Should(Succeed())

		ms := ss.Marshal()
		Expect(ms).Should(MatchJSON(s))
	})
})

func readFile(name string) []byte {
	f, err := os.Open(name)
	Expect(err).Should(Succeed())
	defer f.Close()

	buf, err := ioutil.ReadAll(f)
	Expect(err).Should(Succeed())
	return buf
}
