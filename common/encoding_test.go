package common

import (
	"io/ioutil"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
)

func TestEncoding(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter("../test-reports/common.xml")
	RunSpecsWithDefaultAndCustomReporters(t, "Encoding suite", []Reporter{junitReporter})
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

	It("Encode changes", func() {
		s := readFile("./testfiles/changelist.json")
		ss, err := UnmarshalChangeList(s)
		Expect(err).Should(Succeed())

		for _, change := range ss.Changes {
			proto := change.MarshalProto()
			nc, err := UnmarshalChangeProto(proto)
			Expect(err).Should(Succeed())
			Expect(nc.Sequence).Should(Equal(change.Sequence))
			Expect(nc.ChangeSequence).Should(Equal(change.ChangeSequence))
			Expect(nc.CommitSequence).Should(Equal(change.CommitSequence))
			Expect(nc.CommitIndex).Should(Equal(change.CommitIndex))
			Expect(nc.TransactionID).Should(Equal(change.TransactionID))
			Expect(nc.Operation).Should(Equal(change.Operation))
			Expect(nc.Table).Should(Equal(change.Table))
			Expect(nc.NewRow).Should(Equal(change.NewRow))
			Expect(nc.OldRow).Should(Equal(change.OldRow))
		}
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
