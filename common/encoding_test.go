package common

import (
	"bytes"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"testing/quick"

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
		// Parse file from JSON, then un-parse, and result should be same JSON
		s := readFile("./testfiles/snapshot.json")
		ss, err := UnmarshalSnapshot(s)
		Expect(err).Should(Succeed())
		ms := ss.Marshal()
		Expect(ms).Should(MatchJSON(s))

		// Re-marshal using protobuf, then re-marshal, and result should
		// still be same JSON.
		buf := &bytes.Buffer{}
		err = ss.MarshalProto(buf)
		Expect(err).Should(Succeed())

		ss2, err := UnmarshalSnapshotProto(buf)
		Expect(err).Should(Succeed())
		ms2 := ss2.Marshal()
		Expect(ms2).Should(MatchJSON(s))
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

	It("Encode change", func() {
		err := quick.Check(func(iv int64, sv string, bv bool) bool {
			nr := make(map[string]*ColumnVal)
			nr["iv"] = &ColumnVal{
				Value: iv,
			}
			nr["sv"] = &ColumnVal{
				Value: sv,
			}
			nr["bv"] = &ColumnVal{
				Value: bv,
			}
			c := &Change{
				NewRow: Row(nr),
			}

			// Protobufs should come out totally the same both ways
			buf := c.MarshalProto()
			br, err := UnmarshalChangeProto(buf)
			Expect(err).Should(Succeed())
			Expect(br.NewRow["iv"].Value).Should(Equal(iv))
			Expect(br.NewRow["sv"].Value).Should(Equal(sv))
			Expect(br.NewRow["bv"].Value).Should(Equal(bv))

			// JSON should see everything turned in to a string
			buf = c.Marshal()
			br, err = UnmarshalChange(buf)
			Expect(err).Should(Succeed())
			isv := strconv.FormatInt(iv, 10)
			Expect(br.NewRow["iv"].Value).Should(Equal(isv))
			Expect(br.NewRow["sv"].Value).Should(Equal(sv))
			bsv := strconv.FormatBool(bv)
			Expect(br.NewRow["bv"].Value).Should(Equal(bsv))
			return true
		}, nil)
		Expect(err).Should(Succeed())
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
