package storage

import (
	"fmt"
	"math"
	"time"

	"github.com/30x/changeagent/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Truncation test", func() {
	var truncateStore Storage

	BeforeEach(func() {
		stor, err := CreateRocksDBStorage("./truncatetest", 100)
		Expect(err).Should(Succeed())
		truncateStore = stor
	})

	AfterEach(func() {
		truncateStore.Close()
		err := truncateStore.Delete()
		Expect(err).Should(Succeed())
	})

	It("Truncate empty", func() {
		count, err := truncateStore.CalculateTruncate(1000, time.Minute, math.MaxUint64)
		Expect(err).Should(Succeed())
		Expect(count).Should(BeEquivalentTo(0))
	})

	It("Truncate by count", func() {
		fillRecords(truncateStore, 1, 100, time.Now())
		countEntries(truncateStore, 1, 100)

		pos, err := truncateStore.CalculateTruncate(50, 0, math.MaxUint64)
		Expect(err).Should(Succeed())
		Expect(pos).Should(BeEquivalentTo(51))

		err = truncateStore.TruncateBefore(pos)
		Expect(err).Should(Succeed())
		countEntries(truncateStore, 51, 50)

		pos, err = truncateStore.CalculateTruncate(50, 0, math.MaxUint64)
		Expect(err).Should(Succeed())
		Expect(pos).Should(BeEquivalentTo(0))
	})

	It("Truncate nothing", func() {
		fillRecords(truncateStore, 1, 100, time.Now())

		pos, err := truncateStore.CalculateTruncate(100, 0, math.MaxUint64)
		Expect(err).Should(Succeed())
		Expect(pos).Should(BeEquivalentTo(0))
		countEntries(truncateStore, 1, 100)
	})

	It("Truncate all", func() {
		fillRecords(truncateStore, 1, 100, time.Now())

		pos, err := truncateStore.CalculateTruncate(0, 0, math.MaxUint64)
		Expect(err).Should(Succeed())
		Expect(pos).Should(BeEquivalentTo(101))

		err = truncateStore.TruncateBefore(pos)
		Expect(err).Should(Succeed())

		pos, err = truncateStore.CalculateTruncate(0, 0, math.MaxUint64)
		Expect(err).Should(Succeed())
		Expect(pos).Should(BeEquivalentTo(0))
	})

	It("Truncate by time", func() {
		now := time.Now()
		tenBack := now.Add(-10 * time.Minute)
		fiveBack := now.Add(-5 * time.Minute)
		oneBack := now.Add(-1 * time.Minute)

		fillRecords(truncateStore, 1, 10, tenBack)
		fillRecords(truncateStore, 11, 10, fiveBack)
		fillRecords(truncateStore, 21, 10, oneBack)
		countEntries(truncateStore, 1, 30)

		pos, err := truncateStore.CalculateTruncate(2, 6*time.Minute, math.MaxUint64)
		Expect(err).Should(Succeed())
		Expect(pos).Should(BeEquivalentTo(11))

		err = truncateStore.TruncateBefore(pos)
		Expect(err).Should(Succeed())
		countEntries(truncateStore, 11, 20)

		pos, err = truncateStore.CalculateTruncate(2, 2*time.Minute, math.MaxUint64)
		Expect(err).Should(Succeed())
		Expect(pos).Should(BeEquivalentTo(21))

		err = truncateStore.TruncateBefore(pos)
		Expect(err).Should(Succeed())
		countEntries(truncateStore, 21, 10)
	})

	It("Truncate consider index", func() {
		fillRecords(truncateStore, 1, 100, time.Now())

		pos, err := truncateStore.CalculateTruncate(0, 0, 98)
		Expect(err).Should(Succeed())
		Expect(pos).Should(BeEquivalentTo(98))

		err = truncateStore.TruncateBefore(pos)
		Expect(err).Should(Succeed())

		pos, err = truncateStore.CalculateTruncate(0, 0, math.MaxUint64)
		Expect(err).Should(Succeed())
		Expect(pos).Should(BeEquivalentTo(101))
	})
})

func fillRecords(s Storage, startIndex uint64, count int, rt time.Time) {
	ix := startIndex
	for i := 0; i < count; i++ {
		//fmt.Fprintf(GinkgoWriter, "Inserting %d\n", ix)
		e := common.Entry{
			Index:     ix,
			Timestamp: rt,
			Data:      []byte("Truncation test"),
		}
		err := s.AppendEntry(&e)
		Expect(err).Should(Succeed())
		ix++
	}
	Expect(ix).Should(Equal(startIndex + uint64(count)))
}

func countEntries(s Storage, startIx uint64, count uint) {
	lastIx := startIx + uint64(count)
	for ix := startIx; ix < lastIx; ix++ {
		fmt.Fprintf(GinkgoWriter, "Fetching %d\n", ix)
		e, err := s.GetEntry(ix)
		Expect(err).Should(Succeed())
		Expect(e).ShouldNot(BeNil())
		Expect(e.Index).Should(Equal(ix))
	}
}
