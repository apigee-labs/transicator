package storage

import (
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Conversion", func() {
	It("Identity Key Conversion", func() {
		key := lsnAndOffsetToKey("foo", 3, 5)
		scope, lsn, offset, err := keyToLsnAndOffset(key)
		Expect(err).Should(Succeed())
		Expect(scope).Should(BeEquivalentTo("foo"))
		Expect(lsn).Should(BeEquivalentTo(3))
		Expect(offset).Should(BeEquivalentTo(5))
	})
	It("Empty key conversion", func() {
		key := lsnAndOffsetToKey("", 3, 5)
		scope, lsn, offset, err := keyToLsnAndOffset(key)
		Expect(err).Should(Succeed())
		Expect(scope).Should(BeEquivalentTo(""))
		Expect(lsn).Should(BeEquivalentTo(3))
		Expect(offset).Should(BeEquivalentTo(5))
	})

	It("Index Key Compare", func() {
		s := testIndexKeyCompare("foo", "foo", 123, 123, 456, 456)
		Expect(s).Should(BeTrue())
		err := quick.Check(testIndexKeyCompare, nil)
		Expect(err).Should(Succeed())
	})
})

func testIndexKeyCompare(tag1, tag2 string, lsn1, lsn2 uint64, seq1, seq2 uint32) bool {
	kb1 := lsnAndOffsetToKey(tag1, lsn1, seq1)
	kb2 := lsnAndOffsetToKey(tag2, lsn2, seq2)
	s1, l1, i1, err := keyToLsnAndOffset(kb1)
	Expect(err).Should(Succeed())
	Expect(s1).Should(Equal(tag1))
	Expect(l1).Should(Equal(lsn1))
	Expect(i1).Should(Equal(seq1))
	s2, l2, i2, err := keyToLsnAndOffset(kb2)
	Expect(err).Should(Succeed())
	Expect(s2).Should(Equal(tag2))
	Expect(l2).Should(Equal(lsn2))
	Expect(i2).Should(Equal(seq2))
	c := new(entryComparator)
	cmp := c.Compare(kb1, kb2)

	if tag1 < tag2 {
		return cmp < 0
	}
	if tag1 > tag2 {
		return cmp > 0
	}
	if lsn1 < lsn2 {
		return cmp < 0
	}
	if lsn1 > lsn2 {
		return cmp > 0
	}
	if seq1 < seq2 {
		return cmp < 0
	}
	if seq1 > seq2 {
		return cmp > 0
	}
	return cmp == 0
}
