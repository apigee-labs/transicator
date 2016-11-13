package storage

import (
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Conversion", func() {
	It("Identity Key Conversion", func() {
		key := lsnAndOffsetToKey(IndexKey, "foo", 3, 5)
		scope, lsn, offset, err := keyToLsnAndOffset(key)
		Expect(err).Should(Succeed())
		Expect(scope).Should(BeEquivalentTo("foo"))
		Expect(lsn).Should(BeEquivalentTo(3))
		Expect(offset).Should(BeEquivalentTo(5))
	})
	It("Empty key conversion", func() {
		key := lsnAndOffsetToKey(IndexKey, "", 3, 5)
		scope, lsn, offset, err := keyToLsnAndOffset(key)
		Expect(err).Should(Succeed())
		Expect(scope).Should(BeEquivalentTo(""))
		Expect(lsn).Should(BeEquivalentTo(3))
		Expect(offset).Should(BeEquivalentTo(5))
	})
	It("Int Key", func() {
		s := testIntKey(1, 1234)
		Expect(s).Should(BeTrue())
		err := quick.Check(testIntKey, nil)
		Expect(err).Should(Succeed())
	})

	It("String Key", func() {
		s := testStringKey(1, "foo")
		Expect(s).Should(BeTrue())
		err := quick.Check(testStringKey, nil)
		Expect(err).Should(Succeed())
	})

	It("String Key Compare", func() {
		s := testMetadataIndexCompare("foo", "foo")
		Expect(s).Should(BeTrue())
		s = testMetadataIndexCompare("foo", "bar")
		Expect(s).Should(BeTrue())
		s = testMetadataIndexCompare("foo", "")
		Expect(s).Should(BeTrue())
		s = testMetadataIndexCompare("", "foo")
		Expect(s).Should(BeTrue())

		err := quick.Check(testMetadataIndexCompare, nil)
		Expect(err).Should(Succeed())
	})

	It("Index Key Compare", func() {
		s := testIndexKeyCompare("foo", "foo", 123, 123, 456, 456)
		Expect(s).Should(BeTrue())
		err := quick.Check(testIndexKeyCompare, nil)
		Expect(err).Should(Succeed())
	})
})

func testIntKey(kt int, key uint64) bool {
	if kt < 0 || kt > (1<<8) {
		return true
	}
	keyBytes := uintToKey(kt, key)
	Expect(len(keyBytes)).Should(BeEquivalentTo(9))

	newType, newKey, err := keyToUint(keyBytes)
	Expect(err).Should(Succeed())
	Expect(newType).Should(Equal(kt))
	Expect(newKey).Should(Equal(key))
	return true
}

func testStringKey(kt int, key string) bool {
	if kt < 0 || kt > (1<<8) {
		return true
	}
	keyBytes := stringToKey(kt, key)
	Expect(len(keyBytes) > 0).Should(BeTrue())

	newType, newKey, err := keyToString(keyBytes)
	Expect(err).Should(Succeed())
	Expect(newType).Should(Equal(kt))
	Expect(newKey).Should(Equal(key))
	return true
}

func testIndexKeyCompare(tag1, tag2 string, lsn1, lsn2 uint64, seq1, seq2 uint32) bool {
	kb1 := lsnAndOffsetToKey(IndexKey, tag1, lsn1, seq1)
	kb2 := lsnAndOffsetToKey(IndexKey, tag2, lsn2, seq2)
	c := new(KeyComparator)
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

func testMetadataIndexCompare(k1, k2 string) bool {
	kb1 := stringToKey(StringKey, k1)
	kb2 := stringToKey(StringKey, k2)
	c := new(KeyComparator)
	cmp := c.Compare(kb1, kb2)

	if k1 < k2 {
		return cmp < 0
	}
	if k1 > k2 {
		return cmp > 0
	}
	return cmp == 0
}
