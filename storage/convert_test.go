package storage

import (
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Conversion", func() {
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
	keyBytes, keyLen := uintToKey(kt, key)
	Expect(keyLen).Should(BeEquivalentTo(9))
	defer freePtr(keyBytes)

	newType, newKey, err := keyToUint(keyBytes, keyLen)
	Expect(err).Should(Succeed())
	Expect(newType).Should(Equal(kt))
	Expect(newKey).Should(Equal(key))
	return true
}

func testStringKey(kt int, key string) bool {
	if kt < 0 || kt > (1<<8) {
		return true
	}
	keyBytes, keyLen := stringToKey(kt, key)
	Expect(keyLen > 0).Should(BeTrue())
	defer freePtr(keyBytes)

	newType, newKey, err := keyToString(keyBytes, keyLen)
	Expect(err).Should(Succeed())
	Expect(newType).Should(Equal(kt))
	Expect(newKey).Should(Equal(key))
	return true
}

func testIndexKeyCompare(tag1, tag2 string, lsn1, lsn2 uint64, seq1, seq2 uint32) bool {
	kb1, kl1 := indexToKey(IndexKey, tag1, lsn1, seq1)
	defer freePtr(kb1)
	kb2, kl2 := indexToKey(IndexKey, tag2, lsn2, seq2)
	defer freePtr(kb2)

	cmp := compareKeys(kb1, kl1, kb2, kl2)

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
	keyBytes1, keyLen1 := stringToKey(StringKey, k1)
	defer freePtr(keyBytes1)
	keyBytes2, keyLen2 := stringToKey(StringKey, k2)
	defer freePtr(keyBytes2)

	cmp := compareKeys(keyBytes1, keyLen1, keyBytes2, keyLen2)

	if k1 < k2 {
		return cmp < 0
	}
	if k1 > k2 {
		return cmp > 0
	}
	return cmp == 0
}
