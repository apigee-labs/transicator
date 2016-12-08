/*
Copyright 2016 The Transicator Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package storage

import (
	"testing/quick"

	"bytes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
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

	It("Add and remove timestamps", func() {
		now := time.Now()
		msg := []byte("Hello, World!")
		b := prependTimestamp(now, msg)
		et, eb := extractTimestamp(b)
		Expect(et).Should(Equal(now))
		Expect(bytes.Equal(eb, msg)).Should(BeTrue())

		b = prependTimestamp(now, nil)
		et, eb = extractTimestamp(b)
		Expect(et).Should(Equal(now))
		Expect(eb).Should(BeEmpty())
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
	cmp := entryComparator.Compare(kb1, kb2)

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
