/*
Copyright 2016 Google Inc.

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
package common

import (
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Sequence tests", func() {
	It("Basic test", func() {
		Expect(testSequence(1, 2)).Should(BeTrue())
	})

	It("Quick test", func() {
		err := quick.Check(testSequence, nil)
		Expect(err).Should(Succeed())
	})

	It("Basic compare", func() {
		Expect(testSequenceCompare(1, 1, 1, 1)).Should(BeTrue())
		Expect(testSequenceCompare(1, 1, 2, 1)).Should(BeTrue())
		Expect(testSequenceCompare(2, 1, 1, 1)).Should(BeTrue())
		Expect(testSequenceCompare(1, 1, 1, 2)).Should(BeTrue())
		Expect(testSequenceCompare(1, 2, 1, 1)).Should(BeTrue())
	})

	It("Quick compare", func() {
		err := quick.Check(testSequenceCompare, nil)
		Expect(err).Should(Succeed())
	})
})

func testSequence(lsn uint64, index uint32) bool {
	seq := MakeSequence(lsn, index)
	seqStr := seq.String()
	rseq, err := ParseSequence(seqStr)
	Expect(err).Should(Succeed())
	Expect(rseq.LSN).Should(Equal(lsn))
	Expect(rseq.Index).Should(Equal(index))

	seqBytes := seq.Bytes()
	rseq, err = ParseSequenceBytes(seqBytes)
	Expect(err).Should(Succeed())
	Expect(rseq.LSN).Should(Equal(lsn))
	Expect(rseq.Index).Should(Equal(index))

	return true
}

func testSequenceCompare(lsn1 uint64, index1 uint32, lsn2 uint64, index2 uint32) bool {
	s1 := MakeSequence(lsn1, index1)
	s2 := MakeSequence(lsn2, index2)

	if lsn1 < lsn2 {
		return s1.Compare(s2) < 0
	}
	if lsn1 > lsn2 {
		return s1.Compare(s2) > 0
	}
	if index1 < index2 {
		return s1.Compare(s2) < 0
	}
	if index1 > index2 {
		return s1.Compare(s2) > 0

	}
	return s1.Compare(s2) == 0
}
