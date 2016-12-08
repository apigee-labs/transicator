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
package common

import (
	"bytes"
	"fmt"
	"io"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pipe tests", func() {
	var pr io.ReadCloser
	var pw io.WriteCloser

	BeforeEach(func() {
		pr, pw = MakeNBPipe()
	})

	It("Empty", func() {
		len, err := pr.Read(make([]byte, 10))
		Expect(err).Should(Succeed())
		Expect(len).Should(BeZero())
	})

	It("EOF", func() {
		err := pw.Close()
		Expect(err).Should(Succeed())
		_, err = pr.Read(make([]byte, 10))
		Expect(err).Should(Equal(io.EOF))
	})

	It("Smaller", func() {
		wb := []byte("12345")
		len, err := pw.Write(wb)
		Expect(len).Should(Equal(5))
		Expect(err).Should(Succeed())
		err = pw.Close()
		Expect(err).Should(Succeed())

		buf := make([]byte, 10)
		len, err = pr.Read(buf)
		Expect(len).Should(Equal(5))
		Expect(err).Should(Succeed())
		Expect(bytes.Equal(buf[:5], wb)).Should(BeTrue())

		len, err = pr.Read(buf)
		Expect(len).Should(BeZero())
		Expect(err).Should(Equal(io.EOF))
	})

	It("Bigger", func() {
		wb := []byte("1234567890")
		fmt.Fprintf(GinkgoWriter, "wb: %s\n", string(wb))
		len, err := pw.Write(wb)
		Expect(len).Should(Equal(10))
		Expect(err).Should(Succeed())
		err = pw.Close()
		Expect(err).Should(Succeed())

		buf := make([]byte, 5)
		len, err = pr.Read(buf)
		Expect(len).Should(Equal(5))
		Expect(err).Should(Succeed())
		fmt.Fprintf(GinkgoWriter, "Got: %s\n", string(buf))
		Expect(bytes.Equal(buf, wb[:5])).Should(BeTrue())

		len, err = pr.Read(buf)
		Expect(len).Should(Equal(5))
		Expect(err).Should(Succeed())
		fmt.Fprintf(GinkgoWriter, "Got: %s\n", string(buf))
		Expect(bytes.Equal(buf, wb[5:])).Should(BeTrue())

		len, err = pr.Read(buf)
		Expect(len).Should(BeZero())
		Expect(err).Should(Equal(io.EOF))
	})
})
