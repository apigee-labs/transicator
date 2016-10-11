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
