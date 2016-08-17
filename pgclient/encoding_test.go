package pgclient

import (
	"bytes"
	"encoding/binary"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Encoding Tests", func() {
	It("Int Field", func() {
		om := NewOutputMessage('a')
		om.WriteInt32(123)
		buf := om.Encode()
		Expect(len(buf)).Should(Equal(9))

		ir := bytes.NewBuffer(buf)
		typ, err := ir.ReadByte()
		Expect(err).Should(Succeed())
		Expect(typ).Should(BeEquivalentTo('a'))

		var len int32
		err = binary.Read(ir, networkByteOrder, &len)
		Expect(err).Should(Succeed())
		Expect(len).Should(BeEquivalentTo(8))

		im := NewInputMessage('a', buf[5:])
		val, err := im.ReadInt32()
		Expect(err).Should(Succeed())
		Expect(val).Should(BeEquivalentTo(123))
		Expect(im.Type()).Should(BeEquivalentTo('a'))
	})

	It("String Field", func() {
		msg := "Hello, World!"
		om := NewOutputMessage('b')
		om.WriteString(msg)
		buf := om.Encode()
		Expect(len(buf)).Should(Equal(19))

		ir := bytes.NewBuffer(buf)
		typ, err := ir.ReadByte()
		Expect(err).Should(Succeed())
		Expect(typ).Should(BeEquivalentTo('b'))

		var len int32
		err = binary.Read(ir, networkByteOrder, &len)
		Expect(err).Should(Succeed())
		Expect(len).Should(BeEquivalentTo(18))

		im := NewInputMessage('b', buf[5:])
		val, err := im.ReadString()
		fmt.Fprintf(GinkgoWriter, "Result:   \"%v\"\n", []byte(val))
		fmt.Fprintf(GinkgoWriter, "Expected: \"%v\"\n", []byte(msg))
		Expect(err).Should(Succeed())
		Expect(val).Should(Equal(msg))
	})

	It("Empty String Field", func() {
		msg := ""
		om := NewOutputMessage('b')
		om.WriteString(msg)
		buf := om.Encode()
		Expect(len(buf)).Should(Equal(6))

		ir := bytes.NewBuffer(buf)
		typ, err := ir.ReadByte()
		Expect(err).Should(Succeed())
		Expect(typ).Should(BeEquivalentTo('b'))

		var len int32
		err = binary.Read(ir, networkByteOrder, &len)
		Expect(err).Should(Succeed())
		Expect(len).Should(BeEquivalentTo(5))

		im := NewInputMessage('b', buf[5:])
		val, err := im.ReadString()
		fmt.Fprintf(GinkgoWriter, "Result:   \"%v\"\n", []byte(val))
		fmt.Fprintf(GinkgoWriter, "Expected: \"%v\"\n", []byte(msg))
		Expect(err).Should(Succeed())
		Expect(val).Should(Equal(msg))
	})

	It("Complex Message", func() {
		om := NewOutputMessage('b')
		om.WriteString("Hello")
		om.WriteInt32(123)
		om.WriteString("to")
		om.WriteString("world")
		om.WriteInt32(-99)
		om.WriteInt32(0)
		buf := om.Encode()

		ir := bytes.NewBuffer(buf)
		typ, err := ir.ReadByte()
		Expect(err).Should(Succeed())
		Expect(typ).Should(BeEquivalentTo('b'))

		var len int32
		err = binary.Read(ir, networkByteOrder, &len)
		Expect(err).Should(Succeed())

		im := NewInputMessage('b', buf[5:])
		val, err := im.ReadString()
		Expect(err).Should(Succeed())
		Expect(val).Should(Equal("Hello"))

		iv, err := im.ReadInt32()
		Expect(err).Should(Succeed())
		Expect(iv).Should(BeEquivalentTo(123))

		val, err = im.ReadString()
		Expect(err).Should(Succeed())
		Expect(val).Should(Equal("to"))
		val, err = im.ReadString()
		Expect(err).Should(Succeed())
		Expect(val).Should(Equal("world"))

		iv, err = im.ReadInt32()
		Expect(err).Should(Succeed())
		Expect(iv).Should(BeEquivalentTo(-99))
		iv, err = im.ReadInt32()
		Expect(err).Should(Succeed())
		Expect(iv).Should(BeEquivalentTo(0))
	})

	It("Startup Message", func() {
		om := NewStartupMessage()
		om.WriteInt32(123)
		om.WriteString("Start me up!")
		buf := om.Encode()
		Expect(len(buf)).Should(BeEquivalentTo(21))

		ir := bytes.NewBuffer(buf)
		var len int32
		err := binary.Read(ir, networkByteOrder, &len)
		Expect(err).Should(Succeed())
		Expect(len).Should(BeEquivalentTo(21))

		im := NewInputMessage(0, buf[4:])
		iv, err := im.ReadInt32()
		Expect(err).Should(Succeed())
		Expect(iv).Should(BeEquivalentTo(123))

		val, err := im.ReadString()
		Expect(err).Should(Succeed())
		Expect(val).Should(Equal("Start me up!"))
	})
})
