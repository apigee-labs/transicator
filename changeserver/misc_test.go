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
package main

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/apigee-labs/transicator/common"
	"github.com/golang/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Miscellaneous tests", func() {
	It("Sanitize slot name", func() {
		Expect(sanitizeSlotName("foo")).Should(Equal("foo"))
		Expect(sanitizeSlotName("foo bar")).Should(Equal("foobar"))
		Expect(sanitizeSlotName("foo-bar")).Should(Equal("foo_bar"))
		Expect(sanitizeSlotName("foo_bar baz%^&*-123")).Should(Equal("foo_barbaz_123"))
		Expect(sanitizeSlotName(" *&^*&^!")).Should(Equal(""))
	})

	It("Encode data", func() {
		c := &common.Change{
			Operation:     common.Insert,
			Table:         "foo",
			TransactionID: 1234,
			Timestamp:     time.Now().Unix(),
		}

		buf := encodeChangeProto(c)
		ca, err := decodeChangeProto(buf)
		Expect(err).Should(Succeed())
		Expect(ca.Operation).Should(Equal(c.Operation))
		Expect(ca.Table).Should(Equal(c.Table))
		Expect(ca.TransactionID).Should(Equal(c.TransactionID))
		Expect(ca.Timestamp).Should(Equal(c.Timestamp))

		txid, err := decodeChangeTXID(buf)
		Expect(err).Should(Succeed())
		Expect(txid).Should(Equal(c.TransactionID))

		ts, err := decodeChangeTimestamp(buf)
		Expect(err).Should(Succeed())
		Expect(ts).Should(Equal(c.Timestamp))
	})

	It("Decode old record", func() {
		ts := time.Now().Unix()
		cp := &common.ChangePb{
			Operation:     proto.Int32(int32(common.Insert)),
			Table:         proto.String("foo"),
			Timestamp:     proto.Int64(ts),
			TransactionID: proto.Uint32(1234),
		}

		// manually construct an old-version record
		var smallTID uint32 = 1234
		buf := &bytes.Buffer{}
		buf.WriteByte(1)
		binary.Write(buf, networkByteOrder, &smallTID)
		marsh, err := proto.Marshal(cp)
		Expect(err).Should(Succeed())
		buf.Write(marsh)
		enc := buf.Bytes()

		ca, err := decodeChangeProto(enc)
		Expect(err).Should(Succeed())
		Expect(ca.Operation).Should(BeEquivalentTo(common.Insert))
		Expect(ca.Table).Should(Equal("foo"))
		Expect(ca.TransactionID).Should(BeEquivalentTo(smallTID))
		Expect(ca.Timestamp).Should(Equal(ts))

		txid, err := decodeChangeTXID(enc)
		Expect(err).Should(Succeed())
		Expect(txid).Should(BeEquivalentTo(smallTID))

		ts, err = decodeChangeTimestamp(enc)
		Expect(err).Should(Succeed())
		Expect(ts).Should(BeZero())
	})
})
