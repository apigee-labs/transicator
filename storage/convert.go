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
package storage

import (
	"bytes"
	"encoding/binary"
)

/*
 * Records in the "entries" column family have a composite key.
 * The ordering is first by scope, next by LSN, and finally by index
 * within the LSN.
 *
 * Format 10: Indexed Entry Record
 *   Consists of three parts: scope name, sequence, and index
 *   Length of scope name (uint16, can be nil)
 *   Scope (string, UTF-8)
 *   Sequeuence (int64)
 *   Index (int32)
 */

// Byte order does not matter since all comparisons are done in Go, but
// for portability we'll pick the native Intel order
var storageByteOrder = binary.LittleEndian

func intToBytes(v int64) []byte {
	buf := &bytes.Buffer{}
	binary.Write(buf, storageByteOrder, v)
	return buf.Bytes()
}

func bytesToInt(bb []byte) (ret int64) {
	buf := bytes.NewBuffer(bb)
	binary.Read(buf, storageByteOrder, &ret)
	return
}

/*
Create an entry key.
*/
func lsnAndOffsetToKey(scope string, lsn uint64, index uint32) []byte {
	buf := &bytes.Buffer{}
	ib := make([]byte, binary.MaxVarintLen64)

	scopeBytes := []byte(scope)
	c := binary.PutUvarint(ib, uint64(len(scopeBytes)))
	buf.Write(ib[:c])
	buf.Write(scopeBytes)

	c = binary.PutUvarint(ib, lsn)
	buf.Write(ib[:c])

	c = binary.PutUvarint(ib, uint64(index))
	buf.Write(ib[:c])

	return buf.Bytes()
}

func keyToLsnAndOffset(key []byte) (scope string, lsn uint64, index uint32, err error) {
	buf := bytes.NewBuffer(key)

	iv, err := binary.ReadUvarint(buf)
	if err != nil {
		return
	}
	scopeBytes := make([]byte, int(iv))
	_, err = buf.Read(scopeBytes)
	if err != nil {
		return
	}
	scope = string(scopeBytes)

	iv, err = binary.ReadUvarint(buf)
	if err != nil {
		return
	}
	lsn = iv

	iv, err = binary.ReadUvarint(buf)
	if err != nil {
		return
	}
	index = uint32(iv)
	return
}
