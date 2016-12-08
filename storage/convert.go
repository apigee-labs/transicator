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
	"bytes"
	"encoding/binary"
	"time"
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

/*
Functions for putting timestamps in front of records and taking them out.
*/
func prependTimestamp(t time.Time, b []byte) []byte {
	tb := make([]byte, binary.MaxVarintLen64)
	tl := binary.PutVarint(tb, t.UnixNano())
	return append(tb[:tl], b...)
}

func extractTimestamp(b []byte) (time.Time, []byte) {
	bb := bytes.NewBuffer(b)
	ts, err := binary.ReadVarint(bb)
	if err != nil {
		panic(err.Error())
	}
	return time.Unix(0, ts), bb.Bytes()
}
