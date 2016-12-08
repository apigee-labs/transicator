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
	"errors"
	"fmt"

	"github.com/apigee-labs/transicator/common"
)

const (
	minMsgVersion byte = 1
	msgVersion    byte = 3
)

var networkByteOrder = binary.BigEndian

/*
encodeChangeProto encodes a change from the API for storage in the database.
We prepend the protocol buffer with a version number,
the integer transaction ID, and the timestamp. That way we can
efficiently filter changes
by transaction ID without all the protobuf decoding.
*/
func encodeChangeProto(change *common.Change) []byte {
	buf := &bytes.Buffer{}
	buf.WriteByte(msgVersion)
	binary.Write(buf, networkByteOrder, &change.TransactionID)
	binary.Write(buf, networkByteOrder, &change.Timestamp)
	buf.Write(change.MarshalProto())
	return buf.Bytes()
}

func decodeChangeProto(rawBuf []byte) (*common.Change, error) {
	if len(rawBuf) < 5 {
		return nil, errors.New("Input record too short")
	}
	buf := bytes.NewBuffer(rawBuf)
	version, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	if version < minMsgVersion || version > msgVersion {
		return nil, fmt.Errorf("Invalid message version %d", version)
	}
	// Skip transaction ID and maybe timestamp
	buf.Next(4)
	if version > 2 {
		buf.Next(4)
	}
	if version > 1 {
		buf.Next(8)
	}
	return common.UnmarshalChangeProto(buf.Bytes())
}

func decodeChangeTXID(rawBuf []byte) (uint64, error) {
	if len(rawBuf) < 5 {
		return 0, errors.New("Input record too short")
	}
	buf := bytes.NewBuffer(rawBuf)
	version, err := buf.ReadByte()
	if err != nil {
		return 0, err
	}
	if version < minMsgVersion || version > msgVersion {
		return 0, fmt.Errorf("Invalid message version %d", version)
	}

	if version > 2 {
		var txid uint64
		err = binary.Read(buf, networkByteOrder, &txid)
		return txid, err
	}

	var txid uint32
	err = binary.Read(buf, networkByteOrder, &txid)
	return uint64(txid), err
}

func decodeChangeTimestamp(rawBuf []byte) (int64, error) {
	if len(rawBuf) < 5 {
		return 0, errors.New("Input record too short")
	}
	buf := bytes.NewBuffer(rawBuf)
	version, err := buf.ReadByte()
	if err != nil {
		return 0, err
	}
	if version < minMsgVersion || version > msgVersion {
		return 0, fmt.Errorf("Invalid message version %d", version)
	}
	if version < 2 {
		return 0, nil
	}

	// Skip txid
	buf.Next(4)
	if version > 2 {
		buf.Next(4)
	}

	var ts int64
	err = binary.Read(buf, networkByteOrder, &ts)
	return ts, err
}
