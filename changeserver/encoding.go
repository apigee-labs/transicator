package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/30x/transicator/common"
)

const (
	msgVersion byte = 1
)

var networkByteOrder = binary.BigEndian

/*
encodeChangeProto encodes a change from the API for storage in the database.
We prepend the protocol buffer with a version number, and then
the integer transaction ID so that we can efficiently filter changes
by transaction ID without all the protobuf decoding.
*/
func encodeChangeProto(change *common.Change) []byte {
	buf := &bytes.Buffer{}
	buf.WriteByte(msgVersion)
	binary.Write(buf, networkByteOrder, &change.TransactionID)
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
	if version != msgVersion {
		return nil, fmt.Errorf("Invalid message version %d", version)
	}
	// Skip transaction ID
	buf.Next(4)
	return common.UnmarshalChangeProto(buf.Bytes())
}

func decodeChangeTXID(rawBuf []byte) (uint32, error) {
	if len(rawBuf) < 5 {
		return 0, errors.New("Input record too short")
	}
	buf := bytes.NewBuffer(rawBuf)
	version, err := buf.ReadByte()
	if err != nil {
		return 0, err
	}
	if version != msgVersion {
		return 0, fmt.Errorf("Invalid message version %d", version)
	}
	var txid uint32
	err = binary.Read(buf, networkByteOrder, &txid)
	return txid, err
}
