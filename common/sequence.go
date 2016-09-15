package common

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

var sequenceRe = regexp.MustCompile("([a-fA-F0-9]+)\\.([a-fA-F0-9]+)\\.([a-fA-F0-9]+)")
var networkByteOrder = binary.BigEndian

/*
A Sequence represents a unique position in the list of changes. It consists
of a postgres LSN that represents when a transaction was committed, and a
index into the list of changes in that transaction.
*/
type Sequence struct {
	LSN   uint64
	Index uint32
}

/*
MakeSequence makes a new sequence from an LSN and an index.
*/
func MakeSequence(lsn uint64, index uint32) Sequence {
	return Sequence{
		LSN:   lsn,
		Index: index,
	}
}

/*
ParseSequence parses a stringified sequence.
*/
func ParseSequence(s string) (Sequence, error) {
	parsed := sequenceRe.FindStringSubmatch(s)
	if parsed == nil {
		return Sequence{}, errors.New("Invalid sequence string")
	}

	l1, err := strconv.ParseUint(parsed[1], 16, 32)
	if err != nil {
		return Sequence{}, fmt.Errorf("Invalid sequence: %s", err)
	}

	l2, err := strconv.ParseUint(parsed[2], 16, 32)
	if err != nil {
		return Sequence{}, fmt.Errorf("Invalid sequence: %s", err)
	}
	ix, err := strconv.ParseUint(parsed[3], 16, 32)
	if err != nil {
		return Sequence{}, fmt.Errorf("Invalid sequence: %s", err)
	}

	return Sequence{
		LSN:   (l1 << 32) | l2,
		Index: uint32(ix),
	}, nil
}

/*
ParseSequenceBytes parses bytes written using Bytes().
*/
func ParseSequenceBytes(b []byte) (Sequence, error) {
	if len(b) != 12 {
		return Sequence{}, errors.New("Invalid byte sequence")
	}

	buf := bytes.NewBuffer(b)

	var lsn uint64
	err := binary.Read(buf, networkByteOrder, &lsn)
	if err != nil {
		return Sequence{}, fmt.Errorf("Invalid byte sequence: %s", err)
	}

	var ix uint32
	err = binary.Read(buf, networkByteOrder, &ix)
	if err != nil {
		return Sequence{}, fmt.Errorf("Invalid byte sequence: %s", err)
	}

	return Sequence{
		LSN:   lsn,
		Index: ix,
	}, nil
}

/*
String turns the sequence into the canonical string form, which looks
like this: "XXX.YYY.ZZZ," where each component is a hexadecimal number.
This is different from the standard Postgres format, which includes a
slash in the first part, in that it doesn't need URL encoding.
*/
func (s Sequence) String() string {
	return strconv.FormatUint(s.LSN>>32, 16) + "." +
		strconv.FormatUint(s.LSN&0xffffffff, 16) + "." +
		strconv.FormatUint(uint64(s.Index), 16)
}

/*
Bytes turns the sequence into an array of 12 bytes, in "network" (aka
big-endian) byte order.
*/
func (s Sequence) Bytes() []byte {
	buf := &bytes.Buffer{}
	binary.Write(buf, networkByteOrder, s.LSN)
	binary.Write(buf, networkByteOrder, s.Index)
	return buf.Bytes()
}

/*
Compare returns if the current sequence is less than, greater to, or
equal to the specified sequence.
*/
func (s Sequence) Compare(o Sequence) int {
	if s.LSN < o.LSN {
		return -1
	}
	if s.LSN > o.LSN {
		return 1
	}
	if s.Index < o.Index {
		return -1
	}
	if s.Index > o.Index {
		return 1
	}
	return 0
}
