package storage

/*
#include <stdlib.h>
#include "storage_native.h"
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"
)

/* These have to match constants in leveldb_native.h */
const (
	KeyVersion = 1
	StringKey  = 1
	IndexKey   = 10
)

/*
 * Key format: There are three. The comparator in storage_native.c will
 * keep all three of them in order.
 *
 * All keys start with a single byte that is divided into two parts: version and type:
 *   High 4 bits: version (currently 1)
 *   Low 4 bits: type
 *
 * Format 1: String
 *   Key is a null-terminated string, sorted as per "strcmp".
 *
 * Format 10: Indexed Entry Record
 *   Consists of three parts: tag, transaction id, and index
 *   Tenant name is null-terminated ASCII
 *   Transaction is is an int64
 *   Index is an int32
 */

// Byte order needs to match the native byte order of the host,
// or the C code to compare keys doesn't work.
// So this code will fail on a non-Intel CPU...
var storageByteOrder binary.ByteOrder = binary.LittleEndian

/*
 * uint64 key, used for entries. Used in tests so not dead code.
 */
func uintToKey(keyType int, v uint64) (unsafe.Pointer, C.size_t) {
	buf := &bytes.Buffer{}
	binary.Write(buf, storageByteOrder, keyPrefix(keyType)[0])
	binary.Write(buf, storageByteOrder, v)
	return bytesToPtr(buf.Bytes())
}

/*
 * uint64 key, used for entries. Used in tests so not dead code.
 */
func keyToUint(ptr unsafe.Pointer, len C.size_t) (int, uint64, error) {
	if len < 1 {
		return 0, 0, errors.New("Invalid key")
	}
	bb := ptrToBytes(ptr, len)
	buf := bytes.NewBuffer(bb)

	var ktb byte
	binary.Read(buf, storageByteOrder, &ktb)
	vers, kt := parseKeyPrefix(ktb)
	if vers != KeyVersion {
		return 0, 0, fmt.Errorf("Invalid key version %d", vers)
	}
	var key uint64
	binary.Read(buf, storageByteOrder, &key)

	return kt, key, nil
}

/*
 * String key, used for metadata.
 */
func stringToKey(keyType int, k string) (unsafe.Pointer, C.size_t) {
	buf := &bytes.Buffer{}
	binary.Write(buf, storageByteOrder, keyPrefix(keyType)[0])
	buf.WriteString(k)
	buf.WriteByte(0)
	return bytesToPtr(buf.Bytes())
}

/*
keyToString is unused in the code but it is used in the test. Since it uses
cgo it cannot be in a test file, however.
*/
func keyToString(ptr unsafe.Pointer, l C.size_t) (int, string, error) {
	if l < 1 {
		return 0, "", errors.New("Invalid key")
	}
	bb := ptrToBytes(ptr, l)
	buf := bytes.NewBuffer(bb)

	var ktb byte
	binary.Read(buf, storageByteOrder, &ktb)
	vers, kt := parseKeyPrefix(ktb)
	if vers != KeyVersion {
		return 0, "", fmt.Errorf("Invalid key version %d", vers)
	}

	keyBytes, _ := buf.ReadBytes(0)
	var key string
	if len(keyBytes) > 0 {
		key = string(keyBytes[:len(keyBytes)-1])
	}
	return kt, key, nil
}

/*
Create an entry key.
*/
func indexToKey(keyType int, tag string, lsn int64, index int32) (unsafe.Pointer, C.size_t) {
	// TODO convert to ASCII here?
	buf := &bytes.Buffer{}
	binary.Write(buf, storageByteOrder, keyPrefix(keyType)[0])
	buf.WriteString(tag)
	buf.WriteByte(0)
	binary.Write(buf, storageByteOrder, lsn)
	binary.Write(buf, storageByteOrder, index)
	return bytesToPtr(buf.Bytes())
}

/*
Parse an entry key. Again this uses cgo so we need it for tests even though
it will be flagged as "dead code."
*/
func keyToIndex(ptr unsafe.Pointer, length C.size_t) (string, int64, int32, error) {
	if length < 1 {
		return "", 0, 0, errors.New("Invalid key")
	}
	bb := ptrToBytes(ptr, length)
	buf := bytes.NewBuffer(bb)

	tagBytes, _ := buf.ReadBytes(0)
	var tag string
	if len(tagBytes) > 0 {
		tag = string(tagBytes[:len(tagBytes)-1])
	}

	var lsn int64
	binary.Read(buf, storageByteOrder, &lsn)
	var index int32
	binary.Read(buf, storageByteOrder, &index)

	return tag, lsn, index, nil
}

func freeString(c *C.char) {
	C.free(unsafe.Pointer(c))
}

func freePtr(ptr unsafe.Pointer) {
	C.free(ptr)
}

func stringToError(c *C.char) error {
	es := C.GoString(c)
	return errors.New(es)
}

func uintToPtr(v uint64) (unsafe.Pointer, C.size_t) {
	bb := uintToBytes(v)
	return bytesToPtr(bb)
}

func bytesToPtr(bb []byte) (unsafe.Pointer, C.size_t) {
	bsLen := C.size_t(len(bb))
	buf := C.malloc(bsLen)
	copy((*[1 << 30]byte)(buf)[:], bb)
	return buf, bsLen
}

func uintToBytes(v uint64) []byte {
	buf := &bytes.Buffer{}
	binary.Write(buf, storageByteOrder, v)
	return buf.Bytes()
}

func ptrToBytes(ptr unsafe.Pointer, len C.size_t) []byte {
	bb := make([]byte, int(len))
	copy(bb[:], (*[1 << 30]byte)(unsafe.Pointer(ptr))[:])
	return bb
}

func ptrToUint(ptr unsafe.Pointer, len C.size_t) uint64 {
	bb := ptrToBytes(ptr, len)
	buf := bytes.NewBuffer(bb)
	var ret uint64
	binary.Read(buf, storageByteOrder, &ret)
	return ret
}

func keyPrefix(keyType int) []byte {
	flag := (KeyVersion << 4) | (keyType & 0xf)
	return []byte{byte(flag)}
}

func parseKeyPrefix(b byte) (int, int) {
	bi := int(b)
	vers := (bi >> 4) & 0xf
	kt := bi & 0xf
	return vers, kt
}

/*
compareKeys is unused in the code but it is used by the tests. Since it uses
cgo it cannot be in a test file, however.
*/
func compareKeys(ptra unsafe.Pointer, lena C.size_t, ptrb unsafe.Pointer, lenb C.size_t) int {
	cmp := C.go_compare_bytes(nil, ptra, lena, ptrb, lenb)
	return int(cmp)
}
