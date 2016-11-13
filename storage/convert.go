package storage


import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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
var storageByteOrder = binary.LittleEndian

/*
 * uint64 key, used for entries. Used in tests so not dead code.
 */
func uintToKey(keyType int, v uint64) ([]byte) {
	buf := &bytes.Buffer{}
	binary.Write(buf, storageByteOrder, keyPrefix(keyType))
	binary.Write(buf, storageByteOrder, v)
	return buf.Bytes()
}

/*
 * uint64 key, used for entries. Used in tests so not dead code.
 */
func keyToUint(key []byte) (vers int, intKey uint64, err error) {
	if len(key) < 1 {
		return 0, 0, errors.New("Invalid key")
	}
	buf := bytes.NewBuffer(key)

	var ktb byte
	binary.Read(buf, storageByteOrder, &ktb)
	vers, _ = parseKeyPrefix(ktb)
	if vers != KeyVersion {
		err = fmt.Errorf("Invalid key version %d", vers)
		return
	}
	binary.Read(buf, storageByteOrder, &intKey)
	return
}

/*
 * String key, used for metadata.
 */
func stringToKey(keyType int, k string) (key []byte) {
	buf := &bytes.Buffer{}
	binary.Write(buf, storageByteOrder, keyPrefix(keyType))
	buf.WriteString(k)
	buf.WriteByte(0)
	key = buf.Bytes()
	return
}

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
keyToString is unused in the code but it is used in the test. Since it uses
cgo it cannot be in a test file, however.
*/
func keyToString(key []byte) (keyType int, keyString string, err error) {
	if len(key) < 1 {
		err = errors.New("Invalid key")
		return
	}
	buf := bytes.NewBuffer(key)

	var ktb byte
	var vers int
	binary.Read(buf, storageByteOrder, &ktb)
	vers, keyType = parseKeyPrefix(ktb)
	if vers != KeyVersion {
		err = fmt.Errorf("Invalid key version %d", vers)
		return
	}

	keyBytes, _ := buf.ReadBytes(0)
	if len(keyBytes) > 0 {
		keyString = string(keyBytes[:len(keyBytes)-1])
	}
	return
}

/*
Create an entry key.
*/
func lsnAndOffsetToKey(keyType int, scope string, lsn uint64, index uint32) (key []byte) {
	buf := &bytes.Buffer{}
	binary.Write(buf, storageByteOrder, keyPrefix(keyType))
	buf.WriteString(scope)
	buf.WriteByte(0)
	binary.Write(buf, storageByteOrder, lsn)
	binary.Write(buf, storageByteOrder, index)
	key = buf.Bytes()
	return
}

func keyToLsnAndOffset(key []byte) (scope string, lsn uint64, index uint32, err error) {
	buf := bytes.NewBuffer(key)
	var version byte
	err = binary.Read(buf, storageByteOrder, &version)
	if err != nil {
		fmt.Printf("Failed to read version byte %s\n", err)

		return
	}
	var scopeBytes []byte
	var currentByte byte
	var cnt int
	for cnt == 0 || (currentByte != 0x0) {
		err = binary.Read(buf, storageByteOrder, &currentByte)
		cnt++
		if err != nil {
			fmt.Printf("Failed to read string byte %s\n", err)
			return
		}
		if currentByte != 0x0 {
			scopeBytes = append(scopeBytes, currentByte)
		}
	}
	if len(scopeBytes) > 0 {
		scope = string(scopeBytes)
	}

	err = binary.Read(buf, storageByteOrder, &lsn)
	if err != nil {
		fmt.Printf("Failed to read lsn %s\n", err)

		return
	}
	err = binary.Read(buf, storageByteOrder, &index)
	if err != nil {
		fmt.Printf("Failed to read index %s\n", err)

		return
	}
	return
}

/*
Parse an entry key. Again this uses cgo so we need it for tests even though
it will be flagged as "dead code."
*/
func keyToIndex(key []byte) (tag string, lsn int64, index int32, err error) {
	if len(key) < 1 {
		err = errors.New("Invalid key")
		return
	}
	buf := bytes.NewBuffer(key)

	tagBytes, _ := buf.ReadBytes(0)
	if len(tagBytes) > 0 {
		tag = string(tagBytes[:len(tagBytes)-1])
	}

	binary.Read(buf, storageByteOrder, &lsn)
	binary.Read(buf, storageByteOrder, &index)

	return
}

func keyPrefix(keyType int) byte {
	flag := (KeyVersion << 4) | (keyType & 0xf)
	return byte(flag)
}

func parseKeyPrefix(b byte) (int, int) {
	bi := int(b)
	vers := (bi >> 4) & 0xf
	kt := bi & 0xf
	return vers, kt
}

