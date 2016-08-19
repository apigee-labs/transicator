package storage

/*
#include <stdlib.h>
#include "storage_native.h"
#cgo CFLAGS: -g -O3 -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lleveldb
*/
import "C"

import (
	"math"
	"sync"
	"unsafe"

	log "github.com/Sirupsen/logrus"
)

var defaultWriteOptions = C.leveldb_writeoptions_create()
var defaultReadOptions = C.leveldb_readoptions_create()
var dbInitOnce sync.Once

/*
A DB is a handle to a LevelDB database.
*/
type DB struct {
	baseFile string
	dbHandle *C.GoDb
	db       *C.leveldb_t
}

/*
OpenDB opens a LevelDB database and makes it available for reads and writes.
Opened databases should be closed when done.

The "baseFile" parameter refers to the name of a directory where RocksDB can
store its data. RocksDB will create many files inside this directory. To create
an empty database, make sure that it is empty.

The "cacheSize" parameter specifies the maximum number of bytes to use in memory
for an LRU cache of database contents. How this cache is used is up to RocksDB.
*/
func OpenDB(baseFile string, cacheSize uint) (*DB, error) {
	stor := &DB{
		baseFile: baseFile,
	}

	// One-time init of comparator functions
	dbInitOnce.Do(func() {
		C.go_db_init()
	})

	var dbh *C.GoDb
	e := C.go_db_open(
		C.CString(baseFile),
		C.size_t(cacheSize),
		&dbh)

	if e != nil {
		defer freeString(e)
		err := stringToError(e)
		return nil, err
	}

	log.Infof("Opened LevelDB file in %s", baseFile)
	log.Infof("LevelDB version %d.%d",
		C.leveldb_major_version(), C.leveldb_minor_version())

	stor.dbHandle = dbh
	stor.db = dbh.db

	return stor, nil
}

/*
GetDataPath returns the path of the data directory.
*/
func (s *DB) GetDataPath() string {
	return s.baseFile
}

/*
Close closes the database cleanly.
*/
func (s *DB) Close() {
	C.go_db_close(s.dbHandle)
	freePtr(unsafe.Pointer(s.dbHandle))
}

/*
Delete deletes all the files used by the database.
*/
func (s *DB) Delete() error {
	var e *C.char
	opts := C.leveldb_options_create()
	defer C.leveldb_options_destroy(opts)

	dbCName := C.CString(s.baseFile)
	defer freeString(dbCName)
	C.leveldb_destroy_db(opts, dbCName, &e)
	if e == nil {
		return nil
	}
	defer freeString(e)
	err := stringToError(e)
	if err != nil {
		log.Warningf("Error destroying LevelDB database: %s", err)
	}
	return err
}

/*
GetUintMetadata returns metadata with the specified key and converts it
into a uint64. It returns 0 if the key cannot be found.
*/
func (s *DB) GetUintMetadata(key string) (uint64, error) {
	keyBuf, keyLen := stringToKey(StringKey, key)
	defer freePtr(keyBuf)

	val, valLen, err := s.readEntry(keyBuf, keyLen)
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}

	defer freePtr(val)
	return ptrToUint(val, valLen), nil
}

/*
GetMetadata returns the raw metadata matching the specified key,
or nil if the key is not found.
*/
func (s *DB) GetMetadata(key string) ([]byte, error) {
	keyBuf, keyLen := stringToKey(StringKey, key)
	defer freePtr(keyBuf)

	val, valLen, err := s.readEntry(keyBuf, keyLen)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	defer freePtr(val)
	return ptrToBytes(val, valLen), nil
}

/*
SetUintMetadata sets the metadata with the specified key to
the integer value.
*/
func (s *DB) SetUintMetadata(key string, val uint64) error {
	keyBuf, keyLen := stringToKey(StringKey, key)
	defer freePtr(keyBuf)
	valBuf, valLen := uintToPtr(val)
	defer freePtr(valBuf)

	return s.putEntry(keyBuf, keyLen, valBuf, valLen)
}

/*
SetMetadata sets the metadata with the specified key.
*/
func (s *DB) SetMetadata(key string, val []byte) error {
	keyBuf, keyLen := stringToKey(StringKey, key)
	defer freePtr(keyBuf)
	valBuf, valLen := bytesToPtr(val)
	defer freePtr(valBuf)

	return s.putEntry(keyBuf, keyLen, valBuf, valLen)
}

/*
PutEntry writes an entry to the database indexed by tag, lsn, and index in order
*/
func (s *DB) PutEntry(tag string, lsn int64, index int32, data []byte) error {
	keyBuf, keyLen := indexToKey(IndexKey, tag, lsn, index)
	defer freePtr(keyBuf)
	valBuf, valLen := bytesToPtr(data)
	defer freePtr(valBuf)

	return s.putEntry(keyBuf, keyLen, valBuf, valLen)
}

/*
GetEntry returns what was written by PutEntry.
*/
func (s *DB) GetEntry(tag string, lsn int64, index int32) ([]byte, error) {
	keyBuf, keyLen := indexToKey(IndexKey, tag, lsn, index)
	defer freePtr(keyBuf)

	valBuf, valLen, err := s.readEntry(keyBuf, keyLen)
	if err != nil {
		return nil, err
	}
	defer freePtr(valBuf)
	return ptrToBytes(valBuf, valLen), nil
}

/*
GetEntries returns entries in sequence number order for the given
tag. The first entry returned will be the first entry that matches the specified
startLSN and startIndex. No more than "limit" entries will be returned.
To retrieve the very next entry after an entry, simply increment the index
by 1.
*/
func (s *DB) GetEntries(tag string, startLSN int64,
	startIndex int32, limit uint) ([][]byte, error) {
	startKeyBuf, startKeyLen := indexToKey(IndexKey, tag, startLSN, startIndex)
	defer freePtr(startKeyBuf)
	endKeyBuf, endKeyLen := indexToKey(IndexKey, tag, math.MaxInt64, math.MaxInt32)
	defer freePtr(endKeyBuf)

	it := C.leveldb_create_iterator(s.db, defaultReadOptions)
	C.go_db_iter_seek(it, startKeyBuf, startKeyLen)

	var count uint
	var results [][]byte

	for count < limit && C.leveldb_iter_valid(it) != 0 {
		var keyLen C.size_t
		iterKey := C.leveldb_iter_key(it, &keyLen)

		if compareKeys(unsafe.Pointer(iterKey), keyLen, endKeyBuf, endKeyLen) > 0 {
			// Reached the end of our range
			break
		}

		var dataLen C.size_t
		data := C.leveldb_iter_value(it, &dataLen)
		newVal := ptrToBytes(unsafe.Pointer(data), dataLen)
		results = append(results, newVal)
		count++
		C.leveldb_iter_next(it)
	}

	return results, nil
}

func (s *DB) deleteEntry(keyPtr unsafe.Pointer, keyLen C.size_t) error {
	var e *C.char
	C.go_db_delete(s.db, defaultWriteOptions, keyPtr, keyLen, &e)
	if e != nil {
		defer freeString(e)
		return stringToError(e)
	}
	return nil
}

func (s *DB) putEntry(
	keyPtr unsafe.Pointer, keyLen C.size_t,
	valPtr unsafe.Pointer, valLen C.size_t) error {

	var e *C.char
	C.go_db_put(
		s.db, defaultWriteOptions,
		keyPtr, keyLen, valPtr, valLen,
		&e)
	if e == nil {
		return nil
	}
	defer freeString(e)
	return stringToError(e)
}

func (s *DB) readEntry(
	keyPtr unsafe.Pointer, keyLen C.size_t) (unsafe.Pointer, C.size_t, error) {

	var valLen C.size_t
	var e *C.char

	val := C.go_db_get(
		s.db, defaultReadOptions,
		keyPtr, keyLen,
		&valLen, &e)

	if val == nil {
		if e == nil {
			return nil, 0, nil
		}
		defer freeString(e)
		return nil, 0, stringToError(e)
	}

	return unsafe.Pointer(val), valLen, nil
}
