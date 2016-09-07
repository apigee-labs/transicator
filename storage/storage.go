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
	"sort"
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

type readResult struct {
	lsn   int64
	index int32
	data  []byte
}
type readResults []readResult

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
GetIntMetadata returns metadata with the specified key and converts it
into a uint64. It returns 0 if the key cannot be found.
*/
func (s *DB) GetIntMetadata(key string) (int64, error) {
	keyBuf, keyLen := stringToKey(StringKey, key)
	defer freePtr(keyBuf)

	val, valLen, err := s.readEntry(keyBuf, keyLen)
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}

	defer C.leveldb_free(unsafe.Pointer(val))
	return ptrToInt(val, valLen), nil
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

	defer C.leveldb_free(unsafe.Pointer(val))
	return ptrToBytes(val, valLen), nil
}

/*
SetIntMetadata sets the metadata with the specified key to
the integer value.
*/
func (s *DB) SetIntMetadata(key string, val int64) error {
	keyBuf, keyLen := stringToKey(StringKey, key)
	defer freePtr(keyBuf)
	valBuf, valLen := intToPtr(val)
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
PutEntry writes an entry to the database indexed by scope, lsn, and index in order
*/
func (s *DB) PutEntry(scope string, lsn int64, index int32, data []byte) error {
	keyBuf, keyLen := indexToKey(IndexKey, scope, lsn, index)
	defer freePtr(keyBuf)
	valBuf, valLen := bytesToPtr(data)
	defer freePtr(valBuf)

	return s.putEntry(keyBuf, keyLen, valBuf, valLen)
}

/*
PutEntryAndMetadata writes an entry to the database in the same batch as
a metadata write.
*/
func (s *DB) PutEntryAndMetadata(scope string, lsn int64, index int32,
	data []byte, metaKey string, metaVal int64) error {

	batch := C.leveldb_writebatch_create()
	defer C.leveldb_writebatch_destroy(batch)

	keyBuf, keyLen := indexToKey(IndexKey, scope, lsn, index)
	defer freePtr(keyBuf)
	valBuf, valLen := bytesToPtr(data)
	defer freePtr(valBuf)
	C.go_db_writebatch_put(batch, keyBuf, keyLen, valBuf, valLen)

	mkeyBuf, mkeyLen := stringToKey(StringKey, metaKey)
	defer freePtr(mkeyBuf)
	mvalBuf, mvalLen := intToPtr(metaVal)
	defer freePtr(mvalBuf)
	C.go_db_writebatch_put(batch, mkeyBuf, mkeyLen, mvalBuf, mvalLen)

	var e *C.char
	C.leveldb_write(s.db, defaultWriteOptions, batch, &e)

	if e == nil {
		return nil
	}
	defer C.leveldb_free(unsafe.Pointer(e))
	return stringToError(e)
}

/*
GetEntry returns what was written by PutEntry.
*/
func (s *DB) GetEntry(scope string, lsn int64, index int32) ([]byte, error) {
	keyBuf, keyLen := indexToKey(IndexKey, scope, lsn, index)
	defer freePtr(keyBuf)

	valBuf, valLen, err := s.readEntry(keyBuf, keyLen)
	if err != nil {
		return nil, err
	}
	defer C.leveldb_free(unsafe.Pointer(valBuf))
	return ptrToBytes(valBuf, valLen), nil
}

/*
GetEntries returns entries in sequence number order for the given
scope. The first entry returned will be the first entry that matches the specified
startLSN and startIndex. No more than "limit" entries will be returned.
To retrieve the very next entry after an entry, simply increment the index
by 1.
*/
func (s *DB) GetEntries(scope string, startLSN int64,
	startIndex int32, limit int) ([][]byte, error) {

	rr, err := s.readOneRange(scope, startLSN, startIndex, limit, defaultReadOptions)
	if err != nil {
		return nil, err
	}

	var results [][]byte
	for _, r := range rr {
		results = append(results, r.data)
	}
	return results, nil
}

/*
GetMultiEntries returns entries in sequence number order a list of scopes.
The first entry returned will be the first entry that matches the specified
startLSN and startIndex. No more than "limit" entries will be returned.
To retrieve the very next entry after an entry, simply increment the index
by 1. This method uses a snapshot to guarantee consistency even if data is
being inserted to the database -- as long as the data is being inserted
in LSN order!
*/
func (s *DB) GetMultiEntries(scopes []string, startLSN int64,
	startIndex int32, limit int) ([][]byte, error) {

	// Do this all inside a level DB snapshot so that we get a repeatable read
	snap := C.leveldb_create_snapshot(s.db)
	defer C.leveldb_release_snapshot(s.db, snap)

	ropts := C.leveldb_readoptions_create()
	C.leveldb_readoptions_set_snapshot(ropts, snap)
	defer C.leveldb_readoptions_destroy(ropts)

	// Read range for each scope
	var results readResults
	for _, scope := range scopes {
		rr, err := s.readOneRange(scope, startLSN, startIndex, limit, ropts)
		if err != nil {
			return nil, err
		}
		results = append(results, rr...)
	}

	// Sort and then take limit
	sort.Sort(results)

	var final [][]byte
	for count := 0; count < len(results) && count < limit; count++ {
		final = append(final, results[count].data)
	}
	return final, nil
}

func (s *DB) readOneRange(scope string, startLSN int64,
	startIndex int32, limit int, ro *C.leveldb_readoptions_t) (readResults, error) {
	startKeyBuf, startKeyLen := indexToKey(IndexKey, scope, startLSN, startIndex)
	defer freePtr(startKeyBuf)
	endKeyBuf, endKeyLen := indexToKey(IndexKey, scope, math.MaxInt64, math.MaxInt32)
	defer freePtr(endKeyBuf)

	it := C.leveldb_create_iterator(s.db, ro)
	defer C.leveldb_iter_destroy(it)
	C.go_db_iter_seek(it, startKeyBuf, startKeyLen)

	var results readResults

	for count := 0; count < limit && C.leveldb_iter_valid(it) != 0; count++ {
		var keyLen C.size_t
		iterKey := C.leveldb_iter_key(it, &keyLen)

		if compareKeys(unsafe.Pointer(iterKey), keyLen, endKeyBuf, endKeyLen) > 0 {
			// Reached the end of our range
			break
		}

		_, iterLSN, iterIx, err := keyToIndex(unsafe.Pointer(iterKey), keyLen)
		if err != nil {
			return nil, err
		}

		var dataLen C.size_t
		iterData := C.leveldb_iter_value(it, &dataLen)
		newVal := ptrToBytes(unsafe.Pointer(iterData), dataLen)

		result := readResult{
			lsn:   iterLSN,
			index: iterIx,
			data:  newVal,
		}
		results = append(results, result)
		C.leveldb_iter_next(it)
	}

	return results, nil
}

func (s *DB) deleteEntry(keyPtr unsafe.Pointer, keyLen C.size_t) error {
	var e *C.char
	C.go_db_delete(s.db, defaultWriteOptions, keyPtr, keyLen, &e)
	if e != nil {
		defer C.leveldb_free(unsafe.Pointer(e))
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
	defer C.leveldb_free(unsafe.Pointer(e))
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
		defer C.leveldb_free(unsafe.Pointer(e))
		return nil, 0, stringToError(e)
	}

	return unsafe.Pointer(val), valLen, nil
}

// Needed to sort read results by LSN and index

func (r readResults) Len() int {
	return len(r)
}

func (r readResults) Less(i, j int) bool {
	if r[i].lsn < r[j].lsn {
		return true
	}
	if r[i].lsn == r[j].lsn && r[i].index < r[j].index {
		return true
	}
	return false
}

func (r readResults) Swap(i, j int) {
	tmp := r[i]
	r[i] = r[j]
	r[j] = tmp
}
