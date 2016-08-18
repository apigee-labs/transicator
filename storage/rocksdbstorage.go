package storage

/*
#include <stdlib.h>
#include "storage_native.h"
#cgo CFLAGS: -g -O3 -I/usr/local/include
#cgo LDFLAGS: -L/usr/local/lib -lleveldb
*/
import "C"

import (
	"fmt"
	"io"
	"sync"
	"time"
	"unsafe"

	"github.com/30x/changeagent/common"
	"github.com/golang/glog"
)

var defaultWriteOptions = C.rocksdb_writeoptions_create()
var defaultReadOptions = C.rocksdb_readoptions_create()
var rocksInitOnce sync.Once

type rocksDBStorage struct {
	baseFile string
	dbHandle *C.GoRocksDb
	db       *C.rocksdb_t
	metadata *C.rocksdb_column_family_handle_t
	entries  *C.rocksdb_column_family_handle_t
}

/*
CreateRocksDBStorage creates an instance of the Storage interface using RocksDB.

The "baseFile" parameter refers to the name of a directory where RocksDB can
store its data. RocksDB will create many files inside this directory. To create
an empty database, make sure that it is empty.

The "cacheSize" parameter specifies the maximum number of bytes to use in memory
for an LRU cache of database contents. How this cache is used is up to RocksDB.
*/
func CreateRocksDBStorage(baseFile string, cacheSize uint) (Storage, error) {
	stor := &rocksDBStorage{
		baseFile: baseFile,
	}

	rocksInitOnce.Do(func() {
		C.go_rocksdb_init()
	})

	var dbh *C.GoRocksDb
	e := C.go_rocksdb_open(
		C.CString(baseFile),
		C.size_t(cacheSize),
		&dbh)

	if e != nil {
		defer freeString(e)
		err := stringToError(e)
		glog.Errorf("Error opening RocksDB file %s: %v", baseFile, err)
		return nil, err
	}

	glog.Infof("Opened RocksDB file in %s", baseFile)

	stor.dbHandle = dbh
	stor.db = dbh.db
	stor.metadata = dbh.metadata
	stor.entries = dbh.entries

	return stor, nil
}

func (s *rocksDBStorage) GetDataPath() string {
	return s.baseFile
}

func (s *rocksDBStorage) Close() {
	C.go_rocksdb_close(s.dbHandle)
	freePtr(unsafe.Pointer(s.dbHandle))
}

func (s *rocksDBStorage) Delete() error {
	var e *C.char
	opts := C.rocksdb_options_create()
	defer C.rocksdb_options_destroy(opts)

	dbCName := C.CString(s.baseFile)
	defer freeString(dbCName)
	C.rocksdb_destroy_db(opts, dbCName, &e)
	if e == nil {
		glog.Infof("Destroyed RocksDB database in %s", s.baseFile)
		return nil
	}
	defer freeString(e)
	err := stringToError(e)
	if err != nil {
		glog.Infof("Error destroying RocksDB database: %s", err)
	}
	return err
}

func (s *rocksDBStorage) Dump(out io.Writer, max int) {
	mit := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.metadata)
	defer C.rocksdb_iter_destroy(mit)

	fmt.Fprintln(out, "* Metadata *")
	C.rocksdb_iter_seek_to_first(mit)

	for i := 0; i < max && C.rocksdb_iter_valid(mit) != 0; i++ {
		var keyLen C.size_t
		var valLen C.size_t

		keyPtr := unsafe.Pointer(C.rocksdb_iter_key(mit, &keyLen))
		C.rocksdb_iter_value(mit, &valLen)

		_, mkey, _ := keyToUint(keyPtr, keyLen)
		fmt.Fprintf(out, "Metadata   (%d) %d bytes\n", mkey, valLen)
		C.rocksdb_iter_next(mit)
	}

	/*
	  iit := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.indices)
	  defer C.rocksdb_iter_destroy(iit)

	  fmt.Fprintln(out, "* Indices *")
	  C.rocksdb_iter_seek_to_first(iit)

	  for i := 0; i < max && C.rocksdb_iter_valid(iit) != 0; i++ {
	    //var keyLen C.size_t
	    var valLen C.size_t

	    //keyPtr := unsafe.Pointer(C.rocksdb_iter_key(iit, &keyLen))
	    C.rocksdb_iter_value(iit, &valLen)

	    tenName, colName, ixLen, _ := ptrToIndexType(keyPtr, keyLen)

	    if colName == "" {
	      switch ixLen {
	      case startRange:
	        fmt.Fprintf(out, "Tenant (%s) start\n", tenName)
	      case endRange:
	        fmt.Fprintf(out, "Tenant (%s) end\n", tenName)
	      default:
	        fmt.Fprintf(out, "Tenant (%s) %d bytes\n", tenName, valLen)
	      }
	    } else {
	      switch ixLen {
	      case startRange:
	        fmt.Fprintf(out, "Collection (%s) start\n", colName)
	      case endRange:
	        fmt.Fprintf(out, "Collection (%s) end\n", colName)
	      default:
	        fmt.Fprintf(out, "Collection (%s) %d bytes\n", colName, valLen)
	      }
	    }

	    C.rocksdb_iter_next(iit)
	  }
	*/

	eit := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.entries)
	defer C.rocksdb_iter_destroy(eit)

	fmt.Fprintln(out, "* Entries *")
	C.rocksdb_iter_seek_to_first(eit)

	for i := 0; i < max && C.rocksdb_iter_valid(eit) != 0; i++ {
		var keyLen C.size_t
		var valLen C.size_t

		keyPtr := unsafe.Pointer(C.rocksdb_iter_key(eit, &keyLen))
		valPtr := C.rocksdb_iter_value(eit, &valLen)

		_, ekey, _ := keyToUint(keyPtr, keyLen)
		entry, _ := ptrToEntry(unsafe.Pointer(valPtr), valLen)
		fmt.Fprintf(out, "Entry       (%d) %s\n", ekey, entry)

		C.rocksdb_iter_next(eit)
	}
}

func (s *rocksDBStorage) GetUintMetadata(key string) (uint64, error) {
	keyBuf, keyLen := stringToKey(MetadataKey, key)
	defer freePtr(keyBuf)

	val, valLen, err := s.readEntry(s.metadata, keyBuf, keyLen)
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}

	defer freePtr(val)
	return ptrToUint(val, valLen), nil
}

func (s *rocksDBStorage) GetMetadata(key string) ([]byte, error) {
	keyBuf, keyLen := stringToKey(MetadataKey, key)
	defer freePtr(keyBuf)

	val, valLen, err := s.readEntry(s.metadata, keyBuf, keyLen)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	defer freePtr(val)
	return ptrToBytes(val, valLen), nil
}

func (s *rocksDBStorage) SetUintMetadata(key string, val uint64) error {
	keyBuf, keyLen := stringToKey(MetadataKey, key)
	defer freePtr(keyBuf)
	valBuf, valLen := uintToPtr(val)
	defer freePtr(valBuf)

	return s.putEntry(s.metadata, keyBuf, keyLen, valBuf, valLen)
}

func (s *rocksDBStorage) SetMetadata(key string, val []byte) error {
	keyBuf, keyLen := stringToKey(MetadataKey, key)
	defer freePtr(keyBuf)
	valBuf, valLen := bytesToPtr(val)
	defer freePtr(valBuf)

	return s.putEntry(s.metadata, keyBuf, keyLen, valBuf, valLen)
}

// Methods for the Raft index

func (s *rocksDBStorage) AppendEntry(entry *common.Entry) error {
	keyPtr, keyLen := uintToKey(EntryKey, entry.Index)
	defer freePtr(keyPtr)

	valPtr, valLen := entryToPtr(entry)
	defer freePtr(valPtr)

	glog.V(2).Infof("Appending entry: %s", entry)
	return s.putEntry(s.entries, keyPtr, keyLen, valPtr, valLen)
}

// Get term and data for entry. Return term 0 if not found.
func (s *rocksDBStorage) GetEntry(index uint64) (*common.Entry, error) {
	keyPtr, keyLen := uintToKey(EntryKey, index)
	defer freePtr(keyPtr)

	valPtr, valLen, err := s.readEntry(s.entries, keyPtr, keyLen)
	if err != nil {
		return nil, err
	}
	if valPtr == nil {
		return nil, nil
	}

	defer freePtr(valPtr)
	return ptrToEntry(valPtr, valLen)
}

func (s *rocksDBStorage) GetEntries(
	since uint64, max uint, filter func(*common.Entry) bool) ([]common.Entry, error) {

	it := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.entries)
	defer C.rocksdb_iter_destroy(it)

	var entries []common.Entry
	var count uint

	firstKeyPtr, firstKeyLen := uintToKey(EntryKey, since+1)
	defer freePtr(firstKeyPtr)

	C.go_rocksdb_iter_seek(it, firstKeyPtr, firstKeyLen)

	for (count < max) && (C.rocksdb_iter_valid(it) != 0) {
		_, keyType, entry, err := readIterPosition(it)
		if err != nil {
			return nil, err
		}

		if keyType != EntryKey {
			break
		}

		if filter(entry) {
			entries = append(entries, *entry)
			count++
		}

		C.rocksdb_iter_next(it)
	}

	return entries, nil
}

func (s *rocksDBStorage) GetLastIndex() (uint64, uint64, error) {
	it := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.entries)
	defer C.rocksdb_iter_destroy(it)

	C.rocksdb_iter_seek_to_last(it)

	if C.rocksdb_iter_valid(it) == 0 {
		return 0, 0, nil
	}

	index, keyType, entry, err := readIterPosition(it)
	if err != nil {
		return 0, 0, err
	}

	if keyType != EntryKey {
		return 0, 0, nil
	}
	return index, entry.Term, nil
}

func (s *rocksDBStorage) GetFirstIndex() (uint64, error) {
	it := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.entries)
	defer C.rocksdb_iter_destroy(it)

	C.rocksdb_iter_seek_to_first(it)

	if C.rocksdb_iter_valid(it) == 0 {
		return 0, nil
	}

	index, keyType, _, err := readIterPosition(it)
	if err != nil {
		return 0, err
	}

	if keyType != EntryKey {
		return 0, nil
	}
	return index, nil
}

/*
 * Read index, term, and data from current iterator position and free pointers
 * to data returned by RocksDB. Assumes that the iterator is valid at this
 * position!
 */
func readIterPosition(it *C.rocksdb_iterator_t) (uint64, int, *common.Entry, error) {
	var keyLen C.size_t
	keyPtr := C.rocksdb_iter_key(it, &keyLen)

	keyType, key, err := keyToUint(unsafe.Pointer(keyPtr), keyLen)
	if err != nil {
		return 0, 0, nil, err
	}

	if keyType != EntryKey {
		// This function is just for reading index entries. Short-circuit if we see something else.
		return key, keyType, nil, nil
	}

	var valLen C.size_t
	valPtr := C.rocksdb_iter_value(it, &valLen)

	entry, err := ptrToEntry(unsafe.Pointer(valPtr), valLen)
	if err != nil {
		return 0, 0, nil, err
	}
	return key, keyType, entry, nil
}

// Return index and term of everything from index to the end
func (s *rocksDBStorage) GetEntryTerms(first uint64) (map[uint64]uint64, error) {
	it := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.entries)
	defer C.rocksdb_iter_destroy(it)

	terms := make(map[uint64]uint64)

	firstKeyPtr, firstKeyLen := uintToKey(EntryKey, first)
	defer freePtr(firstKeyPtr)

	C.go_rocksdb_iter_seek(it, firstKeyPtr, firstKeyLen)

	for C.rocksdb_iter_valid(it) != 0 {
		index, keyType, entry, err := readIterPosition(it)
		if err != nil {
			return nil, err
		}
		if keyType != EntryKey {
			return terms, nil
		}

		terms[index] = entry.Term

		C.rocksdb_iter_next(it)
	}
	return terms, nil
}

// Delete everything that is greater than or equal to the index
func (s *rocksDBStorage) DeleteEntriesAfter(first uint64) error {
	it := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.entries)
	defer C.rocksdb_iter_destroy(it)

	firstKeyPtr, firstKeyLen := uintToKey(EntryKey, first)
	defer freePtr(firstKeyPtr)

	C.go_rocksdb_iter_seek(it, firstKeyPtr, firstKeyLen)

	for C.rocksdb_iter_valid(it) != 0 {
		var keyLen C.size_t
		keyPtr := C.rocksdb_iter_key(it, &keyLen)

		keyType, key, err := keyToUint(unsafe.Pointer(keyPtr), keyLen)
		if err != nil {
			return err
		}
		if keyType != EntryKey {
			return nil
		}

		err = s.deleteEntry(key)
		if err != nil {
			return err
		}
		C.rocksdb_iter_next(it)
	}
	return nil
}

// Delete everything that is less than the index
func (s *rocksDBStorage) TruncateBefore(max uint64) error {
	it := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.entries)
	defer C.rocksdb_iter_destroy(it)

	C.rocksdb_iter_seek_to_first(it)

	for C.rocksdb_iter_valid(it) != 0 {
		var keyLen C.size_t
		keyPtr := C.rocksdb_iter_key(it, &keyLen)

		keyType, key, err := keyToUint(unsafe.Pointer(keyPtr), keyLen)
		if err != nil {
			return err
		}
		if keyType != EntryKey {
			// We unexpectedly found the wrong type of entry
			return nil
		}

		if key >= max {
			// Done
			return nil
		}

		err = s.deleteEntry(key)
		if err != nil {
			return err
		}
		C.rocksdb_iter_next(it)
	}
	return nil
}

func (s *rocksDBStorage) deleteEntry(ix uint64) error {
	delPtr, delLen := uintToKey(EntryKey, ix)
	defer freePtr(delPtr)

	var e *C.char
	C.go_rocksdb_delete(s.db, defaultWriteOptions, s.entries, delPtr, delLen, &e)
	if e != nil {
		defer freeString(e)
		return stringToError(e)
	}
	return nil
}

// Figure out what index to truncate.
func (s *rocksDBStorage) CalculateTruncate(
	minEntries uint64,
	minDur time.Duration,
	lastIndex uint64) (uint64, error) {

	min, err := s.GetFirstIndex()
	if err != nil {
		return 0, err
	}
	if min == 0 {
		glog.V(2).Info("Empty database -- not truncating")
		return 0, nil
	}

	max, _, err := s.GetLastIndex()
	if err != nil {
		return 0, err
	}
	if max > (lastIndex - 1) {
		max = lastIndex - 1
	}

	// Total number of entries in the DB minus "minIndex"
	total := max - min + 1
	glog.V(2).Infof("Min index: %d. Max: %d. Total = %d", min, max, total)
	if total <= minEntries {
		glog.V(2).Infof("Only %d entries in database -- not truncating", total)
		return 0, nil
	}

	truncMin := max - minEntries
	// Maximum time -- truncate only if before that time
	maxTime := time.Now().Add(-minDur)

	it := C.rocksdb_create_iterator_cf(s.db, defaultReadOptions, s.entries)
	defer C.rocksdb_iter_destroy(it)

	lastKeyPtr, lastKeyLen := uintToKey(EntryKey, truncMin)
	defer freePtr(lastKeyPtr)

	glog.V(3).Infof("Maxtime = %s truncMin = %d", maxTime, truncMin)
	// Iterate backwards until we get a record old enough
	C.go_rocksdb_iter_seek(it, lastKeyPtr, lastKeyLen)

	for {
		if C.rocksdb_iter_valid(it) == 0 {
			// No more records to read, so we can't delete anything
			glog.V(2).Info("Truncate reached start of entries")
			return 0, nil
		}
		ix, _, e, err := readIterPosition(it)
		if err != nil {
			glog.V(2).Infof("Error on truncate: %s", err)
			return 0, err
		}
		glog.V(3).Infof("Entry: ix = %d time = %s", ix, e.Timestamp)
		if !e.Timestamp.After(maxTime) {
			// Reached an old enough one so we're done
			glog.V(2).Infof("Truncate reached an old enough record = %d", ix)
			// Truncate everything BEFORE this one, so add one.
			return ix + 1, nil
		}

		C.rocksdb_iter_prev(it)
	}
}

func (s *rocksDBStorage) truncateBatch(it *C.rocksdb_iterator_t, maxCount uint64, maxTime time.Time) (uint64, error) {
	var deleteCount uint64

	for deleteCount < maxCount {
		if C.rocksdb_iter_valid(it) == 0 {
			return deleteCount, nil
		}

		ix, _, e, err := readIterPosition(it)
		if err != nil {
			return deleteCount, err
		}

		if e.Timestamp.After(maxTime) {
			return deleteCount, err
		}

		err = s.deleteEntry(ix)
		if err != nil {
			return deleteCount, err
		}
		deleteCount++

		C.rocksdb_iter_next(it)
	}
	return deleteCount, nil
}

func (s *rocksDBStorage) putEntry(
	cf *C.rocksdb_column_family_handle_t,
	keyPtr unsafe.Pointer, keyLen C.size_t,
	valPtr unsafe.Pointer, valLen C.size_t) error {

	var e *C.char
	C.go_rocksdb_put(
		s.db, defaultWriteOptions, cf,
		keyPtr, keyLen, valPtr, valLen,
		&e)
	if e == nil {
		return nil
	}
	defer freeString(e)
	return stringToError(e)
}

func (s *rocksDBStorage) readEntry(
	cf *C.rocksdb_column_family_handle_t,
	keyPtr unsafe.Pointer, keyLen C.size_t) (unsafe.Pointer, C.size_t, error) {

	var valLen C.size_t
	var e *C.char

	val := C.go_rocksdb_get(
		s.db, defaultReadOptions, cf,
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
*/
