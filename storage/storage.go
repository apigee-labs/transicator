package storage

import (
	"github.com/tecbot/gorocksdb"
	"math"
	"sort"
	log "github.com/Sirupsen/logrus"
        "bytes"
	"errors"
)


var defaultWriteOptions = gorocksdb.NewDefaultWriteOptions()
var defaultReadOptions = gorocksdb.NewDefaultReadOptions()

const (
	ComparatorName = "TRANSICATOR-V1"
)

type KeyComparator struct {
}

func (k KeyComparator) Compare(a, b []byte) int {
	if len(a) < 1 || len(b) < 1 {
		return 0
	}

	// Do something reasonable if the versions do not match
	vers1 := (int)((a[0] >> 4) & 0xf)
	vers2 := (int)((b[0] >> 4) & 0xf)
	if (vers1 != KeyVersion) || (vers2 != KeyVersion) {
		return vers1 + 100 /* WTF IS THIS */
	}

	// The types don't match, just compare the types
	type1 := a[0] & 0xf
	type2 := b[0] & 0xf

	if type1 < type2 {
		return -1
	}

	if type1 > type2 {
		return 1
	}

	switch type1 {
	case StringKey:
		return bytes.Compare(a,b)
	case IndexKey:
		return k.CompareIndexKey(a, b)
	default:
		return 999
	}

}

func (k KeyComparator) CompareIndexKey(a,b []byte) int {
	aScope, aLsn, aIndex, err := keyToLsnAndOffset(a)
	if err != nil {
		return 999 // Might as well follow the above when things are berzerk
	}
	bScope, bLsn, bIndex, err := keyToLsnAndOffset(b)
	if err != nil {
		return 999
	}

	if aScope == bScope {
		if aLsn < bLsn {
			return -1
		} else if aLsn > bLsn {
			return 1
		}

		if aIndex < bIndex {
			return -1
		} else if aIndex > bIndex {
			return 1
		}
		return 0
	} else {
		return bytes.Compare([]byte(aScope), []byte(bScope))
	}



}

func (k KeyComparator) Name() string {
	return ComparatorName
}

/*
A DB is a handles to a LevelDB database.
*/
type DB struct {
	baseFile string
	db       *gorocksdb.DB
	options  *gorocksdb.Options
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

	var err error

	stor.options = gorocksdb.NewDefaultOptions()
	stor.options.SetCreateIfMissing(true)
	stor.options.SetComparator(new(KeyComparator))

	stor.db, err = gorocksdb.OpenDb(stor.options, baseFile)
	if err != nil {
		return nil, err
	}

	log.Infof("Opened LevelDB file in %s", baseFile)

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
	log.Infof("Closed DB in %s", s.baseFile)
	s.db.Close()
	s.options.Destroy()
}

/*
Delete deletes all the files used by the database.
*/
func (s *DB) Delete() error {
	options := gorocksdb.NewDefaultOptions()
	defer options.Destroy()
        return gorocksdb.DestroyDb(s.baseFile, options)
}

/*
GetIntMetadata returns metadata with the specified key and converts it
into a uint64. It returns 0 if the key cannot be found.
*/
func (s *DB) GetIntMetadata(key string) (int64, error) {
	keyBuf := stringToKey(StringKey, key)

        val, err := s.db.GetBytes(defaultReadOptions, keyBuf)
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}

        return bytesToInt(val), err
}

/*
GetMetadata returns the raw metadata matching the specified key,
or nil if the key is not found.
*/
func (s *DB) GetMetadata(key string) ([]byte, error) {
	return s.readMetadataKey(key, defaultReadOptions)
}

func (s *DB) readMetadataKey(key string, ro *gorocksdb.ReadOptions) ([]byte, error) {
	keyBuf := stringToKey(StringKey, key)

	return s.readEntry(keyBuf, ro)
}

/*
SetIntMetadata sets the metadata with the specified key to
the integer value.
*/
func (s *DB) SetIntMetadata(key string, val int64) error {
	keyBuf := stringToKey(StringKey, key)
	valBuf := intToBytes(val)

	return s.putEntry(keyBuf, valBuf, defaultWriteOptions)
}

/*
SetMetadata sets the metadata with the specified key.
*/
func (s *DB) SetMetadata(key string, val []byte) error {
	keyBuf := stringToKey(StringKey, key)

	return s.putEntry(keyBuf, val, defaultWriteOptions)
}

/*
PutEntry writes an entry to the database indexed by scope, lsn, and index in order
*/
func (s *DB) PutEntry(scope string, lsn uint64, index uint32, data []byte) error {
	keyBuf := lsnAndOffsetToKey(IndexKey, scope, lsn, index)

	return s.putEntry(keyBuf, data, defaultWriteOptions)
}

/*
PutEntryAndMetadata writes an entry to the database in the same batch as
a metadata write.
*/
func (s *DB) PutEntryAndMetadata(scope string, lsn uint64, index uint32,
	data []byte, metaKey string, metaVal []byte) (err error) {
	if len(data) == 0 || len(metaVal) == 0{
		err = errors.New("Data or metaval is empty")
		return
	}

	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	keyBuf := lsnAndOffsetToKey(IndexKey, scope, lsn, index)
	batch.Put(keyBuf, data)

	mkeyBuf := stringToKey(StringKey, metaKey)
	batch.Put(mkeyBuf, metaVal)
	err = s.db.Write(defaultWriteOptions, batch)
	return
}

/*
GetEntry returns what was written by PutEntry.
*/
func (s *DB) GetEntry(scope string, lsn uint64, index uint32) ([]byte, error) {
	keyBuf := lsnAndOffsetToKey(IndexKey, scope, lsn, index)
	return s.readEntry(keyBuf, defaultReadOptions)
}

/*
GetEntries returns entries in sequence number order for the given
scope. The first entry returned will be the first entry that matches the specified
startLSN and startIndex. No more than "limit" entries will be returned.
To retrieve the very next entry after an entry, simply increment the index
by 1.
*/
func (s *DB) GetEntries(scope string, startLSN uint64,
	startIndex uint32, limit int, filter func([]byte) bool) ([][]byte, error) {

	rr, err := s.readOneRange(scope, startLSN, startIndex, limit,
		defaultReadOptions, filter)
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
It also returns a set of metadata.
The first entry returned will be the first entry that matches the specified
startLSN and startIndex. No more than "limit" entries will be returned.
To retrieve the very next entry after an entry, simply increment the index
by 1. This method uses a snapshot to guarantee consistency even if data is
being inserted to the database -- as long as the data is being inserted
in LSN order!
The first array returned is the array of entries (again, in "sequence" order).
The second is the array of metadata values, if any, in the order in which
the keys were supplied.
*/
func (s *DB) GetMultiEntries(scopes []string, metadataKeys []string,
	startLSN uint64, startIndex uint32,
	limit int, filter func([]byte) bool) ([][]byte, [][]byte, error) {

	// Do this all inside a level DB snapshot so that we get a repeatable read
	snap := s.db.NewSnapshot()
	defer snap.Release()

	ropts := gorocksdb.NewDefaultReadOptions()
	ropts.SetSnapshot(snap)
	defer ropts.Destroy()

	// Read range for each scope
	var results readResults
	for _, scope := range scopes {
		rr, err := s.readOneRange(scope, startLSN, startIndex, limit, ropts, filter)
		if err != nil {
			return nil, nil, err
		}
		results = append(results, rr...)
	}

	// Read metadata for each key
	var metadata [][]byte
	for _, key := range metadataKeys {
		data, err := s.readMetadataKey(key, ropts)
		if err != nil {
			if err != nil {
				return nil, nil, err
			}
		}
		metadata = append(metadata, data)
	}

	// Sort and then take limit
	sort.Sort(results)

	var final [][]byte
	for count := 0; count < len(results) && count < limit; count++ {
		final = append(final, results[count].data)
	}
	return final, metadata, nil
}

/*
PurgeEntries deletes everything from the database for which "filter" returns
true. It always returns the number of records that were actually deleted
(which could be zero). If there is an error during the purge process,
then a non-nil error will be returned. Be aware that this operation may
take a long time, so it is important to run it in a separate goroutine.
*/
func (s *DB) PurgeEntries(filter func([]byte) bool) (purgeCount uint64, err error) {
	it := s.db.NewIterator(defaultReadOptions)
	defer it.Close()

	it.SeekToFirst()

	var rowCount int
	for it = it; it.Valid(); it.Next() {
		dataBuf := it.Value().Data()
		if filter(dataBuf) {
			err = s.deleteEntry(it.Key().Data())
			if err != nil {
				return
			}
			purgeCount++
		}
		rowCount++
	}
	return
}

func (s *DB) readOneRange(scope string, startLSN uint64,
	startIndex uint32, limit int, ro *gorocksdb.ReadOptions,
	filter func([]byte) bool) (readResults, error) {

	startKeyBuf := lsnAndOffsetToKey(IndexKey, scope, startLSN, startIndex)
	endKeyBuf := lsnAndOffsetToKey(IndexKey, scope, math.MaxInt64, math.MaxInt32)


	it := s.db.NewIterator(ro)
	defer it.Close()

	it.Seek(startKeyBuf)

	var results readResults
	c := new(KeyComparator)

	for it = it; it.Valid() && len(results) < limit; it.Next() {
		iterKey := it.Key().Data()
		if c.Compare(iterKey, endKeyBuf) > 0 {
			// Reached the end of our range
			//fmt.Printf("Stopped due to end of range, iterKey is %s, endKey is %s\n", hex.Dump(iterKey), hex.Dump(endKeyBuf))
			break
		}

		_, iterLSN, iterIx, err := keyToIndex(iterKey)
		if err != nil {
			return nil, err
		}

		newVal := it.Value().Data()

		if filter == nil || filter(newVal) {
			result := readResult{
				lsn:   iterLSN,
				index: iterIx,
				data:  newVal,
			}
			results = append(results, result)
		}
	}

	return results, nil
}

func (s *DB) deleteEntry(key []byte) (err error) {
	err = s.db.Delete(defaultWriteOptions, key)
	return
}

func zeroByteSlice(slice []byte) (allZeros bool) {
	allZeros = true
	for s, _ := range(slice) {
		if s != 0x0 {
			allZeros = false
			return
		}
	}
	return
}

func (s *DB) putEntry(key, value []byte, wo *gorocksdb.WriteOptions) (err error) {
	err = s.db.Put(wo,key,value)
	return
}

func (s *DB) readEntry(key []byte, ro *gorocksdb.ReadOptions) (val []byte, err error) {
	val, err = s.db.GetBytes(ro, key)
	return
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
