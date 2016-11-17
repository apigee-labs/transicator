package storage

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/tecbot/gorocksdb"
)

var defaultWriteOptions = gorocksdb.NewDefaultWriteOptions()
var defaultReadOptions = gorocksdb.NewDefaultReadOptions()

const (
	// ComparatorName gets persisted in the DB and must be handled if changed
	ComparatorName = "transicator-entries-v1"
	// defaultCFName is the name of the default column family where we keep metadata
	defaultCFName = "default"
	// entriesCFName is the name of the column family for indexed entries.
	entriesCFName = "entries"
	// metadataCFName is the name of the column family for metadata entries
	metadataCFName = "metadata"
)

type entryComparator struct {
}

/*
Compare tests the order of two keys in the "entries" collection. Keys are sorted
primarily in scope order, then by LSN, then by index within the LSN. This
allows searches to be linear for a given scope.
*/
func (c entryComparator) Compare(a, b []byte) int {
	aScope, aLsn, aIndex, err := keyToLsnAndOffset(a)
	if err != nil {
		panic(fmt.Sprintf("Error parsing database key: %s", err))
	}
	bScope, bLsn, bIndex, err := keyToLsnAndOffset(b)
	if err != nil {
		panic(fmt.Sprintf("Error parsing database key: %s", err))
	}

	scopeCmp := strings.Compare(aScope, bScope)
	if scopeCmp == 0 {
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
	}
	return scopeCmp
}

/*
Name is part of the comparator interface.
*/
func (c entryComparator) Name() string {
	return ComparatorName
}

/*
A DB is a handle to a RocksDB database.
*/
type DB struct {
	baseFile    string
	db          *gorocksdb.DB
	metadataCF  *gorocksdb.ColumnFamilyHandle
	entriesCF   *gorocksdb.ColumnFamilyHandle
	dbOpts      *gorocksdb.Options
	dfltOpts    *gorocksdb.Options
	entriesOpts *gorocksdb.Options
}

type readResult struct {
	lsn   uint64
	index uint32
	data  []byte
}
type readResults []readResult

/*
OpenDB opens a RocksDB database and makes it available for reads and writes.
Opened databases should be closed when done.

The "baseFile" parameter refers to the name of a directory where RocksDB can
store its data. RocksDB will create many files inside this directory. To create
an empty database, make sure that it is empty.
*/
func OpenDB(baseFile string) (*DB, error) {
	stor := &DB{
		baseFile: baseFile,
	}

	var err error

	dbOpts := gorocksdb.NewDefaultOptions()
	defer dbOpts.Destroy()
	dbOpts.SetCreateIfMissing(true)
	dbOpts.SetCreateIfMissingColumnFamilies(true)
	stor.dbOpts = dbOpts

	stor.dfltOpts = gorocksdb.NewDefaultOptions()

	stor.entriesOpts = gorocksdb.NewDefaultOptions()
	stor.entriesOpts.SetComparator(new(entryComparator))

	var cfs []*gorocksdb.ColumnFamilyHandle
	stor.db, cfs, err = gorocksdb.OpenDbColumnFamilies(
		dbOpts,
		baseFile,
		[]string{defaultCFName, entriesCFName, metadataCFName},
		[]*gorocksdb.Options{stor.dfltOpts, stor.entriesOpts, stor.dfltOpts},
	)
	if err != nil {
		return nil, err
	}
	stor.entriesCF = cfs[1]
	stor.metadataCF = cfs[2]

	log.Infof("Opened RocksDB file in %s", baseFile)

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
	s.dfltOpts.Destroy()
	s.entriesOpts.Destroy()
	s.dbOpts.Destroy()
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
	keyBuf := []byte(key)

	val, err := s.db.GetCF(defaultReadOptions, s.metadataCF, keyBuf)
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}

	defer val.Free()
	return bytesToInt(val.Data()), err
}

/*
GetMetadata returns the raw metadata matching the specified key,
or nil if the key is not found.
*/
func (s *DB) GetMetadata(key string) ([]byte, error) {
	return s.readMetadataKey(key, defaultReadOptions)
}

func (s *DB) readMetadataKey(key string, ro *gorocksdb.ReadOptions) ([]byte, error) {
	keyBuf := []byte(key)
	return s.readEntry(keyBuf, s.metadataCF, ro)
}

/*
SetIntMetadata sets the metadata with the specified key to
the integer value.
*/
func (s *DB) SetIntMetadata(key string, val int64) error {
	keyBuf := []byte(key)
	valBuf := intToBytes(val)

	return s.db.PutCF(defaultWriteOptions, s.metadataCF, keyBuf, valBuf)
}

/*
SetMetadata sets the metadata with the specified key.
*/
func (s *DB) SetMetadata(key string, val []byte) error {
	keyBuf := []byte(key)
	return s.db.PutCF(defaultWriteOptions, s.metadataCF, keyBuf, val)
}

/*
PutEntry writes an entry to the database indexed by scope, lsn, and index in order
*/
func (s *DB) PutEntry(scope string, lsn uint64, index uint32, data []byte) (err error) {
	keyBuf := lsnAndOffsetToKey(scope, lsn, index)
	err = s.db.PutCF(defaultWriteOptions, s.entriesCF, keyBuf, data)
	return
}

/*
PutEntryAndMetadata writes an entry to the database in the same batch as
a metadata write.
*/
func (s *DB) PutEntryAndMetadata(scope string, lsn uint64, index uint32,
	data []byte, metaKey string, metaVal []byte) (err error) {
	if len(data) == 0 || len(metaVal) == 0 {
		err = errors.New("Data or metaval is empty")
		return
	}

	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	keyBuf := lsnAndOffsetToKey(scope, lsn, index)
	batch.PutCF(s.entriesCF, keyBuf, data)

	mkeyBuf := []byte(metaKey)
	batch.PutCF(s.metadataCF, mkeyBuf, metaVal)
	err = s.db.Write(defaultWriteOptions, batch)
	return
}

/*
GetEntry returns what was written by PutEntry.
*/
func (s *DB) GetEntry(scope string, lsn uint64, index uint32) ([]byte, error) {
	keyBuf := lsnAndOffsetToKey(scope, lsn, index)
	return s.readEntry(keyBuf, s.entriesCF, defaultReadOptions)
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
	it := s.db.NewIteratorCF(defaultReadOptions, s.entriesCF)
	defer it.Close()

	it.SeekToFirst()

	var rowCount int
	for ; it.Valid(); it.Next() {
		dataBuf := it.Value().Data()
		if filter(dataBuf) {
			err = s.db.DeleteCF(defaultWriteOptions, s.entriesCF, it.Key().Data())
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

	startKeyBuf := lsnAndOffsetToKey(scope, startLSN, startIndex)
	endKeyBuf := lsnAndOffsetToKey(scope, math.MaxInt64, math.MaxInt32)

	it := s.db.NewIteratorCF(ro, s.entriesCF)
	defer it.Close()

	it.Seek(startKeyBuf)

	var results readResults
	c := new(entryComparator)

	for ; it.Valid() && len(results) < limit; it.Next() {
		iterKey := it.Key().Data()
		if c.Compare(iterKey, endKeyBuf) > 0 {
			// Reached the end of our range
			//fmt.Printf("Stopped due to end of range, iterKey is %s, endKey is %s\n", hex.Dump(iterKey), hex.Dump(endKeyBuf))
			break
		}

		_, iterLSN, iterIx, err := keyToLsnAndOffset(iterKey)
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

func (s *DB) readEntry(key []byte, cf *gorocksdb.ColumnFamilyHandle, ro *gorocksdb.ReadOptions) (val []byte, err error) {
	d, err := s.db.GetCF(ro, cf, key)
	if err == nil {
		if d == nil {
			return
		}
		if d.Data() == nil {
			return
		}
		val = make([]byte, d.Size())
		copy(val, d.Data())
		d.Free()
		return
	}
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
