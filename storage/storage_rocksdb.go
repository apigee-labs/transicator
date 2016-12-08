// +build rocksdb

/*
Copyright 2016 Google Inc.

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

package storage

import (
	"math"
	"sort"

	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/apigee-labs/transicator/common"
	"github.com/tecbot/gorocksdb"
)

var defaultWriteOptions = gorocksdb.NewDefaultWriteOptions()
var defaultReadOptions = gorocksdb.NewDefaultReadOptions()

const (
	// defaultCFName is the name of the default column family where we keep metadata
	defaultCFName = "default"
	// entriesCFName is the name of the column family for indexed entries.
	entriesCFName = "entries"
	// sequenceCFName is the name of the column family for sequence entries
	sequenceCFName = "sequence"
)

/*
An RocksDB is a handle to a RocksDB database.
*/
type RocksDB struct {
	baseFile     string
	db           *gorocksdb.DB
	sequenceCF   *gorocksdb.ColumnFamilyHandle
	entriesCF    *gorocksdb.ColumnFamilyHandle
	dbOpts       *gorocksdb.Options
	dfltOpts     *gorocksdb.Options
	sequenceOpts *gorocksdb.Options
	entriesOpts  *gorocksdb.Options
}

type readResult struct {
	lsn   uint64
	index uint32
	data  []byte
}
type readResults []readResult

/*
Open opens a RocksDB database and makes it available for reads and writes.
Opened databases should be closed when done.

The "baseFile" parameter refers to the name of a directory where RocksDB can
store its data. RocksDB will create many files inside this directory. To create
an empty database, make sure that it is empty.
*/
func Open(baseFile string) (DB, error) {

	success := false
	stor := &RocksDB{
		baseFile: baseFile,
	}
	defer func() {
		if !success {
			cleanup(stor)
		}
	}()

	var err error

	dbOpts := gorocksdb.NewDefaultOptions()
	dbOpts.SetCreateIfMissing(true)
	dbOpts.SetCreateIfMissingColumnFamilies(true)
	stor.dbOpts = dbOpts

	stor.dfltOpts = gorocksdb.NewDefaultOptions()

	stor.entriesOpts = gorocksdb.NewDefaultOptions()
	stor.entriesOpts.SetComparator(entryComparator)

	stor.sequenceOpts = gorocksdb.NewDefaultOptions()
	stor.sequenceOpts.SetComparator(sequenceComparator)

	var cfs []*gorocksdb.ColumnFamilyHandle
	stor.db, cfs, err = gorocksdb.OpenDbColumnFamilies(
		dbOpts,
		baseFile,
		[]string{defaultCFName, entriesCFName, sequenceCFName},
		[]*gorocksdb.Options{stor.dfltOpts, stor.entriesOpts, stor.sequenceOpts},
	)
	if err != nil {
		return nil, err
	}
	stor.entriesCF = cfs[1]
	stor.sequenceCF = cfs[2]

	log.Infof("Opened RocksDB file in %s", baseFile)
	success = true

	return stor, nil
}

/*
Close closes the database cleanly.
*/
func (s *RocksDB) Close() {
	log.Infof("Closed DB in %s", s.baseFile)
	s.db.Close()
	cleanup(s)
}

func cleanup(s *RocksDB) {
	if s.dfltOpts != nil {
		s.dfltOpts.Destroy()
	}
	if s.sequenceOpts != nil {
		s.sequenceOpts.Destroy()
	}
	if s.entriesOpts != nil {
		s.entriesOpts.Destroy()
	}
	if s.dbOpts != nil {
		s.dbOpts.Destroy()
	}
}

/*
Delete deletes all the files used by the database.
*/
func (s *RocksDB) Delete() error {
	options := gorocksdb.NewDefaultOptions()
	defer options.Destroy()
	return gorocksdb.DestroyDb(s.baseFile, options)
}

/*
Put writes an entry to the database indexed by scope, lsn, and index in order
*/
func (s *RocksDB) Put(scope string, lsn uint64, index uint32, data []byte) error {
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	s.putOne(batch, scope, lsn, index, data)

	return s.db.Write(defaultWriteOptions, batch)
}

/*
PutBatch does multiple Put operations in a single batch.
*/
func (s *RocksDB) PutBatch(entries []Entry) error {
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, entry := range entries {
		s.putOne(batch, entry.Scope, entry.LSN, entry.Index, entry.Data)
	}

	return s.db.Write(defaultWriteOptions, batch)
}

func (s *RocksDB) putOne(b *gorocksdb.WriteBatch, scope string, lsn uint64, index uint32, data []byte) {
	keyBuf := lsnAndOffsetToKey(scope, lsn, index)
	b.PutCF(s.entriesCF, keyBuf, prependTimestamp(time.Now(), data))

	seqBuf := common.MakeSequence(lsn, index).Bytes()
	b.PutCF(s.sequenceCF, seqBuf, nil)
}

/*
Get returns what was written by PutEntry.
*/
func (s *RocksDB) Get(scope string, lsn uint64, index uint32) ([]byte, error) {
	keyBuf := lsnAndOffsetToKey(scope, lsn, index)
	bb, err := s.readEntry(keyBuf, s.entriesCF, defaultReadOptions)
	if err != nil {
		return nil, err
	}
	if bb == nil {
		return nil, nil
	}
	_, ret := extractTimestamp(bb)
	return ret, nil
}

/*
Scan returns entries in sequence number order a list of scopes.
It also returns the sequences of the first and last records in the DB.
The first entry returned will be the first entry that matches the specified
startLSN and startIndex. No more than "limit" entries will be returned.
To retrieve the very next entry after an entry, simply increment the index
by 1. This method uses a snapshot to guarantee consistency even if data is
being inserted to the database -- as long as the data is being inserted
in LSN order!
The array returned is the array of entries (again, in "sequence" order).
*/
func (s *RocksDB) Scan(
	scopes []string,
	startLSN uint64, startIndex uint32,
	limit int, filter func([]byte) bool) (final [][]byte, firstSeq common.Sequence, lastSeq common.Sequence, err error) {

	// Do this all inside a level DB snapshot so that we get a repeatable read
	snap := s.db.NewSnapshot()
	defer snap.Release()

	ropts := gorocksdb.NewDefaultReadOptions()
	ropts.SetSnapshot(snap)
	defer ropts.Destroy()

	firstSeq, lastSeq, err = s.readLimits(ropts)
	if err != nil {
		return
	}

	// Read range for each scope
	var results readResults
	for _, scope := range scopes {
		var rr readResults
		rr, err = s.readOneRange(scope, startLSN, startIndex, limit, ropts, filter)
		if err != nil {
			return
		}
		results = append(results, rr...)
	}

	// Sort and then take limit
	sort.Sort(results)

	for count := 0; count < len(results) && count < limit; count++ {
		final = append(final, results[count].data)
	}
	return
}

/*
Purge deletes everything from the database that was inserted before the time
specified in "oldest." If there is an error during the purge process,
then a non-nil error will be returned. Be aware that this operation may
take a long time, so it is important to run it in a separate goroutine.
*/
func (s *RocksDB) Purge(oldest time.Time) (purgeCount uint64, err error) {
	it := s.db.NewIteratorCF(defaultReadOptions, s.entriesCF)
	defer it.Close()
	it.SeekToFirst()

	for ; it.Valid(); it.Next() {
		// No need to copy the slice here -- we just read the timestamp
		ts, _ := extractTimestamp(it.Value().Data())
		if ts.Before(oldest) {
			err = s.deleteIterKey(it)
			if err != nil {
				return
			}
			purgeCount++
		}
	}
	return
}

func (s *RocksDB) deleteIterKey(it *gorocksdb.Iterator) error {
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	keyData := readIterKey(it)
	_, lsn, index, err := keyToLsnAndOffset(keyData)
	if err != nil {
		return err
	}

	batch.DeleteCF(s.sequenceCF, common.MakeSequence(lsn, index).Bytes())
	batch.DeleteCF(s.entriesCF, keyData)
	return s.db.Write(defaultWriteOptions, batch)
}

func (s *RocksDB) readOneRange(scope string, startLSN uint64,
	startIndex uint32, limit int, ro *gorocksdb.ReadOptions,
	filter func([]byte) bool) (readResults, error) {

	startKeyBuf := lsnAndOffsetToKey(scope, startLSN, startIndex)
	endKeyBuf := lsnAndOffsetToKey(scope, math.MaxInt64, math.MaxInt32)

	it := s.db.NewIteratorCF(ro, s.entriesCF)
	defer it.Close()

	it.Seek(startKeyBuf)

	var results readResults

	for ; it.Valid() && len(results) < limit; it.Next() {
		iterKey := it.Key().Data()
		if entryComparator.Compare(iterKey, endKeyBuf) > 0 {
			// Reached the end of our range
			//fmt.Printf("Stopped due to end of range, iterKey is %s, endKey is %s\n", hex.Dump(iterKey), hex.Dump(endKeyBuf))
			break
		}

		_, iterLSN, iterIx, err := keyToLsnAndOffset(iterKey)
		if err != nil {
			return nil, err
		}

		bb := readSlice(it.Value())
		_, buf := extractTimestamp(bb)

		if filter == nil || filter(buf) {
			result := readResult{
				lsn:   iterLSN,
				index: iterIx,
				data:  buf,
			}
			results = append(results, result)
		}
	}

	return results, nil
}

func (s *RocksDB) readEntry(key []byte, cf *gorocksdb.ColumnFamilyHandle, ro *gorocksdb.ReadOptions) ([]byte, error) {
	d, err := s.db.GetCF(ro, cf, key)
	if err == nil {
		return readSlice(d), nil
	}
	return nil, err
}

func (s *RocksDB) readLimits(ro *gorocksdb.ReadOptions) (firstSeq, lastSeq common.Sequence, err error) {
	// Read first and last sequences
	seqIter := s.db.NewIteratorCF(ro, s.sequenceCF)
	defer seqIter.Close()

	seqIter.SeekToFirst()
	kb := readIterKey(seqIter)
	if kb != nil {
		firstSeq, err = common.ParseSequenceBytes(kb)
		if err != nil {
			return
		}

		seqIter.SeekToLast()
		kb = readIterKey(seqIter)
		if kb == nil {
			lastSeq = firstSeq
		} else {
			lastSeq, err = common.ParseSequenceBytes(kb)
			if err != nil {
				return
			}
		}
	}
	return
}

func readIterKey(it *gorocksdb.Iterator) []byte {
	if !it.Valid() {
		return nil
	}
	return readSlice(it.Key())
}

func readSlice(s *gorocksdb.Slice) []byte {
	if s == nil {
		return nil
	}
	if s.Data() == nil {
		return nil
	}
	ret := make([]byte, s.Size())
	copy(ret, s.Data())
	s.Free()
	return ret
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
