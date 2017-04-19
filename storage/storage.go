/*
Copyright 2016 The Transicator Authors

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
	"time"

	"github.com/apigee-labs/transicator/common"
	"io"
)

/*
An Entry represents a whole record in one go.
*/
type Entry struct {
	Scope string
	LSN   uint64
	Index uint32
	Data  []byte
}

/*
BackupProgress records are returned on a channel by the backup API.
*/
type BackupProgress struct {
	PagesRemaining int
	Done           bool
	Error          error
}

/*
A DB is a generic interface to the storage system. It maintains
entries indexec by scope, lsn, and index within an LSN. It allows
entries to be retrieved sequentially, or to be purged by time.
*/
type DB interface {
	// Close the database
	Close()

	// Delete everything from the database. Must be closed first.
	Delete() error

	// Insert a new entry.
	Put(scope string, lsn uint64, index uint32, data []byte) error

	// Insert a bunch of entries in one atomic batch
	PutBatch(entries []Entry) error

	// Retrieve a single entry
	Get(scope string, lsn uint64, index uint32) ([]byte, error)

	// Scan entries in order from startLSN and startIndex for all scopes
	// in the "scopes" array. If filter is non-nil, return only entries
	// for which it returns true.
	Scan(scopes []string,
		startLSN uint64, startIndex uint32,
		limit int,
		filter func([]byte) bool) ([][]byte, common.Sequence, common.Sequence, error)

	// Delete entries older than "oldest"
	Purge(oldest time.Time) (purgeCount uint64, err error)

	// Make a backup of the current database at the specified file name. The
	// function will return a channel that can be used to read the status of
	// the backup as it is being made.
	Backup(dest string) <-chan BackupProgress

	// GetBackup creates a backup of the current database in a temp filename,
	// and write the persistent db file to the specified writer.
	GetBackup(w io.Writer) error
}
