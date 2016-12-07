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
	"github.com/apigee-labs/transicator/common"
	"time"
)

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

	// Retrieve a single entry
	Get(scope string, lsn uint64, index uint32) ([]byte, error)

	// Scan entries in order from startLSN and startIndex for all scopes
	// in the "scopes" array. If filter is non-nil, return only entries
	// for which it returns true.
	Scan(scopes []string,
	     startLSN uint64, startIndex uint32,
	     limit int,
	     filter func ([]byte) bool) ([][]byte, common.Sequence, common.Sequence, error)

  // Delete entries older than "oldest"
	Purge(oldest time.Time) (purgeCount uint64, err error)
}
