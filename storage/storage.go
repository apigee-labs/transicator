package storage

/*
DB is the main interface to the storage service.
*/
type DB struct {
}

/*
	// Methods for all kinds of metadata used for maintenance and operation
	GetUintMetadata(key string) (uint64, error)
	GetMetadata(key string) ([]byte, error)
	SetUintMetadata(key string, val uint64) error
	SetMetadata(key string, val []byte) error

	// Methods for the Raft index
	AppendEntry(e *common.Entry) error

	// Get term and data for entry. Return term 0 if not found.
	GetEntry(index uint64) (*common.Entry, error)

	// Get entries >= since, with a maximum count of "uint".
	// "filter" is a function that must return true for any valid entries.
	GetEntries(since uint64, max uint, filter func(*common.Entry) bool) ([]common.Entry, error)

	// Return the highest index and term in the database
	GetLastIndex() (uint64, uint64, error)

	// Return the lowest index in the databsae
	GetFirstIndex() (uint64, error)

	// Return index and term of everything from index to the end
	GetEntryTerms(index uint64) (map[uint64]uint64, error)

	// Delete everything that is greater than or equal to the index
	DeleteEntriesAfter(index uint64) error

	// Truncate removes all entries OLDER than "minIndex" and returns
	// the number removed. It may take some time, so should be run in a
	// goroutine. It is idempotent in case it is interrupted.
	// Return the number of entries actually deleted
	TruncateBefore(minIndex uint64) error

	// CalculateTruncate decides which entries should be truncated,
	// ensuring that at least "minEntries" are left
	// in the database, that any entries younger than "minAge" are retained,
	// and that any index including or after "minIndex" is retained.
	// It has to scan the database, so it may take some time.
	// It returns an index that can be used as input to "Truncate".
	CalculateTruncate(minEntries uint64, minAge time.Duration, minIndex uint64) (uint64, error)

	// Maintenance
	Close()
	Delete() error
	GetDataPath() string
	Dump(out io.Writer, max int)
}
*/
