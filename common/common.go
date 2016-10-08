package common

//go:generate stringer -type Operation .
//go:generate protoc --go_out=. transicator.proto

/*
Operation lists the types of operations on each row in a change list.
*/
type Operation int

// Operation constants. (Not using "iota" here because these IDs are persisted.)
const (
	Insert Operation = 1
	Update Operation = 2
	Delete Operation = 3
)

/*
A ColumnVal is a single column value in a result. It just contains a
value and a type.
*/
type ColumnVal struct {
	// Column value, in the string format that comes straight from Postgres
	Value interface{} `json:"value"`
	// The Postgres type of the column, as defined in the Postgres source
	Type int32 `json:"type"`
}

/*
A Row is a single row in a table. It may be aggregated into an entire set of rows
for a snapshot of a table, or into a list of row operations in a change
list. The key for each property in the row is the name of the field, and the
value is a ColumnVal that includes both the value and type.
*/
type Row map[string]*ColumnVal

/*
A Change is a list of changed rows. Each Change describes how the row changed
(due to an insert, delete, or update), when it changed (LSNs and transaction
IDs from Postgres), and what changed (the new and/or old rows).
*/
type Change struct {
	// Why the row was changed: insert, update, or delete
	Operation Operation `json:"operation"`
	// The name of the table that was updated
	Table string `json:"table"`
	// A value that indicates when the row is changed. It includes the
	// "commit LSN" and "CommitIndex" from below. This is the value that
	// clients should return to the API when looking for a list of changes.
	Sequence string `json:"sequence,omitempty"`
	// The LSN when the change was committed.
	CommitSequence uint64 `json:"commitSequence"`
	// The Postgres LSN when the row changed. This will be interleaved
	// in the change list because transactions commit at different times.
	ChangeSequence uint64 `json:"changeSequence"`
	// The order in which the change happened within the commit. For a transaction
	// affects multiple rows, changes will be listed in order, and this value
	// will begin at zero and be incremented by one.
	CommitIndex uint32 `json:"commitIndex"`
	// The Postgres Transaction ID when this change committed. This may be used
	// to find a CommitSequence from a particular snapshot. (TXIDs are 32
	// bits inside Postgres but we may be able to expand this in the future.)
	TransactionID uint32 `json:"txid"`
	// For an insert operation, the columns that are being inserted. For an update,
	// the new value of the columns
	NewRow Row `json:"newRow,omitempty"`
	// For an update oepration, the old value of the columns. For a delete, the
	// value of the columns that are being deleted
	OldRow Row `json:"oldRow,omitempty"`
	// Sometimes we need to return an error instead of a change. If this is
	// not null, then it means a replication error occurred.
	Error error `json:"-"`
}

/*
GetSequence returns an object that combines the commit LSN and index.
*/
func (c *Change) GetSequence() Sequence {
	return MakeSequence(uint64(c.CommitSequence), uint32(c.CommitIndex))
}

/*
A Table represents a snapshot of a table at a particular point in time.
*/
type Table struct {
	Name string `json:"name"`
	Rows []Row  `json:"rows"`
}

/*
A Snapshot represents a consistent view of a group of tables, all generated
at a consistent point.
*/
type Snapshot struct {
	// SnapshotInfo represents a Postgres transaction snapshot, in the format
	// "xmin:xmax:xip1,xip2,xip3..."
	SnapshotInfo string `json:"snapshotInfo"`
	// Timestamp is a Postgres timestamp that describes when the snapshot was created
	Timestamp string `json:"timestamp"`
	// The tables in the snapshot
	Tables []Table `json:"tables"`
}

/*
A ChangeList represents a set of changes returned from the change server.
It contains a list of rows, and it also contains information about the list.
*/
type ChangeList struct {
	// The highest value for "Sequence" in the whole database. The
	// client may use this to understand when they are at the end of the list
	// of changes.
	LastSequence string `json:"lastSequence"`
	// The lowest value for "Sequence" in the whole database. The client
	// may use this to determine if they are at the beginning of the whole
	// change list and must request a new snapshot to get consistent data.
	FirstSequence string `json:"firstSequence"`
	// All the changes, in order of "commit sequence" and "commit index"
	Changes []Change `json:"changes"`
}
