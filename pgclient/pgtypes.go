package pgclient

/*
PgType represents a Postgres type OID.
*/
type PgType int

//go:generate stringer -type PgType .

// Constants for well-known OIDs that we care about
const (
	Bytea       PgType = 17
	TimestampTZ PgType = 1184
)

// isBinary returns true if the type should be represented as a []byte,
// and not converted into a string. We will only do this for binary
// ("bytea") data.
func (t PgType) isBinary() bool {
	return t == Bytea
}

// isTimestamp returns true if the type represents a timestamp that we can
// parse into a time.Time object.
func (t PgType) isTimestamp() bool {
	return t == TimestampTZ
}
