package snapshotserver

import (
	"github.com/apigee-labs/transicator/pgclient"
	"fmt"
)

type sqliteType int

const (
	sqlInteger sqliteType = iota
	sqlReal    sqliteType = iota
	sqlText    sqliteType = iota
	sqlBlob    sqliteType = iota
)

func (t sqliteType) String() string {
	switch t {
	case sqlInteger:
		return "integer"
	case sqlReal:
		return "real"
	case sqlText:
		return "text"
	case sqlBlob:
		return "blob"
	default:
		panic(fmt.Sprintf("Invalid type %d", t))
	}
}

func pgToSqliteType(typid int) sqliteType {
	switch pgclient.PgType(typid) {
	case
		pgclient.Int2, pgclient.Int4, pgclient.Int8, pgclient.OID,
		pgclient.Timestamp, pgclient.TimestampTZ:
		return sqlInteger
	case pgclient.Float4, pgclient.Float8:
		return sqlReal
	case pgclient.Bytea:
		return sqlBlob
	default:
		return sqlText
	}
}

func convertPgType(typid int) string {
	return pgToSqliteType(typid).String()
}
