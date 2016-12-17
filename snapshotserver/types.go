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
package snapshotserver

import (
	"fmt"

	"github.com/apigee-labs/transicator/pgclient"
)

type sqliteType int

const (
	sqlInteger   sqliteType = iota
	sqlReal      sqliteType = iota
	sqlText      sqliteType = iota
	sqlBlob      sqliteType = iota
	sqlTimestamp sqliteType = iota
)

func (t sqliteType) String() string {
	switch t {
	case sqlInteger:
		return "integer"
	case sqlReal:
		return "real"
	case sqlText:
		return "text"
	case sqlBlob, sqlTimestamp:
		return "blob"
	default:
		panic(fmt.Sprintf("Invalid type %d", t))
	}
}

func pgToSqliteType(typid int) sqliteType {
	switch pgclient.PgType(typid) {
	case pgclient.Int2, pgclient.Int4, pgclient.Int8, pgclient.OID:
		return sqlInteger
	case pgclient.Float4, pgclient.Float8:
		return sqlReal
	case pgclient.Bytea:
		return sqlBlob
	case pgclient.Timestamp, pgclient.TimestampTZ:
		return sqlTimestamp
	default:
		return sqlText
	}
}

func convertPgType(typid int) string {
	return pgToSqliteType(typid).String()
}
