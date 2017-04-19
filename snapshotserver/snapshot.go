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
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/30x/goscaffold"
	"github.com/apigee-labs/transicator/common"
	"github.com/julienschmidt/httprouter"

	log "github.com/Sirupsen/logrus"
)

const (
	jsonType       = "json"
	protoType      = "proto"
	sqliteDataType = "sqlite"
	jsonMediaType  = "application/json"
	protoMediaType = "application/transicator+protobuf"
	sqlMediaType   = "application/transicator+sqlite"
)

/*
GetTenants returns a list of tenant IDs turned into a string.
*/
func GetTenants(tenantID []string) string {

	var str bytes.Buffer

	str.WriteString("(")
	for idx, tid := range tenantID {
		log.Debugf("Get table id: %s %d", tid, idx)
		str.WriteString("'" + tid + "'")
		if idx != len(tenantID)-1 {
			str.WriteString(",")
		}
	}
	str.WriteString(")")
	return str.String()

}

/*
GetScopes returns the set of scopes for a particular cluster.
*/
func GetScopes(
	w http.ResponseWriter, r *http.Request,
	db *sql.DB, p httprouter.Params) {

	cid := p[0].Value
	if cid == "" {
		log.Errorf("apidclusterId Missing, Request Ignored")
		return
	}

	data, err := GetScopeData(cid, db)
	if err != nil {
		log.Errorf("GetOrgSnapshot error: %v", err)
		return
	}

	size, err := w.Write(data)
	if err != nil {
		log.Errorf("Writing snapshot id %s : Err: %s", cid, err)
		return
	}

	log.Debugf("Downloaded Scopes for id %s, size %d", cid, size)
	return
}

/*
GetScopeData actually pulls the data for a scope.
*/
func GetScopeData(cid string, db *sql.DB) (b []byte, err error) {

	var (
		snapInfo, snapTime string
	)

	sdataItem := []common.Table{}
	snapData := common.Snapshot{
		Tables:       sdataItem,
		SnapshotInfo: snapInfo,
		Timestamp:    snapTime,
	}

	tx, err := db.Begin()
	if err != nil {
		log.Errorf("Error starting transaction: %s", err)
		return nil, err
	}
	defer tx.Commit()

	rows, err := tx.Query("select * from APID_CLUSTER where id = $1", cid)
	if err != nil {
		log.Errorf("Failed to query APID_CLUSTER. Err: %s", err)
		return nil, err
	}

	err = fillTable(rows, &snapData, "APID_CLUSTER")
	if err != nil {
		log.Errorf("Failed to Insert rows, (Ignored) Err: %s", err)
		return nil, err
	}
	rows.Close()

	rows, err = tx.Query("select * from DATA_SCOPE where apid_cluster_id = $1", cid)
	if err != nil {
		log.Errorf("Failed to query DATA_SCOPE. Err: %s", err)
		return nil, err
	}

	err = fillTable(rows, &snapData, "DATA_SCOPE")
	if err != nil {
		log.Errorf("Failed to Insert rows, (Ignored) Err: %s", err)
		return nil, err
	}
	rows.Close()

	b, err = json.Marshal(snapData)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func fillTable(rows *sql.Rows, snapData *common.Snapshot, table string) (err error) {

	srvItems := []common.Row{}
	stdItem := common.Table{
		Rows: srvItems,
		Name: table,
	}

	columnNames, columnTypes, err := parseColumnNames(rows)
	if err != nil {
		log.Errorf("Failed to get tenant data in Table %s : %+v", table, err)
		return err
	}

	for rows.Next() {
		srvItem := common.Row{}
		cols := make([]interface{}, len(columnNames))
		for i := range cols {
			cols[i] = new(interface{})
		}
		err = rows.Scan(cols...)
		if err != nil {
			log.Errorf("Failed to get tenant data  in Table %s : %+v", table, err)
			return err
		}

		for i, cv := range cols {
			cvp := cv.(*interface{})
			scv := &common.ColumnVal{
				Value: *cvp,
				Type:  columnTypes[i],
			}
			srvItem[columnNames[i]] = scv
		}
		stdItem.AddRowstoTable(srvItem)
	}
	snapData.AddTables(stdItem)
	return nil
}

/*
GetTenantSnapshotData pulls the snapshot for a given set of tenants and sends
them back to a response writer.
*/
func GetTenantSnapshotData(
	tenantID []string, mediaType string,
	db *sql.DB, w io.Writer) error {

	var (
		snapInfo, snapTime string
	)

	log.Debug("Starting snapshot")
	tx, err := db.Begin()
	if err != nil {
		log.Errorf("Failed to set Isolation level : %+v", err)
		return err
	}
	defer tx.Commit()

	row := db.QueryRow("select now()")
	err = row.Scan(&snapTime)
	if err != nil {
		log.Errorf("Failed to get DB timestamp : %+v", err)
		return err
	}

	row = db.QueryRow("select txid_current_snapshot()")
	err = row.Scan(&snapInfo)
	if err != nil {
		log.Errorf("Failed to get DB snapshot TXID : %+v", err)
		return err
	}

	tables, err := getSchemaAndTableNames(db)
	if err != nil {
		log.Errorf("Failed to table names: %+v", err)
		return err
	}
	log.Debugf("Tables in snapshot: %v", tables)

	sdataItem := []common.Table{}
	snapData := &common.Snapshot{
		Tables:       sdataItem,
		SnapshotInfo: snapInfo,
		Timestamp:    snapTime,
	}

	switch mediaType {
	case jsonType:
		return writeJSONSnapshot(snapData, tables, tenantID, db, w)
	case protoType:
		return writeProtoSnapshot(snapData, tables, tenantID, db, w)
	default:
		panic("Media type processing failed")
	}
}

func writeJSONSnapshot(
	snapData *common.Snapshot, tables []string, tenantID []string,
	db *sql.DB, w io.Writer) error {

	for _, tn := range tables {
		// Postgres won't let us parameterize the table name here, and we don't
		// know how to parameterize the list in the "in" parameter
		q := fmt.Sprintf("select * from %s where %s in %s",
			tn, selectorColumn, GetTenants(tenantID))
		rows, err := db.Query(q)
		if err != nil {
			if strings.Contains(err.Error(), "errorMissingColumn") {
				log.Debugf("Skipping table %s: no %s column", tn, selectorColumn)
				continue
			}
			log.Errorf("Failed to get tenant data <Query: %s> in Table %s : %+v", q, tn, err)
			return err
		}
		defer rows.Close()
		err = fillTable(rows, snapData, tn)
		if err != nil {
			log.Errorf("Failed to Insert Table [%s] - Ignored. Err: %+v", tn, err)
		}
	}

	json := snapData.Marshal()
	w.Write(json)
	return nil
}

func writeProtoSnapshot(
	snapData *common.Snapshot, tables []string, tenantID []string,
	db *sql.DB, w io.Writer) error {

	sw, err := common.CreateSnapshotWriter(
		snapData.Timestamp, snapData.SnapshotInfo, w)
	if err != nil {
		log.Errorf("Failed to start snapshot: %s", err)
		return err
	}

	for _, t := range tables {
		q := fmt.Sprintf("select * from %s where %s in %s", t, selectorColumn, GetTenants(tenantID))
		rows, err := db.Query(q)
		if err != nil {
			if strings.Contains(err.Error(), "errorMissingColumn") {
				log.Debugf("Skipping table %s: no %s column", t, selectorColumn)
				continue
			}
			log.Errorf("Failed to get tenant data <Query: %s> in Table %s : %+v", q, t, err)
			return err
		}
		defer rows.Close()

		columnNames, columnTypes, err := parseColumnNames(rows)
		if err != nil {
			log.Errorf("Failed to get tenant data <Query: %s> in Table %s : %+v", q, t, err)
			return err
		}
		var cis []common.ColumnInfo
		for i := range columnNames {
			ci := common.ColumnInfo{
				Name: columnNames[i],
				Type: columnTypes[i],
			}
			cis = append(cis, ci)
		}

		err = sw.StartTable(t, cis)
		if err != nil {
			log.Errorf("Failed to start table: %s", err)
			return err
		}

		for rows.Next() {
			cols := make([]interface{}, len(columnNames))
			for i := range cols {
				cols[i] = new(interface{})
			}
			err = rows.Scan(cols...)
			if err != nil {
				log.Errorf("Failed to get tenant data <Query: %s> in Table %s : %+v", q, t, err)
				return err
			}

			err = sw.WriteRow(cols)
			if err != nil {
				log.Errorf("Error writing column values: %s", err)
				return err
			}
		}

		err = sw.EndTable()
		if err != nil {
			log.Errorf("Error ending table: %s", err)
			return err
		}

		// OK to close more than once. Do it here to free up SQL connection.
		rows.Close()
	}

	return nil
}

func getSchemaAndTableNames(db *sql.DB) ([]string, error) {
	nameRows, err := db.Query(`
		SELECT table_schema, table_name FROM information_schema.tables
		WHERE table_schema not in ('pg_catalog', 'information_schema')
		`)
	if err != nil {
		log.Errorf("Failed to get tables from DB : %+v", err)
		return nil, err
	}
	defer nameRows.Close()

	var tables []string
	for nameRows.Next() {
		var schema, name string
		err = nameRows.Scan(&schema, &name)
		if err != nil {
			log.Errorf("Failed to get table names from DB : %+v", err)
			return nil, err
		}
		tables = append(tables, schema+"."+name)
	}
	return tables, nil
}

/*
parseColumnNames uses a feature of our Postgres driver that returns column
names in the format "name:type" instead of just "name". "type" in this case
is the numeric Postgres type ID.
*/
func parseColumnNames(rows *sql.Rows) ([]string, []int32, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	names := make([]string, len(cols))
	types := make([]int32, len(cols))

	for i, n := range cols {
		sn := strings.SplitN(n, ":", 2)
		if len(sn) > 0 {
			names[i] = sn[0]
		}
		if len(sn) > 1 {
			tn, err := strconv.ParseInt(sn[1], 10, 32)
			if err != nil {
				return nil, nil, err
			}
			types[i] = int32(tn)
		}
	}

	return names, types, nil
}

/*
GenSnapshot is currently implemented in SYNC mode, where in, it
simply returns the scope back the ID redirect URL to query upon,
to get the snapshot - which is yet another SYNC operation
*/
func GenSnapshot(w http.ResponseWriter, r *http.Request) {

	var changeSelectorParam string

	r.ParseForm()
	changeSelectorInput := r.URL.Query()["scope"]
	if len(changeSelectorInput) == 0 {
		sendAPIError(missingScope, "", w, r)
		return
	}

	mediaType := goscaffold.SelectMediaType(r,
		[]string{jsonMediaType, sqlMediaType, protoMediaType})
	typeParam := ""

	switch mediaType {
	case jsonMediaType:
		typeParam = jsonType
	case sqlMediaType:
		typeParam = sqliteDataType
	case protoMediaType:
		typeParam = protoType
	default:
		sendAPIError(unsupportedMediaType, "", w, r)
		return
	}
	for _, selector := range changeSelectorInput {
		changeSelectorParam += "scope=" + selector + "&"
	}
	redURL := "/data?" + changeSelectorParam + "type=" + typeParam

	http.Redirect(w, r, redURL, http.StatusSeeOther)
}

/*
DownloadSnapshot downloads and returns the JSON related to the scope
*/
func DownloadSnapshot(
	w http.ResponseWriter, r *http.Request,
	db *sql.DB, p httprouter.Params) {
	scopes := r.URL.Query()["scope"]
	if len(scopes) == 0 {
		sendAPIError(missingScope, "", w, r)
		return
	}

	mediaType := r.URL.Query().Get("type")
	if mediaType == "" {
		mediaType = jsonType
	}

	switch mediaType {
	case jsonType:
		w.Header().Add("Content-Type", jsonMediaType)
	case sqliteDataType:
		err := WriteSqliteSnapshot(scopes, db, w, r)
		if err != nil {
			log.Errorf("GetTenantSnapshotData error: %v", err)
		}
		return
	case protoType:
		w.Header().Add("Content-Type", protoMediaType)
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err := GetTenantSnapshotData(scopes, mediaType, db, w)
	if err != nil {
		log.Errorf("GetTenantSnapshotData error: %v", err)
		sendAPIError(http.StatusInternalServerError, err.Error(), w, r)
		return
	}

	log.Debugf("Downloaded snapshot", scopes)
	return
}
