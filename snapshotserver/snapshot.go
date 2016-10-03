package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/30x/transicator/common"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
)

func GetTenants(tenantID []string) string {

	var str bytes.Buffer

	str.WriteString("(")
	for idx, tid := range tenantID {
		log.Info("Get table id: ", tid, idx)
		str.WriteString("'" + tid + "'")
		if idx != len(tenantID)-1 {
			str.WriteString(",")
		}
	}
	str.WriteString(")")
	return str.String()

}

func GetTenantSnapshotData(tenantID []string, db *sql.DB) (b []byte, err error) {

	var (
		snapInfo, snapTime string
	)

	log.Info("Starting snapshot")
	tx, err := db.Begin()
	if err != nil {
		log.Errorf("Failed to set Isolation level : %+v", err)
		return nil, err
	}
	defer tx.Commit()

	row := db.QueryRow("select now()")
	err = row.Scan(&snapTime)
	if err != nil {
		log.Errorf("Failed to get DB timestamp : %+v", err)
		return nil, err
	}

	row = db.QueryRow("select txid_current_snapshot()")
	err = row.Scan(&snapInfo)
	if err != nil {
		log.Errorf("Failed to get DB snapshot TXID : %+v", err)
		return nil, err
	}

	tables, err := getTableNames(db)
	if err != nil {
		log.Errorf("Failed to table names: %+v", err)
		return nil, err
	}
	log.Infof("Tables in snapshot %v", tables)

	sdataItem := []common.Table{}
	snapData := common.Snapshot{
		Tables:       sdataItem,
		SnapshotInfo: snapInfo,
		Timestamp:    snapTime,
	}

	for _, t := range tables {
		q := fmt.Sprintf("select * from %s where _apid_scope in %s", t, GetTenants(tenantID))
		rows, err := db.Query(q)
		if err != nil {
			if strings.Contains(err.Error(), "errorMissingColumn") {
				log.Warnf("Skipping table %s: no _apid_scope column", t)
				continue
			}
			log.Errorf("Failed to get tenant data <Query: %s> in Table %s : %+v", q, t, err)
			return nil, err
		}
		defer rows.Close()

		srvItems := []common.Row{}
		stdItem := common.Table{
			Rows: srvItems,
			Name: t,
		}

		columnNames, columnTypes, err := parseColumnNames(rows)
		if err != nil {
			log.Errorf("Failed to get tenant data <Query: %s> in Table %s : %+v", q, t, err)
			return nil, err
		}

		for rows.Next() {
			srvItem := common.Row{}
			cols := make([]interface{}, len(columnNames))
			for i := range cols {
				cols[i] = new(string)
			}
			err = rows.Scan(cols...)
			if err != nil {
				log.Errorf("Failed to get tenant data <Query: %s> in Table %s : %+v", q, t, err)
				return nil, err
			}

			// TODO where do we get the type? Crud.
			for i, cv := range cols {
				cvs := cv.(*string)
				scv := &common.ColumnVal{
					Value: *cvs,
					Type:  columnTypes[i],
				}
				srvItem[columnNames[i]] = scv
			}
			stdItem.AddRowstoTable(srvItem)
		}
		snapData.AddTables(stdItem)
	}
	b, err = json.Marshal(snapData)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func getTableNames(db *sql.DB) ([]string, error) {
	nameRows, err := db.Query(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
	if err != nil {
		log.Errorf("Failed to get tables from DB : %+v", err)
		return nil, err
	}
	defer nameRows.Close()
	var tables []string
	for nameRows.Next() {
		var tableName string
		err = nameRows.Scan(&tableName)
		if err != nil {
			log.Errorf("Failed to get table names from DB : %+v", err)
			return nil, err
		}
		tables = append(tables, tableName)
	}
	return tables, nil
}

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

	r.ParseForm()
	scopes := r.URL.Query().Get("scopes")
	if scopes == "" {
		log.Errorf("'scopes' missing in Request")
		return
	}
	scopes, _ = url.QueryUnescape(scopes)

	redURL := "/data/" + scopes
	http.Redirect(w, r, redURL, http.StatusSeeOther)

}

/*
DownloadSnapshot downloads and resturns the JSON related to the scope
*/
func DownloadSnapshot(w http.ResponseWriter, r *http.Request, db *sql.DB) {

	vars := mux.Vars(r)
	sid := vars["snapshotid"]
	if sid == "" {
		log.Errorf("snapshot Id Missing, Request Ignored")
		return
	}
	tenantIds := strings.Split(sid, ",")
	log.Infof("'scopes' in Request %s", tenantIds)

	data, err := GetTenantSnapshotData(tenantIds, db)
	if err != nil {
		log.Errorf("GetOrgSnapshot error: %v", err)
		return
	}

	size, err := w.Write(data)
	if err != nil {
		log.Errorf("Writing snapshot id %s : Err: %s", sid, err)
		return
	}

	log.Infof("Downloaded snapshot id %s, size %d", sid, size)
	return

}
