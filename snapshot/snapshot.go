package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/30x/transicator/common"
	"github.com/30x/transicator/pgclient"
	"strings"
	"time"
	"net/http"
	"math/rand"
	"net/url"

	"github.com/gorilla/mux"
	log "github.com/Sirupsen/logrus"
)

var src = rand.NewSource(time.Now().UnixNano())

func GetTenants(tenantId []string) string {

	var str bytes.Buffer

	str.WriteString("(")
	for idx, tid := range tenantId {
		log.Info("Get table id: ", tid, idx)
		str.WriteString("'" + tid + "'")
		if idx != len(tenantId)-1 {
			str.WriteString(",")
		}
	}
	str.WriteString(")")
	return str.String()

}

func GetTenantSnapshotData(tenantId []string, conn *pgclient.PgConnection) (b []byte, err error) {

	var (
		snapInfo, snapTime string
	)

	log.Info("Starting snapshot")
	_, _, err = conn.SimpleQuery("begin isolation level repeatable read")
	if err != nil {
		log.Errorf("Failed to set Isolation level : %+v", err)
		return nil, err
	}
	defer conn.SimpleQuery("commit")
	_, s, err := conn.SimpleQuery("select now()")
	if err != nil {
		log.Errorf("Failed to get DB timestamp : %+v", err)
		return nil, err
	}
	snapTime = s[0][0]

	_, s, err = conn.SimpleQuery("select txid_current_snapshot()")
	if err != nil {
		log.Errorf("Failed to get DB snapshot TXID : %+v", err)
		return nil, err
	}
	snapInfo = s[0][0]

	_, s2, err := conn.SimpleQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
	if err != nil {
		log.Errorf("Failed to get tables from DB : %+v", err)
		return nil, err
	}
	var tables []string
	for _, t := range s2 {
		tables = append(tables, t[0])
	}
	log.Infof("Tables in snapshot %v", tables)

	sdataItem := []common.Table{}
	snapData := common.Snapshot{
		Tables:       sdataItem,
		SnapshotInfo: snapInfo,
		Timestamp:    snapTime,
	}

	for _, t := range tables {
		q := fmt.Sprintf("select * from %s where _apid_scope in %s", t, GetTenants(tenantId))
		ci, s3, err := conn.SimpleQuery(q)
		if err != nil {
			log.Errorf("Failed to get tenant data <Query: %s> in Table %s : %+v", q, t, err)
			return nil, err
		}

		srvItems := []common.Row{}
		stdItem := common.Table{
			Rows: srvItems,
			Name: t,
		}

		for _, x := range s3 {
			srvItem := common.Row{}
			for ind, y := range x {
				scv := &common.ColumnVal{
					Value: y,
					Type:  ci[ind].Type,
				}
				srvItem[ci[ind].Name] = scv
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

/*
 * This operation is currently implemented in SYNC mode, where in, it
 * simply returns the scope back the ID redirect URL to query upon,
 * to get the snapshot - which is yet another SYNC operation
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
 * The JSON related to the scope is downloaded and returned
 */
func DownloadSnapshot(w http.ResponseWriter, r *http.Request, conn *pgclient.PgConnection) {

	vars := mux.Vars(r)
	sid := vars["snapshotid"]
	if sid == "" {
		log.Errorf("snapshot Id Missing, Request Ignored")
		return
	}
	tenantIds := strings.Split(sid, ",")
	log.Infof("'scopes' in Request %s", tenantIds)

	data, err := GetTenantSnapshotData(tenantIds, conn)
	if err != nil {
		log.Errorf("GetOrgSnapshot error: %v", err)
		return
	}

	size, err := w.Write(data)
	if (err != nil) {
		log.Errorf("Writing snapshot id %s : Err: ", sid, err)
		return
	}

	log.Infof("Downloaded snapshot id %s, size %d", sid, size)
	return

}


