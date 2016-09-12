package snapshot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/30x/transicator/common"
	"github.com/30x/transicator/pgclient"
	"os"

	log "github.com/Sirupsen/logrus"
)

type SnapshotClient struct {
	conn *pgclient.PgConnection
}

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

func GetTenantSnapshotData(connect string, tenantId []string) (b []byte, err error) {

	var (
		snapInfo, snapTime string
	)
	conn, err := pgclient.Connect(connect) // + "?default_transaction_isolation=repeatable read")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	sc := &SnapshotClient{
		conn: conn,
	}

	log.Info("Starting snapshot")
	_, _, err = sc.conn.SimpleQuery("begin isolation level repeatable read")
	if err != nil {
		return nil, err
	}

	_, s, err := sc.conn.SimpleQuery("select now()")
	if err != nil {
		return nil, err
	}
	snapTime = s[0][0]

	_, s, err = sc.conn.SimpleQuery("select txid_current_snapshot()")
	if err != nil {
		return nil, err
	}
	snapInfo = s[0][0]

	_, s2, err := sc.conn.SimpleQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
	if err != nil {
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
		ci, s3, err := sc.conn.SimpleQuery(q)
		if err != nil {
			log.Infof("Failed to get tenant data <Query: %s> in Table %s : %+v", q, t, err)
			continue
		}

		srvItems := []common.Row{}
		stdItem := common.Table{
			Rows: srvItems,
			Name: t,
		}

		for _, x := range s3 {
			srvItem := common.Row{}
			for ind, y := range x {
				scv := common.ColumnVal{
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

func GenerateSnapshotFile(connect string, tenantId []string, destDir string, file string) error {

	data, err := GetTenantSnapshotData(connect, tenantId)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetOrgSnapshot error: %v\n", err)
		return err
	}
	if err = os.MkdirAll(destDir, 0700); err != nil {
		fmt.Fprintf(os.Stderr, "Failed dest directory creation: %v\n", err)
		return err
	}
	destFileName := file + ".json"
	destFilePath := destDir + "/" + destFileName
	f, err := os.Create(destFilePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed dest file creation: %v\n", err)
		return err
	}

	bw, err := f.Write(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write to destination file: %v\n", err)
		return err
	}
	log.Infof("Snapshot complete at %s [Bytes written %d]", destFilePath, bw)
	return nil
}
