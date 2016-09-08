package snapshot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/30x/transicator/pgclient"
	"os"

	log "github.com/Sirupsen/logrus"
)

type SnapshotClient struct {
	conn *pgclient.PgConnection
}

type SingleColVal struct {
	Key    string `json:"key"`
	StrVal string `json:"strValue"`
	Type   int32  `json:"type"`
}

type SingleRowVals struct {
	Rowvals []SingleColVal `json:"rows"`
}

type SingleTableData struct {
	Tname string          `json:"table"`
	Tbd   []SingleRowVals `json:"details"`
}

type SnapshotData struct {
	LSN   string            `json:"lsn"`
	Sdata []SingleTableData `json:"snapshotData"`
}

func (sd *SnapshotData) AddTables(tb SingleTableData) []SingleTableData {
	sd.Sdata = append(sd.Sdata, tb)
	return sd.Sdata
}

func (sid *SingleTableData) AddRowstoTable(rv SingleRowVals) []SingleRowVals {
	sid.Tbd = append(sid.Tbd, rv)
	return sid.Tbd
}

func (sco *SingleRowVals) CreateRow(rd SingleColVal) []SingleColVal {
	sco.Rowvals = append(sco.Rowvals, rd)
	return sco.Rowvals
}

func GetTenantSnapshotData(connect string, tenantId []string) (b []byte, err error) {

	var (
		snapInfo string
		str      bytes.Buffer
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

	_, s, err := sc.conn.SimpleQuery("select txid_current_snapshot()")
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

	sdataItem := []SingleTableData{}
	snapData := SnapshotData{
		Sdata: sdataItem,
		LSN:   snapInfo,
	}

	str.WriteString("(")
	for idx, tid := range tenantId {
		log.Info("Get table id: ", tid, idx)
		str.WriteString("'" + tid + "'")
		if idx != len(tenantId)-1 {
			str.WriteString(",")
		}
	}
	str.WriteString(")")

	for _, t := range tables {
		q := fmt.Sprintf("select * from %s where _scope in %s", t, str.String())
		ci, s3, err := sc.conn.SimpleQuery(q)
		if err != nil {
			log.Errorf("Failed to get tenant data <Query: %s> in Table %s : %+v", q, t, err)
			continue
		}

		srvItems := []SingleRowVals{}
		stdItem := SingleTableData{
			Tbd:   srvItems,
			Tname: t,
		}
		for _, x := range s3 {

			scvItems := []SingleColVal{}
			srvItem := SingleRowVals{
				Rowvals: scvItems,
			}

			for ind, y := range x {
				scv := SingleColVal{
					Key:    ci[ind].Name,
					StrVal: y,
					Type:   ci[ind].Type,
				}
				srvItem.CreateRow(scv)
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
