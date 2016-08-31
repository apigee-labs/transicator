package snapshot

import (
	"io"

	"github.com/30x/transicator/pgclient"

	"fmt"

	log "github.com/Sirupsen/logrus"
)

type SnapshotClient struct {
	conn *pgclient.PgConnection
}

func GetTenantSnapshotData(connect, tenantId string, cf pgclient.CopyFormat) (snapInfo string, data io.ReadCloser, err error) {
	conn, err := pgclient.Connect(connect) // + "?default_transaction_isolation=repeatable read")
	if err != nil {
		return
	}
	sc := &SnapshotClient{
		conn: conn,
	}

	log.Info("Starting snapshot")
	_, _, err = sc.conn.SimpleQuery("begin isolation level repeatable read")
	if err != nil {
		return
	}

	_, s, err := sc.conn.SimpleQuery("select txid_current_snapshot()")
	if err != nil {
		return
	}
	snapInfo = s[0][0]

	_, s2, err := sc.conn.SimpleQuery("select table_name from table_catalog")
	if err != nil {
		return
	}
	var tables []string
	for _, t := range s2 {
		tables = append(tables, t[0])
	}
	log.Infof("Tables in snapshot %v", tables)

	r, w := io.Pipe()
	go func() {
		defer w.Close()
		defer conn.Close()

		var wr io.Writer = w
		for _, t := range tables {
			log.Info("Get table: ", t)
			//TODO: sanitize me
			q := fmt.Sprintf("select * from %s where tenant_id = '%s'", t, tenantId)
			wr, err = sc.conn.CopyTo(wr, q, cf)
			if err != nil {
				log.Errorf("Failed to get tenant data: %+v", err)
				return
			}
		}
	}()
	return snapInfo, r, nil
}
