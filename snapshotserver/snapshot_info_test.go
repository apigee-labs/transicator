package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/30x/transicator/common"
	"github.com/30x/transicator/pgclient"
	"github.com/30x/transicator/replication"
	"github.com/Sirupsen/logrus"

	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
)

const (
	debugTests = false
)

var (
	dbURL string

	repl *replication.Replicator
	db   *sql.DB
)

func TestSnapshot(t *testing.T) {
	dbURL = os.Getenv("TEST_PG_URL")
	if dbURL == "" {
		fmt.Println("Skipping replication tests because TEST_PG_URL not set")
		fmt.Println("  Example: postgres://user:password@host:port/database")
	} else {
		RegisterFailHandler(Fail)
		junitReporter := reporters.NewJUnitReporter("../test-reports/snapshot.xml")
		RunSpecsWithDefaultAndCustomReporters(t, "Snapshot suite", []Reporter{junitReporter})
	}
}

var _ = BeforeSuite(func() {
	if debugTests {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.Debug("Debug on")
	}

	fmt.Fprintf(GinkgoWriter, "Opening database at %s\n", dbURL)
	var err error
	db, err = sql.Open("transicator", dbURL)
	Expect(err).Should(Succeed())

	pgdriver := db.Driver().(*pgclient.PgDriver)
	pgdriver.SetIsolationLevel("repeatable read")
	pgdriver.SetExtendedColumnNames(true)

	if tableExists("snapshot_test") {
		err = truncateTable("snapshot_test")
		Expect(err).Should(Succeed())
	} else {
		_, err = db.Exec(testTableSQL)
		Expect(err).Should(Succeed())
	}

	repl, err = replication.Start(dbURL, "snapshot_test_slot")
	Expect(err).Should(Succeed())
	// There may be duplicates, so always drain first
	drainReplication(repl)
})

var _ = AfterSuite(func() {
	fmt.Println(GinkgoWriter, "AfterSuite: stop replication")
	if repl != nil {
		repl.Stop()
	}

	if db != nil {
		fmt.Println(GinkgoWriter, "AfterSuite: drop replication slot")
		db.Exec("select * from pg_drop_replication_slot('snapshot_test_slot')")
		tables := []string{"snapshot_test", "APP", "DEVELOPER"}
		for _, table := range tables {
			dropTable(table)
		}
		db.Close()
	}
})

func tableExists(name string) bool {
	fmt.Fprintf(GinkgoWriter, "Checking for %s\n", name)
	_, err := db.Query(fmt.Sprintf("select * from %s limit 0", name))
	return err == nil
}

func truncateTable(name string) error {
	// Deleting all the rows of the table does not lock as much and does not block tests
	_, err := db.Exec(fmt.Sprintf("delete from %s", name))
	return err
}

func dropTable(name string) error {
	_, err := db.Exec(fmt.Sprintf("drop table %s", name))
	return err
}

type SnapInfo struct {
	xmin    int32
	xmax    int32
	xipList []int32
}

func getCurrentSnapshotInfo() *SnapInfo {
	row := db.QueryRow("select txid_current_snapshot()")
	var s string
	err := row.Scan(&s)
	Expect(err).Should(Succeed())

	a := strings.Split(s, ":")
	si := new(SnapInfo)
	xmin, _ := strconv.Atoi(a[0])
	si.xmin = int32(xmin)
	xmax, _ := strconv.Atoi(a[1])
	si.xmax = int32(xmax)
	xips := strings.Split(a[2], ",")
	for _, xip := range xips {
		if xip != "" {
			v, _ := strconv.Atoi(xip)
			si.xipList = append(si.xipList, int32(v))
		}
	}
	return si
}

const testTableSQL = `
  create table snapshot_test (
    id varchar(32) primary key,
    bool boolean,
    chars char(64),
    varchars varchar(64),
    int integer,
    smallint smallint,
    bigint bigint,
    float float4,
    double float8,
    date date,
    time time with time zone,
    _apid_scope varchar(255),
    timestamp timestamp with time zone,
    timestampp timestamp
  );
  CREATE TABLE DEVELOPER (
	org varchar(255),
	id varchar(255),
	username varchar(255),
	firstname varchar(255),
	created_at integer,
	created_by varchar(255),
 	_apid_scope varchar(255),
	PRIMARY KEY (id, _apid_scope)
);
alter table developer replica identity full;
  CREATE TABLE APP (
	org varchar(255),
	id varchar(255),
	dev_id varchar(255) null,
	display_name varchar(255),
	name varchar(255),
	created_at integer,
	created_by varchar(255),
 	_apid_scope varchar(255),
	PRIMARY KEY (id, _apid_scope)
);
alter table developer replica identity full;
`

func getCurrentTxid() int32 {
	row := db.QueryRow("select txid_current()")
	var s string
	err := row.Scan(&s)
	Expect(err).Should(Succeed())
	txid, _ := strconv.Atoi(s)
	return int32(txid)
}

func drainReplication(repl *replication.Replicator) {
	// Just pull stuff until we get a bit of a delay
	var maxLSN uint64
	timedOut := false
	for !timedOut {
		timeout := time.After(1 * time.Second)
		select {
		case <-timeout:
			timedOut = true
		case change := <-repl.Changes():
			Expect(change.Error).Should(Succeed())
			fmt.Fprintf(GinkgoWriter, "LSN %d\n", change.CommitSequence)
			if change.CommitSequence > maxLSN {
				maxLSN = change.CommitSequence
			}
		}
	}

	fmt.Fprintf(GinkgoWriter, "Acknowledging %d\n", maxLSN)
	repl.Acknowledge(maxLSN)
}

func getReplChange(r *replication.Replicator) *common.Change {

	var c *common.Change
	Eventually(r.Changes()).Should(Receive(&c))
	Consistently(r.Changes()).ShouldNot(Receive())
	return c
}

var _ = Describe("Taking a snapshot", func() {

	tables := map[string]bool{
		"app":           true,
		"developer":     true,
		"snapshot_test": true,
	}

	BeforeEach(func() {
		// There may be records in the replication queue from deletes done in AfterEach
		drainReplication(repl)
	})

	AfterEach(func() {
		fmt.Println("AfterEach: truncate tables")
		for table := range tables {
			err := truncateTable(table)
			Expect(err).Should(Succeed())
		}
	})

	Context("Insert Tables (App & Developer), Get JSON data for ONE scope", func() {
		It("Insert with single scope, retrieve single scope data", func() {

			keys := []string{"org", "id", "username", "created_at", "_apid_scope",
				"dev_id", "display_name", "created_by", "created_at",
				"firstname", "name"}

			_, err := db.Exec(`
				insert into APP (org, id, dev_id, display_name, created_at, _apid_scope)
				values
				('Pepsi', 'aaa-bbb-ccc', 'xxx-yyy-zzz', 'Oracle', 9859348, 'pepsi__dev')
				`)
			Expect(err).Should(Succeed())

			scope := []string{"pepsi__dev"}
			buf := &bytes.Buffer{}
			err = GetTenantSnapshotData(scope, "json", db, buf)
			Expect(err).Should(Succeed())
			s, err := common.UnmarshalSnapshot(buf.Bytes())
			Expect(err).Should(Succeed())

			for _, table := range s.Tables {
				if !tables[table.Name] {
					fmt.Fprintf(GinkgoWriter, "Skipping unknown table %s\n", table.Name)
					continue
				}

				for _, row := range table.Rows {
					for l, col := range row {
						Expect(keys).To(ContainElement(l))
						if l == "id" {
							Expect(col.Value).Should(Equal("aaa-bbb-ccc"))
							Expect(col.Type).Should(BeEquivalentTo(1043))
						} else if l == "created_at" {
							Expect(col.Value).Should(Equal("9859348"))
							Expect(col.Type).Should(BeEquivalentTo(23))
						}
					}
				}
			}

			// Now do the same thing, but in streaming mode
			buf = &bytes.Buffer{}
			err = GetTenantSnapshotData(scope, "proto", db, buf)
			Expect(err).Should(Succeed())

			sr, err := common.CreateSnapshotReader(buf)
			Expect(err).Should(Succeed())

			rowCount := 0
			var n interface{}
			var curTable common.TableInfo
			for n = sr.Next(); n != io.EOF; n = sr.Next() {
				fmt.Fprintf(GinkgoWriter, "next: %T\n", n)
				switch n.(type) {
				case common.TableInfo:
					curTable = n.(common.TableInfo)
					fmt.Fprintf(GinkgoWriter, "Table: %s\n", curTable.Name)
				case common.Row:
					row := n.(common.Row)
					if curTable.Name == "app" {
						var id string
						err = row.Get("id", &id)
						Expect(err).Should(Succeed())
						var timestamp int64
						err = row.Get("created_at", &timestamp)
						Expect(err).Should(Succeed())

						Expect(rowCount).Should(Equal(0))
						Expect(id).Should(Equal("aaa-bbb-ccc"))
						Expect(timestamp).Should(BeEquivalentTo(9859348))
						rowCount++
					}
				case error:
					Expect(n.(error)).Should(Succeed())
				default:
					fmt.Printf("Got unexpected row %T\n", n)
					Expect(false).Should(BeTrue())
				}
			}
			Expect(rowCount).Should(Equal(1))

			scope = []string{"pepsi_bad"}
			buf = &bytes.Buffer{}
			err = GetTenantSnapshotData(scope, "json", db, buf)
			Expect(err).Should(Succeed())

			s, err = common.UnmarshalSnapshot(buf.Bytes())
			Expect(err).Should(Succeed())

			for _, table := range s.Tables {
				if tables[table.Name] {
					Expect(len(table.Rows)).To(Equal(0))
				}
			}
		})

	})

	Context("Insert Tables (App & Developer), Get JSON data for Multi scopes", func() {
		It("Insert with multiple scopes, retrieve multiple scopes data", func() {
			keys := []string{"org", "id", "username", "created_at", "_apid_scope", "dev_id", "display_name", "created_by", "created_at", "firstname", "name"}
			idvals := []string{"xxx-yyy-zzz", "xxy-yyy-zzz", "xxz-yyy-zzz", "aaa-bbb-ccc"}

			tx, err := db.Begin()
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into DEVELOPER (org, id, username, created_at, _apid_scope) values ('Pepsi', 'xxx-yyy-zzz', 'sundar', 3231231, 'pepsi__dev')")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into DEVELOPER (org, id, username, created_at, _apid_scope) values ('Pepsi', 'xxy-yyy-zzz', 'sundar', 3221231, 'pepsi__dev')")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into DEVELOPER (org, id, username, created_at, _apid_scope) values ('Pepsi', 'xxz-yyy-zzz', 'sundar', 3231231, 'pepsi__test')")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into APP (org, id, dev_id, display_name, created_at, _apid_scope) values ('Pepsi', 'aaa-bbb-ccc', 'xxx-yyy-zzz', 'Oracle', 9859348, 'pepsi__dev')")
			Expect(err).Should(Succeed())
			err = tx.Commit()
			Expect(err).Should(Succeed())

			scope := []string{"pepsi__dev", "pepsi__test"}
			buf := &bytes.Buffer{}
			err = GetTenantSnapshotData(scope, "json", db, buf)
			Expect(err).Should(Succeed())

			s, err := common.UnmarshalSnapshot(buf.Bytes())
			Expect(err).Should(Succeed())

			for _, table := range s.Tables {
				if !tables[table.Name] {
					continue
				}

				for _, row := range table.Rows {
					for l, col := range row {
						Expect(keys).To(ContainElement(l))
						if l == "id" {
							Expect(idvals).To(ContainElement(col.Value))
						}
					}
				}
			}
			scope = []string{"pepsi_bad"}
			buf = &bytes.Buffer{}
			err = GetTenantSnapshotData(scope, "json", db, buf)
			Expect(err).Should(Succeed())

			s, err = common.UnmarshalSnapshot(buf.Bytes())
			Expect(err).Should(Succeed())

			for _, table := range s.Tables {
				if tables[table.Name] {
					Expect(len(table.Rows)).To(Equal(0))
				}
			}
		})
	})
})
