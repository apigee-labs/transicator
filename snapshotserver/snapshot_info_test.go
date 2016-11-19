package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/apigee-labs/transicator/common"
	"github.com/apigee-labs/transicator/pgclient"

	"strconv"
	"strings"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
)

const (
	debugTests = false
)

var (
	dbURL string

	db *sql.DB
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

	for _, table := range allTables {
		dropTable(table)
	}

	_, err = db.Exec(testTableSQL)
	Expect(err).Should(Succeed())
})

var _ = AfterSuite(func() {
	if db != nil {
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

var allTables = []string{
	"public.app",
	"public.developer",
	"public.snapshot_test",
	"public.apid_config_scope",
	"public.apid_config",
	"transicator_tests.schema_table",
}

const testTableSQL = `
  create schema if not exists transicator_tests;
  create table public.snapshot_test (
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
alter table public.developer replica identity full;
  CREATE TABLE public.APP (
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
alter table public.app replica identity full;

CREATE TABLE public.apid_config (
    id character varying(36) NOT NULL,
    name character varying(100) NOT NULL,
    description character varying(255),
    umbrella_org_app_name character varying(255) NOT NULL,
    created timestamp without time zone,
    created_by character varying(100),
    updated timestamp without time zone,
    updated_by character varying(100),
    CONSTRAINT apid_config_pkey PRIMARY KEY (id)
);
alter table public.apid_config replica identity full;

CREATE TABLE public.apid_config_scope (
    id character varying(36) NOT NULL,
    apid_config_id character varying(36) NOT NULL,
    scope character varying(100) NOT NULL,
    created timestamp without time zone,
    created_by character varying(100),
    updated timestamp without time zone,
    updated_by character varying(100),
    CONSTRAINT apid_config_scope_pkey PRIMARY KEY (id),
    CONSTRAINT apid_config_scope_apid_config_id_fk FOREIGN KEY (apid_config_id)
          REFERENCES apid_config (id)
          ON UPDATE NO ACTION ON DELETE NO ACTION
);
alter table public.apid_config_scope replica identity full;

CREATE TABLE transicator_tests.schema_table (
  id character varying primary key,
	created_at integer,
	_apid_scope character varying
);
alter table transicator_tests.schema_table replica identity full;
`

func getCurrentTxid() int32 {
	row := db.QueryRow("select txid_current()")
	var s string
	err := row.Scan(&s)
	Expect(err).Should(Succeed())
	txid, _ := strconv.Atoi(s)
	return int32(txid)
}

var _ = Describe("Taking a snapshot", func() {

	tables := map[string]bool{
		"public.app":                     true,
		"public.developer":               true,
		"public.snapshot_test":           true,
		"public.apid_config_scope":       true,
		"public.apid_config":             true,
		"transicator_tests.schema_table": true,
	}

	AfterEach(func() {
		fmt.Fprintln(GinkgoWriter, "AfterEach: truncate tables")
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
			_, err = db.Exec(`
				insert into transicator_tests.schema_table (id, created_at, _apid_scope)
			 values ('aaa-bbb-ccc', 9859348, 'pepsi__dev')
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
			var curTable common.TableInfo
			for sr.Next() {
				n := sr.Entry()
				fmt.Fprintf(GinkgoWriter, "next: %T\n", n)
				switch n.(type) {
				case common.TableInfo:
					curTable = n.(common.TableInfo)
					fmt.Fprintf(GinkgoWriter, "Table: %s\n", curTable.Name)
				case common.Row:
					row := n.(common.Row)
					if curTable.Name == "public.app" || curTable.Name == "transicator_tests.schema_table" {
						var id string
						err = row.Get("id", &id)
						Expect(err).Should(Succeed())
						var timestamp int64
						err = row.Get("created_at", &timestamp)
						Expect(err).Should(Succeed())

						Expect(id).Should(Equal("aaa-bbb-ccc"))
						Expect(timestamp).Should(BeEquivalentTo(9859348))
						rowCount++
					}
				case error:
					Expect(n.(error)).Should(Succeed())
				default:
					fmt.Fprintf(GinkgoWriter, "Got unexpected row %T\n", n)
					Expect(false).Should(BeTrue())
				}
			}
			Expect(rowCount).Should(Equal(2))

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
	Context("Insert Tables (apid_config and apid_config_scope), Get JSON data for apidconfigId", func() {
		It("Insert apidconfig, scopes, and retrieve based on apiconfigid", func() {
			keys := []string{"id", "name", "umbrella_org_app_name", "scope", "apid_config_scope_pkey"}
			idvals := []string{"111-222-333", "222-333-444", "aaa-bbb-ccc"}

			tx, err := db.Begin()
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into APID_CONFIG (id, name, umbrella_org_app_name) values ('aaa-bbb-ccc', 'ConfigId1', 'pepsi');")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into APID_CONFIG_SCOPE (id, apid_config_id, scope) values ('111-222-333','aaa-bbb-ccc', 'cokescope1');")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into APID_CONFIG_SCOPE (id, apid_config_id, scope) values ('222-333-444','aaa-bbb-ccc', 'cokescope2');")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into APID_CONFIG (id, name, umbrella_org_app_name) values ('aaa-bbb-ddd', 'ConfigId2', 'pepsi');")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into APID_CONFIG_SCOPE (id, apid_config_id, scope) values ('111-222-555','aaa-bbb-ddd', 'pepsiscope1');")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into APID_CONFIG_SCOPE (id, apid_config_id, scope) values ('222-333-666','aaa-bbb-ddd', 'pespsiscope2');")
			Expect(err).Should(Succeed())
			err = tx.Commit()
			Expect(err).Should(Succeed())

			b, err := GetScopeData("aaa-bbb-ccc", db)
			Expect(err).Should(Succeed())

			s, err := common.UnmarshalSnapshot(b)
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

		})

	})

	Context("Test data formats", func() {
		It("Insert NULLs", func() {
			verifySnap := func(s *common.Snapshot) {
				for _, table := range s.Tables {
					if table.Name == "public.snapshot_test" {
						for _, row := range table.Rows {
							fmt.Fprintf(GinkgoWriter, "%v\n", row)
							var id string
							err := row.Get("id", &id)
							Expect(err).Should(Succeed())
							Expect(id).Should(Equal("one"))
							var vc string
							err = row.Get("varchars", &vc)
							Expect(err).Should(Succeed())
							Expect(vc).Should(BeEmpty())
							var ts string
							err = row.Get("timestamp", &ts)
							Expect(err).Should(Succeed())
							Expect(ts).Should(BeZero())
						}
					}
				}
			}

			_, err := db.Exec("insert into snapshot_test (id, _apid_scope) values ('one', 'foo')")
			Expect(err).Should(Succeed())

			buf := &bytes.Buffer{}
			err = GetTenantSnapshotData([]string{"foo"}, "json", db, buf)
			Expect(err).Should(Succeed())

			fmt.Fprintf(GinkgoWriter, "Snapshot: %s\n", buf.String())
			s, err := common.UnmarshalSnapshot(buf.Bytes())
			Expect(err).Should(Succeed())
			verifySnap(s)

			buf = &bytes.Buffer{}
			err = GetTenantSnapshotData([]string{"foo"}, "proto", db, buf)
			Expect(err).Should(Succeed())
			s, err = common.UnmarshalSnapshotProto(buf)
			Expect(err).Should(Succeed())
			verifySnap(s)
		})

		It("Insert real values", func() {
			verifySnap := func(s *common.Snapshot) {
				for _, table := range s.Tables {
					if table.Name == "public.snapshot_test" {
						for _, row := range table.Rows {
							fmt.Fprintf(GinkgoWriter, "%v\n", row)
							var id string
							err := row.Get("id", &id)
							Expect(err).Should(Succeed())
							Expect(id).Should(Equal("one"))
							var b bool
							err = row.Get("bool", &b)
							Expect(err).Should(Succeed())
							Expect(b).Should(BeTrue())
							var i int
							err = row.Get("int", &i)
							Expect(err).Should(Succeed())
							Expect(i).Should(BeEquivalentTo(123))
							var d float64
							err = row.Get("double", &d)
							Expect(err).Should(Succeed())
							Expect(d).Should(BeEquivalentTo(3.14))
							var tsStr string
							err = row.Get("timestamp", &tsStr)
							Expect(err).Should(Succeed())
							Expect(tsStr).ShouldNot(BeEmpty())
						}
					}
				}
			}

			_, err := db.Exec(`
				insert into snapshot_test (id, _apid_scope, bool, int, double, timestamp)
				values ('one', 'foo', true, 123, 3.14, now())
			`)
			Expect(err).Should(Succeed())

			buf := &bytes.Buffer{}
			err = GetTenantSnapshotData([]string{"foo"}, "json", db, buf)
			Expect(err).Should(Succeed())

			fmt.Fprintf(GinkgoWriter, "Snapshot: %s\n", buf.String())
			s, err := common.UnmarshalSnapshot(buf.Bytes())
			Expect(err).Should(Succeed())
			verifySnap(s)

			buf = &bytes.Buffer{}
			err = GetTenantSnapshotData([]string{"foo"}, "proto", db, buf)
			Expect(err).Should(Succeed())
			s, err = common.UnmarshalSnapshotProto(buf)
			Expect(err).Should(Succeed())
			verifySnap(s)
		})
	})
})
