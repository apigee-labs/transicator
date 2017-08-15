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
	"fmt"
	"os"
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/apigee-labs/transicator/common"
	"github.com/apigee-labs/transicator/pgclient"

	"time"

	"github.com/30x/goscaffold"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/spf13/viper"
)

const (
	debugTests = false
)

var (
	dbURL string

	db           *sql.DB
	testStop     chan bool
	testBase     string
	testListener *goscaffold.HTTPScaffold
)

func TestSnapshot(t *testing.T) {
	dbURL = os.Getenv("TEST_PG_URL")
	if dbURL == "" {
		t.Log("Cannot run snapshot tests because TEST_PG_URL not set\n")
		t.Log("  Example: postgres://user:password@host:port/database\n")
		t.Fatal("Aborting snapshot tests because they depend on Postgres.")
	} else {
		RegisterFailHandler(Fail)
		junitReporter := reporters.NewJUnitReporter("../test-reports/snapshot.xml")
		RunSpecsWithDefaultAndCustomReporters(t, "Snapshot suite", []Reporter{junitReporter})
	}
}

var _ = BeforeSuite(func() {
	testStop = make(chan bool)
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

	SetConfigDefaults()
	viper.Set("localBindIpAddr", "127.0.0.1")
	viper.Set("port", 0)
	viper.Set("pgURL", dbURL)
	viper.Set("debug", debugTests)
	viper.Set("connmaxlife", 2)

	testListener, err = Run()
	Expect(err).Should(Succeed())

	fmt.Fprintf(GinkgoWriter, "Listening on %s\n", testListener.InsecureAddress())
	testBase = fmt.Sprintf("http://%s", testListener.InsecureAddress())

	go func() {
		serr := testListener.WaitForShutdown()
		fmt.Fprintf(GinkgoWriter, "Server shut down: %s\n", serr)
		testStop <- (serr == nil)
	}()
})

var _ = AfterSuite(func() {
	fmt.Fprint(GinkgoWriter, "Stopping listener\n")
	testListener.Shutdown(nil)

	for _, table := range allTables {
		dropTable(table)
	}
	if db != nil {
		db.Close()
	}

	Eventually(testStop, 20*time.Second).Should(Receive())
	Close()
})

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

var allTables = []string{
	"public.app",
	"public.developer",
	"public.snapshot_test",
	"public.DATA_SCOPE",
	"public.deployment_history",
	"public.deployment_history2",
	"public.APID_CLUSTER",
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
    blob bytea,
    _change_selector varchar(255),
    timestamp timestamp with time zone,
    timestampp timestamp
  );
CREATE TABLE public.DEVELOPER (
	org varchar(255),
	id varchar(255),
	username varchar(255),
	firstname varchar(255),
	created_at timestamp,
	created_by varchar(255),
 	_change_selector varchar(255),
	PRIMARY KEY (id, _change_selector)
);
alter table public.developer replica identity full;
CREATE TABLE public.APP (
	org varchar(255),
	id varchar(255),
	dev_id varchar(255) null,
	display_name varchar(255),
	name varchar(255),
	created_at timestamp,
	created_by varchar(255),
 	_change_selector varchar(255),
	PRIMARY KEY (id, _change_selector)
);
alter table public.app replica identity full;

CREATE TABLE public.APID_CLUSTER (
    id character varying(36) NOT NULL,
    name character varying(100) NOT NULL,
    description character varying(255),
    umbrella_org_app_name character varying(255) NOT NULL,
    created timestamp without time zone,
    created_by character varying(100),
    updated timestamp without time zone,
    updated_by character varying(100),
    CONSTRAINT APID_CLUSTER_pkey PRIMARY KEY (id)
);
alter table public.APID_CLUSTER replica identity full;

CREATE TABLE public.DATA_SCOPE (
    id character varying(36) NOT NULL,
    APID_CLUSTER_id character varying(36) NOT NULL,
    scope character varying(100) NOT NULL,
    created timestamp without time zone,
    created_by character varying(100),
    updated timestamp without time zone,
    updated_by character varying(100),
    CONSTRAINT DATA_SCOPE_pkey PRIMARY KEY (id),
    CONSTRAINT DATA_SCOPE_APID_CLUSTER_id_fk FOREIGN KEY (APID_CLUSTER_id)
          REFERENCES APID_CLUSTER (id)
          ON UPDATE NO ACTION ON DELETE NO ACTION
);
alter table public.DATA_SCOPE replica identity full;

CREATE TABLE transicator_tests.schema_table (
  id character varying primary key,
	created_at timestamp,
	_change_selector character varying
);
alter table transicator_tests.schema_table replica identity full;

CREATE TABLE public.deployment_history (
  id character varying primary key,
  created_at timestamp,
   __change_selector character varying
);
alter table public.deployment_history replica identity full;

CREATE TABLE public.deployment_history2 (
  id character varying primary key,
  created_at timestamp
);
alter table public.deployment_history2 replica identity full;

CREATE INDEX customer_idx_created_at ON public.APP USING btree (created_at);
CREATE INDEX dev_idx_created_at ON public.DEVELOPER USING btree (created_at);
`

var _ = Describe("Taking a snapshot", func() {

	tables := map[string]bool{
		"public.app":                     true,
		"public.developer":               true,
		"public.snapshot_test":           true,
		"public.DATA_SCOPE":              true,
		"public.APID_CLUSTER":            true,
		"transicator_tests.schema_table": true,
	}

	AfterEach(func() {
		fmt.Fprintln(GinkgoWriter, "AfterEach: truncate tables")
		err := truncateTable("public.app")
		Expect(err).Should(Succeed())
		err = truncateTable("public.developer")
		Expect(err).Should(Succeed())
		err = truncateTable("public.snapshot_test")
		Expect(err).Should(Succeed())
		err = truncateTable("public.DATA_SCOPE")
		Expect(err).Should(Succeed())
		err = truncateTable("public.APID_CLUSTER")
		Expect(err).Should(Succeed())
		err = truncateTable("transicator_tests.schema_table")
		Expect(err).Should(Succeed())
	})

	Context("Insert Tables (App & Developer), Get JSON data for ONE scope", func() {
		It("Insert with single scope, retrieve single scope data", func() {

			keys := []string{"org", "id", "username", "created_at", "_change_selector",
				"dev_id", "display_name", "created_by", "created_at",
				"firstname", "name"}

			_, err := db.Exec(`
				insert into APP (org, id, dev_id, display_name, created_at, _change_selector)
				values
				('Pepsi', 'aaa-bbb-ccc', 'xxx-yyy-zzz', 'Oracle', now(), 'pepsi__dev')
				`)
			Expect(err).Should(Succeed())
			_, err = db.Exec(`
				insert into transicator_tests.schema_table (id, created_at, _change_selector)
			 values ('aaa-bbb-ccc', now(), 'pepsi__dev')
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
						var timestamp time.Time
						err = row.Get("created_at", &timestamp)
						Expect(err).Should(Succeed())

						Expect(id).Should(Equal("aaa-bbb-ccc"))
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
			keys := []string{"org", "id", "username", "created_at", "_change_selector", "dev_id", "display_name", "created_by", "created_at", "firstname", "name"}
			idvals := []string{"xxx-yyy-zzz", "xxy-yyy-zzz", "xxz-yyy-zzz", "aaa-bbb-ccc"}

			tx, err := db.Begin()
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into DEVELOPER (org, id, username, created_at, _change_selector) values ('Pepsi', 'xxx-yyy-zzz', 'sundar', now(), 'pepsi__dev')")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into DEVELOPER (org, id, username, created_at, _change_selector) values ('Pepsi', 'xxy-yyy-zzz', 'sundar', now(), 'pepsi__dev')")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into DEVELOPER (org, id, username, created_at, _change_selector) values ('Pepsi', 'xxz-yyy-zzz', 'sundar', now(), 'pepsi__test')")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into APP (org, id, dev_id, display_name, created_at, _change_selector) values ('Pepsi', 'aaa-bbb-ccc', 'xxx-yyy-zzz', 'Oracle', now(), 'pepsi__dev')")
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
	Context("Insert Tables (APID_CLUSTER and DATA_SCOPE), Get JSON data for apidconfigId", func() {
		It("Insert apidconfig, scopes, and retrieve based on apiconfigid", func() {
			keys := []string{"id", "name", "umbrella_org_app_name", "scope", "DATA_SCOPE_pkey"}
			idvals := []string{"111-222-333", "222-333-444", "aaa-bbb-ccc"}

			tx, err := db.Begin()
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into APID_CLUSTER (id, name, umbrella_org_app_name) values ('aaa-bbb-ccc', 'ConfigId1', 'pepsi');")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into DATA_SCOPE (id, APID_CLUSTER_id, scope) values ('111-222-333','aaa-bbb-ccc', 'cokescope1');")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into DATA_SCOPE (id, APID_CLUSTER_id, scope) values ('222-333-444','aaa-bbb-ccc', 'cokescope2');")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into APID_CLUSTER (id, name, umbrella_org_app_name) values ('aaa-bbb-ddd', 'ConfigId2', 'pepsi');")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into DATA_SCOPE (id, APID_CLUSTER_id, scope) values ('111-222-555','aaa-bbb-ddd', 'pepsiscope1');")
			Expect(err).Should(Succeed())
			_, err = tx.Exec("insert into DATA_SCOPE (id, APID_CLUSTER_id, scope) values ('222-333-666','aaa-bbb-ddd', 'pespsiscope2');")
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

			_, err := db.Exec("insert into snapshot_test (id, _change_selector) values ('one', 'foo')")
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
				insert into snapshot_test (id, _change_selector, bool, int, double, timestamp)
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
