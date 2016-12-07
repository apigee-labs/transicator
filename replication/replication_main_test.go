/*
Copyright 2016 Google Inc.

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
package replication

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"testing"

	"github.com/apigee-labs/transicator/common"
	"github.com/apigee-labs/transicator/pgclient"
	"github.com/Sirupsen/logrus"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
)

const (
	debugTests = false
)

var dbURL string
var db *sql.DB
var testID string

func TestReplication(t *testing.T) {
	dbURL = os.Getenv("TEST_PG_URL")
	if dbURL == "" {
		fmt.Println("Skipping replication tests because TEST_PG_URL not set")
		fmt.Println("  Example: postgres://user:password@host:port/database")
	} else {
		RegisterFailHandler(Fail)
		junitReporter := reporters.NewJUnitReporter("../test-reports/replication.xml")
		RunSpecsWithDefaultAndCustomReporters(t, "Replication suite", []Reporter{junitReporter})
	}
}

var _ = BeforeSuite(func() {
	if debugTests {
		logrus.SetLevel(logrus.DebugLevel)
	}

	idBytes := make([]byte, 8)
	_, err := rand.Read(idBytes)
	Expect(err).Should(Succeed())
	testID = base64.StdEncoding.EncodeToString(idBytes)
	fmt.Fprintf(GinkgoWriter, "Unique test ID: %s\n", testID)

	db, err = sql.Open("transicator", dbURL)
	Expect(err).Should(Succeed())
	pgdriver := db.Driver().(*pgclient.PgDriver)
	pgdriver.SetIsolationLevel("repeatable read")

	cleanUpEverything(false)

	_, err = db.Exec(testTableSQL)
	Expect(err).Should(Succeed())
})

var _ = AfterSuite(func() {
	cleanUpEverything(true)
	db.Close()
})

func cleanUpEverything(logit bool) {
	err := DropSlot(dbURL, "unittestslot")
	if err != nil && logit {
		fmt.Printf("Error deleting replication slot \"unittestslot\": %s", err)
	}
	err = DropSlot(dbURL, "txidtestslot")
	if err != nil && logit {
		fmt.Printf("Error deleting replication slot \"txidtestslot\": %s", err)
	}
	err = DropSlot(dbURL, "droptestslot")
	if err != nil && logit {
		fmt.Printf("Error deleting replication slot \"txidtestslot\": %s", err)
	}
	dropTable("transicator_test")
	dropTable("txid_test")
}

func tableExists(name string) bool {
	_, err := db.Exec(fmt.Sprintf("select * from %s limit 0", name))
	return err == nil
}

func dropTable(name string) error {
	_, err := db.Exec(fmt.Sprintf("drop table %s", name))
	return err
}

func doExecute(query string) {
	_, err := db.Exec(query)
	Expect(err).Should(Succeed())
}

func filterChange(c *common.Change) bool {
	var tid string
	if c.NewRow != nil {
		c.NewRow.Get("testid", &tid)
	} else if c.OldRow != nil {
		c.OldRow.Get("testid", &tid)
	}

	return tid == testID
}

const testTableSQL = `
  create table transicator_test (
    id varchar(32) primary key,
		testid varchar(32),
    bool boolean,
    chars char(64),
    varchars varchar(64),
    int integer,
    smallint smallint,
    bigint bigint,
    float float4,
    double float8,
    date date,
		rawdata bytea,
    time time with time zone,
    timestamp timestamp with time zone,
    timestampp timestamp
  );
	alter table transicator_test replica identity full;
	create table txid_test (
		id varchar(64) primary key,
		testid varchar(32)
	);
	alter table txid_test replica identity full;`
