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
package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/apid/goscaffold"
	"github.com/apigee-labs/transicator/common"
	"github.com/apigee-labs/transicator/pgclient"
	"github.com/apigee-labs/transicator/replication"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
)

const (
	debugTests      = false
	replicationSlot = "change_server_test"
	testDataDir     = "./changeservertestdata"
)

var dbURL string
var baseURL string
var listener net.Listener
var testServer *server
var db *sql.DB
var insertStmt *sql.Stmt
var insertAlterativeSelectorStmt *sql.Stmt

func TestServer(t *testing.T) {
	if debugTests {
		logrus.SetLevel(logrus.DebugLevel)
	}

	dbURL = os.Getenv("TEST_PG_URL")
	if dbURL == "" {
		fmt.Println("Skipping tests because TEST_PG_URL not set")
		fmt.Println("Format:")
		fmt.Println("  postgres://user:password@host:port/database")
	} else {
		RegisterFailHandler(Fail)
		junitReporter := reporters.NewJUnitReporter("../test-reports/changeserver.xml")
		RunSpecsWithDefaultAndCustomReporters(t, "Changeserver suite", []Reporter{junitReporter})
	}
}

var _ = BeforeSuite(func() {
	var err error
	db, err = sql.Open("transicator", dbURL)
	Expect(err).Should(Succeed())
	db.Driver().(*pgclient.PgDriver).SetIsolationLevel("repeatable read")

	// Connect to the database and create our test table
	executeSQL(testTableSQL)
	executeSQL(testAlternateScopeTableSQL)
	insertStmt, err = db.Prepare(
		"insert into changeserver_test (sequence, _change_selector) values ($1, $2)")
	Expect(err).Should(Succeed())

	insertAlterativeSelectorStmt, err = db.Prepare("insert into changeserver_scope_test(sequence, _different_selector) values ($1, $2)")
	Expect(err).Should(Succeed())

	// Start the server, which will be ready to respond to API calls
	// and which will also start replication with the database
	mux := http.NewServeMux()
	testServer, err = createChangeServer(mux, testDataDir, dbURL, replicationSlot, "")
	Expect(err).Should(Succeed())

	// For the tests, filter out changes for tables not part of these unit
	// tests. Otherwise when we run many test suites in succession this suite
	// fails due to the spurious changes.
	testServer.repl.SetChangeFilter(func(c *common.Change) bool {
		return c.Table == "public.changeserver_test"
	})

	testServer.start()

	// Listen on an anonymous port
	scaf := goscaffold.CreateHTTPScaffold()
	ip := net.ParseIP("127.0.0.1")
	scaf.SetlocalBindIPAddressV4(ip)
	err = scaf.Open()
	Expect(err).Should(Succeed())

	baseURL = fmt.Sprintf("http://%s", scaf.InsecureAddress())
	fmt.Fprintf(GinkgoWriter, "Base API URL: %s\n", baseURL)

	// Poll until we are connected to PG and replication has begun
	Eventually(func() replication.State {
		return testServer.repl.State()
	}, 30, 10).Should(Equal(replication.Running))

	// Start listening for HTTP calls
	go func() {
		scaf.Listen(mux)
	}()
})

var _ = AfterSuite(func() {
	if listener != nil {
		listener.Close()
	}
	if testServer != nil {
		testServer.stop()
	}
	replication.DropSlot(dbURL, replicationSlot)

	if insertStmt != nil {
		insertStmt.Close()
	}
	// Drop the test data table
	executeSQL(dropTableSQL)
	executeSQL(dropAlternativeScopeTableSQL)
	// And delete the storage
	testServer.delete()
	if db != nil {
		db.Close()
	}
})

func executeSQL(sql string) {
	fmt.Fprintf(GinkgoWriter, "Executing SQL: \"%s\"\n", sql)
	_, err := db.Exec(sql)
	Expect(err).Should(Succeed())
}

func executeGet(url string) []byte {
	resp, err := http.Get(url)
	Expect(err).Should(Succeed())
	defer resp.Body.Close()

	bod, err := ioutil.ReadAll(resp.Body)
	Expect(err).Should(Succeed())
	return bod
}

const testTableSQL = `
  create table changeserver_test (
  sequence integer primary key,
  _change_selector varchar
)`

const testAlternateScopeTableSQL = `
  create table changeserver_scope_test (
    sequence integer primary key,
    _different_selector varchar
  )
`

const dropTableSQL = "drop table changeserver_test"
const dropAlternativeScopeTableSQL = "drop table changeserver_scope_test"
