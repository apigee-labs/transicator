package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/30x/transicator/pgclient"
	"github.com/30x/transicator/replication"
	"github.com/Sirupsen/logrus"
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

	// Listen on an anonymous port
	listener, err = net.ListenTCP("tcp", &net.TCPAddr{})
	Expect(err).Should(Succeed())

	baseURL = fmt.Sprintf("http://%s", listener.Addr().String())
	fmt.Fprintf(GinkgoWriter, "Base API URL: %s\n", baseURL)

	// Connect to the database and create our test table
	executeSQL(testTableSQL)

	// Start the server, which will be ready to respond to API calls
	// and which will also start replication with the database
	mux := http.NewServeMux()
	testServer, err = startChangeServer(mux, testDataDir, dbURL, replicationSlot, "")
	Expect(err).Should(Succeed())

	// Poll until we are connected to PG and replication has begun
	Eventually(func() replication.State {
		return testServer.repl.State()
	}).Should(Equal(replication.Running))

	// Start listening for HTTP calls
	go func() {
		http.Serve(listener, mux)
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

	// Drop the test data table
	executeSQL(dropTableSQL)
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
  _apid_scope varchar
)`

const dropTableSQL = "drop table changeserver_test"
