package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"

	"github.com/30x/transicator/pgclient"
	"github.com/30x/transicator/replication"
	"github.com/Sirupsen/logrus"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	debugTests      = true
	replicationSlot = "change_server_test"
	testDataDir     = "./changeservertestdata"
)

var dbURL string
var baseURL string
var listener net.Listener
var testServer *server

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
		RunSpecs(t, "changeserver suite")
	}
}

var _ = BeforeSuite(func() {
	// Listen on an anonymous port
	var err error
	listener, err = net.ListenTCP("tcp", &net.TCPAddr{})
	Expect(err).Should(Succeed())

	_, portStr, err := net.SplitHostPort(listener.Addr().String())
	Expect(err).Should(Succeed())
	port, err := strconv.Atoi(portStr)
	Expect(err).Should(Succeed())
	baseURL = fmt.Sprintf("http://localhost:%d", port)
	fmt.Fprintf(GinkgoWriter, "Base API URL: %s\n", baseURL)

	// Connect to the database and create our test table
	executeSQL(testTableSQL)

	// Start the server, which will be ready to respond to API calls
	// and which will also start replication with the database
	mux := http.NewServeMux()
	testServer, err = startChangeServer(mux, testDataDir, dbURL, replicationSlot, "")
	Expect(err).Should(Succeed())

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
	err := replication.DropSlot(dbURL, replicationSlot)
	Expect(err).Should(Succeed())

	// Drop the test data table
	executeSQL(dropTableSQL)
	// And delete the storage
	err = testServer.delete()
	Expect(err).Should(Succeed())
})

func executeSQL(sql string) {
	fmt.Fprintf(GinkgoWriter, "Executing SQL: \"%s\"\n", sql)
	dbConn, err := pgclient.Connect(dbURL)
	Expect(err).Should(Succeed())
	defer dbConn.Close()

	_, _, err = dbConn.SimpleQuery(sql)
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
  tag varchar
)`

const dropTableSQL = "drop table changeserver_test"
