package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/30x/transicator/pgclient"
	"github.com/30x/transicator/replication"
	"github.com/Sirupsen/logrus"
	. "github.com/onsi/ginkgo"
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

func connectDB() *pgclient.PgConnection {
	dbConn, err := pgclient.Connect(dbURL)
	Expect(err).Should(Succeed())
	return dbConn
}

func executeSQL(sql string) {
	fmt.Fprintf(GinkgoWriter, "Executing SQL: \"%s\"\n", sql)
	dbConn := connectDB()
	defer dbConn.Close()

	_, _, err := dbConn.SimpleQuery(sql)
	Expect(err).Should(Succeed())
}

// Execute the SQL in a transaction and return the snapshot txids.
func executeSQLTransaction(sql string) (int32, int32, []int32) {
	fmt.Fprintf(GinkgoWriter, "Executing SQL: \"%s\"\n", sql)
	dbConn := connectDB()
	defer dbConn.Close()

	_, _, err := dbConn.SimpleQuery("begin transaction isolation level repeatable read")
	Expect(err).Should(Succeed())
	_, _, err = dbConn.SimpleQuery(sql)
	Expect(err).Should(Succeed())
	_, txids, err := dbConn.SimpleQuery("select * from txid_current_snapshot()")
	Expect(err).Should(Succeed())
	Expect(len(txids)).Should(Equal(1))
	_, _, err = dbConn.SimpleQuery("commit")
	Expect(err).Should(Succeed())
	return parseTxids(txids[0][0])
}

func executeGet(url string) []byte {
	resp, err := http.Get(url)
	Expect(err).Should(Succeed())
	defer resp.Body.Close()

	bod, err := ioutil.ReadAll(resp.Body)
	Expect(err).Should(Succeed())
	return bod
}

func parseTxids(ids string) (int32, int32, []int32) {
	fmt.Fprintf(GinkgoWriter, "Parsing txids: %s\n", ids)
	ss := strings.SplitN(ids, ":", 3)
	Expect(len(ss)).Should(Equal(3))
	min, _ := strconv.ParseInt(ss[0], 10, 32)
	max, _ := strconv.ParseInt(ss[1], 10, 32)

	var ips []int32
	ipss := strings.Split(ss[2], ",")
	for _, ipss := range ipss {
		if strings.TrimSpace(ipss) != "" {
			ip, _ := strconv.ParseInt(ipss, 10, 32)
			ips = append(ips, int32(ip))
		}
	}

	return int32(min), int32(max), ips
}

const testTableSQL = `
  create table changeserver_test (
  sequence integer primary key,
  tag varchar
)`

const dropTableSQL = "drop table changeserver_test"
