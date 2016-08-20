package replication

import (
	"fmt"
	"os"
	"testing"

	"github.com/30x/transicator/pgclient"
	"github.com/Sirupsen/logrus"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var dbURL string
var dbConn *pgclient.PgConnection
var dbConn2 *pgclient.PgConnection

func TestPGClient(t *testing.T) {
	dbURL = os.Getenv("TEST_PG_URL")
	if dbURL == "" {
		fmt.Println("Skipping replication tests because TEST_PG_URL not set")
		fmt.Println("  Example: postgres://user:password@host:port/database")
	} else {
		RegisterFailHandler(Fail)
		RunSpecs(t, "replication suite")
	}
}

var _ = BeforeSuite(func() {
	logrus.SetLevel(logrus.DebugLevel)

	var err error
	dbConn, err = pgclient.Connect(dbURL)
	Expect(err).Should(Succeed())
	dbConn2, err = pgclient.Connect(dbURL)
	Expect(err).Should(Succeed())

	if !tableExists("transicator_test") {
		_, _, err = dbConn.SimpleQuery(testTableSQL)
		Expect(err).Should(Succeed())
	}
})

var _ = AfterSuite(func() {
	if dbConn != nil {
		dropTable("transicator_test")
		dbConn.Close()
	}
	if dbConn2 != nil {
		dbConn2.Close()
	}
})

func tableExists(name string) bool {
	_, _, err := dbConn.SimpleQuery(fmt.Sprintf("select * from %s limit 0", name))
	return err == nil
}

func dropTable(name string) error {
	_, _, err := dbConn.SimpleQuery(fmt.Sprintf("drop table %s", name))
	return err
}

func doExecute(query string) {
	_, _, err := dbConn.SimpleQuery(query)
	Expect(err).Should(Succeed())
}

func doExecute2(query string) {
	_, _, err := dbConn2.SimpleQuery(query)
	Expect(err).Should(Succeed())
}

const testTableSQL = `
  create table transicator_test (
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
    timestamp timestamp with time zone,
    timestampp timestamp
  )`
