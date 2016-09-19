package replication

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/30x/transicator/pgclient"
	"github.com/Sirupsen/logrus"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	debugTests = false
)

var dbURL string
var db *sql.DB

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
	if debugTests {
		logrus.SetLevel(logrus.DebugLevel)
	}

	var err error
	db, err = sql.Open("transicator", dbURL)
	Expect(err).Should(Succeed())
	pgdriver := db.Driver().(*pgclient.PgDriver)
	pgdriver.SetIsolationLevel("repeatable read")

	if !tableExists("transicator_test") {
		_, err = db.Exec(testTableSQL)
		Expect(err).Should(Succeed())
	}
})

var _ = AfterSuite(func() {
	err := dropTable("transicator_test")
	Expect(err).Should(Succeed())
	err = dropTable("txid_test")
	Expect(err).Should(Succeed())
	err = db.Close()
	Expect(err).Should(Succeed())
})

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
  );
	create table txid_test (
		id varchar(64) primary key
	)`
