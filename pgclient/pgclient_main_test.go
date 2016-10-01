package pgclient

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/Sirupsen/logrus"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	debugTests = false
)

var dbURL string

func TestPGClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pgclient suite")
}

var _ = BeforeSuite(func() {
	if debugTests {
		logrus.SetLevel(logrus.DebugLevel)
	}

	dbURL = os.Getenv("TEST_PG_URL")
	if dbURL == "" {
		fmt.Println("Skipping many tests because TEST_PG_URL not set")
		fmt.Println("Format:")
		fmt.Println("  postgres://user:password@host:port/database")
	} else {
		db, err := sql.Open("transicator", dbURL)
		Expect(err).Should(Succeed())
		defer db.Close()

		_, err = db.Exec(`
			create table client_test (
				id integer primary key,
				string varchar,
				int bigint,
				sint smallint,
				double float8,
				timestamp timestamp with time zone,
				yesno bool,
				blob bytea
			) with oids`)
		Expect(err).Should(Succeed())
	}
})

var _ = AfterSuite(func() {
	if dbURL != "" {
		db, err := sql.Open("transicator", dbURL)
		Expect(err).Should(Succeed())
		defer db.Close()

		db.Exec("drop table client_test")
	}
})
